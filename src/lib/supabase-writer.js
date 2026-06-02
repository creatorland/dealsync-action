/**
 * Supabase service-role writer for the four ingestion business tables.
 *
 * Uses direct fetch() against the Supabase PostgREST REST API — no
 * @supabase/supabase-js dependency. The service-role key bypasses RLS
 * by design; this module is a system actor only (never called from
 * user-facing paths).
 *
 * SCHEMA SOURCE OF TRUTH: dealsync-v2 scheduler-service/migrations/
 *   0001_business_tables.sql (columns), 0008_rls_policies.sql (ownership),
 *   0009_read_views.sql (deal_card_v / contact_card_v).
 *
 * Ownership model (NO subject_user_id column exists on any business table):
 *   deals                 — direct user_id; RLS: user_id = auth.jwt()->>'sub'
 *   contacts              — direct user_id (composite PK user_id,email)
 *   email_thread_evaluations — NO user column; ownership transitive via
 *                           deals.email_thread_evaluation_id = ete.id
 *   ai_evaluation_audits  — NO user column; two-hop via ete.ai_evaluation_audit_id
 *                           = audits.id then deals.email_thread_evaluation_id = ete.id
 *
 * Linkage keys (load-bearing for RLS + read views):
 *   ete.id = deals.email_thread_evaluation_id = threadId
 *   audits.id = ete.ai_evaluation_audit_id = batchId
 *   contacts.email = ete.main_contact_email (lowercased) — contact_card_v join
 *
 * Column ownership: business tables are 100% ingestion-owned (authenticated
 * clients are SELECT-only per 0008). User-mutable state lives in separate
 * tables (deal_user_overrides, thread_user_state). Timestamps (created_at,
 * updated_at) are DB-managed (defaults + touch_updated_at triggers) and are
 * never sent in the request body.
 *
 * Idempotency:
 *   ai_evaluation_audits  — ON CONFLICT (id) DO NOTHING  (append-only; id=batchId)
 *   email_thread_evaluations — ON CONFLICT (id) DO UPDATE (id=threadId, stable)
 *   deals                 — ON CONFLICT (id) DO UPDATE  (id=threadId, stable)
 *   contacts              — ON CONFLICT (user_id,email) DO UPDATE
 */

import * as core from '@actions/core'

let _config = null

function getConfig() {
  if (_config) return _config
  const url = (core.getInput('supabase-url') || '').replace(/\/$/, '')
  const key = core.getInput('supabase-service-role-key')
  if (!url || !key) {
    throw new Error('supabase-url and supabase-service-role-key are required for Supabase writes')
  }
  _config = { url, key }
  return _config
}

/** Reset cached config — test-only. */
export function _resetConfig() {
  _config = null
}

function normEmail(email) {
  return (email || '').trim().toLowerCase() || null
}

// AbortController + cleared timer (mirrors src/lib/db.js withTimeout). Preferred
// over AbortSignal.timeout() so the timer is explicitly cleared on the success
// path — no dangling 30s handle keeping a short-lived Action process (or a Jest
// worker) alive after the fetch resolves.
const REQUEST_TIMEOUT_MS = 30000
function withTimeout(ms = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timeout) }
}

// PostgREST error bodies can echo the failing row (e.g. "Key (user_id, email)=
// (..., alice@co.com) already exists"), and the caller logs the thrown message
// into the Action log — so the raw body must never reach it. Keep only the
// schema-level `code` + `message` (no row data) and drop `details`/`hint`; for a
// non-JSON body, cap the length defensively.
function summarizeError(body) {
  if (!body) return ''
  try {
    const e = JSON.parse(body)
    const parts = []
    if (e.code) parts.push(`[${e.code}]`)
    if (e.message) parts.push(e.message)
    return parts.join(' ') || `(${body.length} bytes)`
  } catch {
    return body.length > 120 ? `${body.slice(0, 120)}… (${body.length} bytes)` : body
  }
}

/**
 * POST rows to a Supabase table via PostgREST.
 *
 * @param {string} table — unquoted Supabase table name
 * @param {object[]} rows — array of plain objects; only included keys appear in DO UPDATE SET
 * @param {string} onConflict — comma-separated conflict column(s) for ON CONFLICT target
 * @param {boolean} ignoreDuplicates — true → DO NOTHING; false → DO UPDATE SET (merge)
 */
async function upsert(table, rows, onConflict, ignoreDuplicates = false) {
  if (!rows || rows.length === 0) return
  const { url, key } = getConfig()
  const resolution = ignoreDuplicates ? 'ignore-duplicates' : 'merge-duplicates'
  const conflictParam = onConflict ? `?on_conflict=${encodeURIComponent(onConflict)}` : ''
  const { signal, clear } = withTimeout()
  let resp
  try {
    resp = await fetch(`${url}/rest/v1/${table}${conflictParam}`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${key}`,
        apikey: key,
        'Content-Type': 'application/json',
        Prefer: `return=minimal, resolution=${resolution}`,
      },
      body: JSON.stringify(rows),
      signal,
    })
  } finally {
    clear()
  }
  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(`Supabase ${table} upsert failed: ${resp.status} ${summarizeError(body)}`)
  }
}

/**
 * DELETE rows from a Supabase table via PostgREST.
 *
 * @param {string} table
 * @param {string} column — column to match on
 * @param {string[]} values — values to match (OR'd with `in.(...)`)
 */
async function deleteWhere(table, column, values) {
  if (!values || values.length === 0) return
  const { url, key } = getConfig()
  const inList = values.map((v) => encodeURIComponent(v)).join(',')
  const { signal, clear } = withTimeout()
  let resp
  try {
    resp = await fetch(`${url}/rest/v1/${table}?${column}=in.(${inList})`, {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${key}`,
        apikey: key,
        Prefer: 'return=minimal',
      },
      signal,
    })
  } finally {
    clear()
  }
  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(`Supabase ${table} delete failed: ${resp.status} ${summarizeError(body)}`)
  }
}

/**
 * Write one ai_evaluation_audits row. id=batchId for stable idempotency.
 * Append-only: ON CONFLICT (id) DO NOTHING — a re-run of the same batch
 * sees the existing audit row and skips, matching the SxT behaviour.
 * `aiEvaluation` is written to a jsonb column — pass the raw object/array,
 * NOT a stringified/escaped value.
 *
 * @param {object} p
 * @param {string} p.batchId
 * @param {number} p.threadCount
 * @param {number} p.emailCount
 * @param {number} p.cost
 * @param {number} p.inputTokens
 * @param {number} p.outputTokens
 * @param {string} p.model
 * @param {object|Array} p.aiEvaluation — parsed AI output (jsonb), e.g. { threads: [...] }
 */
export async function writeAudit({
  batchId,
  threadCount,
  emailCount,
  cost,
  inputTokens,
  outputTokens,
  model,
  aiEvaluation,
}) {
  await upsert(
    'ai_evaluation_audits',
    [
      {
        id: batchId,
        batch_id: batchId,
        thread_count: threadCount,
        email_count: emailCount,
        inference_cost: cost,
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        model_used: model,
        ai_evaluation: aiEvaluation,
      },
    ],
    'id',
    true,
  )
}

/**
 * Upsert email_thread_evaluations rows. id=threadId for stable idempotency.
 * Carries the AI-eval result columns AND the denormalized main_contact_*
 * columns (which live on THIS table, not on deals — per 0001 + 0009).
 * ai_evaluation_audit_id links to ai_evaluation_audits.id (= batchId) for the
 * two-hop ownership RLS policy. updated_at is trigger-managed (omitted).
 *
 * @param {Array<{threadId: string, auditId: string|null, aiInsight: string|null, aiSummary: string|null, isDeal: boolean, likelyScam: boolean, aiScore: number, mainContact: object|null}>} evals
 */
export async function writeEvals(evals) {
  if (!evals || evals.length === 0) return
  await upsert(
    'email_thread_evaluations',
    evals.map(
      ({ threadId, auditId, aiInsight, aiSummary, isDeal, likelyScam, aiScore, mainContact }) => ({
        id: threadId,
        thread_id: threadId,
        ai_evaluation_audit_id: auditId || null,
        ai_insight: aiInsight || null,
        ai_summary: aiSummary || null,
        is_deal: isDeal,
        likely_scam: likelyScam,
        ai_score: typeof aiScore === 'number' ? aiScore : 0,
        main_contact_name: mainContact?.name || null,
        main_contact_email: normEmail(mainContact?.email),
        main_contact_title: mainContact?.title || null,
        main_contact_company: mainContact?.company || null,
        main_contact_phone_number: mainContact?.phone_number || null,
      }),
    ),
    'id',
    false,
  )
}

/**
 * Upsert deals rows. id=threadId for stable idempotency.
 * email_thread_evaluation_id=threadId links to email_thread_evaluations.id
 * (the join key deal_card_v + the transitive-ownership RLS policies depend on).
 * Business tables are fully ingestion-owned (authenticated clients are
 * SELECT-only on deals per 0008 — no INSERT/UPDATE/DELETE policies exist).
 * `category` here is the AI classification; it IS included in the upsert body
 * even though Story 4.9 AC2 listed it as "user-mutable excluded". That AC was
 * written against a pre-schema model where users could write deals directly.
 * In the applied schema, user-mutable category state lives exclusively in
 * deal_user_overrides.manual_category — ingestion cannot clobber user state.
 * main_contact_* are NOT on deals (they live on email_thread_evaluations);
 * only `brand` (= contact company) is denormalized onto deals.
 * created_at/updated_at are DB-managed.
 *
 * @param {Array<{threadId: string, userId: string, category: string|null, dealName: string|null, dealType: string|null, value: number, currency: string, brand: string|null, isAiSorted: boolean}>} deals
 */
export async function writeDeals(deals) {
  if (!deals || deals.length === 0) return
  await upsert(
    'deals',
    deals.map(
      ({ threadId, userId, category, dealName, dealType, value, currency, brand, isAiSorted }) => ({
        id: threadId,
        user_id: userId,
        thread_id: threadId,
        email_thread_evaluation_id: threadId,
        deal_name: dealName || null,
        deal_type: dealType || null,
        category: category || null,
        value: Number.isFinite(value) ? value : 0,
        currency: currency || 'USD',
        brand: brand || null,
        is_ai_sorted: isAiSorted !== false,
      }),
    ),
    'id',
    false,
  )
}

/**
 * Delete deals rows for threads re-classified as non-deal.
 * Service-role bypasses RLS — correct for a system actor.
 *
 * @param {string[]} threadIds
 */
export async function deleteDeals(threadIds) {
  await deleteWhere('deals', 'id', threadIds)
}

/**
 * Upsert contacts rows. Composite PK (user_id, email) — no surrogate id.
 * ON CONFLICT (user_id, email) DO UPDATE SET ingestion-owned columns only.
 * email is lowercased so it matches email_thread_evaluations.main_contact_email
 * (the contact_card_v join key). updated_at is trigger-managed (omitted).
 *
 * @param {Array<{userId: string, email: string, name: string|null, company: string|null, title: string|null, phone: string|null}>} contacts
 */
export async function writeContacts(contacts) {
  if (!contacts || contacts.length === 0) return
  await upsert(
    'contacts',
    contacts.map(({ userId, email, name, company, title, phone }) => ({
      user_id: userId,
      email: normEmail(email),
      name: name || null,
      company_name: company || null,
      title: title || null,
      phone_number: phone || null,
    })),
    'user_id,email',
    false,
  )
}
