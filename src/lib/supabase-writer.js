/**
 * Supabase service-role writer for the four ingestion business tables.
 *
 * Uses direct fetch() against the Supabase PostgREST REST API — no
 * @supabase/supabase-js dependency. The service-role key bypasses RLS
 * by design; this module is a system actor only (never called from
 * user-facing paths).
 *
 * Column-ownership contract (AC2): only ingestion-owned columns appear
 * in the request body. PostgREST generates ON CONFLICT DO UPDATE SET
 * from the body columns, so user-owned columns (category, is_deleted,
 * updated_at) are never touched on re-ingest.
 *
 * Idempotency contract (AC2, AC4):
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
  const resp = await fetch(`${url}/rest/v1/${table}${conflictParam}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${key}`,
      apikey: key,
      'Content-Type': 'application/json',
      Prefer: `return=minimal,resolution=${resolution}`,
    },
    body: JSON.stringify(rows),
  })
  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(`Supabase ${table} upsert failed: ${resp.status} ${body}`)
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
  const resp = await fetch(`${url}/rest/v1/${table}?${column}=in.(${inList})`, {
    method: 'DELETE',
    headers: {
      Authorization: `Bearer ${key}`,
      apikey: key,
      Prefer: 'return=minimal',
    },
  })
  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(`Supabase ${table} delete failed: ${resp.status} ${body}`)
  }
}

/**
 * Write one ai_evaluation_audits row. id=batchId for stable idempotency.
 * Append-only: ON CONFLICT (id) DO NOTHING — a re-run of the same batch
 * sees the existing audit row and skips, matching the SxT try/catch behaviour.
 *
 * @param {object} p
 * @param {string} p.batchId
 * @param {string} p.subjectUserId — owning user (REQUIRED for account-delete cascade)
 * @param {number} p.threadCount
 * @param {number} p.emailCount
 * @param {number} p.cost
 * @param {number} p.inputTokens
 * @param {number} p.outputTokens
 * @param {string} p.model
 * @param {string} p.evaluation — JSON-stringified AI output
 */
export async function writeAudit({
  batchId,
  subjectUserId,
  threadCount,
  emailCount,
  cost,
  inputTokens,
  outputTokens,
  model,
  evaluation,
}) {
  await upsert(
    'ai_evaluation_audits',
    [
      {
        id: batchId,
        subject_user_id: subjectUserId,
        batch_id: batchId,
        thread_count: threadCount,
        email_count: emailCount,
        inference_cost: cost,
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        model_used: model,
        ai_evaluation: evaluation,
      },
    ],
    'id',
    true,
  )
}

/**
 * Upsert email_thread_evaluations rows. id=threadId for stable idempotency.
 * ON CONFLICT (id) DO UPDATE SET ingestion-owned columns only.
 * (updated_at is a server-side trigger — not included here.)
 *
 * @param {Array<{threadId: string, subjectUserId: string, auditId: string|null, aiInsight: string|null, aiSummary: string|null, isDeal: boolean, likelyScam: boolean, aiScore: number}>} evals
 */
export async function writeEvals(evals) {
  if (!evals || evals.length === 0) return
  await upsert(
    'email_thread_evaluations',
    evals.map(({ threadId, subjectUserId, auditId, aiInsight, aiSummary, isDeal, likelyScam, aiScore }) => ({
      id: threadId,
      subject_user_id: subjectUserId,
      thread_id: threadId,
      ai_evaluation_audit_id: auditId || null,
      ai_insight: aiInsight || null,
      ai_summary: aiSummary || null,
      is_deal: isDeal,
      likely_scam: likelyScam,
      ai_score: typeof aiScore === 'number' ? aiScore : 0,
    })),
    'id',
    false,
  )
}

/**
 * Upsert deals rows. id=threadId for stable idempotency.
 * ON CONFLICT (id) DO UPDATE SET ingestion-owned columns only.
 * User-owned columns (category, is_deleted, updated_at) are NOT included —
 * they are absent from the request body and therefore absent from the
 * PostgREST-generated DO UPDATE SET clause.
 *
 * main_contact_* columns are extracted from the thread's main_contact object
 * (the logic originally owned by cancelled Story 4.3).
 *
 * @param {Array<{threadId: string, subjectUserId: string, dealName: string, dealType: string, value: number, currency: string, isAiSorted: boolean, mainContact: object|null}>} deals
 */
export async function writeDeals(deals) {
  if (!deals || deals.length === 0) return
  await upsert(
    'deals',
    deals.map(({ threadId, subjectUserId, dealName, dealType, value, currency, isAiSorted, mainContact }) => ({
      id: threadId,
      subject_user_id: subjectUserId,
      thread_id: threadId,
      email_thread_evaluation_id: threadId,
      deal_name: dealName || null,
      deal_type: dealType || null,
      value: typeof value === 'number' ? value : 0,
      currency: currency || 'USD',
      brand: mainContact?.company || null,
      is_ai_sorted: isAiSorted !== false,
      main_contact_email: mainContact?.email || null,
      main_contact_name: mainContact?.name || null,
      main_contact_company: mainContact?.company || null,
      main_contact_title: mainContact?.title || null,
      main_contact_phone_number: mainContact?.phone_number || null,
    })),
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
 * Upsert contacts rows.
 * ON CONFLICT (user_id, email) DO UPDATE SET ingestion-owned columns only.
 * (updated_at is a server-side trigger — not included here.)
 *
 * @param {Array<{userId: string, email: string, name: string|null, company: string|null, title: string|null, phone: string|null}>} contacts
 */
export async function writeContacts(contacts) {
  if (!contacts || contacts.length === 0) return
  await upsert(
    'contacts',
    contacts.map(({ userId, email, name, company, title, phone }) => ({
      subject_user_id: userId,
      user_id: userId,
      email,
      name: name || null,
      company_name: company || null,
      title: title || null,
      phone_number: phone || null,
    })),
    'user_id,email',
    false,
  )
}
