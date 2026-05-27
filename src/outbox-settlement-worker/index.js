/**
 * outbox-settlement-worker — Phase 0 spike worker, step 2 of 3
 *
 * For each claimed row in the batch:
 *   1. renew_outbox_lease (per-row pre-write lease bump)
 *   2. SxT UPSERT keyed on the natural id (with `-- outbox_id: N` SQL comment
 *      for diagnostic correlation per Architecture §"Idempotency contract with SxT")
 *   3. Capture per-row outcome {outbox_id, aggregate, status, error?, latency_ms}
 *
 * Vendor-inline SxT client (so dealsync-action doesn't need to consume the
 * dealsync-v2 monorepo's @dealsync/spaceandtime). Story 3.1 promotes this
 * to the published rate-limited client.
 *
 * Dry-run mode: when SXT_BISCUIT is absent, the worker LOGS the SxT
 * statement it WOULD have executed and marks each row 'settled-dry-run'.
 * This lets the spike validate orchestration end-to-end (claim/lease/event
 * emission/idempotency floor) without requiring biscuit setup. Story 0.6
 * lifts dry-run when adding biscuit to W3 secret store.
 *
 * Phase 0 simplifications (deferred):
 *   - No dynamic per-call lease sizing (B2 mitigation) — static 60s default
 *   - No SxT degraded-pause / settlement.sxt_degraded_pause event
 *   - No retry mid-row (single attempt; mark_attempt_failed on first error)
 *
 * Architecture source: §"Settlement Worker Specification > W3 workflow shape" lines 1970-1983
 */

import { emitEvent, requireEnv, supabaseRpc } from '../lib/spike-runtime.js'

// Read SXT_API_URL lazily inside callers (not as a module-level const) so the
// GHA `main.js` wrapper can set `process.env.SXT_API_URL` from the
// `sxt-api-url` action input AFTER this module is imported.
function getSxtApiUrl() {
  return process.env.SXT_API_URL || 'https://api.makeinfinite.dev'
}

/**
 * Fetch SxT JWT via backend's proxy. Story 0.1's select-sxt.sh fixture
 * already proved this endpoint returns a valid JWT with the {accessToken}
 * shape (the SxT skill's sample documents the wrong shape; backend's
 * actual response is nested).
 */
async function fetchSxtJwt(sxtAuthUrl, sxtAuthSecret, account = 'external') {
  const url = `${sxtAuthUrl.replace(/\/$/, '')}/${account}`
  const resp = await fetch(url, {
    method: 'GET',
    headers: { 'x-shared-secret': sxtAuthSecret },
  })
  if (!resp.ok) {
    throw new Error(`SxT JWT proxy ${resp.status}: ${await resp.text()}`)
  }
  const body = await resp.json()
  // Handle both shapes per Story 0.1's findings (backend's actual is
  // body.data.accessToken; sxt skill's sample documents body.data directly).
  const jwt = body?.data?.accessToken || body?.data || body?.accessToken
  if (!jwt || typeof jwt !== 'string') {
    throw new Error(
      `SxT JWT proxy returned no recognizable token (shapes checked: body.data.accessToken / body.data / body.accessToken). Got: ${JSON.stringify(body).slice(0, 200)}`,
    )
  }
  return jwt
}

/**
 * Execute a SxT SQL statement. Returns the response body (parsed JSON
 * or text). On 401, re-fetches the JWT once and retries.
 */
async function sxtExecute(jwt, biscuit, sqlText, refreshJwt) {
  const apiUrl = getSxtApiUrl()
  const doFetch = async (token) => {
    return fetch(`${apiUrl}/v1/sql`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sqlText, biscuits: [biscuit] }),
    })
  }

  let resp = await doFetch(jwt)
  if (resp.status === 401 && refreshJwt) {
    jwt = await refreshJwt()
    resp = await doFetch(jwt)
  }
  const text = await resp.text()
  if (!resp.ok) {
    throw new Error(`SxT /v1/sql ${resp.status}: ${text}`)
  }
  try {
    return text ? JSON.parse(text) : null
  } catch {
    return null
  }
}

/**
 * Build the SxT UPSERT statement for a settlement_outbox row.
 *
 * Architecture §"Idempotency contract with SxT" item 1:
 *   INSERT ... ON CONFLICT (id) DO UPDATE keyed by aggregate_id
 *
 * Item 2:
 *   -- outbox_id: N (SQL comment for diagnostic correlation;
 *   informational, does NOT participate in conflict key or commitment hash)
 *
 * Spike scope: the SxT table name is derived from the canonical aggregate.
 * Map: deal → dealsync_stg_v1.deals, contact → dealsync_stg_v1.contacts, etc.
 * Phase 1 stories 1.x formalize these mirrors; for the spike we point at
 * the existing sandbox-era SxT tables shared with prod.
 *
 * Safety invariants enforced here (regression-tested in __tests__/build-sxt-upsert.test.js):
 *   - Empty payload rejected (the resulting `DO UPDATE SET` would be empty
 *     and produce invalid SQL — caller bug, fail loud).
 *   - Column names from payload keys MUST match a strict SQL identifier
 *     regex (`^[a-zA-Z_][a-zA-Z0-9_]*$`). The architecture treats payload
 *     as snake_case business-row state; anything else is a caller bug and
 *     a potential SQL-injection vector through the column position.
 *   - String values are single-quote-escaped; numbers serialize raw; null
 *     becomes literal NULL. Phase 1 T3 hardening (Story 3.1) replaces
 *     string-interpolation with the rate-limiter's prepared-statement path.
 */
const SQL_IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/
export function buildSxtUpsert(row) {
  const tableMap = {
    deal: 'DEALSYNC_STG_V1.DEALS',
    contact: 'DEALSYNC_STG_V1.CONTACTS',
    email_thread_eval: 'DEALSYNC_STG_V1.EMAIL_THREAD_EVALUATIONS',
    ai_eval_audit: 'DEALSYNC_STG_V1.AI_EVALUATION_AUDITS',
    thread_user_state: 'DEALSYNC_STG_V1.THREAD_USER_STATE',
  }
  const sxtTable = tableMap[row.aggregate]
  if (!sxtTable) {
    throw new Error(`outbox-settlement-worker: unknown aggregate ${row.aggregate} for SxT mapping`)
  }
  const payload = row.payload || {}
  const payloadKeys = Object.keys(payload)
  if (payloadKeys.length === 0) {
    // Empty payload would produce `DO UPDATE SET ` (empty) — invalid SQL.
    // The architecture's full body would always have a non-empty row state
    // via DELETE/INSERT/UPDATE...RETURNING. Spike: fail loud on caller bug.
    throw new Error(
      `outbox-settlement-worker: cannot build SxT UPSERT for outbox_id=${row.id} — payload is empty (no columns to insert)`,
    )
  }
  for (const k of payloadKeys) {
    if (!SQL_IDENT_RE.test(k)) {
      // Column-name injection guard: payload keys are interpolated into the
      // SQL string positionally as column identifiers. SxT does not support
      // identifier-quoting on the producer side here (the legacy worker
      // relies on the same invariant). Reject anything that isn't a strict
      // SQL identifier so a malformed key can never alter the SQL shape.
      throw new Error(
        `outbox-settlement-worker: payload key "${k}" is not a valid SQL identifier (must match ${SQL_IDENT_RE}); cannot build UPSERT for outbox_id=${row.id}`,
      )
    }
  }
  const id = row.aggregate_id
  if (typeof id !== 'string' || id.length === 0) {
    throw new Error(
      `outbox-settlement-worker: row.aggregate_id must be a non-empty string for outbox_id=${row.id}`,
    )
  }
  // The diagnostic-correlation SQL comment per Architecture §"Idempotency
  // contract" item 2. Informational; does NOT participate in conflict key
  // or commitment hash.
  const idComment = `-- outbox_id: ${row.id}`
  // Build VALUES clause from payload keys. String-interpolated since SxT
  // doesn't support parameterized queries.
  const cols = ['ID', ...payloadKeys.map((k) => k.toUpperCase())]
  const vals = [
    `'${id.replace(/'/g, "''")}'`,
    ...Object.values(payload).map((v) =>
      v === null
        ? 'NULL'
        : typeof v === 'number'
          ? String(v)
          : `'${String(v).replace(/'/g, "''")}'`,
    ),
  ]
  const updateClause = payloadKeys
    .map((k) => `${k.toUpperCase()} = EXCLUDED.${k.toUpperCase()}`)
    .join(', ')
  return `${idComment}
INSERT INTO ${sxtTable} (${cols.join(', ')})
VALUES (${vals.join(', ')})
ON CONFLICT (ID) DO UPDATE SET ${updateClause}`
}

/**
 * @param {object} inputs
 * @param {object[]} inputs.claimedBatch  Rows from guard
 * @param {string} inputs.workerId
 * @param {number} inputs.leaseSeconds
 * @returns {Promise<{ results: object[] }>}
 */
export async function run(inputs) {
  const supabaseUrl = requireEnv('SUPABASE_URL')
  const supabaseKey = requireEnv('SUPABASE_SERVICE_ROLE_KEY')
  const sxtAuthUrl = process.env.SXT_AUTH_URL
  const sxtAuthSecret = process.env.SXT_AUTH_SECRET
  const sxtBiscuit = process.env.SXT_BISCUIT
  const dryRun = !sxtBiscuit

  const { claimedBatch, workerId, leaseSeconds } = inputs
  if (!Array.isArray(claimedBatch)) {
    throw new Error('outbox-settlement-worker: claimedBatch must be an array')
  }
  if (!workerId) throw new Error('outbox-settlement-worker: workerId is required')

  if (dryRun) {
    emitEvent('settlement.worker_dry_run_mode', {
      run_id: workerId,
      reason: 'SXT_BISCUIT not set; emitting SQL without executing against SxT',
    })
  }

  // Fetch JWT once for the batch. The vendor-inline SxT client handles
  // 401-with-refresh per-call internally.
  let jwt = null
  const refreshJwt = async () => {
    jwt = await fetchSxtJwt(sxtAuthUrl, sxtAuthSecret)
    return jwt
  }
  if (!dryRun) {
    if (!sxtAuthUrl || !sxtAuthSecret) {
      throw new Error(
        'outbox-settlement-worker: SXT_AUTH_URL + SXT_AUTH_SECRET required when SXT_BISCUIT is set (full mode)',
      )
    }
    jwt = await refreshJwt()
  }

  const results = []
  for (const row of claimedBatch) {
    const t0 = Date.now()
    // (1) Per-row lease renewal — fresh window for this row's SxT call
    const renewed = await supabaseRpc(supabaseUrl, supabaseKey, 'renew_outbox_lease', {
      p_row_id: row.id,
      p_expected_claimed_by: workerId,
      p_lease_seconds: leaseSeconds,
    })
    if (renewed !== true) {
      emitEvent('settlement.lease_lost', {
        outbox_id: row.id,
        on: 'pre_sxt_write',
        run_id: workerId,
      })
      // Skip this row; finalizer's lease-ownership check on mark_settled
      // would also catch it, but we save the SxT round-trip.
      continue
    }

    // (2) SxT UPSERT (dry-run logs the SQL; real-mode executes it)
    const sxtSql = buildSxtUpsert(row)
    emitEvent('settlement.attempted', {
      outbox_id: row.id,
      aggregate: row.aggregate,
      trace_id: row.trace_id,
      run_id: workerId,
      ...(dryRun ? { dry_run: true, sxt_sql: sxtSql } : {}),
    })

    let status = 'settled'
    let error = null
    if (!dryRun) {
      try {
        await sxtExecute(jwt, sxtBiscuit, sxtSql, refreshJwt)
      } catch (e) {
        status = 'failed'
        error = e?.message || String(e)
      }
    }
    const latency_ms = Date.now() - t0

    results.push({
      outbox_id: row.id,
      aggregate: row.aggregate,
      status,
      error,
      latency_ms,
    })
  }

  return { results }
}
