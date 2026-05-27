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

/**
 * Aggregate → SxT table mapping. Shared by buildSxtUpsert (for INSERT)
 * and fetchExistingSxtRow (for SELECT). Phase 1 stories 1.x formalize
 * these mirrors; for the spike we point at the existing sandbox-era
 * SxT tables shared with prod.
 */
const SXT_TABLE_FOR_AGGREGATE = {
  deal: 'DEALSYNC_STG_V1.DEALS',
  contact: 'DEALSYNC_STG_V1.CONTACTS',
  email_thread_eval: 'DEALSYNC_STG_V1.EMAIL_THREAD_EVALUATIONS',
  ai_eval_audit: 'DEALSYNC_STG_V1.AI_EVALUATION_AUDITS',
  thread_user_state: 'DEALSYNC_STG_V1.THREAD_USER_STATE',
}

/**
 * Fetch the existing SxT row for an aggregate, keyed by ID.
 *
 * Added 2026-05-27 per Story 0.1 Task 7: SxT validates NOT NULL columns
 * on the INSERT clause of `INSERT ... ON CONFLICT (ID) DO UPDATE` even
 * when the UPDATE branch will be taken. The worker therefore needs to
 * send all columns the table requires, not just the patch's delta.
 * We resolve by reading the existing row, merging the patch onto it,
 * then sending a full-row UPSERT (mergeExistingRowWithPayload).
 *
 * Returns: lowercase-keyed row object (SxT returns UPPERCASE; we
 * normalize so it can merge cleanly with payload's lowercase keys),
 * or null if the row doesn't exist.
 */
/**
 * Convert a SxT result row's UPPERCASE keys to lowercase so it can merge
 * cleanly with payload's lowercase keys. Extracted as a separately-exported
 * helper for unit-testability (the network-using fetchExistingSxtRow itself
 * is exercised end-to-end against staging per Story 0.1 Task 7.4).
 */
export function normalizeSxtRowToLowercase(rawRow) {
  if (!rawRow || typeof rawRow !== 'object') return null
  return Object.fromEntries(Object.entries(rawRow).map(([k, v]) => [k.toLowerCase(), v]))
}

export async function fetchExistingSxtRow(jwt, biscuit, aggregate, aggregateId, refreshJwt) {
  const sxtTable = SXT_TABLE_FOR_AGGREGATE[aggregate]
  if (!sxtTable) {
    throw new Error(
      `outbox-settlement-worker: unknown aggregate ${aggregate} for SxT mapping`,
    )
  }
  if (typeof aggregateId !== 'string' || aggregateId.length === 0) {
    throw new Error(
      `outbox-settlement-worker: aggregateId must be a non-empty string for SxT SELECT`,
    )
  }
  // Length cap matches the architecture's outbox-row aggregate_id contract.
  // Without a cap a pathological caller could load up a multi-megabyte ID and
  // burn rate-limiter quota; this is shape-not-security, but the failure mode
  // shouldn't be "SxT 413 mid-batch" — fail loud here instead.
  if (aggregateId.length > 256) {
    throw new Error(
      `outbox-settlement-worker: aggregateId length ${aggregateId.length} exceeds 256-char cap`,
    )
  }
  const sql = `SELECT * FROM ${sxtTable} WHERE ID = '${aggregateId.replace(/'/g, "''")}' LIMIT 1`
  const result = await sxtExecute(jwt, biscuit, sql, refreshJwt)
  if (!Array.isArray(result) || result.length === 0) return null
  return normalizeSxtRowToLowercase(result[0])
}

/**
 * Merge an existing SxT row's columns with an outbox row's patch payload.
 * The payload wins on overlapping keys (it represents the new state).
 * The 'id' column from the existing row is excluded — buildSxtUpsert
 * uses row.aggregate_id as the canonical ID source so never duplicate it.
 *
 * Added 2026-05-27 per Story 0.1 Task 7. See fetchExistingSxtRow docstring
 * for the SxT NOT NULL motivation.
 *
 * Both branches behave identically w.r.t. 'id': stripped from existingRow
 * (so the canonical ID is row.aggregate_id), and NOT stripped from
 * payload — if a payload ever legitimately carries an `id` column, that's
 * surfaced via `assertValidSqlIdent` in buildSxtUpsert (and is currently
 * rejected since 'id' isn't a typical business-row column). Phase 0 / 0.1
 * Task 7 cleanup (2026-05-27 review).
 *
 * @param {object|null} existingRow  Lowercase-keyed row from fetchExistingSxtRow, or null
 * @param {object} payload           Outbox row's patch payload (lowercase keys)
 * @returns {object}                  Merged payload suitable for buildSxtUpsert
 */
export function mergeExistingRowWithPayload(existingRow, payload) {
  const safePayload = payload || {}
  // Strip 'id' from existingRow consistently in BOTH branches so behavior
  // is the same regardless of whether the SxT row exists. Pre-cleanup the
  // null-existing branch passed payload through without touching it; that
  // looked harmless but left an inconsistency the next maintainer could
  // misread as intentional. Now both paths share the same id-stripping rule.
  const { id: _ignoredExistingId, ...existingMinusId } = existingRow || {}
  return { ...existingMinusId, ...safePayload }
}

/**
 * Serialize a JS value for SxT SQL string-interpolation. SxT doesn't
 * support parameterized queries, so this is the safe-form:
 *   null/undefined → NULL
 *   number         → as-is
 *   boolean        → TRUE / FALSE (NOT 'true'/'false' — those become
 *                    string literals on a boolean column, producing
 *                    type-mismatch in typed engines)
 *   string         → single-quote-escaped string
 *   anything else  → THROW (silently emitting `'[object Object]'` or
 *                    `'1,2,3'` for arrays would be data corruption.
 *                    BigInts, Dates, etc. — callers are responsible for
 *                    pre-converting to a supported type.)
 * Phase 0 / 0.1 Task 7 cleanup (2026-05-27 review).
 */
function serializeSqlValue(v) {
  if (v === null || v === undefined) return 'NULL'
  const t = typeof v
  if (t === 'number') {
    if (!Number.isFinite(v)) {
      throw new Error(
        `outbox-settlement-worker: cannot serialize non-finite number (${v}) — SxT has no equivalent for NaN/Infinity`,
      )
    }
    return String(v)
  }
  if (t === 'boolean') return v ? 'TRUE' : 'FALSE'
  if (t === 'string') return `'${v.replace(/'/g, "''")}'`
  throw new Error(
    `outbox-settlement-worker: cannot serialize value of unsupported type "${t}" for SxT — supported: null, number, boolean, string. Pre-convert before placing in outbox payload.`,
  )
}

/**
 * Validate identifier-shape on a payload key. Payload keys interpolate
 * into SQL positionally as column identifiers; SxT doesn't support
 * identifier-quoting on the producer side, so we reject anything that
 * isn't a strict SQL identifier. Also explicitly rejects keys that
 * uppercase to `ID` — `buildSxtUpsert` adds an `ID` column derived from
 * `row.aggregate_id`, so a payload `id` (or `Id`/`ID`) would produce a
 * duplicate `ID` column in the INSERT clause and either invalid SQL or
 * a confusing runtime SxT error. Fail loud with a clear message instead.
 * (Copilot review on PR #25.)
 */
function assertValidSqlIdent(k, outboxId) {
  if (!SQL_IDENT_RE.test(k)) {
    throw new Error(
      `outbox-settlement-worker: payload key "${k}" is not a valid SQL identifier (must match ${SQL_IDENT_RE}); cannot build UPSERT for outbox_id=${outboxId}`,
    )
  }
  if (k.toUpperCase() === 'ID') {
    throw new Error(
      `outbox-settlement-worker: payload key "${k}" collides with the canonical ID column (derived from row.aggregate_id) for outbox_id=${outboxId} — do not include an "id" column in the outbox payload`,
    )
  }
}

function resolveSxtTable(aggregate) {
  const sxtTable = SXT_TABLE_FOR_AGGREGATE[aggregate]
  if (!sxtTable) {
    throw new Error(
      `outbox-settlement-worker: unknown aggregate ${aggregate} for SxT mapping`,
    )
  }
  return sxtTable
}

function assertNonEmptyAggregateId(row) {
  const id = row.aggregate_id
  if (typeof id !== 'string' || id.length === 0) {
    throw new Error(
      `outbox-settlement-worker: row.aggregate_id must be a non-empty string for outbox_id=${row.id}`,
    )
  }
  return id
}

export function buildSxtUpsert(row) {
  const sxtTable = resolveSxtTable(row.aggregate)
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
  for (const k of payloadKeys) assertValidSqlIdent(k, row.id)
  const id = assertNonEmptyAggregateId(row)
  // The diagnostic-correlation SQL comment per Architecture §"Idempotency
  // contract" item 2. Informational; does NOT participate in conflict key
  // or commitment hash.
  const idComment = `-- outbox_id: ${row.id}`
  const cols = ['ID', ...payloadKeys.map((k) => k.toUpperCase())]
  const vals = [`'${id.replace(/'/g, "''")}'`, ...Object.values(payload).map(serializeSqlValue)]
  const updateClause = payloadKeys
    .map((k) => `${k.toUpperCase()} = EXCLUDED.${k.toUpperCase()}`)
    .join(', ')
  return `${idComment}
INSERT INTO ${sxtTable} (${cols.join(', ')})
VALUES (${vals.join(', ')})
ON CONFLICT (ID) DO UPDATE SET ${updateClause}`
}

/**
 * Build a SxT UPSERT where the INSERT/VALUES clause uses the full merged
 * payload (to satisfy SxT's NOT NULL validation on the INSERT branch — see
 * `fetchExistingSxtRow` docstring) but the `DO UPDATE SET` clause only
 * touches the columns from the ORIGINAL patch (`row.payload`).
 *
 * Why this split matters (Copilot review on PR #24): if the UPDATE SET
 * clause updated all merged columns, then any non-atomic read-modify-write
 * race (e.g., outbox rows for the same aggregate processed out of order)
 * would clobber newer column values with stale values read from the
 * existing SxT row. Restricting UPDATE SET to only the patch's keys keeps
 * the operational semantics aligned with the architecture's "delta only"
 * write contract — the merged payload exists *purely* to make the INSERT
 * branch pass NOT NULL when the row doesn't exist (or to keep `EXCLUDED.X`
 * well-defined for the UPDATE clause).
 *
 * Scope clarification (Story 0.1 Task 7 close-out review 2026-05-27):
 *   - The race-safety guarantee here is ONLY for the UPDATE branch — i.e.
 *     when the SxT row exists at write time. Out-of-order processing of
 *     two outbox rows whose patches touch DIFFERENT columns is safe.
 *     Two outbox rows touching the SAME column still last-writer-wins
 *     on SxT serialization order — that's expected delta-overwrite, not
 *     a clobber.
 *   - On the FIRST-WRITE branch (no row in SxT yet, existingRow=null),
 *     the merged payload equals the patch payload alone. If the patch
 *     omits a NOT NULL column, the INSERT still fails — this function
 *     only fixes the previously-failing UPDATE-branch case where the
 *     row exists but the patch is partial. First-write rows still
 *     require the patch to include all required columns; this is
 *     Phase 0 spike-scope behavior, Phase 1 Story 3.2 hardens it.
 *
 * Conflict-key contract: `ON CONFLICT (ID)` is the canonical Dealsync 2.0
 * outbox path per Architecture §"Idempotency contract with SxT" item 1
 * (NOT the legacy dealsync-action prod pipeline's `ON CONFLICT (THREAD_ID)`).
 * `ID` here refers to the SxT row's surrogate ID, matching `row.aggregate_id`.
 *
 * Added 2026-05-27 per Story 0.1 Task 7.
 */
export function buildSxtMergedUpsert(row, mergedPayload) {
  const sxtTable = resolveSxtTable(row.aggregate)
  const patch = row.payload || {}
  const patchKeys = Object.keys(patch)
  if (patchKeys.length === 0) {
    throw new Error(
      `outbox-settlement-worker: cannot build merged UPSERT for outbox_id=${row.id} — row.payload is empty (nothing to update)`,
    )
  }
  if (!mergedPayload || typeof mergedPayload !== 'object') {
    throw new Error(
      `outbox-settlement-worker: mergedPayload must be a non-null object for outbox_id=${row.id}`,
    )
  }
  const insertKeys = Object.keys(mergedPayload)
  if (insertKeys.length === 0) {
    throw new Error(
      `outbox-settlement-worker: mergedPayload is empty for outbox_id=${row.id} — INSERT clause would be invalid`,
    )
  }
  for (const k of insertKeys) assertValidSqlIdent(k, row.id)
  // Every patch key must appear in the merged payload (otherwise
  // `EXCLUDED.<patchCol>` references a column the INSERT didn't supply).
  for (const k of patchKeys) {
    if (!Object.prototype.hasOwnProperty.call(mergedPayload, k)) {
      throw new Error(
        `outbox-settlement-worker: patch key "${k}" missing from mergedPayload (build merge logic bug) for outbox_id=${row.id}`,
      )
    }
  }
  const id = assertNonEmptyAggregateId(row)
  const idComment = `-- outbox_id: ${row.id}`
  const cols = ['ID', ...insertKeys.map((k) => k.toUpperCase())]
  const vals = [
    `'${id.replace(/'/g, "''")}'`,
    ...Object.values(mergedPayload).map(serializeSqlValue),
  ]
  // ✱ UPDATE SET uses ONLY patchKeys — not insertKeys — so concurrent
  // out-of-order outbox rows don't clobber each other's untouched columns.
  const updateClause = patchKeys
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

    // (2) SxT UPSERT — read-merge-upsert per Story 0.1 Task 7
    //
    // Real-mode: fetch the existing SxT row, merge the patch over it, build
    // a full-row UPSERT. This is required because SxT validates NOT NULL on
    // the INSERT clause even when the conflict path takes the UPDATE branch
    // — a partial-payload UPSERT against an existing row fails with
    // "integrity constraint" against any non-trivial table schema.
    // Verified empirically 2026-05-27 against staging (Story 0.1 Task 5
    // substrate-validation).
    //
    // Dry-run: no SxT connection to read from, so we fall back to the
    // partial-payload UPSERT (informational SQL only, not executed).
    let sxtSql = null
    let dryRunSql = null
    if (dryRun) {
      dryRunSql = buildSxtUpsert(row)
    }
    emitEvent('settlement.attempted', {
      outbox_id: row.id,
      aggregate: row.aggregate,
      trace_id: row.trace_id,
      run_id: workerId,
      ...(dryRun ? { dry_run: true, sxt_sql: dryRunSql } : {}),
    })

    let status = 'settled'
    let error = null
    if (!dryRun) {
      try {
        const existingRow = await fetchExistingSxtRow(
          jwt,
          sxtBiscuit,
          row.aggregate,
          row.aggregate_id,
          refreshJwt,
        )
        // Second lease renewal before the UPSERT. Read-merge-upsert added
        // a SELECT round-trip per row (Story 0.1 Task 7); the pre-PR-24
        // worker did only one SxT call per row, so the static 60s default
        // had ~60s margin. Now each row consumes ~2 round-trips of that
        // margin; if a batch of 100 rows hits even one slow SELECT (network
        // hiccup, JWT refresh, etc.) the lease can expire mid-batch and
        // `mark_settled` in the finalizer rejects on lease-ownership.
        // Renewing here gives the UPSERT its own fresh window. Lease loss
        // here is benign — skip this row; the next worker pass picks it
        // up cleanly.
        const renewedPreWrite = await supabaseRpc(
          supabaseUrl,
          supabaseKey,
          'renew_outbox_lease',
          {
            p_row_id: row.id,
            p_expected_claimed_by: workerId,
            p_lease_seconds: leaseSeconds,
          },
        )
        if (renewedPreWrite !== true) {
          emitEvent('settlement.lease_lost', {
            outbox_id: row.id,
            on: 'between_select_and_upsert',
            run_id: workerId,
          })
          // Lease-loss is benign here — the next worker pass picks the row
          // back up cleanly. We can't `continue` from inside the try/catch
          // since results.push happens after the for-body, so jump straight
          // to writing a `lease_lost` results entry and skip the UPSERT.
          // Throwing into the outer catch would mask this as `failed` which
          // looks like a hard error to the finalizer (Copilot review on #25).
          results.push({
            outbox_id: row.id,
            aggregate: row.aggregate,
            status: 'lease_lost',
            error: null,
            latency_ms: Date.now() - t0,
          })
          continue
        }
        const fullPayload = mergeExistingRowWithPayload(existingRow, row.payload)
        // Build the UPSERT with: INSERT/VALUES from the merged full row
        // (so SxT's NOT NULL validation passes on the INSERT branch), but
        // DO UPDATE SET from the original row.payload keys only (so we
        // don't clobber columns the patch didn't touch with potentially
        // stale values from the existing-row read). See buildSxtMergedUpsert
        // docstring for the race-condition rationale.
        sxtSql = buildSxtMergedUpsert(row, fullPayload)
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
