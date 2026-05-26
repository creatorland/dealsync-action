/**
 * outbox-settlement-finalizer — Phase 0 spike worker, step 3 of 3
 *
 * For each per-row result from the worker:
 *   - 'settled' → call mark_settled(row_id, worker_id); on false (lease lost
 *     to another worker), emit settlement.lease_lost
 *   - 'failed'  → call mark_attempt_failed(row_id, worker_id, error); on
 *     false (lease lost), emit settlement.lease_lost; emit settlement.retry_scheduled
 *
 * Per-row outcome events in spike scope: settlement.succeeded OR
 * settlement.retry_scheduled. The dead-letter decision (attempts + 1 >= 10
 * → settlement.dead_letter) per architecture is DEFERRED to Story 3.1
 * production hardening — the spike's single-row dispatch doesn't exercise
 * the retry-exhaustion path, and emitting it requires post-update row read
 * (current attempts count) which the spike skips to keep the round-trip
 * minimal. Story 3.1's full finalizer reads attempts post-mark_attempt_failed
 * and branches on attempts >= 10 → dead_letter (+ B3 dead_letter_left_gap
 * deterministic-recheck signal, Story 5.2).
 *
 * Phase 0 simplifications (deferred):
 *   - settlement.dead_letter / settlement.dead_letter_left_gap → Story 3.1 + 5.2
 *
 * Architecture source: §"Settlement Worker Specification > W3 workflow shape" lines 1985-2003
 */

import { emitEvent, requireEnv, supabaseRpc } from '../lib/spike-runtime.js'

/**
 * @param {object} inputs
 * @param {object[]} inputs.results       Per-row outcomes from worker (may be SHORTER than the guard's claimed batch if rows were skipped on renew_outbox_lease=false)
 * @param {string} inputs.workerId
 * @param {number} inputs.batchSize       Originally-requested batch size
 * @param {number} inputs.claimedCount    Number of rows the guard claimed (BEFORE worker skips). Used for more-work decision so a partial batch with mid-flight lease-loss skips still reports more-work correctly.
 * @returns {Promise<{ moreWork: boolean, settledCount: number, failedCount: number, deadLetterCount: number }>}
 */
export async function run(inputs) {
  const supabaseUrl = requireEnv('SUPABASE_URL')
  const supabaseKey = requireEnv('SUPABASE_SERVICE_ROLE_KEY')
  const { results, workerId } = inputs
  const batchSize = Number(inputs.batchSize ?? 100)
  // claimedCount defaults to results.length for backwards-compat with callers
  // that don't thread it through (spike-only safety net; the local-runner
  // does thread it). Architecture's more-work decision rule: claimed == batch.
  const claimedCount = Number(inputs.claimedCount ?? results.length)
  if (!Array.isArray(results)) {
    throw new Error('outbox-settlement-finalizer: results must be an array')
  }
  if (!workerId) throw new Error('outbox-settlement-finalizer: workerId is required')

  let settledCount = 0
  let failedCount = 0
  let deadLetterCount = 0

  for (const result of results) {
    if (result.status === 'settled') {
      const applied = await supabaseRpc(supabaseUrl, supabaseKey, 'mark_settled', {
        p_row_id: result.outbox_id,
        p_expected_claimed_by: workerId,
      })
      if (applied !== true) {
        emitEvent('settlement.lease_lost', {
          outbox_id: result.outbox_id,
          on: 'finalize',
          run_id: workerId,
        })
        continue
      }
      emitEvent('settlement.succeeded', {
        outbox_id: result.outbox_id,
        aggregate: result.aggregate,
        latency_ms: result.latency_ms,
        run_id: workerId,
      })
      settledCount++
    } else if (result.status === 'failed') {
      const applied = await supabaseRpc(supabaseUrl, supabaseKey, 'mark_attempt_failed', {
        p_row_id: result.outbox_id,
        p_expected_claimed_by: workerId,
        p_err_message: result.error || 'unknown',
      })
      if (applied !== true) {
        emitEvent('settlement.lease_lost', {
          outbox_id: result.outbox_id,
          on: 'finalize',
          run_id: workerId,
        })
        continue
      }
      // Look up current attempt count to decide retry_scheduled vs dead_letter.
      // Phase 0 spike: simplified — we don't actually query attempts here;
      // mark_attempt_failed just incremented it. The architecture's full
      // path reads the row post-update; spike emits retry_scheduled
      // unconditionally on failed status since dead-lettering only matters
      // after 10 attempts (out of scope for single-row spike test).
      emitEvent('settlement.retry_scheduled', {
        outbox_id: result.outbox_id,
        aggregate: result.aggregate,
        error: result.error,
        run_id: workerId,
      })
      failedCount++
    } else {
      // Unexpected status — should not happen in spike scope
      emitEvent('settlement.unknown_status', {
        outbox_id: result.outbox_id,
        status: result.status,
        run_id: workerId,
      })
    }
  }

  // more-work decision per Architecture §"W3 workflow shape" Finalizer row:
  // true when the GUARD claimed a full batch (suggests more eligible rows
  // are sitting in the outbox). Use claimedCount, not results.length —
  // results.length can be < claimedCount when renew_outbox_lease=false
  // skipped a row, which is not a signal about outbox depth.
  const moreWork = claimedCount === batchSize
  emitEvent('settlement.workflow_completed', {
    run_id: workerId,
    claimed_count: claimedCount,
    settled_count: settledCount,
    failed_count: failedCount,
    dead_letter_count: deadLetterCount,
    more_work: moreWork,
  })

  return { moreWork, settledCount, failedCount, deadLetterCount }
}
