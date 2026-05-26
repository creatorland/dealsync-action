/**
 * outbox-settlement-guard — Phase 0 spike worker, step 1 of 3
 *
 * Claims a batch of eligible rows from settlement_outbox via the
 * claim_outbox_batch SECURITY DEFINER RPC. Stamps each row with the
 * worker_id + claim_expires_at lease so other workers skip them.
 *
 * Spike-mode: when `outbox-id` input is provided, the guard passes
 * batch-size=1 AND post-filters the result to that specific row (the
 * architecture's lower-id same-aggregate ordering guard could otherwise
 * pick a different row).
 *
 * Phase 0 simplifications (deferred to Story 0.6 / 3.x):
 *   - No @actions/core wrapper (reads inputs via env, prints events to stdout)
 *   - No AES-256-GCM encryption of claimed-batch output (per user direction;
 *     spike scope, staging-only, single synthetic row)
 *   - No Rollup bundling (run via node directly; Story 0.6 adds dist/ for W3 dispatch)
 *
 * Architecture source: §"Settlement Worker Specification > W3 workflow shape" lines 1959-2001
 */

import { emitEvent, requireEnv, supabaseRpc } from '../lib/spike-runtime.js'

/**
 * @param {object} inputs
 * @param {number} inputs.batchSize       Max rows to claim. Spike default 100.
 * @param {number} inputs.leaseSeconds    Lease window length. Spike default 60.
 * @param {string} inputs.workerId        Identifier stamped on claimed rows.
 * @param {string|null} inputs.outboxId   Optional: narrow to a specific row id (spike mode).
 * @returns {Promise<{ claimedBatch: object[], claimedCount: number }>}
 */
export async function run(inputs) {
  const supabaseUrl = requireEnv('SUPABASE_URL')
  const supabaseKey = requireEnv('SUPABASE_SERVICE_ROLE_KEY')
  const batchSize = Number(inputs.batchSize ?? 100)
  const leaseSeconds = Number(inputs.leaseSeconds ?? 60)
  const workerId = inputs.workerId
  if (!workerId) throw new Error('outbox-settlement-guard: workerId is required')
  const outboxId = inputs.outboxId ? String(inputs.outboxId) : null

  emitEvent('settlement.worker_started', { run_id: workerId, batch_size: batchSize })

  // When spike-mode targets a single row, request p_limit=1 to scope the
  // claim narrowly. We still post-filter below as a defensive check —
  // the architecture's lower-id same-aggregate ordering guard could
  // otherwise return a different older row whose successor we wanted.
  const p_limit = outboxId ? 1 : batchSize

  const rows = await supabaseRpc(supabaseUrl, supabaseKey, 'claim_outbox_batch', {
    p_worker_id: workerId,
    p_lease_seconds: leaseSeconds,
    p_limit,
  })

  let claimedBatch = rows
  if (outboxId) {
    claimedBatch = rows.filter((r) => String(r.id) === outboxId)
    if (claimedBatch.length === 0 && rows.length > 0) {
      emitEvent('settlement.spike_mode_skipped_other_row', {
        run_id: workerId,
        requested_outbox_id: outboxId,
        claimed_other_id: rows[0]?.id,
        note: 'Guard claimed a different row than spike requested. Releasing claim by not acking.',
      })
      // Lease will expire naturally; next tick can re-claim if needed.
    }
  }

  const claimedCount = claimedBatch.length
  emitEvent('settlement.batch_claimed', { run_id: workerId, claimed_count: claimedCount })

  return { claimedBatch, claimedCount }
}
