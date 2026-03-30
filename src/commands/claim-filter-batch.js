import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { STATUS, sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent, sweepStuckRows } from '../lib/pipeline.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

/**
 * Atomically claims pending deal_states for filtering.
 * Falls back to re-claiming a stuck batch if no pending rows exist.
 *
 * @returns {Promise<{
 *   batch_id: string | null,
 *   count: number,
 *   attempts?: number,
 *   rows?: unknown[],
 *   stuck_failed?: number
 * }>}
 * - New or retriggered batch: `batch_id` set, optional `attempts` / `rows`.
 * - No work and no retrigger-eligible stuck batch: `batch_id: null`, `count: 0`,
 *   and `stuck_failed` (rows moved to failed by the exhausted-batch sweep, may be 0).
 */
export async function runClaimFilterBatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchSize = parseInt(core.getInput('filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '6', 10)

  console.log(`[claim-filter-batch] starting (batchSize=${batchSize}, maxRetries=${maxRetries})`)

  // 1. Authenticate
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // 2. Generate batch ID
  const batchId = uuidv7()

  // 3. Atomically claim pending rows
  await exec(dealStatesSql.claimFilterBatch(schema, batchId, batchSize))

  // 4. SELECT the claimed rows
  const rows = await exec(dealStatesSql.selectEmailsByBatch(schema, batchId))

  const count = rows ? rows.length : 0
  console.log(`[claim-filter-batch] claimed ${count} pending rows`)

  // 5. If claimed > 0, insert batch event and return
  if (count > 0) {
    await insertBatchEvent(exec, schema, {
      triggerHash: batchId,
      batchId,
      batchType: 'filter',
      eventType: 'new',
    })

    return { batch_id: batchId, count, attempts: 0, rows }
  }

  // 6. No pending rows — look for stuck batches
  console.log(`[claim-filter-batch] no pending rows, checking for stuck batches`)

  const stuckBatches = await exec(dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries))

  if (!stuckBatches || stuckBatches.length === 0) {
    const stuckFailed = await sweepStuckRows(exec, schema, {
      activeStatus: STATUS.FILTERING,
      batchType: 'filter',
      maxRetries,
    })
    if (stuckFailed > 0) {
      console.log(
        `[claim-filter-batch] dead-lettered ${stuckFailed} row(s) in exhausted filter batches (no retrigger-eligible stuck batches)`,
      )
    } else {
      console.log(`[claim-filter-batch] no stuck batches found, nothing to do`)
    }
    return { batch_id: null, count: 0, stuck_failed: stuckFailed }
  }

  // 7. Re-claim the stuck batch
  const stuckBatchId = stuckBatches[0].BATCH_ID
  const attempts = stuckBatches[0].ATTEMPTS

  console.log(`[claim-filter-batch] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`)

  // SELECT its rows
  const stuckRows = await exec(dealStatesSql.selectEmailsByBatch(schema, stuckBatchId))

  // UPDATE UPDATED_AT to prevent other instances from grabbing it
  await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

  // Insert batch event with retrigger type and new trigger hash
  const triggerHash = uuidv7()
  await insertBatchEvent(exec, schema, {
    triggerHash,
    batchId: stuckBatchId,
    batchType: 'filter',
    eventType: 'retrigger',
  })

  const stuckCount = stuckRows ? stuckRows.length : 0

  return { batch_id: stuckBatchId, count: stuckCount, attempts, rows: stuckRows }
}
