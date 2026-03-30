import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { STATUS, sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent, sweepStuckRows, sweepOrphanedRows } from '../lib/pipeline.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

/**
 * Atomically claims pending_classification deal_states for classification.
 *
 * Thread-aware: only claims threads where ALL messages in the same
 * sync-state have cleared filtering (no 'pending' or 'filtering' siblings).
 *
 * Returns:
 *  - New batch:   { batch_id, count, attempts: 0, rows }
 *  - Stuck batch: { batch_id, count, attempts, rows }
 *  - Nothing:     { batch_id: null, count: 0, stuck_failed, orphan_failed } — last two are
 *    counts from post-claim sweeps when nothing was claimable (each may be 0).
 */
export async function runClaimClassifyBatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchSize = parseInt(core.getInput('classify-batch-size') || '5', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '6', 10)

  console.log(`[claim-classify-batch] starting (batchSize=${batchSize}, maxRetries=${maxRetries})`)

  // 1. Authenticate
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // 2. Generate batch_id
  const batchId = uuidv7()

  // 3. Claim threads where all messages have cleared filtering
  await exec(dealStatesSql.claimClassifyBatch(schema, batchId, batchSize))

  // 4. SELECT claimed rows
  const rows = await exec(dealStatesSql.selectEmailsByBatch(schema, batchId))

  // 5. If claimed > 0, insert batch event and return
  if (rows && rows.length > 0) {
    console.log(`[claim-classify-batch] claimed ${rows.length} rows (batch=${batchId})`)
    await insertBatchEvent(exec, schema, {
      triggerHash: batchId,
      batchId,
      batchType: 'classify',
      eventType: 'new',
    })
    return { batch_id: batchId, count: rows.length, attempts: 0, rows }
  }

  // 6. No rows claimed — look for stuck batches
  console.log('[claim-classify-batch] no pending rows, checking for stuck batches')
  const stuckRows = await exec(dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries))

  if (stuckRows && stuckRows.length > 0) {
    const stuckBatchId = stuckRows[0].BATCH_ID
    const attempts = parseInt(stuckRows[0].ATTEMPTS, 10)

    // 7. SELECT its rows
    const stuckBatchRows = await exec(dealStatesSql.selectEmailsByBatch(schema, stuckBatchId))

    // UPDATE UPDATED_AT to reset the stuck timer
    await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

    // Insert batch event with retrigger type and new triggerHash
    const triggerHash = uuidv7()
    await insertBatchEvent(exec, schema, {
      triggerHash,
      batchId: stuckBatchId,
      batchType: 'classify',
      eventType: 'retrigger',
    })

    console.log(
      `[claim-classify-batch] retriggering stuck batch ${stuckBatchId} (attempts=${attempts}, rows=${stuckBatchRows.length})`,
    )
    return {
      batch_id: stuckBatchId,
      count: stuckBatchRows.length,
      attempts,
      rows: stuckBatchRows,
    }
  }

  const stuckFailed = await sweepStuckRows(exec, schema, {
    activeStatus: STATUS.CLASSIFYING,
    batchType: 'classify',
    maxRetries,
  })
  const orphanFailed = await sweepOrphanedRows(exec, schema, {
    statuses: [STATUS.PENDING_CLASSIFICATION],
    staleMinutes: 30,
  })
  if (stuckFailed > 0 || orphanFailed > 0) {
    console.log(
      `[claim-classify-batch] dead-lettered ${stuckFailed} classify row(s), ${orphanFailed} orphan pending_classification row(s)`,
    )
  } else {
    console.log('[claim-classify-batch] nothing to claim')
  }
  return { batch_id: null, count: 0, stuck_failed: stuckFailed, orphan_failed: orphanFailed }
}
