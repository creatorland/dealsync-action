import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { STATUS, sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent } from '../lib/pipeline.js'

/**
 * Atomically claims pending_classification deal_states for classification.
 *
 * Thread-aware: only claims threads where ALL messages in the same
 * sync-state have cleared filtering (no 'pending' or 'filtering' siblings).
 *
 * Returns:
 *  - New batch:   { batch_id, count, attempts: 0, rows }
 *  - Stuck batch: { batch_id, count, attempts, rows }
 *  - Nothing:     { batch_id: null, count: 0 }
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
  const claimSql = `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP WHERE THREAD_ID IN (SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}' AND NOT EXISTS (SELECT 1 FROM ${schema}.DEAL_STATES ds2 WHERE ds2.THREAD_ID = ds.THREAD_ID AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')) LIMIT ${batchSize}) AND STATUS = '${STATUS.PENDING_CLASSIFICATION}'`

  await exec(claimSql)

  // 4. SELECT claimed rows
  const selectSql = `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`
  const rows = await exec(selectSql)

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
  const stuckSql = `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${STATUS.CLASSIFYING}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries} LIMIT 1`

  const stuckRows = await exec(stuckSql)

  if (stuckRows && stuckRows.length > 0) {
    const stuckBatchId = stuckRows[0].BATCH_ID
    const attempts = parseInt(stuckRows[0].ATTEMPTS, 10)

    // 7. SELECT its rows
    const stuckSelectSql = `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`
    const stuckBatchRows = await exec(stuckSelectSql)

    // UPDATE UPDATED_AT to reset the stuck timer
    const touchSql = `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${stuckBatchId}'`
    await exec(touchSql)

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

  // 8. Nothing found
  console.log('[claim-classify-batch] nothing to claim')
  return { batch_id: null, count: 0 }
}
