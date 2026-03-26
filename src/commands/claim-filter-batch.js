import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { STATUS, sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent } from '../lib/pipeline.js'

/**
 * Atomically claims pending deal_states for filtering.
 * Falls back to re-claiming a stuck batch if no pending rows exist.
 *
 * Returns { batch_id, count, attempts?, rows? }
 */
export async function runClaimFilterBatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchSize = parseInt(core.getInput('filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '3', 10)

  console.log(`[claim-filter-batch] starting (batchSize=${batchSize}, maxRetries=${maxRetries})`)

  // 1. Authenticate
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // 2. Generate batch ID
  const batchId = uuidv7()

  // 3. Atomically claim pending rows
  await exec(
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize})`,
  )

  // 4. SELECT the claimed rows
  const rows = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

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

  const stuckBatches = await exec(
    `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${STATUS.FILTERING}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries} LIMIT 1`,
  )

  if (!stuckBatches || stuckBatches.length === 0) {
    console.log(`[claim-filter-batch] no stuck batches found, nothing to do`)
    return { batch_id: null, count: 0 }
  }

  // 7. Re-claim the stuck batch
  const stuckBatchId = stuckBatches[0].BATCH_ID
  const attempts = stuckBatches[0].ATTEMPTS

  console.log(`[claim-filter-batch] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`)

  // SELECT its rows
  const stuckRows = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`,
  )

  // UPDATE UPDATED_AT to prevent other instances from grabbing it
  await exec(
    `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${stuckBatchId}'`,
  )

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
