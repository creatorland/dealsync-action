import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId, STATUS } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { isRejected } from '../lib/filter-rules.js'
import { fetchEmails } from '../lib/email-client.js'
import { runPool, insertBatchEvent } from '../lib/pipeline.js'

/**
 * Orchestrator that claims and processes filter batches concurrently
 * until all pending work is exhausted.
 *
 * Composes claim logic (inline) with the filter worker and runs them
 * through a concurrent pool.
 */
export async function runFilterPipeline() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const maxConcurrent = parseInt(core.getInput('max-concurrent') || '5', 10)
  const batchSize = parseInt(core.getInput('filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '6', 10)
  const chunkSize = parseInt(core.getInput('chunk-size') || '50', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '30000', 10)

  console.log(
    `[run-filter-pipeline] starting (maxConcurrent=${maxConcurrent}, batchSize=${batchSize}, maxRetries=${maxRetries}, chunkSize=${chunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )

  // 1. Authenticate to SxT once at start
  const jwt = await authenticate(authUrl, authSecret)

  // 2. Create a bound exec helper
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // Accumulate totals across all batches
  let totalFiltered = 0
  let totalRejected = 0

  // 3. Define claimBatch() inline — same logic as claim-filter-batch.js
  async function claimBatch() {
    const batchId = uuidv7()

    // Atomically claim pending rows
    await exec(
      `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize})`,
    )

    // SELECT the claimed rows
    const rows = await exec(
      `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
    )

    const count = rows ? rows.length : 0
    console.log(`[run-filter-pipeline] claimed ${count} pending rows`)

    // If claimed > 0, insert batch event and return
    if (count > 0) {
      await insertBatchEvent(exec, schema, {
        triggerHash: batchId,
        batchId,
        batchType: 'filter',
        eventType: 'new',
      })

      return { batch_id: batchId, count, attempts: 0, rows }
    }

    // No pending rows — look for stuck batches (filtering >5min, attempts < maxRetries)
    console.log(`[run-filter-pipeline] no pending rows, checking for stuck batches`)

    const stuckBatches = await exec(
      `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${schema}.DEAL_STATES ds LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${STATUS.FILTERING}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries} LIMIT 1`,
    )

    if (!stuckBatches || stuckBatches.length === 0) {
      console.log(`[run-filter-pipeline] no stuck batches found, nothing to do`)
      return null
    }

    // Re-claim the stuck batch
    const stuckBatchId = stuckBatches[0].BATCH_ID
    const attempts = stuckBatches[0].ATTEMPTS

    console.log(
      `[run-filter-pipeline] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`,
    )

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

  // 4. Define processFilterBatch — the per-batch worker
  async function processFilterBatch(batch) {
    const { batch_id, rows } = batch

    console.log(`[run-filter-pipeline] processing batch ${batch_id} (${rows.length} rows)`)

    // a. Build metaByMessageId Map from batch.rows
    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const userId = rows[0].USER_ID
    const syncStateId = rows[0].SYNC_STATE_ID
    const messageIds = rows.map((r) => r.MESSAGE_ID)

    // b. Call fetchEmails() with format: 'metadata'
    const emails = await fetchEmails(messageIds, metaByMessageId, {
      contentFetcherUrl,
      userId,
      syncStateId,
      chunkSize,
      fetchTimeoutMs,
      format: 'metadata',
    })

    // c. Apply isRejected() to each email
    const filteredIds = []
    const rejectedIds = []

    for (const email of emails) {
      if (isRejected(email)) {
        rejectedIds.push(email.id)
      } else {
        filteredIds.push(email.id)
      }
    }

    console.log(
      `[run-filter-pipeline] batch ${batch_id}: ${filteredIds.length} passed, ${rejectedIds.length} rejected`,
    )

    // d. UPDATE passed IDs -> pending_classification
    if (filteredIds.length > 0) {
      const quotedIds = filteredIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await exec(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.PENDING_CLASSIFICATION}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }

    // e. UPDATE rejected IDs -> filter_rejected
    if (rejectedIds.length > 0) {
      const quotedIds = rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await exec(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTER_REJECTED}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }

    // f. Insert BATCH_EVENTS with eventType: 'complete'
    await insertBatchEvent(exec, schema, {
      triggerHash: batch_id,
      batchId: batch_id,
      batchType: 'filter',
      eventType: 'complete',
    })

    // g. Accumulate totals
    totalFiltered += filteredIds.length
    totalRejected += rejectedIds.length
  }

  // 5. Call runPool
  const poolResults = await runPool(claimBatch, processFilterBatch, { maxConcurrent, maxRetries })

  console.log(
    `[run-filter-pipeline] done — batches_processed=${poolResults.processed}, batches_failed=${poolResults.failed}, total_filtered=${totalFiltered}, total_rejected=${totalRejected}`,
  )

  // 6. Return totals
  return {
    batches_processed: poolResults.processed,
    batches_failed: poolResults.failed,
    total_filtered: totalFiltered,
    total_rejected: totalRejected,
  }
}
