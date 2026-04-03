import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { runPool, insertBatchEvent, sweepStuckRows } from '../lib/pipeline.js'
import { authenticate, executeSql, acquireRateLimitToken, logSqlStats } from '../lib/db.js'
import { isRejected, fetchEmails } from '../lib/emails.js'
import {
  sanitizeSchema,
  sanitizeId,
  STATUS,
  dealStates as dealStatesSql,
} from '../lib/sql/index.js'

/**
 * Orchestrator that claims and processes filter batches concurrently
 * until all pending work is exhausted.
 *
 * Composes claim logic (inline) with the filter worker and runs them
 * through a concurrent pool.
 */
export async function runFilterPipeline() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const contentFetcherUrl = core.getInput('email-content-fetcher-url')
  const emailProvider = core.getInput('email-provider') || ''
  const emailServiceUrl = core.getInput('email-service-url')
  const maxConcurrent = parseInt(core.getInput('pipeline-max-concurrent') || '70', 10)
  const batchSize = parseInt(core.getInput('pipeline-filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('pipeline-max-retries') || '6', 10)
  const fetchChunkSize = parseInt(core.getInput('pipeline-fetch-chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('pipeline-fetch-timeout-ms') || '30000', 10)
  const claimSize = parseInt(core.getInput('pipeline-claim-size') || '200', 10)

  console.log(
    `[run-filter-pipeline] starting (maxConcurrent=${maxConcurrent}, batchSize=${batchSize}, claimSize=${claimSize}, maxRetries=${maxRetries}, fetchChunkSize=${fetchChunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )

  // 1. Authenticate to SxT once at start
  const jwt = await authenticate(authUrl, authSecret)

  // 2. Create bound exec helpers
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)
  const execNoRL = (sql) => executeSql(apiUrl, jwt, biscuit, sql, { skipRateLimit: true })

  // Accumulate totals across all batches
  let totalFiltered = 0
  let totalRejected = 0
  let totalClaimed = 0

  let batchCount = 0
  const runStart = Date.now()

  // 3. Define megaSplit() and claimBatch() inline

  /**
   * Split a mega-batch into sub-batches of batchSize rows each,
   * restamp in DB, insert batch events, and return sub-batch objects.
   */
  async function megaSplit(megaBatchId, allRows, attempts) {
    const chunks = []
    for (let i = 0; i < allRows.length; i += batchSize) {
      chunks.push(allRows.slice(i, i + batchSize))
    }

    const groups = chunks.map((chunkRows) => ({
      subBatchId: uuidv7(),
      emailMetadataIds: chunkRows.map((r) => r.EMAIL_METADATA_ID),
    }))

    // Restamp in DB
    await exec(dealStatesSql.restampFilterSubBatches(schema, megaBatchId, groups))

    // Insert batch events
    for (const { subBatchId } of groups) {
      await insertBatchEvent(exec, schema, {
        triggerHash: subBatchId,
        batchId: subBatchId,
        batchType: 'filter',
        eventType: 'new',
      })
    }

    const subBatches = groups.map(({ subBatchId, emailMetadataIds }, i) => ({
      batch_id: subBatchId,
      count: chunks[i].length,
      attempts,
      rows: chunks[i],
    }))

    console.log(
      `[run-filter-pipeline] mega-split ${megaBatchId}: ${allRows.length} rows → ${subBatches.length} sub-batches`,
    )

    return subBatches
  }

  async function claimBatch() {
    const claimStart = Date.now()
    const useMegaClaim = claimSize > batchSize

    if (useMegaClaim) {
      // --- Mega-claim path ---
      const megaId = uuidv7()
      const megaBatchId = `mega:${megaId}`

      await exec(dealStatesSql.claimFilterBatch(schema, megaBatchId, claimSize))

      const rows = await exec(dealStatesSql.selectEmailsByBatch(schema, megaBatchId))

      const count = rows ? rows.length : 0
      const claimMs = Date.now() - claimStart
      batchCount++
      totalClaimed += count
      const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
      console.log(`[run-filter-pipeline] mega-claimed ${count} rows in ${claimMs}ms (claim #${batchCount}, total claimed: ${totalClaimed}, elapsed: ${elapsed}s)`)

      if (count > 0) {
        const splitStart = Date.now()
        const subBatches = await megaSplit(megaBatchId, rows, 0)
        const splitMs = Date.now() - splitStart
        console.log(`[run-filter-pipeline] mega-claim #${batchCount}: ${count} rows → ${subBatches.length} sub-batches of ${batchSize} (splitMs=${splitMs})`)
        return subBatches
      }
    } else {
      // --- Standard single-batch claim path ---
      const batchId = uuidv7()

      await exec(dealStatesSql.claimFilterBatch(schema, batchId, batchSize))

      const rows = await exec(dealStatesSql.selectEmailsByBatch(schema, batchId))

      const count = rows ? rows.length : 0
      const claimMs = Date.now() - claimStart
      batchCount++
      totalClaimed += count
      const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
      console.log(`[run-filter-pipeline] claimed ${count} rows in ${claimMs}ms (claim #${batchCount}, total claimed: ${totalClaimed}, elapsed: ${elapsed}s)`)

      if (count > 0) {
        await insertBatchEvent(exec, schema, {
          triggerHash: batchId,
          batchId,
          batchType: 'filter',
          eventType: 'new',
        })

        return { batch_id: batchId, count, attempts: 0, rows }
      }
    }

    // No pending rows — look for stuck batches (filtering >5min, attempts < maxRetries)
    console.log(`[run-filter-pipeline] no pending rows, checking for stuck batches`)

    const stuckBatches = await exec(
      dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries),
    )

    if (!stuckBatches || stuckBatches.length === 0) {
      console.log(`[run-filter-pipeline] no stuck batches found, nothing to do`)
      return null
    }

    // Re-claim the stuck batch
    const stuckBatchId = stuckBatches[0].BATCH_ID
    const attempts = parseInt(stuckBatches[0].ATTEMPTS, 10)

    console.log(
      `[run-filter-pipeline] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`,
    )

    // Check if this is a stuck mega-batch
    if (stuckBatchId.startsWith('mega:')) {
      const stuckRows = await exec(dealStatesSql.selectEmailsByBatch(schema, stuckBatchId))

      await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

      const stuckCount = stuckRows ? stuckRows.length : 0
      if (stuckCount > 0) {
        return await megaSplit(stuckBatchId, stuckRows, attempts)
      }
      return null
    }

    // Standard stuck batch recovery
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

  // 4. Define processFilterBatch — the per-batch worker
  async function processFilterBatch(batch) {
    const { batch_id, rows } = batch
    const batchStart = Date.now()

    const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
    console.log(`[run-filter-pipeline] processing batch ${batch_id} (${rows.length} rows, elapsed: ${elapsed}s)`)

    // Acquire rate limit tokens in bulk (2 UPDATEs + 1 batch event)
    let t0 = Date.now()
    await acquireRateLimitToken(3)
    const rlMs = Date.now() - t0

    // a. Build metaByMessageId Map from batch.rows
    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const userId = rows[0].USER_ID
    const syncStateId = rows[0].SYNC_STATE_ID
    const messageIds = rows.map((r) => r.MESSAGE_ID)

    // b. Call fetchEmails() with format: 'metadata'
    t0 = Date.now()
    const emails = await fetchEmails(messageIds, metaByMessageId, {
      contentFetcherUrl,
      emailProvider,
      emailServiceUrl,
      userId,
      syncStateId,
      chunkSize: fetchChunkSize,
      fetchTimeoutMs,
      format: 'metadata',
    })
    const fetchMs = Date.now() - t0

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

    const fetched = emails.length
    const unfetched = messageIds.length - fetched
    console.log(
      `[run-filter-pipeline] batch ${batch_id}: fetched ${fetched}/${messageIds.length} emails in ${fetchMs}ms, ${filteredIds.length} passed, ${rejectedIds.length} rejected${unfetched > 0 ? `, ${unfetched} unfetched` : ''}`,
    )

    // d. UPDATE passed IDs -> pending_classification
    t0 = Date.now()
    if (filteredIds.length > 0) {
      const quotedIds = filteredIds.map((id) => `'${sanitizeId(id)}'`)
      await execNoRL(
        dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.PENDING_CLASSIFICATION),
      )
    }

    // e. UPDATE rejected IDs -> filter_rejected
    if (rejectedIds.length > 0) {
      const quotedIds = rejectedIds.map((id) => `'${sanitizeId(id)}'`)
      await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.FILTER_REJECTED))
    }
    const writeMs = Date.now() - t0

    // f. Insert BATCH_EVENTS with eventType: 'complete'
    await insertBatchEvent(execNoRL, schema, {
      triggerHash: batch_id,
      batchId: batch_id,
      batchType: 'filter',
      eventType: 'complete',
    })

    const totalMs = Date.now() - batchStart

    // g. Accumulate totals
    totalFiltered += filteredIds.length
    totalRejected += rejectedIds.length

    const progress = totalFiltered + totalRejected
    const rowsPerSec = progress > 0 ? (progress / ((Date.now() - runStart) / 1000)).toFixed(1) : '0'
    console.log(
      `[run-filter-pipeline] batch ${batch_id} done in ${totalMs}ms (fetch=${fetchMs}ms, write=${writeMs}ms, rl=${rlMs}ms) | progress: ${progress} rows processed, ${rowsPerSec} rows/sec`,
    )
  }

  // 5. Dead-letter: persist failed status when pool gives up on a batch
  async function onDeadLetter(batch) {
    const bid = batch.batch_id
    if (!bid) return
    const safeBid = sanitizeId(bid)
    await execNoRL(
      dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.FILTERING, STATUS.FAILED),
    )
    await insertBatchEvent(execNoRL, schema, {
      triggerHash: uuidv7(),
      batchId: bid,
      batchType: 'filter',
      eventType: 'dead_letter',
    })
    console.log(`[run-filter-pipeline] dead-lettered batch ${bid} → status=failed`)
  }

  const poolResults = await runPool(claimBatch, processFilterBatch, {
    maxConcurrent,
    maxRetries,
    onDeadLetter,
  })

  // 6. Sweep: DB rows stuck in filtering with exhausted retrigger attempts (Explorer had "nothing to process" but SxT still had leftovers)
  const stuckFailed = await sweepStuckRows(exec, schema, {
    activeStatus: STATUS.FILTERING,
    batchType: 'filter',
    maxRetries,
  })

  const runMs = Date.now() - runStart
  const totalProcessed = totalFiltered + totalRejected
  const throughput = totalProcessed > 0 ? ((totalProcessed / runMs) * 3600000).toFixed(0) : '0'
  logSqlStats()
  console.log(
    `[run-filter-pipeline] done — ${totalProcessed} rows in ${(runMs / 1000).toFixed(1)}s (${throughput} rows/hr) | passed=${totalFiltered}, rejected=${totalRejected}, batches=${poolResults.processed}, failed=${poolResults.failed}, stuck_failed=${stuckFailed}`,
  )

  return {
    batches_processed: poolResults.processed,
    batches_failed: poolResults.failed,
    total_filtered: totalFiltered,
    total_rejected: totalRejected,
    stuck_failed: stuckFailed,
  }
}
