/**
 * Concurrency pool + batch event helper.
 * Used by both run-filter-pipeline and run-classify-pipeline.
 */

import * as core from '@actions/core'
import { sanitizeId, sanitizeString, sanitizeSchema } from './queries.js'

/**
 * Concurrency pool that claims and processes batches.
 *
 * @param {Function} claimFn - async function returning a batch or null when exhausted
 * @param {Function} workerFn - async function(batch, { attempt }) to process a batch
 * @param {{ maxConcurrent: number, maxRetries: number }} opts
 * @returns {Promise<{ processed: number, failed: number }>}
 */
export async function runPool(claimFn, workerFn, { maxConcurrent, maxRetries }) {
  const active = new Set()
  const results = { processed: 0, failed: 0 }

  function runWorker(batch) {
    let currentAttempt = batch.attempts || 0
    return (async () => {
      while (currentAttempt < maxRetries) {
        try {
          await workerFn(batch, { attempt: currentAttempt })
          results.processed++
          return
        } catch (err) {
          currentAttempt++
          core.error(
            `Batch ${batch.batch_id} failed (attempt ${currentAttempt}/${maxRetries}): ${err.message}`,
          )
          if (currentAttempt >= maxRetries) {
            core.error(`Batch ${batch.batch_id} dead-lettered after ${maxRetries} attempts`)
            results.failed++
            return
          }
          const delay = 2000 * Math.pow(2, currentAttempt - 1)
          await new Promise((r) => setTimeout(r, delay))
        }
      }
    })()
  }

  while (true) {
    if (active.size < maxConcurrent) {
      const batch = await claimFn()
      if (batch === null) {
        if (active.size === 0) break
        await Promise.race(active)
        continue
      }
      const worker = runWorker(batch)
      active.add(worker)
      worker.finally(() => active.delete(worker))
    } else {
      await Promise.race(active)
    }
  }

  return results
}

/**
 * Insert a row into {schema}.BATCH_EVENTS.
 *
 * @param {Function} executeSqlFn - async function(sql) to execute SQL
 * @param {string} schema - schema name
 * @param {{ triggerHash: string, batchId: string, batchType: string, eventType: string }} event
 */
export async function insertBatchEvent(
  executeSqlFn,
  schema,
  { triggerHash, batchId, batchType, eventType },
) {
  const safeSchema = sanitizeSchema(schema)
  const safeTriggerHash = sanitizeId(triggerHash)
  const safeBatchId = sanitizeId(batchId)
  const safeBatchType = sanitizeString(batchType)
  const safeEventType = sanitizeString(eventType)

  const sql = `INSERT INTO ${safeSchema}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ('${safeTriggerHash}', '${safeBatchId}', '${safeBatchType}', '${safeEventType}', CURRENT_TIMESTAMP)`

  await executeSqlFn(sql)
}
