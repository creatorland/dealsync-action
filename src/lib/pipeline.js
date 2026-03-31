/**
 * Concurrency pool + batch event helper.
 * Used by both run-filter-pipeline and run-classify-pipeline.
 */

import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sleep, backoffMs } from './retry.js'
import {
  sanitizeId,
  sanitizeSchema,
  STATUS,
  dealStates as dealStatesSql,
  batchEvents as batchEventsSql,
} from './sql/index.js'

/**
 * Concurrency pool that claims and processes batches.
 *
 * @param {Function} claimFn - async function returning a batch or null when exhausted
 * @param {Function} workerFn - async function(batch, { attempt }) to process a batch
 * @param {{ maxConcurrent: number, maxRetries: number, onDeadLetter?: (batch: object) => Promise<void> }} opts
 * @returns {Promise<{ processed: number, failed: number }>}
 */
export async function runPool(claimFn, workerFn, { maxConcurrent, maxRetries, onDeadLetter }) {
  const active = new Set()
  const results = { processed: 0, failed: 0 }

  async function deadLetter(batch, reason, attemptCount) {
    const attemptsShown = attemptCount ?? batch.attempts ?? 0
    core.error(`Batch ${batch.batch_id} ${reason} (${attemptsShown}/${maxRetries}), dead-lettered`)
    if (typeof onDeadLetter === 'function' && batch.batch_id) {
      try {
        await onDeadLetter(batch)
      } catch (e) {
        core.error(`onDeadLetter failed for ${batch.batch_id}: ${e.message}`)
      }
    }
    results.failed++
  }

  function runWorker(batch) {
    let currentAttempt = batch.attempts || 0
    return (async () => {
      if (currentAttempt >= maxRetries) {
        await deadLetter(batch, 'already exhausted', currentAttempt)
        return
      }
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
            await deadLetter(batch, 'after max retries', currentAttempt)
            return
          }
          await sleep(backoffMs(currentAttempt - 1, { max: 30000 }))
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

const STUCK_INTERVAL_MINUTES = 5

/**
 * Fail batches stuck in an active status with exhausted retrigger attempts (>= maxRetries distinct TRIGGER_HASH).
 *
 * @param {Function} exec - async (sql) => rows
 * @param {string} schema
 * @param {{ activeStatus: string, batchType: string, maxRetries: number }} opts
 * @returns {Promise<number>} rows transitioned to failed
 */
export async function sweepStuckRows(exec, schema, { activeStatus, batchType, maxRetries }) {
  const safeSchema = sanitizeSchema(schema)
  const statusSql = activeStatus

  const exhausted = await exec(
    dealStatesSql.findDeadBatches(safeSchema, statusSql, STUCK_INTERVAL_MINUTES, maxRetries),
  )

  if (!exhausted || exhausted.length === 0) {
    return 0
  }

  let totalRows = 0
  for (const row of exhausted) {
    const bid = row.BATCH_ID
    const safeBid = sanitizeId(bid)
    const countRows = await exec(
      dealStatesSql.countByBatchAndStatus(safeSchema, safeBid, statusSql),
    )
    const n = Number(countRows?.[0]?.C ?? 0) || 0
    if (n === 0) {
      core.info(
        `[sweepStuckRows] skip batch ${bid}: no rows still in status=${activeStatus} (race with other workers)`,
      )
      continue
    }
    await exec(dealStatesSql.updateStatusByBatch(safeSchema, safeBid, statusSql, STATUS.FAILED))
    await insertBatchEvent(exec, safeSchema, {
      triggerHash: uuidv7(),
      batchId: bid,
      batchType,
      eventType: 'dead_letter',
    })
    totalRows += n
    core.info(
      `[sweepStuckRows] dead-lettered exhausted batch ${bid} (${n} rows, status=${activeStatus})`,
    )
  }

  return totalRows
}

/**
 * Fail rows stuck in intermediate statuses with no batch id (never claimed), older than staleMinutes.
 *
 * @param {Function} exec
 * @param {string} schema
 * @param {{ statuses: string[], staleMinutes: number|string }} opts — staleMinutes may be a non-negative integer or a numeric string (coerced before SQL)
 * @returns {Promise<number>} rows transitioned to failed
 */
export async function sweepOrphanedRows(exec, schema, { statuses, staleMinutes }) {
  const safeSchema = sanitizeSchema(schema)
  if (!Array.isArray(statuses) || statuses.length === 0) return 0

  const minutesNumber =
    typeof staleMinutes === 'string' && staleMinutes.trim() !== ''
      ? Number(staleMinutes)
      : staleMinutes
  if (!Number.isInteger(minutesNumber) || minutesNumber < 0) {
    throw new TypeError(
      `[sweepOrphanedRows] staleMinutes must be a non-negative integer, got: ${staleMinutes}`,
    )
  }
  const safeStaleMinutes = minutesNumber

  const countRows = await exec(dealStatesSql.countOrphaned(safeSchema, statuses, safeStaleMinutes))
  const n = Number(countRows?.[0]?.C ?? 0) || 0
  if (n === 0) return 0

  await exec(dealStatesSql.markOrphanedAsFailed(safeSchema, statuses, safeStaleMinutes))

  core.info(
    `[sweepOrphanedRows] failed ${n} orphaned row(s) (statuses=${statuses.join(',')}, staleMinutes=${safeStaleMinutes})`,
  )
  return n
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
  await executeSqlFn(batchEventsSql.upsert(schema, triggerHash, batchId, batchType, eventType))
}
