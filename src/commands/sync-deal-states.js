import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import {
  sanitizeSchema,
  sanitizeId,
  STATUS,
  dealStates as dealStatesSql,
} from '../lib/sql/index.js'
import { insertBatchEvent } from '../lib/pipeline.js'

const DEAD_LETTER_EVENT_COUNT = 3
const STUCK_INTERVAL_MINUTES = 5

/**
 * Sync email_metadata into deal_states — insert missing rows with status='pending'.
 * Also marks batches as failed when they have >= 3 batch_events (retried 3+ times)
 * and are still stuck in an active status.
 */
export async function runSyncDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const emailCoreSchema = sanitizeSchema(core.getInput('email_core_schema') || 'EMAIL_CORE_STAGING')

  console.log(
    `[sync-deal-states] syncing from ${emailCoreSchema}.EMAIL_METADATA → ${schema}.DEAL_STATES`,
  )
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // 1. Sync new rows from email_metadata
  const result = await exec(dealStatesSql.syncFromEmailMetadata(schema, emailCoreSchema))
  const count = Array.isArray(result) ? result.length : 0
  console.log(`[sync-deal-states] synced: ${count} rows`)

  // 2. Dead-letter batches stuck in active statuses with >= 3 batch_events
  const filterFailed = await deadLetterExhausted(exec, schema, STATUS.FILTERING, 'filter')
  const classifyFailed = await deadLetterExhausted(exec, schema, STATUS.CLASSIFYING, 'classify')

  const totalFailed = filterFailed + classifyFailed
  if (totalFailed > 0) {
    console.log(
      `[sync-deal-states] dead-lettered ${totalFailed} stuck rows (filter=${filterFailed}, classify=${classifyFailed})`,
    )
  }

  console.log(`[sync-deal-states] done: synced=${count}, dead_lettered=${totalFailed}`)
  return { synced_count: count, dead_lettered: totalFailed }
}

async function deadLetterExhausted(exec, schema, activeStatus, batchType) {
  const exhausted = await exec(
    dealStatesSql.findDeadBatches(
      schema,
      activeStatus,
      STUCK_INTERVAL_MINUTES,
      DEAD_LETTER_EVENT_COUNT,
    ),
  )

  if (!exhausted || exhausted.length === 0) return 0

  let totalRows = 0
  for (const row of exhausted) {
    const bid = row.BATCH_ID
    const safeBid = sanitizeId(bid)

    const countRows = await exec(
      dealStatesSql.countByBatchAndStatus(schema, safeBid, activeStatus),
    )
    const n = Number(countRows?.[0]?.C ?? 0) || 0
    if (n === 0) continue

    await exec(dealStatesSql.updateStatusByBatch(schema, safeBid, activeStatus, STATUS.FAILED))
    await insertBatchEvent(exec, schema, {
      triggerHash: uuidv7(),
      batchId: bid,
      batchType,
      eventType: 'dead_letter',
    })
    totalRows += n
    console.log(`[sync-deal-states] dead-lettered batch ${bid} (${n} rows, status=${activeStatus})`)
  }

  return totalRows
}
