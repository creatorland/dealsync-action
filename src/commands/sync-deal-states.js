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

const DEAD_LETTER_EVENT_COUNT = parseInt(core.getInput('pipeline-max-retries') || '2', 10)
const STUCK_INTERVAL_MINUTES = 5

/**
 * Sync email_metadata into deal_states — insert missing rows with status='pending'.
 * Also marks batches as failed when they have >= pipeline-max-retries batch_events
 * and are still stuck in an active status.
 */
export async function runSyncDealStates() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const emailCoreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')

  console.log(
    `[sync-deal-states] syncing from ${emailCoreSchema}.EMAIL_METADATA → ${schema}.DEAL_STATES`,
  )
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // 1. Sync new rows from email_metadata (chunked to avoid SxT timeout)
  const syncSql = dealStatesSql.syncFromEmailMetadata(schema, emailCoreSchema)
  console.log(`[sync-deal-states] SQL: ${syncSql}`)
  let totalSynced = 0
  let chunk = 0
  while (true) {
    chunk++
    const result = await exec(syncSql)
    console.log(`[sync-deal-states] chunk ${chunk} raw result: ${JSON.stringify(result)}`)
    const synced = result?.[0]?.UPDATED ?? 0
    totalSynced += synced
    console.log(`[sync-deal-states] chunk ${chunk}: synced ${synced} rows (total: ${totalSynced})`)
    if (synced === 0) break
  }
  const count = totalSynced

  // 2. Dead-letter batches stuck in active statuses with >= maxRetries batch_events
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
