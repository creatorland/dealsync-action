import * as core from '@actions/core'
import { runPool } from '../lib/pipeline.js'
import { authenticate, executeSql } from '../lib/db.js'
import { fetchEmails } from '../lib/emails.js'
import {
  sanitizeSchema,
  sanitizeId,
  STATUS,
  dealStates as dealStatesSql,
} from '../lib/sql/index.js'

export async function runRecoveryPipeline() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const contentFetcherUrl = core.getInput('email-content-fetcher-url')
  const emailProvider = core.getInput('email-provider') || ''
  const emailServiceUrl = core.getInput('email-service-url')
  const maxConcurrent = parseInt(core.getInput('pipeline-recovery-max-concurrent') || '10', 10)
  const claimSize = parseInt(core.getInput('recovery-claim-size') || '500', 10)
  const fetchChunkSize = parseInt(core.getInput('pipeline-fetch-chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('pipeline-fetch-timeout-ms') || '30000', 10)
  const maxRetries = parseInt(core.getInput('pipeline-max-retries') || '2', 10)

  console.log(
    `[run-recovery-pipeline] starting (maxConcurrent=${maxConcurrent}, claimSize=${claimSize}, fetchChunkSize=${fetchChunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  let totalRecovered = 0
  let totalDead = 0
  let usersProcessed = 0
  const runStart = Date.now()

  // Claim function: get next user with failed rows
  let userQueue = []
  let userQueueExhausted = false

  async function claimBatch() {
    // Refill user queue if empty
    if (userQueue.length === 0 && !userQueueExhausted) {
      const users = await exec(dealStatesSql.findUsersWithFailedRows(schema))
      if (!users || users.length === 0) {
        userQueueExhausted = true
        return null
      }
      userQueue = users
      console.log(`[run-recovery-pipeline] found ${users.length} users with failed rows`)
    }

    if (userQueue.length === 0) return null

    const user = userQueue.shift()
    const userId = user.USER_ID

    // Select failed rows for this user
    const rows = await exec(dealStatesSql.selectFailedByUser(schema, userId, claimSize))
    if (!rows || rows.length === 0) return null

    const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
    console.log(
      `[run-recovery-pipeline] claimed ${rows.length} failed rows for user ${userId} (elapsed: ${elapsed}s)`,
    )

    return { batch_id: `recovery:${userId}`, count: rows.length, attempts: 0, rows, userId }
  }

  // Worker function: check fetchability and update statuses
  async function processRecoveryBatch(batch) {
    const { rows, userId } = batch
    const batchStart = Date.now()

    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const syncStateId = rows[0].SYNC_STATE_ID
    const messageIds = rows.map((r) => r.MESSAGE_ID)

    // Fetch with metadata-only format
    let emails = []
    try {
      emails = await fetchEmails(messageIds, metaByMessageId, {
        contentFetcherUrl,
        emailProvider,
        emailServiceUrl,
        userId,
        syncStateId,
        chunkSize: fetchChunkSize,
        fetchTimeoutMs,
        format: 'metadata',
      })
    } catch (err) {
      console.log(
        `[run-recovery-pipeline] fetch failed for user ${userId}: ${err.message}`,
      )
      // All unfetchable on total failure
      emails = []
    }

    const fetchedMessageIds = new Set(emails.map((e) => e.messageId || e.id))

    // Split into recoverable vs dead
    const recoverableIds = []
    const deadIds = []

    for (const row of rows) {
      if (fetchedMessageIds.has(row.MESSAGE_ID)) {
        recoverableIds.push(row.EMAIL_METADATA_ID)
      } else {
        deadIds.push(row.EMAIL_METADATA_ID)
      }
    }

    // Write: reset recoverable to pending (with BATCH_ID = NULL)
    if (recoverableIds.length > 0) {
      const quotedIds = recoverableIds.map((id) => `'${sanitizeId(id)}'`)
      await exec(dealStatesSql.resetToPending(schema, quotedIds))
    }

    // Write: mark dead
    if (deadIds.length > 0) {
      const quotedIds = deadIds.map((id) => `'${sanitizeId(id)}'`)
      await exec(dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.DEAD))
    }

    totalRecovered += recoverableIds.length
    totalDead += deadIds.length
    usersProcessed++

    const totalMs = Date.now() - batchStart
    const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
    console.log(
      `[run-recovery-pipeline] user ${userId}: ${recoverableIds.length} recovered, ${deadIds.length} dead (${totalMs}ms) | total: recovered=${totalRecovered}, dead=${totalDead}, users=${usersProcessed}, elapsed=${elapsed}s`,
    )
  }

  const poolResults = await runPool(claimBatch, processRecoveryBatch, {
    maxConcurrent,
    maxRetries,
    onDeadLetter: async () => {},
  })

  const runMs = Date.now() - runStart
  console.log(
    `[run-recovery-pipeline] done — recovered=${totalRecovered}, dead=${totalDead}, users=${usersProcessed}, batches=${poolResults.processed}, failed=${poolResults.failed} (${(runMs / 1000).toFixed(1)}s)`,
  )

  return {
    total_recovered: totalRecovered,
    total_dead: totalDead,
    users_processed: usersProcessed,
    batches_processed: poolResults.processed,
    batches_failed: poolResults.failed,
  }
}
