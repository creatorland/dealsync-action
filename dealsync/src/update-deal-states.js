import * as core from '@actions/core'
import {
  saveResults,
  detection,
  STATUS,
  sanitizeId,
  sanitizeSchema,
  toSqlIdList,
} from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Step 4: Read audit by batch_id → update deal_states to terminal status.
 * Idempotent: UPDATE with WHERE clause only affects rows still at 'classifying'.
 */
export async function runUpdateDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = core.getInput('batch-id')

  if (!batchId) throw new Error('batch-id is required')

  const jwt = await authenticate(authUrl, authSecret)

  // Read audit
  const audits = await executeSql(apiUrl, jwt, biscuit, saveResults.getAuditByBatchId(schema, batchId))
  if (audits.length === 0 || !audits[0].AI_EVALUATION) {
    console.log('[update-states] no audit found — skipping')
    return { updated: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []

  // Get metadata to map thread → email_metadata_ids
  const metadataRows = await executeSql(apiUrl, jwt, biscuit,
    `SELECT EMAIL_METADATA_ID, THREAD_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`)

  const metadataByThread = {}
  for (const row of metadataRows) {
    if (!metadataByThread[row.THREAD_ID]) metadataByThread[row.THREAD_ID] = []
    metadataByThread[row.THREAD_ID].push(row)
  }

  let dealCount = 0
  let notDealCount = 0
  let failed = 0

  for (const thread of threads) {
    try {
      const threadId = sanitizeId(thread.thread_id)
      const threadEmails = metadataByThread[threadId] || []
      if (threadEmails.length === 0) continue

      const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
      const sqlQuotedIds = toSqlIdList(emailIds)

      if (thread.is_deal) {
        await executeSql(apiUrl, jwt, biscuit, detection.updateDeals(schema, sqlQuotedIds))
        dealCount += emailIds.length
      } else {
        await executeSql(apiUrl, jwt, biscuit, detection.updateNotDeal(schema, sqlQuotedIds))
        notDealCount += emailIds.length
      }
    } catch (err) {
      failed++
      core.error(`Failed to update states for thread ${thread.thread_id}: ${err.message}`)
    }
  }

  console.log(`[update-states] ${dealCount} → deal, ${notDealCount} → not_deal, ${failed} failed`)
  if (failed > 0) throw new Error(`${failed} state update(s) failed`)
  return { deal: dealCount, not_deal: notDealCount }
}
