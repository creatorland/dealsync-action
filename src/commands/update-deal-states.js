import * as core from '@actions/core'
import {
  saveResults,
  detection,
  STATUS,
  sanitizeId,
  sanitizeSchema,
  toSqlIdList,
} from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Step 4: Read audit by batch_id → update deal_states to terminal status.
 * Batched: collects all deal/not_deal email IDs, then issues exactly 2 UPDATEs.
 */
export async function runUpdateDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))

  if (!batchId) throw new Error('batch-id is required')

  const jwt = await authenticate(authUrl, authSecret)

  // Read audit
  const audits = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    saveResults.getAuditByBatchId(schema, batchId),
  )
  if (audits.length === 0 || !audits[0].AI_EVALUATION) {
    console.log('[update-states] no audit found — skipping')
    return { deal: 0, not_deal: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []

  // Get metadata to map thread → email_metadata_ids (by batch_id)
  const metadataRows = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    `SELECT EMAIL_METADATA_ID, THREAD_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

  const metadataByThread = {}
  for (const row of metadataRows) {
    if (!metadataByThread[row.THREAD_ID]) metadataByThread[row.THREAD_ID] = []
    metadataByThread[row.THREAD_ID].push(row)
  }

  // Collect all deal and not_deal email IDs
  const dealEmailIds = []
  const notDealEmailIds = []

  for (const thread of threads) {
    const threadId = sanitizeId(thread.thread_id)
    const threadEmails = metadataByThread[threadId] || []
    if (threadEmails.length === 0) continue

    const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
    if (thread.is_deal) {
      dealEmailIds.push(...emailIds)
    } else {
      notDealEmailIds.push(...emailIds)
    }
  }

  // Issue exactly 2 UPDATEs (one for deals, one for not_deals)
  if (dealEmailIds.length > 0) {
    await executeSql(apiUrl, jwt, biscuit, detection.updateDeals(schema, toSqlIdList(dealEmailIds)))
  }
  if (notDealEmailIds.length > 0) {
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      detection.updateNotDeal(schema, toSqlIdList(notDealEmailIds)),
    )
  }

  const queries = (dealEmailIds.length > 0 ? 1 : 0) + (notDealEmailIds.length > 0 ? 1 : 0)
  console.log(
    `[update-states] ${dealEmailIds.length} → deal, ${notDealEmailIds.length} → not_deal (${queries} queries)`,
  )
  return { deal: dealEmailIds.length, not_deal: notDealEmailIds.length }
}
