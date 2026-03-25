import * as crypto from 'crypto'
import * as core from '@actions/core'
import {
  sanitizeId,
  sanitizeString,
  sanitizeSchema,
  saveResults,
} from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Step 3: Read audit by batch_id → upsert deals.
 * Batched: single multi-row INSERT for deals.
 */
export async function runSaveDeals() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))

  if (!batchId) throw new Error('batch-id is required')

  const jwt = await authenticate(authUrl, authSecret)

  // Read audit
  const audits = await executeSql(apiUrl, jwt, biscuit, saveResults.getAuditByBatchId(schema, batchId))
  if (audits.length === 0 || !audits[0].AI_EVALUATION) {
    console.log('[save-deals] no audit found — skipping')
    return { deals_created: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []

  // Need metadata to get userId per thread
  const metadataRows = await executeSql(apiUrl, jwt, biscuit,
    `SELECT DISTINCT THREAD_ID, USER_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`)
  const userByThread = {}
  for (const row of metadataRows) {
    userByThread[row.THREAD_ID] = row.USER_ID
  }

  // Separate deal vs non-deal threads
  const dealThreads = []
  const notDealThreadIds = []

  for (const thread of threads) {
    if (thread.is_deal) {
      dealThreads.push(thread)
    } else {
      notDealThreadIds.push(sanitizeId(thread.thread_id))
    }
  }

  // Batch DELETE non-deal threads (single query)
  if (notDealThreadIds.length > 0) {
    const quotedIds = notDealThreadIds.map((id) => `'${id}'`).join(',')
    await executeSql(apiUrl, jwt, biscuit,
      `DELETE FROM ${schema}.DEALS WHERE THREAD_ID IN (${quotedIds})`)
    console.log(`[save-deals] deleted ${notDealThreadIds.length} non-deal threads (1 query)`)
  }

  if (dealThreads.length === 0) {
    console.log('[save-deals] no deal threads to save')
    return { deals_created: 0 }
  }

  // Batch upsert deals (single multi-row INSERT)
  const dealValues = dealThreads.map((thread) => {
    const threadId = sanitizeId(thread.thread_id)
    const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
    const dealId = crypto.randomUUID()
    const dealName = sanitizeString(thread.deal_name || '')
    const dealType = sanitizeString(thread.deal_type || '')
    const dealValue = typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
    const currency = sanitizeString(thread.currency || 'USD')
    const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''
    const category = sanitizeString(thread.category || '')
    return `('${dealId}', '${userId}', '${threadId}', '', '${dealName}', '${dealType}', '${category}', ${dealValue}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
  }).join(', ')

  await executeSql(apiUrl, jwt, biscuit,
    `INSERT INTO ${schema}.DEALS
      (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
    VALUES ${dealValues}
    ON CONFLICT (THREAD_ID) DO UPDATE SET
      EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID,
      DEAL_NAME = EXCLUDED.DEAL_NAME,
      DEAL_TYPE = EXCLUDED.DEAL_TYPE,
      CATEGORY = EXCLUDED.CATEGORY,
      VALUE = EXCLUDED.VALUE,
      CURRENCY = EXCLUDED.CURRENCY,
      BRAND = EXCLUDED.BRAND,
      UPDATED_AT = CURRENT_TIMESTAMP`)

  console.log(`[save-deals] ${dealThreads.length} deals upserted`)
  return { deals_created: dealThreads.length }
}
