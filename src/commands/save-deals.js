import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeId, sanitizeString, sanitizeSchema, saveResults } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { dealStates as dealStatesSql, deals as dealsSql } from '../lib/sql/index.js'

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
  const audits = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    saveResults.getAuditByBatchId(schema, batchId),
  )
  if (audits.length === 0 || !audits[0].AI_EVALUATION) {
    console.log('[save-deals] no audit found — skipping')
    return { deals_created: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []

  // Need metadata to get userId per thread
  const metadataRows = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    dealStatesSql.selectDistinctThreadUsers(schema, batchId),
  )
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
    const quotedIds = notDealThreadIds.map((id) => `'${id}'`)
    await executeSql(apiUrl, jwt, biscuit, dealsSql.deleteByThreadIds(schema, quotedIds))
    console.log(`[save-deals] deleted ${notDealThreadIds.length} non-deal threads (1 query)`)
  }

  if (dealThreads.length === 0) {
    console.log('[save-deals] no deal threads to save')
    return { deals_created: 0 }
  }

  // Batch upsert deals (single multi-row INSERT)
  const dealTuples = dealThreads.map((thread) => {
    const threadId = sanitizeId(thread.thread_id)
    const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
    const dealId = uuidv7()
    const dealName = sanitizeString(thread.deal_name || '')
    const dealType = sanitizeString(thread.deal_type || '')
    const dealValue =
      typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
    const currency = sanitizeString(thread.currency || 'USD')
    const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''
    const category = sanitizeString(thread.category || '')
    return `('${dealId}', '${userId}', '${threadId}', '', '${dealName}', '${dealType}', '${category}', ${dealValue}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
  })

  await executeSql(apiUrl, jwt, biscuit, dealsSql.upsert(schema, dealTuples))

  console.log(`[save-deals] ${dealThreads.length} deals upserted`)
  return { deals_created: dealThreads.length }
}
