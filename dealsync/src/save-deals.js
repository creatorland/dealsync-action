import * as crypto from 'crypto'
import * as core from '@actions/core'
import {
  saveResults,
  sanitizeId,
  sanitizeString,
  sanitizeSchema,
} from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Step 3: Read audit by batch_id → upsert deals + deal_contacts.
 * Idempotent: deals use ON CONFLICT (THREAD_ID) DO UPDATE.
 * deal_contacts use DELETE+INSERT (multiple contacts per deal).
 */
export async function runSaveDeals() {
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

  let dealsCreated = 0
  for (const thread of threads) {
    try {
      const threadId = sanitizeId(thread.thread_id)
      const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''

      if (thread.is_deal) {
        const dealId = crypto.randomUUID()
        const evalId = '' // will be linked via thread_id
        const dealName = sanitizeString(thread.deal_name || '')
        const dealType = sanitizeString(thread.deal_type || '')
        const dealValue = typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
        const currency = sanitizeString(thread.currency || 'USD')
        const contactEmail = thread.main_contact ? sanitizeString(thread.main_contact.email || '') : ''
        const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''
        const category = sanitizeString(thread.category || '')

        await executeSql(apiUrl, jwt, biscuit,
          saveResults.upsertDeal(schema, {
            id: dealId, userId, threadId, evalId, dealName, dealType,
            category, value: dealValue, currency, brand,
          }))

        if (contactEmail) {
          await executeSql(apiUrl, jwt, biscuit, saveResults.deleteDealContact(schema, dealId))
          await executeSql(apiUrl, jwt, biscuit,
            saveResults.insertDealContact(schema, { id: crypto.randomUUID(), dealId, contactEmail }))
        }

        dealsCreated++
      } else {
        // Not a deal — remove any existing deal for this thread
        await executeSql(apiUrl, jwt, biscuit, saveResults.deleteDeal(schema, threadId))
      }
    } catch (err) {
      core.error(`Failed deal for thread ${thread.thread_id}: ${err.message}`)
    }
  }

  console.log(`[save-deals] ${dealsCreated} deals created/updated`)
  return { deals_created: dealsCreated }
}
