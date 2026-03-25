import { uuidv7 } from 'uuidv7'
import * as core from '@actions/core'
import { sanitizeId, sanitizeString, sanitizeSchema, saveResults } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Save deal contacts with enrichment data from AI evaluation.
 * Reads audit by batch_id, looks up deals by thread_id,
 * then inserts deal_contacts with enrichment from main_contact.
 */
export async function runSaveDealContacts() {
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
    console.log('[save-deal-contacts] no audit found — skipping')
    return { contacts_created: 0 }
  }

  const aiOutput = JSON.parse(audits[0].AI_EVALUATION)
  const threads = aiOutput.threads || []
  const dealThreads = threads.filter((t) => t.is_deal)

  if (dealThreads.length === 0) {
    console.log('[save-deal-contacts] no deal threads — skipping')
    return { contacts_created: 0 }
  }

  // Look up deal IDs by thread_id (single SELECT — avoids subqueries in INSERT)
  const dealThreadIds = dealThreads.map((t) => sanitizeId(t.thread_id))
  const quotedIds = dealThreadIds.map((id) => `'${id}'`).join(',')

  const deals = await executeSql(apiUrl, jwt, biscuit,
    `SELECT ID, THREAD_ID FROM ${schema}.DEALS WHERE THREAD_ID IN (${quotedIds})`)

  const dealByThread = {}
  for (const row of deals) {
    dealByThread[row.THREAD_ID] = row.ID
  }

  // Delete existing contacts for these deals
  const dealIds = Object.values(dealByThread)
  if (dealIds.length > 0) {
    const quotedDealIds = dealIds.map((id) => `'${sanitizeId(id)}'`).join(',')
    await executeSql(apiUrl, jwt, biscuit,
      `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID IN (${quotedDealIds})`)
  }

  // Build contact rows with enrichment data from main_contact
  const contactValues = []
  for (const thread of dealThreads) {
    const mc = thread.main_contact
    if (!mc || !mc.email) continue

    const threadId = sanitizeId(thread.thread_id)
    const dealId = dealByThread[threadId]
    if (!dealId) {
      console.log(`[save-deal-contacts] no deal found for thread ${threadId} — skipping`)
      continue
    }

    const contactEmail = sanitizeString(mc.email)
    const name = sanitizeString(mc.name || '')
    const company = sanitizeString(mc.company || '')
    const title = sanitizeString(mc.title || '')
    const phone = sanitizeString(mc.phone_number || '')

    contactValues.push(
      `('${uuidv7()}', '${sanitizeId(dealId)}', '${contactEmail}', 'primary', '${name}', '${contactEmail}', '${company}', '${title}', '${phone}', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    )
  }

  if (contactValues.length > 0) {
    await executeSql(apiUrl, jwt, biscuit,
      `INSERT INTO ${schema}.DEAL_CONTACTS
        (ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, NAME, EMAIL, COMPANY, TITLE, PHONE_NUMBER, IS_FAVORITE, CREATED_AT, UPDATED_AT)
      VALUES ${contactValues.join(', ')}`)
  }

  console.log(`[save-deal-contacts] ${contactValues.length} contacts saved`)
  return { contacts_created: contactValues.length }
}
