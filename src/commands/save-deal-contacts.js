import * as core from '@actions/core'
import { sanitizeId, sanitizeString, sanitizeSchema, saveResults } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { deals as dealsSql, contacts as contactsSql, dealContacts as dealContactsSql } from '../lib/sql/index.js'

function toSqlNullable(s) {
  return s ? `'${sanitizeString(s)}'` : 'NULL'
}

/**
 * Save deal contacts with two-table upsert pattern:
 * 1. Core contacts (EMAIL_CORE_STAGING.CONTACTS) — enrichment via COALESCE
 * 2. Deal contacts (DEAL_CONTACTS) — simplified relationship table
 *
 * Reads audit by batch_id, looks up deals by thread_id,
 * then upserts both tables.
 */
export async function runSaveDealContacts() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const coreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')
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

  // Look up deals to get USER_ID per thread
  const dealThreadIds = dealThreads.map((t) => sanitizeId(t.thread_id))
  const quotedIds = dealThreadIds.map((id) => `'${id}'`)

  const deals = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    dealsSql.selectByThreadIds(schema, quotedIds),
  )

  const dealByThread = {}
  for (const row of deals) {
    dealByThread[row.THREAD_ID] = row
  }

  // Build contact rows with two-table pattern
  const coreContactValues = []
  const dealContactValues = []

  for (const thread of dealThreads) {
    const mc = thread.main_contact
    if (!mc) continue
    const email = (mc.email || '').trim().toLowerCase()
    if (!email) continue

    const threadId = sanitizeId(thread.thread_id)
    const deal = dealByThread[threadId]
    if (!deal) {
      console.log(`[save-deal-contacts] no deal found for thread ${threadId} — skipping`)
      continue
    }

    const userId = sanitizeId(deal.USER_ID || '')
    const contactEmail = sanitizeString(email)
    const nameVal = toSqlNullable(mc.name)
    const companyVal = toSqlNullable(mc.company)
    const titleVal = toSqlNullable(mc.title)
    const phoneVal = toSqlNullable(mc.phone_number)

    // Core contacts — COALESCE preserves existing non-null values
    coreContactValues.push(
      `('${userId}', '${contactEmail}', ${nameVal}, ${companyVal}, ${titleVal}, ${phoneVal}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    )

    // Deal contacts — simplified relationship table
    dealContactValues.push(
      `('${threadId}', '${userId}', '${contactEmail}', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    )
  }

  // Upsert core contacts (non-fatal — table may not exist yet)
  if (coreContactValues.length > 0) {
    try {
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        contactsSql.upsert(coreSchema, coreContactValues),
      )
    } catch (err) {
      console.error(`[save-deal-contacts] core contacts upsert failed (non-fatal): ${err.message}`)
    }
  }

  // Upsert deal contacts
  if (dealContactValues.length > 0) {
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dealContactsSql.upsert(schema, dealContactValues),
    )
  }

  console.log(`[save-deal-contacts] ${dealContactValues.length} contacts saved (core + deal)`)
  return { contacts_created: dealContactValues.length }
}
