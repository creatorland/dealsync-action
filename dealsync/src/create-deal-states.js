import * as crypto from 'crypto'
import * as core from '@actions/core'
import { sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Query the diff between email_metadata and deal_states,
 * then insert missing deal_states rows with status='pending'.
 * Inserts all rows in a single INSERT statement (batch size controlled by orchestrator).
 */
export async function runCreateDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const rawOffset = core.getInput('offset')
  const rawLimit = core.getInput('limit')
  const offset = parseInt(rawOffset || '0', 10)
  const limit = parseInt(rawLimit || '500', 10)

  console.log(`[create-deal-states] inputs: offset="${rawOffset}" limit="${rawLimit}" → parsed: offset=${offset} limit=${limit}`)
  const jwt = await authenticate(authUrl, authSecret)

  const diffSql = `SELECT em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID
FROM EMAIL_CORE_STAGING.EMAIL_METADATA em
WHERE em.ID NOT IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES)
ORDER BY em.RECEIVED_AT ASC
LIMIT ${limit} OFFSET ${offset}`

  const rows = await executeSql(apiUrl, jwt, biscuit, diffSql)

  if (!rows || rows.length === 0) {
    console.log('[create-deal-states] no new emails to process')
    return { created_count: 0, skipped_count: 0 }
  }

  console.log(`[create-deal-states] found ${rows.length} new email(s) to insert`)

  const values = rows
    .map((em) => {
      const id = crypto.randomUUID()
      const threadId = em.THREAD_ID || ''
      const messageId = em.MESSAGE_ID || ''
      return `('${id}', '${em.ID}', '${em.USER_ID}', '${threadId}', '${messageId}', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    })
    .join(', ')

  const insertSql = `INSERT INTO ${schema}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) VALUES ${values} ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`

  await executeSql(apiUrl, jwt, biscuit, insertSql)

  console.log(`[create-deal-states] done: inserted ${rows.length} rows`)
  return { created_count: rows.length, skipped_count: 0 }
}
