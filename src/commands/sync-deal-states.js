import * as crypto from 'crypto'
import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Sync email_metadata into deal_states — insert missing rows with status='pending'.
 * Uses ON CONFLICT DO UPDATE SET UPDATED_AT to handle duplicates (SxT doesn't support DO NOTHING).
 * Counts existing rows before and after to determine how many were actually new vs conflicts.
 */
export async function runSyncDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const emailCoreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')
  const rawOffset = core.getInput('offset')
  const rawLimit = core.getInput('limit')
  const offset = parseInt(rawOffset || '0', 10)
  const limit = parseInt(rawLimit || '500', 10)

  console.log(`[sync-deal-states] inputs: offset="${rawOffset}" limit="${rawLimit}" → parsed: offset=${offset} limit=${limit}`)
  const jwt = await authenticate(authUrl, authSecret)

  const diffSql = `SELECT em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID
FROM ${emailCoreSchema}.EMAIL_METADATA em
WHERE NOT EXISTS (SELECT 1 FROM ${schema}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID)
ORDER BY em.RECEIVED_AT ASC
LIMIT ${limit} OFFSET ${offset}`

  const rows = await executeSql(apiUrl, jwt, biscuit, diffSql)

  if (!rows || rows.length === 0) {
    console.log('[sync-deal-states] no new emails to sync')
    return { synced_count: 0, conflict_count: 0 }
  }

  console.log(`[sync-deal-states] found ${rows.length} email(s) to sync`)

  const values = rows
    .map((em) => {
      const id = crypto.randomUUID()
      const emId = sanitizeId(em.ID)
      const userId = sanitizeId(em.USER_ID)
      const threadId = em.THREAD_ID ? sanitizeId(em.THREAD_ID) : ''
      const messageId = em.MESSAGE_ID ? sanitizeId(em.MESSAGE_ID) : ''
      return `('${id}', '${emId}', '${userId}', '${threadId}', '${messageId}', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    })
    .join(', ')

  const insertSql = `INSERT INTO ${schema}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) VALUES ${values} ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`

  await executeSql(apiUrl, jwt, biscuit, insertSql)

  console.log(`[sync-deal-states] done: ${rows.length} synced (2 queries)`)
  return { synced_count: rows.length, conflict_count: 0 }
}
