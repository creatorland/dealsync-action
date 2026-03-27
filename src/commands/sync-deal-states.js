import * as core from '@actions/core'
import { sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

/**
 * Sync email_metadata into deal_states — insert missing rows with status='pending'.
 * Single INSERT...SELECT query, no pagination. Syncs everything in one shot.
 */
export async function runSyncDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const emailCoreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')

  console.log(
    `[sync-deal-states] syncing from ${emailCoreSchema}.EMAIL_METADATA → ${schema}.DEAL_STATES`,
  )
  const jwt = await authenticate(authUrl, authSecret)

  const sql = `INSERT INTO ${schema}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM ${emailCoreSchema}.EMAIL_METADATA em WHERE NOT EXISTS (SELECT 1 FROM ${schema}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID) ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`

  const result = await executeSql(apiUrl, jwt, biscuit, sql)

  const count = Array.isArray(result) ? result.length : 0
  console.log(`[sync-deal-states] done: ${count} synced (1 query)`)
  return { synced_count: count }
}
