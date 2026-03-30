import * as core from '@actions/core'
import { sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

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

  const result = await executeSql(apiUrl, jwt, biscuit, dealStatesSql.syncFromEmailMetadata(schema, emailCoreSchema))

  const count = Array.isArray(result) ? result.length : 0
  console.log(`[sync-deal-states] done: ${count} synced (1 query)`)
  return { synced_count: count }
}
