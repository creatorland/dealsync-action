#!/usr/bin/env node
/**
 * Reset a user's data across all SxT tables for clean E2E testing.
 *
 * Usage:
 *   node --experimental-wasm-modules reset-user.js <userId>
 *
 * Deletes from: email_core_staging (email_metadata, sync_states, sync_events, email_senders)
 *               dealsync_stg_v1 (deal_states, deals, deal_contacts, email_thread_evaluations, ai_evaluation_audits, contacts)
 */

import { authenticate, generateMasterBiscuit, executeSql } from './sxt-client.js'

const userId = process.argv[2]
if (!userId) {
  console.error('Usage: reset-user.js <userId>')
  process.exit(1)
}

const { jwt } = await authenticate()
const pk = process.env.SXT_PRIVATE_KEY

async function deleteFrom(schema, table, sql) {
  const resource = `${schema}.${table}`
  const biscuit = generateMasterBiscuit(resource, pk)
  try {
    await executeSql(jwt, sql, biscuit)
    const count = await executeSql(
      jwt,
      `SELECT COUNT(*) AS CNT FROM ${schema.toUpperCase()}.${table.toUpperCase()} WHERE USER_ID = '${userId}'`,
      biscuit,
    )
    console.log(`  ${resource}: deleted (${count[0]?.CNT ?? '?'} remaining)`)
  } catch (err) {
    if (err.message.includes('does not exist') || err.message.includes('not found')) {
      console.log(`  ${resource}: table not found (skip)`)
    } else {
      console.log(`  ${resource}: ERROR — ${err.message.substring(0, 80)}`)
    }
  }
}

async function deleteBySubquery(schema, table, sql) {
  const resource = `${schema}.${table}`
  const biscuit = generateMasterBiscuit(resource, pk)
  try {
    await executeSql(jwt, sql, biscuit)
    console.log(`  ${resource}: deleted`)
  } catch (err) {
    if (err.message.includes('does not exist') || err.message.includes('not found')) {
      console.log(`  ${resource}: table not found (skip)`)
    } else {
      console.log(`  ${resource}: ERROR — ${err.message.substring(0, 80)}`)
    }
  }
}

console.log(`\n═══ Resetting user: ${userId} ═══\n`)

// Email Core
console.log('Email Core (email_core_staging):')
await deleteBySubquery(
  'email_core_staging',
  'sync_events',
  `DELETE FROM EMAIL_CORE_STAGING.SYNC_EVENTS WHERE SYNC_STATE_ID IN (SELECT ID FROM EMAIL_CORE_STAGING.SYNC_STATES WHERE USER_ID = '${userId}')`,
)
await deleteBySubquery(
  'email_core_staging',
  'email_senders',
  `DELETE FROM EMAIL_CORE_STAGING.EMAIL_SENDERS WHERE EMAIL_METADATA_ID IN (SELECT ID FROM EMAIL_CORE_STAGING.EMAIL_METADATA WHERE USER_ID = '${userId}')`,
)
await deleteFrom(
  'email_core_staging',
  'sync_states',
  `DELETE FROM EMAIL_CORE_STAGING.SYNC_STATES WHERE USER_ID = '${userId}'`,
)
await deleteFrom(
  'email_core_staging',
  'email_metadata',
  `DELETE FROM EMAIL_CORE_STAGING.EMAIL_METADATA WHERE USER_ID = '${userId}'`,
)

// Dealsync
console.log('\nDealsync (dealsync_stg_v1):')
await deleteBySubquery(
  'dealsync_stg_v1',
  'deal_contacts',
  `DELETE FROM DEALSYNC_STG_V1.DEAL_CONTACTS WHERE DEAL_ID IN (SELECT ID FROM DEALSYNC_STG_V1.DEALS WHERE USER_ID = '${userId}')`,
)
await deleteFrom(
  'dealsync_stg_v1',
  'deals',
  `DELETE FROM DEALSYNC_STG_V1.DEALS WHERE USER_ID = '${userId}'`,
)
await deleteBySubquery(
  'dealsync_stg_v1',
  'email_thread_evaluations',
  `DELETE FROM DEALSYNC_STG_V1.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (SELECT DISTINCT THREAD_ID FROM DEALSYNC_STG_V1.DEAL_STATES WHERE USER_ID = '${userId}')`,
)
await deleteFrom(
  'dealsync_stg_v1',
  'deal_states',
  `DELETE FROM DEALSYNC_STG_V1.DEAL_STATES WHERE USER_ID = '${userId}'`,
)

console.log('\nDone.\n')
