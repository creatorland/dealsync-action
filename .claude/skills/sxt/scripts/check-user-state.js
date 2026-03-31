#!/usr/bin/env node
/**
 * Check a user's state across all SxT tables.
 *
 * Usage:
 *   node --experimental-wasm-modules check-user-state.js <userId>
 *
 * Env vars: see sxt-client.js
 */

import { authenticate, generateBiscuit, executeSql } from './sxt-client.js'

const userId = process.argv[2]
if (!userId) {
  console.error('Usage: check-user-state.js <userId>')
  process.exit(1)
}

const DEALSYNC = 'dealsync_stg_v1'
const EMAIL_CORE = 'email_core_staging'

const queries = [
  {
    label: `${EMAIL_CORE}.email_metadata`,
    sql: `SELECT COUNT(*) AS CNT FROM ${EMAIL_CORE}.EMAIL_METADATA WHERE USER_ID = '${userId}'`,
    resource: `${EMAIL_CORE}.email_metadata`,
  },
  {
    label: `${EMAIL_CORE}.sync_states`,
    sql: `SELECT COUNT(*) AS CNT FROM ${EMAIL_CORE}.SYNC_STATES WHERE USER_ID = '${userId}'`,
    resource: `${EMAIL_CORE}.sync_states`,
  },
  {
    label: `${EMAIL_CORE}.email_senders`,
    sql: `SELECT COUNT(*) AS CNT FROM ${EMAIL_CORE}.EMAIL_SENDERS WHERE EMAIL_METADATA_ID IN (SELECT ID FROM ${EMAIL_CORE}.EMAIL_METADATA WHERE USER_ID = '${userId}')`,
    resource: `${EMAIL_CORE}.email_senders`,
  },
  {
    label: `${DEALSYNC}.deal_states`,
    sql: `SELECT COUNT(*) AS CNT FROM ${DEALSYNC}.DEAL_STATES WHERE USER_ID = '${userId}'`,
    resource: `${DEALSYNC}.deal_states`,
  },
  {
    label: `${DEALSYNC}.deal_states (by status)`,
    sql: `SELECT STATUS, COUNT(*) AS CNT FROM ${DEALSYNC}.DEAL_STATES WHERE USER_ID = '${userId}' GROUP BY STATUS ORDER BY STATUS`,
    resource: `${DEALSYNC}.deal_states`,
  },
  {
    label: `${DEALSYNC}.deal_states (dead letters)`,
    sql: `SELECT COUNT(*) AS CNT FROM ${DEALSYNC}.DEAL_STATES WHERE USER_ID = '${userId}' AND ATTEMPTS >= 3 AND STATUS IN ('filtering', 'classifying')`,
    resource: `${DEALSYNC}.deal_states`,
  },
  {
    label: `${DEALSYNC}.contacts`,
    sql: `SELECT COUNT(*) AS CNT FROM ${DEALSYNC}.CONTACTS WHERE EMAIL IN (SELECT DISTINCT SENDER_EMAIL FROM ${EMAIL_CORE}.EMAIL_SENDERS WHERE EMAIL_METADATA_ID IN (SELECT ID FROM ${EMAIL_CORE}.EMAIL_METADATA WHERE USER_ID = '${userId}'))`,
    resource: `${DEALSYNC}.contacts`,
  },
  {
    label: `${DEALSYNC}.deals`,
    sql: `SELECT COUNT(*) AS CNT FROM ${DEALSYNC}.DEALS WHERE USER_ID = '${userId}'`,
    resource: `${DEALSYNC}.deals`,
  },
  {
    label: `${DEALSYNC}.email_thread_evaluations`,
    sql: `SELECT COUNT(*) AS CNT FROM ${DEALSYNC}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (SELECT DISTINCT THREAD_ID FROM ${DEALSYNC}.DEAL_STATES WHERE USER_ID = '${userId}')`,
    resource: `${DEALSYNC}.email_thread_evaluations`,
  },
]

async function main() {
  console.log(`\n═══ User State: ${userId} ═══\n`)

  const { jwt } = await authenticate()
  const privateKey = process.env.SXT_PRIVATE_KEY

  for (const q of queries) {
    try {
      const biscuit = generateBiscuit('dql_select', q.resource, privateKey)
      const result = await executeSql(jwt, q.sql, biscuit)

      if (q.label.includes('by status')) {
        const statuses = result.map((r) => `${r.STATUS}: ${r.CNT}`).join(', ')
        console.log(`  ${q.label}: ${statuses || '(empty)'}`)
      } else {
        console.log(`  ${q.label}: ${result[0]?.CNT ?? 0} rows`)
      }
    } catch (err) {
      const msg = err.message.includes('does not exist')
        ? '(table not found)'
        : err.message.substring(0, 60)
      console.log(`  ${q.label}: ERROR — ${msg}`)
    }
  }

  console.log()
}

main().catch((err) => {
  console.error('Failed:', err.message)
  process.exit(1)
})
