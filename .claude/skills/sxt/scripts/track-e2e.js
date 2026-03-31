#!/usr/bin/env node
/**
 * Track the full E2E pipeline state for a user.
 * Polls SxT to show ingestion → deal_states → W3 workflow progress.
 *
 * Usage:
 *   node --experimental-wasm-modules track-e2e.js <userId> [--poll <seconds>]
 *
 * Examples:
 *   node --experimental-wasm-modules track-e2e.js 38Jeic1UdHYI8wwJQyrPu
 *   node --experimental-wasm-modules track-e2e.js 38Jeic1UdHYI8wwJQyrPu --poll 10
 *
 * Env vars: see sxt-client.js
 */

import { authenticate, generateBiscuit, executeSql } from './sxt-client.js'

const userId = process.argv[2]
const pollIdx = process.argv.indexOf('--poll')
const pollInterval = pollIdx !== -1 ? parseInt(process.argv[pollIdx + 1] || '10') * 1000 : null

if (!userId) {
  console.error('Usage: track-e2e.js <userId> [--poll <seconds>]')
  process.exit(1)
}

const EC = 'EMAIL_CORE_STAGING'
const DS = 'DEALSYNC_STG_V1'
const ec = EC.toLowerCase()
const ds = DS.toLowerCase()

async function query(jwt, pk, sql, resource) {
  const biscuit = generateBiscuit('dql_select', resource, pk)
  return executeSql(jwt, sql, biscuit)
}

async function safeQuery(jwt, pk, sql, resource) {
  try {
    return await query(jwt, pk, sql, resource)
  } catch {
    return null
  }
}

async function snapshot(jwt, pk) {
  const results = {}

  // ── Email Core: Ingestion Progress ──
  results.syncStates = await safeQuery(
    jwt,
    pk,
    `SELECT STATUS, COUNT(*) AS CNT FROM ${EC}.SYNC_STATES WHERE USER_ID = '${userId}' GROUP BY STATUS`,
    `${ec}.sync_states`,
  )

  results.emailMetadata = await safeQuery(
    jwt,
    pk,
    `SELECT PROCESSING_STATUS, COUNT(*) AS CNT FROM ${EC}.EMAIL_METADATA WHERE USER_ID = '${userId}' GROUP BY PROCESSING_STATUS`,
    `${ec}.email_metadata`,
  )

  results.emailMetadataTotal = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${EC}.EMAIL_METADATA WHERE USER_ID = '${userId}'`,
    `${ec}.email_metadata`,
  )

  results.emailSenders = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${EC}.EMAIL_SENDERS WHERE EMAIL_METADATA_ID IN (SELECT ID FROM ${EC}.EMAIL_METADATA WHERE USER_ID = '${userId}')`,
    `${ec}.email_senders`,
  )

  results.latestSyncEvent = await safeQuery(
    jwt,
    pk,
    `SELECT EVENT, CREATED_AT FROM ${EC}.SYNC_EVENTS WHERE SYNC_STATE_ID IN (SELECT ID FROM ${EC}.SYNC_STATES WHERE USER_ID = '${userId}') ORDER BY CREATED_AT DESC LIMIT 5`,
    `${ec}.sync_events`,
  )

  // ── Dealsync: Deal States Pipeline ──
  results.dealStatuses = await safeQuery(
    jwt,
    pk,
    `SELECT STATUS, COUNT(*) AS CNT FROM ${DS}.DEAL_STATES WHERE USER_ID = '${userId}' GROUP BY STATUS ORDER BY STATUS`,
    `${ds}.deal_states`,
  )

  results.dealStatesTotal = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${DS}.DEAL_STATES WHERE USER_ID = '${userId}'`,
    `${ds}.deal_states`,
  )

  results.deadLetters = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${DS}.DEAL_STATES WHERE USER_ID = '${userId}' AND ATTEMPTS >= 3 AND STATUS IN ('filtering', 'classifying')`,
    `${ds}.deal_states`,
  )

  results.deals = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${DS}.DEALS WHERE USER_ID = '${userId}'`,
    `${ds}.deals`,
  )

  results.contacts = await safeQuery(
    jwt,
    pk,
    `SELECT COUNT(*) AS CNT FROM ${DS}.CONTACTS WHERE EMAIL IN (SELECT DISTINCT SENDER_EMAIL FROM ${EC}.EMAIL_SENDERS WHERE EMAIL_METADATA_ID IN (SELECT ID FROM ${EC}.EMAIL_METADATA WHERE USER_ID = '${userId}'))`,
    `${ds}.contacts`,
  )

  return results
}

function formatStatuses(statuses) {
  if (!statuses || statuses.length === 0) return '(none)'
  return statuses.map((r) => `${r.STATUS}: ${r.CNT}`).join(', ')
}

function print(results) {
  const ts = new Date().toISOString().substring(11, 19)
  console.log(`\n[${ts}] ═══ E2E Status: ${userId} ═══`)

  // Ingestion
  console.log('\n  📥 INGESTION (email_core_staging)')
  const syncStatus = results.syncStates?.map((r) => `${r.STATUS}: ${r.CNT}`).join(', ') || '(none)'
  console.log(`    sync_states: ${syncStatus}`)

  const totalEmails = results.emailMetadataTotal?.[0]?.CNT ?? 0
  const processingStatus =
    results.emailMetadata?.map((r) => `${r.PROCESSING_STATUS}: ${r.CNT}`).join(', ') || '(none)'
  console.log(`    email_metadata: ${totalEmails} total (${processingStatus})`)

  console.log(`    email_senders: ${results.emailSenders?.[0]?.CNT ?? 0}`)

  if (results.latestSyncEvent?.length > 0) {
    const events = results.latestSyncEvent.map((e) => `${e.EVENT} (${e.CREATED_AT})`).join(' → ')
    console.log(`    latest events: ${events}`)
  }

  // Deal states
  console.log('\n  🔄 PIPELINE (dealsync_stg_v1)')
  const totalDealStates = results.dealStatesTotal?.[0]?.CNT ?? 0
  console.log(`    deal_states: ${totalDealStates} total`)

  const getCount = (status) => results.dealStatuses?.find((r) => r.STATUS === status)?.CNT ?? 0
  const pending = getCount('pending')
  const filtering = getCount('filtering')
  const pendingClassification = getCount('pending_classification')
  const classifying = getCount('classifying')
  const deal = getCount('deal')
  const notDeal = getCount('not_deal')
  const filterRejected = getCount('filter_rejected')
  const terminal = deal + notDeal + filterRejected
  const inflight = filtering + classifying

  if (totalDealStates > 0) {
    console.log(`    ┌─ pending:                ${String(pending).padStart(6)}`)
    console.log(`    ├─ filtering:              ${String(filtering).padStart(6)}`)
    console.log(`    ├─ pending_classification:  ${String(pendingClassification).padStart(6)}`)
    console.log(`    ├─ classifying:            ${String(classifying).padStart(6)}`)
    console.log(`    ├─ filter_rejected:        ${String(filterRejected).padStart(6)}`)
    console.log(`    ├─ deal:                   ${String(deal).padStart(6)}`)
    console.log(`    ├─ not_deal:               ${String(notDeal).padStart(6)}`)
    console.log(
      `    └─ dead letters (≥3 att):  ${String(results.deadLetters?.[0]?.CNT ?? 0).padStart(6)}`,
    )
  }

  console.log(`    deals: ${results.deals?.[0]?.CNT ?? 0}`)
  console.log(`    contacts: ${results.contacts?.[0]?.CNT ?? 0}`)

  // Summary
  console.log('\n  📊 PROGRESS')
  if (totalDealStates === 0) {
    console.log('    ⏳ Waiting for deal_states to be created...')
  } else if (pending + pendingClassification + inflight === 0 && terminal > 0) {
    console.log(
      `    ✅ COMPLETE — ${terminal} processed (${deal} deals, ${notDeal} not-deal, ${filterRejected} filter-rejected)`,
    )
  } else {
    const pct = totalDealStates > 0 ? Math.round((terminal / totalDealStates) * 100) : 0
    console.log(
      `    🔄 ${pct}% done — ${terminal}/${totalDealStates} at terminal status, ${inflight} in-flight, ${pending + pendingClassification} queued`,
    )
  }
}

async function main() {
  const { jwt } = await authenticate()
  const pk = process.env.SXT_PRIVATE_KEY

  if (pollInterval) {
    console.log(`Polling every ${pollInterval / 1000}s. Ctrl+C to stop.`)
    while (true) {
      const results = await snapshot(jwt, pk)
      print(results)
      await new Promise((r) => setTimeout(r, pollInterval))
    }
  } else {
    const results = await snapshot(jwt, pk)
    print(results)
  }
}

main().catch((err) => {
  console.error('Failed:', err.message)
  process.exit(1)
})
