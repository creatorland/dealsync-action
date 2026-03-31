#!/usr/bin/env node
/**
 * Trigger a full E2E sync for a user.
 * Creates a sync_state in SxT and calls core-email-metadata-ingestion.
 *
 * Usage:
 *   node --experimental-wasm-modules trigger-sync.js <userId> [lookbackDays]
 *
 * Env vars: see sxt-client.js + INGESTION_URL (defaults to staging)
 */

import { authenticate, generateMasterBiscuit, executeSql } from './sxt-client.js'
import { randomUUID } from 'crypto'

const userId = process.argv[2]
const lookbackDays = parseInt(process.argv[3] || '45')

if (!userId) {
  console.error('Usage: trigger-sync.js <userId> [lookbackDays]')
  process.exit(1)
}

const INGESTION_URL =
  process.env.INGESTION_URL ||
  'https://core-email-metadata-ingestion-360321061826.us-central1.run.app'

const { jwt } = await authenticate()
const pk = process.env.SXT_PRIVATE_KEY

// 1. Create sync_state
const syncStateId = randomUUID()
const now = new Date()
const start = new Date(now.getTime() - lookbackDays * 24 * 60 * 60 * 1000)

const b = generateMasterBiscuit('email_core_staging.sync_states', pk)
const insertSql = `INSERT INTO EMAIL_CORE_STAGING.SYNC_STATES (ID, USER_ID, SYNC_STRATEGY, STATUS, TOTAL_MESSAGES, DATE_RANGE_START, DATE_RANGE_END, RETRY_COUNT, MAX_RETRIES, CREATED_AT, UPDATED_AT) VALUES ('${syncStateId}', '${userId}', 'LOOKBACK', 'pending', 0, TIMESTAMP '${start.toISOString()}', TIMESTAMP '${now.toISOString()}', 0, 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`

await executeSql(jwt, insertSql, b)
console.log(`Created sync_state: ${syncStateId} (${lookbackDays}-day lookback)`)

// 2. Trigger ingestion
console.log(`Triggering ingestion at ${INGESTION_URL}/email-metadata...`)
const resp = await fetch(`${INGESTION_URL}/email-metadata`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ userId, syncStateId }),
})

if (!resp.ok) {
  const body = await resp.text()
  console.error(`Ingestion failed: ${resp.status} ${body}`)
  process.exit(1)
}

const result = await resp.json()
console.log('Ingestion triggered:', JSON.stringify(result))
console.log(`\nMonitor with: node --experimental-wasm-modules track-e2e.js ${userId} --poll 10`)
