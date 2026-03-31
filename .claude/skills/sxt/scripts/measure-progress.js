#!/usr/bin/env node
/**
 * Measure deal_states pipeline progress.
 *
 * Shows status breakdown with weighted progress toward completion.
 * 100% = all rows in terminal states (deal, not_deal, filter_rejected).
 *
 * State machine weights (how far through the pipeline):
 *   pending                  → 0%
 *   filtering                → 25%
 *   pending_classification   → 50%
 *   classifying              → 75%
 *   deal / not_deal / filter_rejected → 100% (terminal)
 *
 * Usage:
 *   node --experimental-wasm-modules measure-progress.js [userId]
 *
 * Env vars: see sxt-client.js
 */

import { authenticate, generateBiscuit, executeSql } from './sxt-client.js'

const userId = process.argv[2]

const WEIGHTS = {
  pending: 0,
  filtering: 0.25,
  pending_classification: 0.5,
  classifying: 0.75,
  deal: 1,
  not_deal: 1,
  filter_rejected: 1,
}

const TERMINAL = new Set(['deal', 'not_deal', 'filter_rejected'])

async function main() {
  const { jwt } = await authenticate()
  const pk = process.env.SXT_PRIVATE_KEY
  const biscuit = generateBiscuit('dql_select', 'dealsync_stg_v1.deal_states', pk)

  const whereClause = userId ? ` WHERE USER_ID = '${userId}'` : ''
  const sql = `SELECT STATUS, COUNT(*) AS CNT FROM DEALSYNC_STG_V1.DEAL_STATES${whereClause} GROUP BY STATUS ORDER BY CNT DESC`

  const rows = await executeSql(jwt, sql, biscuit)

  if (!rows.length) {
    console.log('No deal_states found.')
    return
  }

  let total = 0
  let weightedSum = 0
  let terminalCount = 0

  console.log(`\n═══ Pipeline Progress${userId ? ` (${userId})` : ''} ═══\n`)
  console.log('Status                    Count     Weight')
  console.log('─'.repeat(50))

  for (const row of rows) {
    const status = row.STATUS
    const count = parseInt(row.CNT, 10)
    const weight = WEIGHTS[status] ?? 0
    total += count
    weightedSum += count * weight
    if (TERMINAL.has(status)) terminalCount += count
    const pct = (weight * 100).toFixed(0).padStart(4) + '%'
    console.log(`${status.padEnd(26)}${String(count).padStart(6)}    ${pct}`)
  }

  const overallProgress = total > 0 ? (weightedSum / total) * 100 : 0
  const terminalPct = total > 0 ? (terminalCount / total) * 100 : 0

  console.log('─'.repeat(50))
  console.log(`${'TOTAL'.padEnd(26)}${String(total).padStart(6)}`)
  console.log()
  console.log(`Weighted progress:  ${overallProgress.toFixed(1)}%`)
  console.log(`Terminal (done):    ${terminalPct.toFixed(1)}% (${terminalCount}/${total})`)
  console.log(`In-progress:        ${total - terminalCount} rows`)
  console.log()
}

main().catch((err) => {
  console.error('Failed:', err.message)
  process.exit(1)
})
