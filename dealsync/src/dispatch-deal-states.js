import * as core from '@actions/core'
import { sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function parseNonNegativeInt(value, name) {
  const n = parseInt(value, 10)
  if (isNaN(n) || n < 0) throw new Error(`${name} must be non-negative integer, got: ${value}`)
  return n
}

/**
 * Dispatch deal-state worker workflows.
 *
 * Counts emails in EMAIL_METADATA that have no corresponding DEAL_STATES row,
 * calculates how many worker batches are needed, and triggers each via W3 JSON-RPC.
 */
export async function runDispatchDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const creatorName = core.getInput('creator-name')
  const batchSize = parseNonNegativeInt(
    core.getInput('deal-state-batch-size') || '500',
    'deal-state-batch-size',
  )
  const maxEmails = parseNonNegativeInt(
    core.getInput('deal-state-max-emails') || '5000',
    'deal-state-max-emails',
  )

  console.log('[dispatch-deal-states] Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  // Count emails without deal_states
  const countSql = `SELECT COUNT(*) AS CNT FROM EMAIL_CORE_STAGING.EMAIL_METADATA em WHERE em.ID NOT IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES)`
  const rows = await executeSql(apiUrl, jwt, biscuit, countSql)
  const diffCount = rows[0]?.CNT ?? 0

  console.log(`[dispatch-deal-states] ${diffCount} emails without deal_states`)

  if (diffCount === 0) {
    console.log('[dispatch-deal-states] Nothing to dispatch')
    return { workers_triggered: 0, total_emails: 0 }
  }

  const emailsToProcess = Math.min(diffCount, maxEmails)
  const numWorkers = Math.ceil(emailsToProcess / batchSize)

  console.log(
    `[dispatch-deal-states] Dispatching ${numWorkers} worker(s) for ${emailsToProcess} emails (batch=${batchSize})`,
  )

  let workersTriggered = 0
  for (let i = 0; i < numWorkers; i++) {
    const offset = i * batchSize
    const limit = batchSize

    const payload = {
      jsonrpc: '2.0',
      method: 'w3_triggerWorkflow',
      params: {
        workflowName: creatorName,
        body: { offset: String(offset), limit: String(limit) },
      },
      id: i + 1,
    }

    try {
      const resp = await fetch(w3RpcUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      const triggerHash = result.result?.triggerHash || result.triggerHash || ''

      workersTriggered++
      console.log(
        `[dispatch-deal-states] Worker ${i + 1}/${numWorkers}: offset=${offset} limit=${limit} trigger=${triggerHash.substring(0, 16)}`,
      )
    } catch (err) {
      console.warn(
        `[dispatch-deal-states] Worker ${i + 1}/${numWorkers} trigger failed: ${err.message}`,
      )
    }

    if (i < numWorkers - 1) {
      await sleep(100)
    }
  }

  console.log(
    `[dispatch-deal-states] Done: ${workersTriggered}/${numWorkers} workers triggered for ${emailsToProcess} emails`,
  )
  return { workers_triggered: workersTriggered, total_emails: emailsToProcess }
}
