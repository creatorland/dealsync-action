import * as core from '@actions/core'
import { sanitizeSchema } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'

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
export async function runDispatchDealStateSync() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const syncWorkflowName = core.getInput('sync-workflow-name')
  const batchSize = parseNonNegativeInt(
    core.getInput('deal-state-batch-size') || '500',
    'deal-state-batch-size',
  )
  const maxEmails = parseNonNegativeInt(
    core.getInput('deal-state-max-emails') || '5000',
    'deal-state-max-emails',
  )

  console.log('[dispatch-deal-state-sync] Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  // Count emails without deal_states
  const countSql = `SELECT COUNT(*) AS CNT FROM EMAIL_CORE_STAGING.EMAIL_METADATA em WHERE em.ID NOT IN (SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES)`
  const rows = await executeSql(apiUrl, jwt, biscuit, countSql)
  const diffCount = rows[0]?.CNT ?? 0

  console.log(`[dispatch-deal-state-sync] ${diffCount} emails without deal_states`)

  if (diffCount === 0) {
    console.log('[dispatch-deal-state-sync] Nothing to dispatch')
    return { workers_triggered: 0, total_emails: 0 }
  }

  const emailsToProcess = Math.min(diffCount, maxEmails)
  const numWorkers = Math.ceil(emailsToProcess / batchSize)

  console.log(
    `[dispatch-deal-state-sync] Dispatching ${numWorkers} worker(s) for ${emailsToProcess} emails (batch=${batchSize})`,
  )

  let workersTriggered = 0
  for (let i = 0; i < numWorkers; i++) {
    const offset = i * batchSize
    const limit = batchSize

    const inputs = { offset: String(offset), limit: String(limit) }
    const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(syncWorkflowName)}/trigger`
    const MAX_TRIGGER_RETRIES = 3

    let triggered = false
    for (let attempt = 0; attempt <= MAX_TRIGGER_RETRIES; attempt++) {
      try {
        const resp = await fetch(triggerUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ inputs }),
        })
        if (!resp.ok) {
          const body = await resp.text()
          if (attempt < MAX_TRIGGER_RETRIES) {
            const delay = Math.min(1000 * Math.pow(2, attempt), 30000)
            console.warn(
              `[dispatch-deal-state-sync] Worker ${i + 1}/${numWorkers} HTTP ${resp.status} (attempt ${attempt + 1}/${MAX_TRIGGER_RETRIES + 1}), retrying in ${delay}ms`,
            )
            await sleep(delay)
            continue
          }
          throw new Error(`HTTP ${resp.status}: ${body}`)
        }
        const result = await resp.json()
        const triggerHash = result.triggerHash || ''

        workersTriggered++
        triggered = true
        console.log(
          `[dispatch-deal-state-sync] Worker ${i + 1}/${numWorkers}: offset=${offset} limit=${limit} trigger=${triggerHash.substring(0, 16)}`,
        )
        break
      } catch (err) {
        if (attempt < MAX_TRIGGER_RETRIES) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 30000)
          console.warn(
            `[dispatch-deal-state-sync] Worker ${i + 1}/${numWorkers} error (attempt ${attempt + 1}/${MAX_TRIGGER_RETRIES + 1}): ${err.message}, retrying in ${delay}ms`,
          )
          await sleep(delay)
        } else {
          console.warn(
            `[dispatch-deal-state-sync] Worker ${i + 1}/${numWorkers} failed after ${MAX_TRIGGER_RETRIES + 1} attempts: ${err.message}`,
          )
        }
      }
    }

    if (triggered && i < numWorkers - 1) {
      await sleep(100)
    }
  }

  console.log(
    `[dispatch-deal-state-sync] Done: ${workersTriggered}/${numWorkers} workers triggered for ${emailsToProcess} emails`,
  )
  return { workers_triggered: workersTriggered, total_emails: emailsToProcess }
}
