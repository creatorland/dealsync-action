import * as core from '@actions/core'
import { dispatch, STATUS, sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function parseNonNegativeInt(value, name) {
  const n = parseInt(value, 10)
  if (isNaN(n) || n < 0) throw new Error(`${name} must be non-negative integer, got: ${value}`)
  return n
}

function generateBatchId() {
  const now = Date.now()
  const hex = (n, len) => n.toString(16).padStart(len, '0')
  const rand = () => Math.floor(Math.random() * 0x10000).toString(16).padStart(4, '0')
  const ts = hex(now, 12)
  const ver = '7' + rand().slice(1)
  const variant = (0x8 | (Math.random() * 4) | 0).toString(16) + rand().slice(1)
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-${ver}-${variant}-${rand()}${rand()}${rand()}`
}

/**
 * Dispatch command — claim-then-verify pattern.
 *
 * No separate concurrency check step needed. The dispatch:
 * 1. Claims a batch (atomic UPDATE with unique batch_id)
 * 2. Counts total in-flight to verify we're under the limit
 * 3. If over limit, releases the batch
 * 4. If under limit, triggers the processor
 */
export async function runDispatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const processorName = core.getInput('processor-name') || 'Dealsync Processor'

  const maxFilter = parseNonNegativeInt(core.getInput('max-filter') || '30000', 'max-filter')
  const maxClassify = parseNonNegativeInt(core.getInput('max-classify') || '750', 'max-classify')
  const filterBatchSize = parseNonNegativeInt(
    core.getInput('filter-batch-size') || '200',
    'filter-batch-size',
  )
  const classifyBatchSize = parseNonNegativeInt(
    core.getInput('classify-batch-size') || '5',
    'classify-batch-size',
  )

  console.log('[dispatch] Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  let dispatchedFilter = 0
  let dispatchedClassify = 0

  // Dispatch filter batches: claim → verify → trigger
  while (true) {
    const batchId = generateBatchId()

    // 1. Claim batch
    await executeSql(apiUrl, jwt, biscuit, dispatch.claimFilterBatch(schema, batchId, filterBatchSize))
    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break // nothing left to claim

    // 2. Verify: count total in-flight, release if over limit
    const inflight = await executeSql(apiUrl, jwt, biscuit, dispatch.countInFlight(schema, STATUS.FILTERING))
    const totalInFlight = inflight[0]?.CNT ?? 0

    if (totalInFlight > maxFilter) {
      console.log(`[dispatch] Over filter limit (${totalInFlight}/${maxFilter}), releasing batch ${batchId}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING))
      break
    }

    // 3. Trigger processor
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: { batch_type: 'filter', batch_id: batchId } }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      dispatchedFilter++
      console.log(`[dispatch] Filter batch dispatched: ${batchId} (${claimed} emails, ${totalInFlight} total in-flight)`)
    } catch (err) {
      console.log(`[dispatch] Filter trigger failed for ${batchId}: ${err.message}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING))
      break
    }

    await sleep(100)
  }

  // Dispatch classify batches: claim → verify → trigger
  while (true) {
    const batchId = generateBatchId()

    // 1. Claim batch (by thread)
    await executeSql(apiUrl, jwt, biscuit, dispatch.claimClassifyBatch(schema, batchId, classifyBatchSize))
    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break

    // 2. Verify limit
    const inflight = await executeSql(apiUrl, jwt, biscuit, dispatch.countInFlight(schema, STATUS.CLASSIFYING))
    const totalInFlight = inflight[0]?.CNT ?? 0

    if (totalInFlight > maxClassify) {
      console.log(`[dispatch] Over classify limit (${totalInFlight}/${maxClassify}), releasing batch ${batchId}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING_CLASSIFICATION))
      break
    }

    // 3. Trigger processor
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs: { batch_type: 'classify', batch_id: batchId } }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      dispatchedClassify++
      console.log(`[dispatch] Classify batch dispatched: ${batchId} (${claimed} emails, ${totalInFlight} total in-flight)`)
    } catch (err) {
      console.log(`[dispatch] Classify trigger failed for ${batchId}: ${err.message}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING_CLASSIFICATION))
      break
    }

    await sleep(100)
  }

  console.log(`[dispatch] Done: ${dispatchedFilter} filter, ${dispatchedClassify} classify`)
  return { dispatched_filter_count: dispatchedFilter, dispatched_classify_count: dispatchedClassify }
}
