import * as core from '@actions/core'
import { dispatch, batchEvents, STATUS, sanitizeSchema } from '../../shared/queries.js'
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

async function claimFilter(apiUrl, jwt, biscuit, schema, batchSize, maxInFlight) {
  const batchId = generateBatchId()
  await executeSql(apiUrl, jwt, biscuit, dispatch.claimFilterBatch(schema, batchId, batchSize))
  const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
  const claimed = rows[0]?.CNT ?? 0
  if (claimed === 0) return null

  const inflight = await executeSql(apiUrl, jwt, biscuit, dispatch.countInFlight(schema, STATUS.FILTERING))
  if ((inflight[0]?.CNT ?? 0) > maxInFlight) {
    await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING))
    return null
  }

  return { batchId, claimed }
}

async function claimClassify(apiUrl, jwt, biscuit, schema, batchSize, maxInFlight) {
  const batchId = generateBatchId()
  await executeSql(apiUrl, jwt, biscuit, dispatch.claimClassifyBatch(schema, batchId, batchSize))
  const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
  const claimed = rows[0]?.CNT ?? 0
  if (claimed === 0) return null

  const inflight = await executeSql(apiUrl, jwt, biscuit, dispatch.countInFlight(schema, STATUS.CLASSIFYING))
  if ((inflight[0]?.CNT ?? 0) > maxInFlight) {
    await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, batchId, STATUS.PENDING_CLASSIFICATION))
    return null
  }

  return { batchId, claimed }
}

/**
 * Dispatch new batches only. Stuck batch retriggering is handled by retrigger-stuck command.
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
  const filterBatchSize = parseNonNegativeInt(core.getInput('filter-batch-size') || '200', 'filter-batch-size')
  const classifyBatchSize = parseNonNegativeInt(core.getInput('classify-batch-size') || '5', 'classify-batch-size')

  console.log('[dispatch] Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  let dispatchedFilter = 0
  let dispatchedClassify = 0
  let filterExhausted = false
  let classifyExhausted = false

  while (!filterExhausted || !classifyExhausted) {
    let filterBatch = null
    let classifyBatch = null

    if (!filterExhausted) {
      filterBatch = await claimFilter(apiUrl, jwt, biscuit, schema, filterBatchSize, maxFilter)
      if (!filterBatch) filterExhausted = true
    }

    if (!classifyExhausted) {
      classifyBatch = await claimClassify(apiUrl, jwt, biscuit, schema, classifyBatchSize, maxClassify)
      if (!classifyBatch) classifyExhausted = true
    }

    if (!filterBatch && !classifyBatch) break

    const inputs = {}
    if (filterBatch) inputs.filter_batch_id = filterBatch.batchId
    if (classifyBatch) inputs.classify_batch_id = classifyBatch.batchId

    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ inputs }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const triggerResult = await resp.json()
      const triggerHash = triggerResult.triggerHash || ''

      // Log batch events
      const eventValues = []
      if (filterBatch) {
        eventValues.push(`('${triggerHash}', '${filterBatch.batchId}', 'filter', 'new', CURRENT_TIMESTAMP)`)
        dispatchedFilter++
        console.log(`[dispatch] Filter: ${filterBatch.batchId} (${filterBatch.claimed} emails) trigger=${triggerHash.substring(0, 16)}`)
      }
      if (classifyBatch) {
        eventValues.push(`('${triggerHash}', '${classifyBatch.batchId}', 'classify', 'new', CURRENT_TIMESTAMP)`)
        dispatchedClassify++
        console.log(`[dispatch] Classify: ${classifyBatch.batchId} (${classifyBatch.claimed} emails) trigger=${triggerHash.substring(0, 16)}`)
      }
      if (eventValues.length > 0) {
        try {
          await executeSql(apiUrl, jwt, biscuit, batchEvents.insert(schema, eventValues.join(', ')))
        } catch (e) {
          console.log(`[dispatch] batch_events insert failed: ${e.message.substring(0, 80)}`)
        }
      }
    } catch (err) {
      console.log(`[dispatch] Trigger failed: ${err.message}`)
      if (filterBatch) {
        await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, filterBatch.batchId, STATUS.PENDING))
      }
      if (classifyBatch) {
        await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimed(schema, classifyBatch.batchId, STATUS.PENDING_CLASSIFICATION))
      }
      break
    }

    await sleep(100)
  }

  console.log(`[dispatch] Done: ${dispatchedFilter} filter, ${dispatchedClassify} classify`)
  return { dispatched_filter_count: dispatchedFilter, dispatched_classify_count: dispatchedClassify }
}
