import * as core from '@actions/core'
import { dispatch, sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function parseNonNegativeInt(value, name) {
  const n = parseInt(value, 10)
  if (isNaN(n) || n < 0) throw new Error(`${name} must be non-negative integer, got: ${value}`)
  return n
}

export async function runDispatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const processorName = core.getInput('processor-name') || 'Dealsync Processor'

  const activeFilter = parseNonNegativeInt(core.getInput('active-filter'), 'active-filter')
  const activeDetect = parseNonNegativeInt(core.getInput('active-detect'), 'active-detect')
  const pendingFilter = parseNonNegativeInt(core.getInput('pending-filter'), 'pending-filter')
  const pendingDetect = parseNonNegativeInt(core.getInput('pending-detect'), 'pending-detect')
  const maxFilter = parseNonNegativeInt(core.getInput('max-filter') || '600', 'max-filter')
  const maxDetect = parseNonNegativeInt(core.getInput('max-detect') || '300', 'max-detect')
  const filterBatchSize = parseNonNegativeInt(
    core.getInput('filter-batch-size') || '200',
    'filter-batch-size',
  )
  const detectBatchSize = parseNonNegativeInt(
    core.getInput('detect-batch-size') || '50',
    'detect-batch-size',
  )

  if (pendingFilter === 0 && pendingDetect === 0) {
    core.info('No pending emails to dispatch')
    return { dispatched_filter_count: 0, dispatched_detect_count: 0 }
  }

  core.info('Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  let filterSlots = maxFilter - activeFilter
  let detectSlots = maxDetect - activeDetect
  const filterBatches = []
  const detectBatches = []

  // Claim filter batches
  let batchIndex = 0
  while (filterSlots > 0 && pendingFilter > 0) {
    const stage = 1001 + batchIndex
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimFilterBatch(schema, stage, filterBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countAtStage(schema, stage))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break
    filterBatches.push({ stage, count: claimed })
    filterSlots -= claimed
    batchIndex++
    core.info(`Filter batch: stage=${stage}, claimed=${claimed}`)
  }

  // Claim detection batches
  batchIndex = 0
  while (detectSlots > 0 && pendingDetect > 0) {
    const stage = 11001 + batchIndex
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimDetectBatch(schema, stage, detectBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countAtStage(schema, stage))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break
    detectBatches.push({ stage, count: claimed })
    detectSlots -= claimed
    batchIndex++
    core.info(`Detect batch: stage=${stage}, claimed=${claimed}`)
  }

  // Trigger processors
  let dispatchedFilter = 0
  let dispatchedDetect = 0

  for (const batch of filterBatches) {
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: {
            batch_type: 'filter',
            transition_stage: String(batch.stage),
            reset_stage: '2',
          },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      dispatchedFilter++
      core.info(`Triggered filter processor: stage=${batch.stage}, hash=${result.triggerHash}`)
    } catch (err) {
      core.error(`Filter trigger failed for stage ${batch.stage}: ${err.message}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimedEmails(schema, batch.stage, 2))
    }
    await sleep(100)
  }

  for (const batch of detectBatches) {
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: {
            batch_type: 'detection',
            transition_stage: String(batch.stage),
            reset_stage: '3',
          },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      dispatchedDetect++
      core.info(`Triggered detect processor: stage=${batch.stage}, hash=${result.triggerHash}`)
    } catch (err) {
      core.error(`Detect trigger failed for stage ${batch.stage}: ${err.message}`)
      await executeSql(apiUrl, jwt, biscuit, dispatch.resetClaimedEmails(schema, batch.stage, 3))
    }
    await sleep(100)
  }

  return { dispatched_filter_count: dispatchedFilter, dispatched_detect_count: dispatchedDetect }
}
