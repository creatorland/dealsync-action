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
  let dispatchedFilter = 0
  let dispatchedDetect = 0

  // Dispatch filter batches
  while (filterSlots > 0 && pendingFilter > 0) {
    // 1. Trigger processor to get a trigger hash
    let triggerHash
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: { batch_type: 'filter' },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      triggerHash = result.triggerHash
    } catch (err) {
      core.error(`Filter trigger failed: ${err.message}`)
      break
    }

    // 2. Claim batch using trigger hash
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimFilterBatch(schema, triggerHash, filterBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, triggerHash))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) {
      // Nothing claimed — reset and stop
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        dispatch.resetClaimed(schema, triggerHash, STATUS.PENDING),
      )
      break
    }

    filterSlots -= claimed
    dispatchedFilter++
    core.info(`Filter batch: triggerHash=${triggerHash}, claimed=${claimed}`)
    await sleep(100)
  }

  // Dispatch detection batches
  while (detectSlots > 0 && pendingDetect > 0) {
    // 1. Trigger processor to get a trigger hash
    let triggerHash
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: { batch_type: 'detection' },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      triggerHash = result.triggerHash
    } catch (err) {
      core.error(`Detect trigger failed: ${err.message}`)
      break
    }

    // 2. Claim batch using trigger hash
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimDetectBatch(schema, triggerHash, detectBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, triggerHash))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) {
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        dispatch.resetClaimed(schema, triggerHash, STATUS.PENDING_CLASSIFICATION),
      )
      break
    }

    detectSlots -= claimed
    dispatchedDetect++
    core.info(`Detect batch: triggerHash=${triggerHash}, claimed=${claimed}`)
    await sleep(100)
  }

  return { dispatched_filter_count: dispatchedFilter, dispatched_detect_count: dispatchedDetect }
}
