import * as core from '@actions/core'
import { SpaceAndTime } from 'sxt-nodejs-sdk'
import { validatePositiveInt } from './validators.js'

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  let lastError
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, options)
      if (!response.ok) {
        const body = await response.text()
        if (attempt < maxRetries && [429, 500, 502, 503, 504].includes(response.status)) {
          await sleep(Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 30000))
          continue
        }
        throw new Error(`HTTP ${response.status}: ${body}`)
      }
      return response
    } catch (err) {
      lastError = err
      if (attempt < maxRetries && !err.message?.startsWith('HTTP ')) {
        await sleep(Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 30000))
        continue
      }
      throw err
    }
  }
  throw lastError
}

function classifySql(sql) {
  const trimmed = sql.trim().toUpperCase()
  if (trimmed.startsWith('SELECT')) return 'dql_select'
  if (trimmed.startsWith('INSERT')) return 'dml_insert'
  if (trimmed.startsWith('UPDATE')) return 'dml_update'
  if (trimmed.startsWith('DELETE')) return 'dml_delete'
  return 'dql_select'
}

function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) throw new Error(`Invalid schema: ${schema}`)
  return schema
}

async function getAuthToken(authUrl, authSecret) {
  const resp = await fetchWithRetry(authUrl, {
    method: 'GET',
    headers: { 'x-shared-secret': authSecret },
  })
  const data = await resp.json()
  return data.data || data.accessToken || data
}

function generateBiscuit(privateKey, operation, resource) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  const result = auth.CreateBiscuitToken(
    [{ operation, resource: resource.toLowerCase() }],
    privateKey,
  )
  return result.data[0]
}

async function executeSql(apiUrl, token, biscuit, resource, sql) {
  const resp = await fetchWithRetry(`${apiUrl}/v1/sql`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      sqlText: sql,
      biscuits: [biscuit],
      resources: [resource.toLowerCase()],
    }),
  })
  return resp.json()
}

async function sxtQuery(apiUrl, authToken, privateKey, resource, sql) {
  const operation = classifySql(sql)
  const biscuit = generateBiscuit(privateKey, operation, resource)
  return executeSql(apiUrl, authToken, biscuit, resource, sql)
}

export async function run() {
  try {
    const authUrl = core.getInput('auth-url')
    const authSecret = core.getInput('auth-secret')
    const apiUrl = core.getInput('sxt-api-url')
    const privateKey = core.getInput('private-key')
    const schema = sanitizeSchema(core.getInput('sxt-schema'))
    const w3RpcUrl = core.getInput('w3-rpc-url')
    const resource = `${schema}.EMAIL_METADATA`

    const activeFilter = validatePositiveInt(core.getInput('active-filter'), 'active-filter')
    const activeDetect = validatePositiveInt(core.getInput('active-detect'), 'active-detect')
    const pendingFilter = validatePositiveInt(core.getInput('pending-filter'), 'pending-filter')
    const pendingDetect = validatePositiveInt(core.getInput('pending-detect'), 'pending-detect')
    const maxFilter = validatePositiveInt(core.getInput('max-filter'), 'max-filter')
    const maxDetect = validatePositiveInt(core.getInput('max-detect'), 'max-detect')
    const filterBatchSize = validatePositiveInt(core.getInput('filter-batch-size'), 'filter-batch-size')
    const detectBatchSize = validatePositiveInt(core.getInput('detect-batch-size'), 'detect-batch-size')

    // Early exit
    if (pendingFilter === 0 && pendingDetect === 0) {
      core.info('No pending emails to dispatch')
      core.setOutput('success', 'true')
      core.setOutput('dispatched_filter_count', '0')
      core.setOutput('dispatched_detect_count', '0')
      return
    }

    // Auth once
    core.info('Authenticating...')
    const authToken = await getAuthToken(authUrl, authSecret)

    let filterSlots = maxFilter - activeFilter
    let detectSlots = maxDetect - activeDetect
    const filterBatches = []
    const detectBatches = []

    // Claim filter batches
    let batchIndex = 0
    while (filterSlots > 0 && pendingFilter > 0) {
      const stage = 1001 + batchIndex
      await sxtQuery(apiUrl, authToken, privateKey, resource,
        `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${stage} WHERE ID IN (SELECT ID FROM ${schema}.EMAIL_METADATA WHERE STAGE = 2 LIMIT ${filterBatchSize})`)

      const rows = await sxtQuery(apiUrl, authToken, privateKey, resource,
        `SELECT COUNT(*) AS CNT FROM ${schema}.EMAIL_METADATA WHERE STAGE = ${stage}`)
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
      await sxtQuery(apiUrl, authToken, privateKey, resource,
        `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${stage} WHERE ID IN (SELECT em.ID FROM ${schema}.EMAIL_METADATA em WHERE em.STAGE = 3 AND NOT EXISTS (SELECT 1 FROM ${schema}.EMAIL_METADATA m2 WHERE m2.THREAD_ID = em.THREAD_ID AND m2.USER_ID = em.USER_ID AND m2.STAGE IN (1, 2)) LIMIT ${detectBatchSize})`)

      const rows = await sxtQuery(apiUrl, authToken, privateKey, resource,
        `SELECT COUNT(*) AS CNT FROM ${schema}.EMAIL_METADATA WHERE STAGE = ${stage}`)
      const claimed = rows[0]?.CNT ?? 0

      if (claimed === 0) break
      detectBatches.push({ stage, count: claimed })
      detectSlots -= claimed
      batchIndex++
      core.info(`Detect batch: stage=${stage}, claimed=${claimed}`)
    }

    // Trigger processors
    let callIndex = 1
    let dispatchedFilter = 0
    let dispatchedDetect = 0

    for (const batch of filterBatches) {
      try {
        const resp = await fetch(w3RpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            jsonrpc: '2.0',
            method: 'w3_triggerWorkflow',
            params: {
              workflowName: 'Dealsync Processor v2',
              body: { batch_type: 'filter', transition_stage: String(batch.stage), reset_stage: '2' },
            },
            id: callIndex++,
          }),
        })
        const result = await resp.json()
        if (result.error) throw new Error(result.error.message)
        dispatchedFilter++
        core.info(`Triggered filter processor: stage=${batch.stage}, hash=${result.result?.triggerHash}`)
      } catch (err) {
        core.error(`Filter trigger failed for stage ${batch.stage}: ${err.message}`)
        await sxtQuery(apiUrl, authToken, privateKey, resource,
          `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 2 WHERE STAGE = ${batch.stage}`)
      }
      await sleep(100)
    }

    for (const batch of detectBatches) {
      try {
        const resp = await fetch(w3RpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            jsonrpc: '2.0',
            method: 'w3_triggerWorkflow',
            params: {
              workflowName: 'Dealsync Processor v2',
              body: { batch_type: 'detection', transition_stage: String(batch.stage), reset_stage: '3' },
            },
            id: callIndex++,
          }),
        })
        const result = await resp.json()
        if (result.error) throw new Error(result.error.message)
        dispatchedDetect++
        core.info(`Triggered detect processor: stage=${batch.stage}, hash=${result.result?.triggerHash}`)
      } catch (err) {
        core.error(`Detect trigger failed for stage ${batch.stage}: ${err.message}`)
        await sxtQuery(apiUrl, authToken, privateKey, resource,
          `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 3 WHERE STAGE = ${batch.stage}`)
      }
      await sleep(100)
    }

    const totalClaimed = filterBatches.length + detectBatches.length
    const totalDispatched = dispatchedFilter + dispatchedDetect
    const hasFailures = totalDispatched < totalClaimed

    core.setOutput('dispatched_filter_count', String(dispatchedFilter))
    core.setOutput('dispatched_detect_count', String(dispatchedDetect))

    if (hasFailures) {
      core.setOutput('success', 'false')
      core.setFailed(`${totalClaimed - totalDispatched} of ${totalClaimed} triggers failed`)
    } else {
      core.setOutput('success', 'true')
    }
  } catch (error) {
    core.setOutput('success', 'false')
    core.setOutput('dispatched_filter_count', '0')
    core.setOutput('dispatched_detect_count', '0')
    core.setFailed(error.message)
  }
}
