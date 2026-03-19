import * as core from '@actions/core'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

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
          const delay = Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 30000)
          core.warning(`SxT ${response.status}, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${maxRetries})`)
          await sleep(delay)
          continue
        }
        throw new Error(`HTTP ${response.status}: ${body}`)
      }
      return response
    } catch (err) {
      lastError = err
      if (attempt < maxRetries && !err.message?.startsWith('HTTP ')) {
        const delay = Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 30000)
        await sleep(delay)
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

export async function run() {
  try {
    const authUrl = core.getInput('auth-url')
    const authSecret = core.getInput('auth-secret')
    const apiUrl = core.getInput('api-url')
    const privateKey = core.getInput('private-key')
    const sql = core.getInput('sql')
    const resource = core.getInput('resource')
    const extractOutputs = core.getInput('extract-outputs')

    // 1. Auth — GET proxy with shared secret
    core.info('Authenticating via SxT proxy...')
    const authResponse = await fetchWithRetry(authUrl, {
      method: 'GET',
      headers: { 'x-shared-secret': authSecret },
    })
    const authData = await authResponse.json()
    const token = authData.data || authData.accessToken || authData
    if (!token || typeof token !== 'string') {
      throw new Error(`Auth failed: unexpected response format: ${JSON.stringify(authData).substring(0, 200)}`)
    }
    core.info('Auth successful')

    // 2. Generate biscuit using SDK
    const operation = classifySql(sql)
    core.info(`Generating biscuit for ${operation} on ${resource}`)
    const sxt = new SpaceAndTime()
    const authorization = sxt.Authorization()
    const biscuitResult = authorization.CreateBiscuitToken(
      [{ operation, resource }],
      privateKey,
    )
    if (!biscuitResult.data || !biscuitResult.data[0]) {
      throw new Error(`Biscuit generation failed: ${JSON.stringify(biscuitResult)}`)
    }
    const biscuit = biscuitResult.data[0]
    core.info('Biscuit generated')

    // 3. Execute SQL
    core.info(`Executing: ${sql.substring(0, 100)}...`)
    const sqlResponse = await fetchWithRetry(`${apiUrl}/v1/sql`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sqlText: sql,
        biscuits: [biscuit],
      }),
    })
    const result = await sqlResponse.json()
    core.info(`Query returned ${Array.isArray(result) ? result.length : 0} rows`)

    // 4. Set outputs
    if (extractOutputs && Array.isArray(result) && result.length > 0) {
      const row = result[0] // Unwrap single-element array
      for (const field of extractOutputs.split(',').map((f) => f.trim())) {
        if (row[field] !== undefined) {
          core.setOutput(field, String(row[field]))
        }
      }
    }

    core.setOutput('response', JSON.stringify(result))
    core.setOutput('success', 'true')
  } catch (error) {
    core.setOutput('success', 'false')
    core.setOutput('response', '[]')
    core.error(error.message)
    core.setFailed(error.message)
  }
}
