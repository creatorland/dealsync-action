import * as core from '@actions/core'
import { encryptValue, tryDecrypt } from '../../shared/crypto.js'

// --- Inlined helpers from base64-decode (avoids dependency on unbuilt module) ---

/**
 * Decode a base64url-encoded string to UTF-8.
 */
function decodeBase64Url(str) {
  const base64 = str.replace(/-/g, '+').replace(/_/g, '/')
  return Buffer.from(base64, 'base64').toString('utf-8')
}

/**
 * Parse select-keys input: "FIELD:key1,key2;FIELD2:key3"
 * Returns { FIELD: ['key1', 'key2'], FIELD2: ['key3'] }
 */
function parseSelectKeys(selectKeys) {
  const result = {}
  if (!selectKeys) return result
  for (const part of selectKeys.split(';')) {
    const colonIdx = part.indexOf(':')
    if (colonIdx === -1) continue
    const field = part.slice(0, colonIdx).trim()
    const keys = part
      .slice(colonIdx + 1)
      .split(',')
      .map((k) => k.trim())
    result[field] = keys
  }
  return result
}

/**
 * Extract specific keys from a JSON array of {name, value} objects.
 * Returns a flat object: { key1: val1, key2: val2 }
 */
function extractKeys(jsonString, keys) {
  const arr = JSON.parse(jsonString)
  const selected = arr.filter((item) => keys.includes(item.name))
  return Object.fromEntries(selected.map((item) => [item.name, item.value]))
}

// --- Retry helpers ---

function calculateDelay(attempt, baseDelay, backoff) {
  const jitter = Math.random() * baseDelay
  if (backoff === 'exponential') {
    return Math.min(baseDelay * Math.pow(2, attempt) + jitter, 30000)
  }
  return Math.min(baseDelay * (attempt + 1) + jitter, 30000)
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function fetchWithRetry(url, options, config) {
  const { maxRetries, baseDelay, retryStatuses, backoff } = config
  let lastError
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, options)
      core.setOutput('success', response.ok ? 'true' : 'false')
      core.setOutput('status-code', response.status.toString())
      if (!response.ok) {
        if (attempt < maxRetries && retryStatuses.includes(response.status)) {
          const retryAfter = response.headers.get('retry-after')
          const delay = retryAfter
            ? parseInt(retryAfter) * 1000
            : calculateDelay(attempt, baseDelay, backoff)
          core.info(
            `Retry ${attempt + 1}/${maxRetries} after ${delay}ms (status ${response.status})`
          )
          await sleep(delay)
          continue
        }
        const body = await response.text()
        throw new Error(`HTTP ${response.status}: ${body}`)
      }
      return response
    } catch (err) {
      lastError = err
      if (attempt < maxRetries && !err.message?.startsWith('HTTP ')) {
        core.info(
          `Retry ${attempt + 1}/${maxRetries} after network error: ${err.message}`
        )
        await sleep(calculateDelay(attempt, baseDelay, backoff))
        continue
      }
      throw err
    }
  }
  throw lastError
}

// --- Path-based decryption ---

function getNestedValue(obj, path) {
  const parts = path.split('.')
  let current = obj
  for (const part of parts) {
    if (current === undefined || current === null) return undefined
    current = current[part]
  }
  return current
}

function decryptAtPath(obj, pathParts, encryptionKey) {
  let current = obj
  for (let i = 0; i < pathParts.length - 1; i++) {
    if (current[pathParts[i]] === undefined) return obj
    current = current[pathParts[i]]
  }
  const lastKey = pathParts[pathParts.length - 1]
  const value = current[lastKey]
  if (typeof value !== 'string') return obj

  // Handle Bearer prefix
  if (value.startsWith('Bearer ')) {
    const token = value.slice(7)
    const decrypted = tryDecrypt(token, encryptionKey)
    if (decrypted !== null) {
      current[lastKey] = `Bearer ${decrypted}`
    }
    return obj
  }

  const decrypted = tryDecrypt(value, encryptionKey)
  if (decrypted !== null) {
    current[lastKey] = decrypted
  }
  return obj
}

// --- Main action ---

export async function run() {
  try {
    const encryptionKey = core.getInput('encryption-key')

    const url = core.getInput('url', { required: true })
    const method = core.getInput('method') || 'GET'
    const headersRaw = core.getInput('headers') || '{}'
    const bodyRaw = core.getInput('body')

    // Retry configuration
    const maxRetries = parseInt(core.getInput('max-retries') || '3')
    const baseDelay = parseInt(core.getInput('retry-delay-ms') || '1000')
    const retryStatuses = (core.getInput('retry-on-status') || '429,500,502,503,504')
      .split(',')
      .map((s) => parseInt(s.trim()))
    const backoff = core.getInput('retry-backoff') || 'exponential'

    // Parse mutable copies
    const inputStore = {
      headers: JSON.parse(headersRaw),
    }

    // Try parsing body as JSON for path-based decryption
    let bodyIsJson = false
    if (bodyRaw) {
      try {
        inputStore.body = JSON.parse(bodyRaw)
        bodyIsJson = true
      } catch {
        inputStore.body = bodyRaw
      }
    }

    // decrypt-inputs: path-based decryption (comma-separated)
    const decryptInputs = core.getInput('decrypt-inputs')
    if (decryptInputs && encryptionKey) {
      for (const path of decryptInputs.split(',').map((p) => p.trim())) {
        const parts = path.split('.')
        const inputName = parts[0]
        const fieldParts = parts.slice(1)
        if (inputStore[inputName] === undefined) continue
        if (fieldParts.length > 0) {
          decryptAtPath(inputStore[inputName], fieldParts, encryptionKey)
        } else if (typeof inputStore[inputName] === 'string') {
          const decrypted = tryDecrypt(inputStore[inputName], encryptionKey)
          if (decrypted !== null) {
            try {
              inputStore[inputName] = JSON.parse(decrypted)
              if (inputName === 'body') bodyIsJson = true
            } catch {
              inputStore[inputName] = decrypted
            }
          }
        }
      }
    }

    const headers = inputStore.headers || {}
    const body = bodyIsJson ? JSON.stringify(inputStore.body) : inputStore.body

    // Auto-set Content-Type for JSON bodies if not already set
    if (body && method !== 'GET' && method !== 'HEAD') {
      const hasContentType = Object.keys(headers).some(
        (k) => k.toLowerCase() === 'content-type',
      )
      if (!hasContentType) {
        try {
          JSON.parse(typeof body === 'string' ? body : JSON.stringify(body))
          headers['Content-Type'] = 'application/json'
        } catch {
          // Not JSON, let fetch use default
        }
      }
    }

    core.info(`${method} ${url}`)

    const options = { method, headers }
    if (body && method !== 'GET' && method !== 'HEAD') {
      options.body = body
    }

    const response = await fetchWithRetry(url, options, {
      maxRetries,
      baseDelay,
      retryStatuses,
      backoff,
    })
    const responseBody = await response.text()

    const responseBytes = Buffer.byteLength(responseBody, 'utf-8')
    core.info(
      `Response: ${response.status} | Size: ${responseBytes} bytes (${(responseBytes / 1024).toFixed(1)} KB)`
    )

    const extractOutputs = core.getInput('extract-outputs')
    const encryptOutputsList = core.getInput('encrypt-outputs')
    const fieldsToEncrypt = encryptOutputsList
      ? encryptOutputsList.split(',').map((f) => f.trim())
      : []

    function setOutput(name, value) {
      if (fieldsToEncrypt.includes(name) && encryptionKey) {
        core.setOutput(name, encryptValue(value, encryptionKey))
      } else {
        core.setOutput(name, value)
      }
    }

    if (extractOutputs) {
      // SxT array unwrapping: when response is a single-element array, unwrap it
      let parsed = JSON.parse(responseBody)
      if (Array.isArray(parsed) && parsed.length === 1) {
        parsed = parsed[0]
      }

      for (const fieldPath of extractOutputs.split(',').map((f) => f.trim())) {
        const value = getNestedValue(parsed, fieldPath)
        if (value !== undefined) {
          const leafName = fieldPath.split('.').pop()
          const strValue =
            typeof value === 'string' ? value : JSON.stringify(value)
          setOutput(leafName, strValue)
        }
      }
    }

    if (!extractOutputs) {
      // Optional post-processing for JSON array responses
      const decodeFields = core.getInput('decode-fields')
      const outputFields = core.getInput('output-fields')
      const selectKeys = core.getInput('select-keys')
      const limit = parseInt(core.getInput('limit') || '0', 10)

      let processedOutput = responseBody

      if (decodeFields || outputFields || selectKeys || limit > 0) {
        let rows = JSON.parse(responseBody)

        if (limit > 0 && rows.length > limit) {
          core.info(`Limiting from ${rows.length} to ${limit} rows`)
          rows = rows.slice(0, limit)
        }

        const fieldsToDecode = decodeFields
          ? decodeFields.split(',').map((f) => f.trim())
          : []
        const keySelections = parseSelectKeys(selectKeys)
        const fieldsToOutput = outputFields
          ? outputFields.split(',').map((f) => f.trim())
          : null

        rows = rows.map((row) => {
          let result = { ...row }

          for (const field of fieldsToDecode) {
            if (result[field]) {
              result[field] = decodeBase64Url(result[field])
            }
          }

          for (const [field, keys] of Object.entries(keySelections)) {
            if (result[field]) {
              result[field] = extractKeys(result[field], keys)
            }
          }

          if (fieldsToOutput) {
            const filtered = {}
            for (const f of fieldsToOutput) {
              if (f in result) filtered[f] = result[f]
            }
            result = filtered
          }

          return result
        })

        processedOutput = JSON.stringify(rows)
        const outputBytes = Buffer.byteLength(processedOutput, 'utf-8')
        core.info(
          `Processed: ${outputBytes} bytes (${(outputBytes / 1024).toFixed(1)} KB) | Rows: ${rows.length}`
        )
      }

      setOutput('response', processedOutput)
    }
  } catch (error) {
    core.error(error.stack || error.toString())
    core.setFailed(error.message)
  }
}
