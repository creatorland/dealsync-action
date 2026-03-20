import * as core from '@actions/core'
import { encryptValue, tryDecrypt } from '../../shared/crypto.js'
import { HttpClient } from './client.js'

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

export async function run() {
  try {
    const encrypt = core.getInput('encrypt') !== 'false'
    const encryptionKey = encrypt ? core.getInput('encryption-key') : null

    const url = core.getInput('url', { required: true })
    const method = core.getInput('method') || 'GET'
    const headersRaw = core.getInput('headers') || '{}'
    const bodyRaw = core.getInput('body')

    const maxRetries = parseInt(core.getInput('max-retries') || '3')
    const baseDelay = parseInt(core.getInput('retry-delay-ms') || '1000')
    const retryStatuses = (core.getInput('retry-on-status') || '429,500,502,503,504')
      .split(',')
      .map((s) => parseInt(s.trim()))

    // Parse mutable copies
    const inputStore = { headers: JSON.parse(headersRaw) }
    let bodyIsJson = false
    if (bodyRaw) {
      try {
        inputStore.body = JSON.parse(bodyRaw)
        bodyIsJson = true
      } catch {
        inputStore.body = bodyRaw
      }
    }

    // Decrypt inputs at specified paths
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

    core.info(`${method} ${url}`)

    const client = new HttpClient({
      maxRetries,
      baseDelay,
      retryStatuses,
      log: (msg) => core.info(msg),
    })

    const response = await client.request(url, { method, headers, body })
    const responseBody = await response.text()

    core.setOutput('success', 'true')
    core.setOutput('status-code', response.status.toString())

    const responseBytes = Buffer.byteLength(responseBody, 'utf-8')
    core.info(
      `Response: ${response.status} | Size: ${responseBytes} bytes (${(responseBytes / 1024).toFixed(1)} KB)`,
    )

    // Output encryption setup
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

    // Extract specific fields from JSON response
    const extractOutputs = core.getInput('extract-outputs')
    if (extractOutputs) {
      let parsed = JSON.parse(responseBody)
      // SxT array unwrapping: single-element array → object
      if (Array.isArray(parsed) && parsed.length === 1) {
        parsed = parsed[0]
      }
      for (const fieldPath of extractOutputs.split(',').map((f) => f.trim())) {
        const value = getNestedValue(parsed, fieldPath)
        if (value !== undefined) {
          const leafName = fieldPath.split('.').pop()
          const strValue = typeof value === 'string' ? value : JSON.stringify(value)
          setOutput(leafName, strValue)
        }
      }
    } else {
      setOutput('response', responseBody)
    }
  } catch (error) {
    if (error.status) {
      core.setOutput('success', 'false')
      core.setOutput('status-code', error.status.toString())
    }
    core.error(error.stack || error.toString())
    core.setFailed(error.message)
  }
}
