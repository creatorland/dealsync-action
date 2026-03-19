import * as core from '@actions/core'
import { readFileSync } from 'fs'
import { createEncryptedOutput } from '../../shared/crypto.js'

/**
 * Decode a base64url-encoded string to UTF-8.
 */
export function decodeBase64Url(str) {
  const base64 = str.replace(/-/g, '+').replace(/_/g, '/')
  return Buffer.from(base64, 'base64').toString('utf-8')
}

/**
 * Parse select-keys input: "FIELD:key1,key2;FIELD2:key3"
 * Returns { FIELD: ['key1', 'key2'], FIELD2: ['key3'] }
 */
export function parseSelectKeys(selectKeys) {
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
export function extractKeys(jsonString, keys) {
  const arr = JSON.parse(jsonString)
  const selected = arr.filter((item) => keys.includes(item.name))
  return Object.fromEntries(selected.map((item) => [item.name, item.value]))
}

export async function run() {
  try {
    const encryptionKey = core.getInput('encryption-key')

    const setEncryptedOutput = createEncryptedOutput(core, encryptionKey)

    const inputFile = core.getInput('input-file')
    const input = core.getInput('input')
    const fields = core
      .getInput('fields', { required: true })
      .split(',')
      .map((f) => f.trim())
    const limit = parseInt(core.getInput('limit') || '0', 10)
    const outputFields = core.getInput('output-fields')
    const selectKeys = core.getInput('select-keys')

    const keySelections = parseSelectKeys(selectKeys)
    const fieldsToOutput = outputFields
      ? outputFields.split(',').map((f) => f.trim())
      : null

    let rawData
    if (input) {
      rawData = input
    } else if (inputFile) {
      core.info(`Reading ${inputFile}`)
      rawData = readFileSync(inputFile, 'utf-8')
    } else {
      throw new Error('Either input or input-file must be provided')
    }

    const rows = JSON.parse(rawData)

    const limitedRows = limit > 0 ? rows.slice(0, limit) : rows
    if (limit > 0 && rows.length > limit) {
      core.info(`Limiting from ${rows.length} to ${limit} rows`)
    }

    const decoded = limitedRows.map((row) => {
      let result = { ...row }

      for (const field of fields) {
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

    const resultJson = JSON.stringify(decoded)
    const inputBytes = Buffer.byteLength(rawData, 'utf-8')
    const resultBytes = Buffer.byteLength(resultJson, 'utf-8')
    core.info(
      `Input: ${inputBytes} bytes (${(inputBytes / 1024).toFixed(1)} KB) | Result: ${resultBytes} bytes (${(resultBytes / 1024).toFixed(1)} KB) | Rows: ${decoded.length}`
    )

    setEncryptedOutput('result', resultJson)
    core.setOutput('encrypted', encryptionKey ? 'true' : 'false')
  } catch (error) {
    core.error(error.stack || error.toString())
    core.setFailed(error.message)
  }
}
