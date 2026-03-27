import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId } from '../lib/queries.js'
import { authenticate, executeSql, withTimeout } from '../lib/sxt-client.js'
import { isRejected } from '../lib/filter-rules.js'

/**
 * Combined fetch-headers + filter command.
 * Fetches email headers (metadata only, no body) from content fetcher,
 * applies filter rules, and returns filtered/rejected ID lists.
 */
export async function runFetchAndFilter() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))
  const chunkSize = parseInt(core.getInput('chunk-size') || '50', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '30000', 10)

  if (!batchId) throw new Error('batch-id is required')

  console.log(
    `[fetch-and-filter] starting for batch ${batchId} (chunk=${chunkSize}, timeout=${fetchTimeoutMs}ms)`,
  )

  // 1. Authenticate + fetch metadata from SxT
  const jwt = await authenticate(authUrl, authSecret)
  const metadataRows = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[fetch-and-filter] no rows found for batch')
    return { filtered_ids: '', rejected_ids: '', total: 0 }
  }

  console.log(`[fetch-and-filter] found ${metadataRows.length} deal_states`)

  // 2. Fetch headers only from content fetcher (format=metadata, no body)
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const userId = metadataRows[0].USER_ID
  const syncStateId = metadataRows[0].SYNC_STATE_ID
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)

  const allEmails = []
  const metaByMessageId = new Map(metadataRows.map((r) => [r.MESSAGE_ID, r]))

  const MAX_RETRIES = 3
  for (let i = 0; i < messageIds.length; i += chunkSize) {
    const chunk = messageIds.slice(i, i + chunkSize)
    const chunkNum = Math.floor(i / chunkSize) + 1
    let fetched = false

    for (let attempt = 0; attempt < MAX_RETRIES && !fetched; attempt++) {
      try {
        const { signal, clear } = withTimeout(fetchTimeoutMs)
        const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            userId,
            ...(syncStateId ? { syncStateId } : {}),
            messageIds: chunk,
            format: 'metadata',
          }),
          signal,
        })
        clear()
        if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
        const result = await resp.json()
        const emails = result.data || result

        for (const email of emails) {
          const meta = metaByMessageId.get(email.messageId)
          if (meta) {
            email.id = meta.EMAIL_METADATA_ID
          }
          allEmails.push(email)
        }
        fetched = true
      } catch (err) {
        if (attempt < MAX_RETRIES - 1) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 10000)
          console.log(
            `[fetch-and-filter] chunk ${chunkNum} failed (attempt ${attempt + 1}/${MAX_RETRIES}): ${err.message}, retrying in ${delay}ms`,
          )
          await new Promise((r) => setTimeout(r, delay))
        } else {
          console.log(
            `[fetch-and-filter] chunk ${chunkNum} failed after ${MAX_RETRIES} attempts: ${err.message}`,
          )
        }
      }
    }
  }

  if (allEmails.length === 0 && metadataRows.length > 0) {
    throw new Error(`all content fetches failed — 0/${metadataRows.length} emails retrieved`)
  }

  console.log(`[fetch-and-filter] fetched ${allEmails.length} emails, applying filter rules`)

  // 3. Apply filter rules
  const filteredIds = []
  const rejectedIds = []

  for (const email of allEmails) {
    if (isRejected(email)) {
      rejectedIds.push(email.id)
    } else {
      filteredIds.push(email.id)
    }
  }

  console.log(
    `[fetch-and-filter] result: ${filteredIds.length} passed, ${rejectedIds.length} rejected`,
  )

  return {
    filtered_ids: filteredIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    rejected_ids: rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    total: allEmails.length,
  }
}
