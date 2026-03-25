import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId } from '../lib/queries.js'
import { authenticate, executeSql, withTimeout } from '../lib/sxt-client.js'
import { isRejected } from '../lib/filter-rules.js'

/**
 * Combined fetch-headers + filter command.
 * Fetches email headers from content fetcher, applies filter rules,
 * and returns filtered/rejected ID lists ready for SxT updates.
 *
 * No large data passes between steps — only small ID lists in output.
 */
export async function runFetchAndFilter() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))

  if (!batchId) throw new Error('batch-id is required')

  console.log(`[fetch-and-filter] starting for batch ${batchId}`)

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

  // 2. Fetch headers from content fetcher
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const userId = metadataRows[0].USER_ID
  const syncStateId = metadataRows[0].SYNC_STATE_ID
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)

  const MAX_PER_CHUNK = 10
  const allEmails = []
  const metaByMessageId = new Map(metadataRows.map((r) => [r.MESSAGE_ID, r]))

  for (let i = 0; i < messageIds.length; i += MAX_PER_CHUNK) {
    const chunk = messageIds.slice(i, i + MAX_PER_CHUNK)
    try {
      const { signal, clear } = withTimeout()
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, ...(syncStateId ? { syncStateId } : {}), messageIds: chunk }),
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
          // Only keep header fields
          delete email.body
          delete email.replyBody
          delete email.attachments
        }
        allEmails.push(email)
      }
    } catch (err) {
      console.log(`[fetch-and-filter] content fetch failed for chunk: ${err.message}`)
    }
  }

  if (allEmails.length === 0 && metadataRows.length > 0) {
    throw new Error(`all content fetches failed — 0/${metadataRows.length} emails retrieved`)
  }

  console.log(`[fetch-and-filter] fetched ${allEmails.length} emails, applying filter rules`)

  // 4. Apply filter rules
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
