import * as core from '@actions/core'
import { encryptValue, tryDecrypt } from '../../shared/crypto.js'

const MAX_MESSAGE_IDS = 50

/**
 * Fetch email content from the content fetcher service.
 * Handles chunking (max 50 messageIds per call) and result merging.
 *
 * Input: metadata (JSON array from SxT), content-fetcher-url, encryption-key
 * Output: array of email content objects (optionally encrypted)
 */
export async function runFetchContent() {
  const encrypt = core.getInput('encrypt') !== 'false'
  const encryptionKey = encrypt ? core.getInput('encryption-key') : null
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const fieldsInput = core.getInput('fields') // comma-separated field names, e.g. "messageId,labelIds,topLevelHeaders"

  let metadataRaw = core.getInput('metadata')
  if (!metadataRaw || metadataRaw === '[]') {
    return '[]'
  }

  // Decrypt metadata if encrypted
  if (encryptionKey) {
    const decrypted = tryDecrypt(metadataRaw, encryptionKey)
    if (decrypted !== null) metadataRaw = decrypted
  }

  const rows = JSON.parse(metadataRaw)
  if (rows.length === 0) {
    return '[]'
  }

  const userId = rows[0].USER_ID
  const syncStateId = rows[0].SYNC_STATE_ID
  const allMessageIds = rows.map((r) => r.MESSAGE_ID)

  // Chunk messageIds into batches of 50
  const chunks = []
  for (let i = 0; i < allMessageIds.length; i += MAX_MESSAGE_IDS) {
    chunks.push(allMessageIds.slice(i, i + MAX_MESSAGE_IDS))
  }

  console.log(`[fetch-content] ${allMessageIds.length} messages in ${chunks.length} batch(es), url=${contentFetcherUrl}`)
  core.info(`Fetching content: ${allMessageIds.length} messages in ${chunks.length} batch(es)`)

  const allEmails = []
  const errors = []

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i]
    try {
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, syncStateId, messageIds: chunk }),
      })

      if (!resp.ok) {
        const body = await resp.text()
        throw new Error(`HTTP ${resp.status}: ${body}`)
      }

      const result = await resp.json()
      const emails = result.data || result

      // If fields specified, strip unnecessary data (e.g. body, replyBody, attachments)
      const fields = fieldsInput ? fieldsInput.split(',').map((f) => f.trim()) : null

      // Merge metadata (threadId, emailMetadataId, previousAiSummary) into each email
      for (const email of emails) {
        if (fields) {
          for (const key of Object.keys(email)) {
            if (!fields.includes(key) && key !== 'messageId') delete email[key]
          }
        }
        const meta = rows.find((r) => r.MESSAGE_ID === email.messageId)
        if (meta) {
          email.id = meta.EMAIL_METADATA_ID
          email.threadId = meta.THREAD_ID
          if (meta.PREVIOUS_AI_SUMMARY) {
            email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
          }
          if (meta.EXISTING_DEAL_ID) {
            email.existingDealId = meta.EXISTING_DEAL_ID
          }
        }
        allEmails.push(email)
      }

      console.log(`[fetch-content] batch ${i + 1}/${chunks.length}: fetched ${emails.length} emails`)
    } catch (err) {
      console.log(`[fetch-content] batch ${i + 1}/${chunks.length} FAILED: ${err.message}`)
      core.error(`Batch ${i + 1}/${chunks.length} failed: ${err.message}`)
      // Track failed messageIds for the batch
      for (const msgId of chunk) {
        const meta = rows.find((r) => r.MESSAGE_ID === msgId)
        if (meta) errors.push(meta.EMAIL_METADATA_ID)
      }
    }
  }

  core.info(`Total: ${allEmails.length} fetched, ${errors.length} failed`)

  const emailsJson = JSON.stringify(allEmails)

  // Output emails directly — no wrapper object
  // Downstream commands (filter, build-prompt, classify) expect the emails array directly
  if (errors.length > 0) {
    core.setOutput('failed_ids', errors.map((id) => `'${id}'`).join(','))
  }

  return encryptionKey ? encryptValue(emailsJson, encryptionKey) : emailsJson
}
