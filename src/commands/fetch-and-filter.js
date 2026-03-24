import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId } from '../lib/queries.js'
import { authenticate, executeSql, withTimeout } from '../lib/sxt-client.js'
import { getHeader } from '../lib/email-utils.js'

// Import filter rules directly
import blockedDomains from '../../config/blocked-domains.json'
import blockedPrefixes from '../../config/blocked-prefixes.json'
import automatedSubjects from '../../config/automated-subjects.json'
import freeEmailPatterns from '../../config/free-email-patterns.json'
import nonPersonalizedNames from '../../config/non-personalized-names.json'
import marketingHeaders from '../../config/marketing-headers.json'

function extractEmailAddress(from) {
  const match = from.match(/<([^>]+)>/)
  return (match ? match[1] : from).trim().toLowerCase()
}

function extractDisplayName(from) {
  const match = from.match(/^(.+?)\s*</)
  return match ? match[1].trim().replace(/^["']|["']$/g, '').toLowerCase() : ''
}

function isRejected(email) {
  // Rule 1: Authentication results
  const authResults = getHeader(email, 'authentication-results')
  if (authResults) {
    const hasDkim = authResults.includes('dkim=pass')
    const hasSpf = authResults.includes('spf=pass')
    const hasDmarc = authResults.includes('dmarc=pass')
    if (!hasDkim && !hasSpf && !hasDmarc) return true
  }

  // Rule 2: Blocked sender
  const fromValue = getHeader(email, 'from')
  if (fromValue) {
    const emailAddr = extractEmailAddress(fromValue)
    for (const prefix of blockedPrefixes) {
      if (emailAddr.startsWith(prefix)) return true
    }
    const atIndex = emailAddr.indexOf('@')
    if (atIndex !== -1) {
      const domain = emailAddr.slice(atIndex + 1)
      for (const blockedDomain of blockedDomains) {
        if (domain.includes(blockedDomain)) return true
      }
    }
  }

  // Rule 3: Bulk headers
  const headers = email.topLevelHeaders || []
  for (const bulkHeader of marketingHeaders.headers) {
    if (headers.find((h) => h.name.toLowerCase() === bulkHeader.toLowerCase())) return true
  }
  for (const header of headers) {
    const value = (header.value || '').toLowerCase()
    for (const tool of marketingHeaders.tools) {
      if (value.includes(tool)) return true
    }
  }
  const precedence = getHeader(email, 'precedence').toLowerCase()
  for (const val of marketingHeaders.values) {
    if (precedence.includes(val)) return true
  }

  // Rule 4: Automated subject
  const subject = getHeader(email, 'subject').toLowerCase()
  if (subject) {
    for (const term of automatedSubjects) {
      if (subject.includes(term.toLowerCase())) return true
    }
  }

  // Rule 5: Non-personalized sender name
  if (fromValue) {
    const displayName = extractDisplayName(fromValue)
    if (displayName && nonPersonalizedNames.some((name) => displayName === name.toLowerCase())) {
      return true
    }
  }

  // Rule 6: Free email with non-personal prefix
  if (fromValue) {
    const emailAddr = extractEmailAddress(fromValue)
    for (const pattern of freeEmailPatterns) {
      if (new RegExp(pattern, 'i').test(emailAddr)) return true
    }
  }

  return false
}

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

  for (let i = 0; i < messageIds.length; i += MAX_PER_CHUNK) {
    const chunk = messageIds.slice(i, i + MAX_PER_CHUNK)
    try {
      const { signal, clear } = withTimeout()
      const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, syncStateId, messageIds: chunk }),
        signal,
      })
      clear()
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      const result = await resp.json()
      const emails = result.data || result

      for (const email of emails) {
        const meta = metadataRows.find((r) => r.MESSAGE_ID === email.messageId)
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
