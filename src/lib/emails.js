/**
 * Email utilities — sanitization, header helpers, filter rules, and content fetching.
 *
 * Combines email-sanitizer, filter-rules, and email-client into a single module
 * since they form a tight unit operating on email data.
 */

import * as core from '@actions/core'
import { convert } from 'html-to-text'
import EmailReplyParser from 'email-reply-parser'
import { withTimeout } from './db.js'
import { sleep, backoffMs } from './retry.js'
import blockedDomains from '../../config/blocked-domains.json'
import blockedPrefixes from '../../config/blocked-prefixes.json'
import automatedSubjects from '../../config/automated-subjects.json'
import freeEmailPatterns from '../../config/free-email-patterns.json'
import nonPersonalizedNames from '../../config/non-personalized-names.json'
import marketingHeaders from '../../config/marketing-headers.json'

// ---------------------------------------------------------------------------
// Header utilities & body sanitization
// ---------------------------------------------------------------------------

export function getHeader(email, name) {
  const header = email.topLevelHeaders?.find((h) => h.name.toLowerCase() === name.toLowerCase())
  return header?.value || ''
}

const MAX_BODY_CHARS = 3000 // Per email — keeps token usage reasonable for batches of 5

/**
 * Sanitize an email body for AI classification.
 * @param {string} body - Raw email body (may be HTML or plaintext)
 * @returns {string} Cleaned plaintext suitable for AI prompt
 */
export function sanitizeEmailBody(body) {
  if (!body || typeof body !== 'string') return ''

  // Step 1: HTML to plaintext
  let text = body
  if (text.includes('<') && (text.includes('</') || text.includes('/>'))) {
    text = convert(text, {
      wordwrap: false,
      selectors: [
        { selector: 'a', options: { hideLinkHrefIfSameAsText: true } },
        { selector: 'img', format: 'skip' },
        { selector: 'style', format: 'skip' },
        { selector: 'script', format: 'skip' },
      ],
    })
  }

  // Step 2: Strip quoted replies and signatures using email-reply-parser
  try {
    const parsed = new EmailReplyParser().read(text)
    // Get only visible (non-quoted, non-signature) fragments
    const visible = parsed.getVisibleText({ aggressive: true })
    if (visible && visible.trim().length > 0) {
      text = visible
    }
  } catch {
    // Parser failed — use original text
  }

  // Step 3: Collapse whitespace
  text = text
    .replace(/\r\n/g, '\n') // Normalize line endings
    .replace(/\n{3,}/g, '\n\n') // Max 2 consecutive newlines
    .replace(/[ \t]{2,}/g, ' ') // Collapse horizontal whitespace
    .replace(/^\s+$/gm, '') // Remove whitespace-only lines
    .trim()

  // Step 4: Truncate
  if (text.length > MAX_BODY_CHARS) {
    text = text.substring(0, MAX_BODY_CHARS) + '\n[... truncated]'
  }

  return text
}

// ---------------------------------------------------------------------------
// Filter rules
// ---------------------------------------------------------------------------

function extractEmailAddress(from) {
  if (!from) return ''
  const match = from.match(/<([^>]+)>/)
  return (match ? match[1] : from).trim().toLowerCase()
}

function extractDisplayName(from) {
  if (!from) return ''
  const match = from.match(/^(.+?)\s*</)
  return match ? match[1].trim().replace(/^["']|["']$/g, '') : ''
}

/**
 * Check whether a sender address matches blocked prefixes / domains.
 * Mirrors filter rule 2 so main_contact fallback does not derive from
 * automated senders, bounce addresses, or marketing-only domains.
 */
export function isBlockedSenderAddress(addr) {
  if (!addr) return true
  const lower = addr.trim().toLowerCase()
  if (!lower) return true
  for (const prefix of blockedPrefixes) {
    if (lower.startsWith(prefix)) return true
  }
  const atIndex = lower.indexOf('@')
  if (atIndex !== -1) {
    const domain = lower.slice(atIndex + 1)
    for (const blockedDomain of blockedDomains) {
      if (domain.includes(blockedDomain)) return true
    }
  }
  return false
}

/**
 * Derive a fallback main_contact from a thread's messages when the AI
 * did not return one. Picks the latest external sender (not the creator,
 * not blocked) and returns it in the same shape as AI's main_contact.
 *
 * @param {object[]} threadEmails - fetched email objects for a single thread
 * @param {string} creatorEmail - creator's email (excluded from candidates)
 * @returns {{email: string, name: string|null, company: null, title: null, phone_number: null}|null}
 */
export function deriveFallbackMainContact(threadEmails, creatorEmail) {
  if (!threadEmails || threadEmails.length === 0) return null
  const creatorLower = (creatorEmail || '').trim().toLowerCase()

  const dated = threadEmails.map((email) => ({
    email,
    t: new Date(getHeader(email, 'date') || 0).getTime() || 0,
  }))
  dated.sort((a, b) => b.t - a.t)

  for (const { email } of dated) {
    const fromHeader = getHeader(email, 'from')
    if (!fromHeader) continue
    const addr = extractEmailAddress(fromHeader)
    if (!addr) continue
    if (creatorLower && addr === creatorLower) continue
    if (isBlockedSenderAddress(addr)) continue
    const displayName = extractDisplayName(fromHeader)
    return {
      email: addr,
      name: displayName || null,
      company: null,
      title: null,
      phone_number: null,
    }
  }
  return null
}

export function isRejected(email) {
  // Rule 1: Authentication results
  const authResults = getHeader(email, 'authentication-results')
  if (authResults) {
    const hasDkim = authResults.includes('dkim=pass')
    const hasSpf = authResults.includes('spf=pass')
    const hasDmarc = authResults.includes('dmarc=pass')
    if (!hasDkim && !hasSpf && !hasDmarc) return true
  }

  // Rule 2: Blocked sender (single source of truth with isBlockedSenderAddress)
  const fromValue = getHeader(email, 'from')
  if (fromValue) {
    const emailAddr = extractEmailAddress(fromValue)
    if (emailAddr && isBlockedSenderAddress(emailAddr)) return true
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
    const displayName = extractDisplayName(fromValue).toLowerCase()
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

// ---------------------------------------------------------------------------
// Email content fetcher
// ---------------------------------------------------------------------------

const DEFAULT_MAX_RETRIES = parseInt(core.getInput('email-max-retries') || '3', 10)

/**
 * Enrich fetched emails with metadata and collect them into the output array.
 * @param {object[]} emails - raw email objects from the content fetcher
 * @param {object[]} allEmails - accumulator array to push enriched emails into
 * @param {Map} metaByMessageId - metadata lookup map
 */
function enrichAndCollect(emails, allEmails, metaByMessageId) {
  for (const email of emails) {
    const meta = metaByMessageId.get(email.messageId)
    if (meta) {
      email.id = meta.EMAIL_METADATA_ID
      email.threadId = meta.THREAD_ID
      if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
    }
    allEmails.push(email)
  }
}

/**
 * Fetch emails from the email-service API using compat format.
 * Returns the same shape as the content-fetcher for pipeline compatibility.
 */
/**
 * Fetch emails from the email-service API using compat format.
 * Uses the same chunked retry logic as content-fetcher since the
 * email-service now returns matching HTTP status codes (200/207/502).
 */
async function fetchEmailsFromService(messageIds, metaByMessageId, opts) {
  const { emailServiceUrl, userId, fetchTimeoutMs, chunkSize, maxRetries = DEFAULT_MAX_RETRIES, format } = opts

  if (!messageIds || messageIds.length === 0) return []

  const allEmails = []
  const fetchStart = Date.now()
  let totalChunks = 0

  for (let i = 0; i < messageIds.length; i += chunkSize) {
    const chunk = messageIds.slice(i, i + chunkSize)
    const chunkIndex = Math.floor(i / chunkSize) + 1
    totalChunks++
    let pendingIds = [...chunk]

    for (let attempt = 0; attempt < maxRetries && pendingIds.length > 0; attempt++) {
      try {
        const { signal, clear } = withTimeout(fetchTimeoutMs)
        try {
          // Group message IDs by user_id from metadata (batch can have mixed users)
          const byUser = {}
          for (const id of pendingIds) {
            const meta = metaByMessageId.get(id)
            const uid = meta?.USER_ID || userId
            if (!byUser[uid]) byUser[uid] = []
            byUser[uid].push(id)
          }
          const requests = Object.entries(byUser).map(([user_id, ids]) => ({ user_id, ids }))

          if (attempt === 0) {
            console.log(
              `[email-service] chunk ${chunkIndex}: ${pendingIds.length} msgs across ${requests.length} users`,
            )
          }

          const resp = await fetch(`${emailServiceUrl}/v1/emails/messages`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              requests,
              format: 'compat',
            }),
            signal,
          })
          clear()

          // 502 = total failure
          if (resp.status === 502) {
            let failedIds = pendingIds
            try {
              const errBody = await resp.json()
              if (errBody.errors && Array.isArray(errBody.errors)) {
                failedIds = errBody.errors.map((e) => e.messageId).filter(Boolean)
                if (failedIds.length === 0) failedIds = pendingIds
              }
            } catch {}
            pendingIds = failedIds
            console.log(
              `[email-service] 502 — ${pendingIds.length} messageIds to retry ` +
                `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
            )
            if (attempt < maxRetries - 1) {
              await sleep(backoffMs(attempt, { base: 1000 }))
            }
            continue
          }

          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
          }

          const result = await resp.json()

          // 207 = partial success
          if (resp.status === 207) {
            const successEmails = result.data || []
            const errors = result.errors || []
            enrichAndCollect(successEmails, allEmails, metaByMessageId)
            const failedIds = errors.map((e) => e.messageId).filter(Boolean)
            if (failedIds.length > 0) {
              pendingIds = failedIds
              console.log(
                `[email-service] 207 partial — ${successEmails.length} ok, ${failedIds.length} failed ` +
                  `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
              )
              if (attempt < maxRetries - 1) {
                await sleep(backoffMs(attempt, { base: 1000 }))
              }
              continue
            }
            pendingIds = []
            continue
          }

          // 200 = all success
          const emails = result.data || result
          enrichAndCollect(emails, allEmails, metaByMessageId)
          pendingIds = []
        } catch (err) {
          clear()
          throw err
        }
      } catch (err) {
        console.log(
          `[email-service] chunk ${chunkIndex} fetch failed ` +
            `(attempt ${attempt + 1}/${maxRetries}): ${err.message}`,
        )
        if (attempt < maxRetries - 1) {
          await sleep(backoffMs(attempt, { base: 1000 }))
        }
      }
    }
  }

  if (allEmails.length === 0 && messageIds.length > 0) {
    throw new Error(`All email-service fetches failed — 0/${messageIds.length} emails retrieved`)
  }

  const fetchTotalMs = Date.now() - fetchStart
  console.log(
    `[email-service] fetch done: ${allEmails.length}/${messageIds.length} emails, ${totalChunks} chunks, ${fetchTotalMs}ms`,
  )

  return allEmails
}

/**
 * Fetch email content, enriching each email with metadata from the provided map.
 * Supports two providers via opts.emailProvider:
 *   - 'content-fetcher' (default): calls content-fetcher service
 *   - 'email-service': calls email-service API with compat format
 *
 * @param {string[]} messageIds - message IDs to fetch
 * @param {Map} metaByMessageId - Map<messageId, { EMAIL_METADATA_ID, THREAD_ID, PREVIOUS_AI_SUMMARY? }>
 * @param {object} opts
 * @param {string} opts.contentFetcherUrl - base URL for content fetcher
 * @param {string} [opts.emailServiceUrl] - base URL for email-service
 * @param {string} [opts.emailProvider='content-fetcher'] - which provider to use
 * @param {string} opts.userId - user ID for the request
 * @param {string} [opts.syncStateId] - optional sync state ID
 * @param {number} opts.chunkSize - messages per request
 * @param {number} opts.fetchTimeoutMs - timeout per request
 * @param {number} [opts.maxRetries=3] - retries per chunk
 * @param {string} [opts.format] - 'metadata' (headers only) or undefined (full content)
 * @returns {Promise<object[]>} enriched email objects
 */
export async function fetchEmails(messageIds, metaByMessageId, opts) {
  if (opts.emailProvider === 'email-service') {
    return fetchEmailsFromService(messageIds, metaByMessageId, opts)
  }
  const {
    contentFetcherUrl,
    userId,
    syncStateId,
    chunkSize,
    fetchTimeoutMs,
    maxRetries = DEFAULT_MAX_RETRIES,
    format,
  } = opts

  if (!messageIds || messageIds.length === 0) {
    return []
  }

  const allEmails = []

  const fetchStart = Date.now()
  let totalChunks = 0

  for (let i = 0; i < messageIds.length; i += chunkSize) {
    const chunk = messageIds.slice(i, i + chunkSize)
    const chunkIndex = Math.floor(i / chunkSize) + 1
    totalChunks++
    let pendingIds = [...chunk]

    for (let attempt = 0; attempt < maxRetries && pendingIds.length > 0; attempt++) {
      try {
        const { signal, clear } = withTimeout(fetchTimeoutMs)
        try {
          const body = {
            userId,
            ...(syncStateId ? { syncStateId } : {}),
            messageIds: pendingIds,
            ...(format ? { format } : {}),
          }

          const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal,
          })
          clear()

          // Handle 429 rate limiting
          if (resp.status === 429) {
            const retryBody = await resp.json().catch(() => ({}))
            const retryAfterMs = retryBody.retryAfterMs || backoffMs(attempt, { base: 1000 })
            console.log(
              `[email-client] 429 rate limited, waiting ${retryAfterMs}ms ` +
                `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
            )
            await sleep(retryAfterMs)
            continue
          }

          // Handle 502 — try to parse per-messageId errors
          if (resp.status === 502) {
            let failedIds = pendingIds // default: retry all
            try {
              const errBody = await resp.json()
              if (errBody.errors && Array.isArray(errBody.errors)) {
                failedIds = errBody.errors.map((e) => e.messageId).filter(Boolean)
                if (failedIds.length === 0) failedIds = pendingIds
              }
            } catch {
              // Non-JSON body — retry all pendingIds
            }
            pendingIds = failedIds
            console.log(
              `[email-client] 502 — ${pendingIds.length} messageIds to retry ` +
                `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
            )
            if (attempt < maxRetries - 1) {
              const waitMs = backoffMs(attempt, { base: 1000 })
              await sleep(waitMs)
            }
            continue
          }

          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
          }

          const result = await resp.json()

          // Handle 207 partial success
          if (resp.status === 207) {
            const successEmails = result.data || []
            const errors = result.errors || []
            enrichAndCollect(successEmails, allEmails, metaByMessageId)
            const failedIds = errors.map((e) => e.messageId).filter(Boolean)
            if (failedIds.length > 0) {
              pendingIds = failedIds
              console.log(
                `[email-client] 207 partial — ${successEmails.length} ok, ${failedIds.length} failed ` +
                  `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
              )
              if (attempt < maxRetries - 1) {
                const waitMs = backoffMs(attempt, { base: 1000 })
                await sleep(waitMs)
              }
              continue
            }
            // No failures — done with this chunk
            pendingIds = []
            continue
          }

          // 200 success — all messages in response
          const emails = result.data || result
          enrichAndCollect(emails, allEmails, metaByMessageId)
          pendingIds = []
        } catch (err) {
          clear()
          throw err
        }
      } catch (err) {
        console.log(
          `[email-client] chunk ${chunkIndex} fetch failed ` +
            `(attempt ${attempt + 1}/${maxRetries}): ${err.message}`,
        )

        // If not the last attempt, wait with exponential backoff before retry
        if (attempt < maxRetries - 1) {
          const waitMs = backoffMs(attempt, { base: 1000 })
          await sleep(waitMs)
        }
      }
    }
  }

  if (allEmails.length === 0 && messageIds.length > 0) {
    throw new Error(`All content fetches failed — 0/${messageIds.length} emails retrieved`)
  }

  const fetchTotalMs = Date.now() - fetchStart
  console.log(
    `[email-client] fetch done: ${allEmails.length}/${messageIds.length} emails, ${totalChunks} chunks, ${fetchTotalMs}ms`,
  )

  return allEmails
}
