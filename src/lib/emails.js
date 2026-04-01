/**
 * Email utilities — sanitization, header helpers, filter rules, and content fetching.
 *
 * Combines email-sanitizer, filter-rules, and email-client into a single module
 * since they form a tight unit operating on email data.
 */

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
  const match = from.match(/<([^>]+)>/)
  return (match ? match[1] : from).trim().toLowerCase()
}

function extractDisplayName(from) {
  const match = from.match(/^(.+?)\s*</)
  return match
    ? match[1]
        .trim()
        .replace(/^["']|["']$/g, '')
        .toLowerCase()
    : ''
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

// ---------------------------------------------------------------------------
// Email content fetcher
// ---------------------------------------------------------------------------

const DEFAULT_MAX_RETRIES = 3

/**
 * Fetch email content from the content-fetcher service in chunks,
 * enriching each email with metadata from the provided map.
 *
 * @param {string[]} messageIds - message IDs to fetch
 * @param {Map} metaByMessageId - Map<messageId, { EMAIL_METADATA_ID, THREAD_ID, PREVIOUS_AI_SUMMARY? }>
 * @param {object} opts
 * @param {string} opts.contentFetcherUrl - base URL for content fetcher
 * @param {string} opts.userId - user ID for the request
 * @param {string} [opts.syncStateId] - optional sync state ID
 * @param {number} opts.chunkSize - messages per request
 * @param {number} opts.fetchTimeoutMs - timeout per request
 * @param {number} [opts.maxRetries=3] - retries per chunk
 * @param {string} [opts.format] - 'metadata' (headers only) or undefined (full content)
 * @returns {Promise<object[]>} enriched email objects
 */
export async function fetchEmails(messageIds, metaByMessageId, opts) {
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

  for (let i = 0; i < messageIds.length; i += chunkSize) {
    const chunk = messageIds.slice(i, i + chunkSize)
    const chunkIndex = Math.floor(i / chunkSize) + 1
    let fetched = false

    for (let attempt = 0; attempt < maxRetries && !fetched; attempt++) {
      try {
        const { signal, clear } = withTimeout(fetchTimeoutMs)
        try {
          const body = {
            userId,
            ...(syncStateId ? { syncStateId } : {}),
            messageIds: chunk,
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

          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
          }

          const result = await resp.json()
          const emails = result.data || result

          for (const email of emails) {
            const meta = metaByMessageId.get(email.messageId)
            if (meta) {
              email.id = meta.EMAIL_METADATA_ID
              email.threadId = meta.THREAD_ID
              if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
            }
            allEmails.push(email)
          }

          fetched = true
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

  return allEmails
}
