/**
 * Email utilities — sanitization, header helpers, filter rules, and content fetching.
 *
 * Combines email-sanitizer, filter-rules, and email-client into a single module
 * since they form a tight unit operating on email data.
 */

import { convert } from 'html-to-text'
import EmailReplyParser from 'email-reply-parser'
import { withTimeout } from './db.js'
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

/**
 * Fetch email content from the content-fetcher service in chunks,
 * enriching each email with metadata from the provided map.
 *
 * Single-shot: fires all chunks concurrently via Promise.allSettled,
 * never retries, never throws. Returns { fetched, failed }.
 *
 * @param {string[]} messageIds - message IDs to fetch
 * @param {Map} metaByMessageId - Map<messageId, { EMAIL_METADATA_ID, THREAD_ID, PREVIOUS_AI_SUMMARY? }>
 * @param {object} opts
 * @param {string} opts.contentFetcherUrl - base URL for content fetcher
 * @param {string} opts.userId - user ID for the request
 * @param {string} [opts.syncStateId] - optional sync state ID
 * @param {number} opts.chunkSize - messages per request
 * @param {number} opts.fetchTimeoutMs - timeout per request
 * @param {string} [opts.format] - 'metadata' (headers only) or undefined (full content)
 * @returns {Promise<{ fetched: object[], failed: { messageId: string, error: string }[] }>}
 */
export async function fetchEmails(messageIds, metaByMessageId, opts) {
  const { contentFetcherUrl, userId, syncStateId, chunkSize, fetchTimeoutMs, format } = opts

  if (!messageIds || messageIds.length === 0) {
    return { fetched: [], failed: [] }
  }

  // Split into chunks
  const chunks = []
  for (let i = 0; i < messageIds.length; i += chunkSize) {
    chunks.push(messageIds.slice(i, i + chunkSize))
  }
  const totalChunks = chunks.length

  // Fire all chunks concurrently
  const results = await Promise.allSettled(
    chunks.map((chunk, idx) =>
      fetchChunk(chunk, idx + 1, totalChunks, {
        contentFetcherUrl,
        userId,
        syncStateId,
        fetchTimeoutMs,
        format,
        metaByMessageId,
      }),
    ),
  )

  // Aggregate results
  const allFetched = []
  const allFailed = []
  for (let i = 0; i < results.length; i++) {
    const r = results[i]
    if (r.status === 'fulfilled') {
      allFetched.push(...r.value.fetched)
      allFailed.push(...r.value.failed)
    } else {
      // Safety net: fetchChunk should never reject, but if it does,
      // map the chunk's messageIds to failures so none are silently lost.
      const chunk = chunks[i]
      const errMsg = r.reason?.message || String(r.reason) || 'unknown error'
      console.log(`[fetchEmails] chunk ${i + 1}/${totalChunks}: unexpected rejection — ${errMsg}`)
      allFailed.push(...chunk.map((messageId) => ({ messageId, error: errMsg })))
    }
  }

  return { fetched: allFetched, failed: allFailed }
}

/**
 * Fetch a single chunk. Returns { fetched, failed } — never throws.
 */
async function fetchChunk(chunk, chunkIndex, totalChunks, opts) {
  const { contentFetcherUrl, userId, syncStateId, fetchTimeoutMs, format, metaByMessageId } = opts
  const label = `[fetchEmails] chunk ${chunkIndex}/${totalChunks}`

  const formatLabel = format ? ` (format=${format})` : ''
  console.log(`${label}: requesting ${chunk.length} messageIds${formatLabel}`)

  const t0 = Date.now()
  let clear

  try {
    let resp
    const timeout = withTimeout(fetchTimeoutMs)
    clear = timeout.clear

    const body = {
      userId,
      ...(syncStateId ? { syncStateId } : {}),
      messageIds: chunk,
      ...(format ? { format } : {}),
    }

    resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: timeout.signal,
    })

    const elapsed = Date.now() - t0

    // --- HTTP 200: full success ---
    if (resp.status === 200) {
      const result = await resp.json()
      const emails = result.data || result
      enrichEmails(emails, metaByMessageId)
      console.log(`${label}: HTTP 200 — ${emails.length} fetched (${elapsed}ms)`)
      return { fetched: emails, failed: [] }
    }

    // --- HTTP 207: partial success ---
    if (resp.status === 207) {
      const result = await resp.json()
      const emails = result.data || []
      const errors = result.errors || []
      enrichEmails(emails, metaByMessageId)

      const failed = errors.map((e) => ({ messageId: e.messageId, error: e.error }))
      console.log(
        `${label}: HTTP 207 partial — ${emails.length} fetched, ${failed.length} failed (${elapsed}ms)`,
      )
      if (failed.length > 0) {
        const details = failed.map((f) => `${f.messageId}: ${f.error}`).join(', ')
        console.log(`${label}: failed messageIds: ${details}`)
      }
      return { fetched: emails, failed }
    }

    // --- HTTP 502: try to parse JSON body ---
    if (resp.status === 502) {
      const raw = await resp.text()
      try {
        const result = JSON.parse(raw)
        if (result.errors && Array.isArray(result.errors) && result.errors.length > 0) {
          const failed = result.errors.map((e) => ({ messageId: e.messageId, error: e.error }))
          console.log(`${label}: HTTP 502 total failure — ${failed.length} failed (${elapsed}ms)`)
          return { fetched: [], failed }
        }
      } catch {
        // Non-JSON 502 body — fall through
      }
      // No usable per-message errors (non-JSON body, or JSON without errors array)
      console.log(`${label}: HTTP 502 total failure — ${chunk.length} failed (${elapsed}ms)`)
      return {
        fetched: [],
        failed: chunk.map((messageId) => ({ messageId, error: `HTTP 502: ${raw}` })),
      }
    }

    // --- Other HTTP errors (500, 503, etc.) ---
    const text = await resp.text()
    console.log(`${label}: HTTP ${resp.status} — treating as transport error (${elapsed}ms)`)
    return {
      fetched: [],
      failed: chunk.map((messageId) => ({ messageId, error: `HTTP ${resp.status}: ${text}` })),
    }
  } catch (err) {
    console.log(`${label}: transport error — ${err.message}`)
    return {
      fetched: [],
      failed: chunk.map((messageId) => ({ messageId, error: err.message })),
    }
  } finally {
    if (clear) clear()
  }
}

function enrichEmails(emails, metaByMessageId) {
  for (const email of emails) {
    const meta = metaByMessageId.get(email.messageId)
    if (meta) {
      email.id = meta.EMAIL_METADATA_ID
      email.threadId = meta.THREAD_ID
      if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
    }
  }
}
