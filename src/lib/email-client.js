/**
 * Shared email content fetcher with standardized retry logic.
 *
 * Extracts the duplicated HTTP fetch+enrich pattern from
 * fetch-and-filter.js and fetch-and-classify.js into a single
 * reusable function with exponential backoff and 429 handling.
 */

import { withTimeout } from './sxt-client.js'

const DEFAULT_MAX_RETRIES = 3
const BACKOFF_BASE_MS = 1000
const BACKOFF_MULTIPLIER = 2

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
            const retryAfterMs = retryBody.retryAfterMs || backoffMs(attempt)
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
          const waitMs = backoffMs(attempt)
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

function backoffMs(attempt) {
  return BACKOFF_BASE_MS * Math.pow(BACKOFF_MULTIPLIER, attempt)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
