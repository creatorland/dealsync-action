# Content Fetcher Partial Failure Handling — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Handle HTTP 207/502 partial failure responses from the content fetcher, retry only failed messageIds, and group emails by thread so incomplete threads are never processed.

**Architecture:** `fetchEmails()` becomes a single-shot fire-and-parse function (no retry). A new `fetchThreadEmails()` sits on top, managing a retry loop with thread-aware completeness checks, exponential backoff, and a wall-clock deadline. Both pipelines call `fetchThreadEmails()` instead of `fetchEmails()` directly.

**Tech Stack:** Node 24 ESM, Jest with `--experimental-vm-modules`, existing `sleep`/`backoffMs` from `src/lib/retry.js`

**Design doc:** [2026-04-01-content-fetcher-design.md](2026-04-01-content-fetcher-design.md)
**Flow diagrams:** [2026-04-01-content-fetcher-flows.md](2026-04-01-content-fetcher-flows.md)

---

### Task 1: Refactor fetchEmails() to single-shot with 200/207/502 parsing

**Files:**

- Modify: `src/lib/emails.js:190-285`
- Test: `__tests__/emails.test.js`

**Step 1: Write failing tests for the new return type and HTTP status handling**

Add these tests to `__tests__/emails.test.js`. The existing tests will also need updating since the return type changes from `EmailContent[]` to `{ fetched, failed }`.

```javascript
// --- New tests for 207/502 handling ---

describe('fetchEmails (single-shot, no retry)', () => {
  it('returns { fetched, failed } on 200 success', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(2)
    expect(result.failed).toHaveLength(0)
    expect(result.fetched[0]).toMatchObject({
      messageId: 'msg-1',
      id: 'meta-msg-1',
      threadId: 'thread-msg-1',
    })
  })

  it('parses 207 partial response into fetched + failed', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 207,
      json: async () => ({
        status: 'partial',
        data: [{ messageId: 'msg-1' }, { messageId: 'msg-2' }],
        errors: [{ messageId: 'msg-3', error: 'rate limited' }],
      }),
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(2)
    expect(result.failed).toEqual([{ messageId: 'msg-3', error: 'rate limited' }])
  })

  it('parses 502 failure response into all failed', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 502,
      json: async () => ({
        status: 'failure',
        data: [],
        errors: [
          { messageId: 'msg-1', error: 'timeout' },
          { messageId: 'msg-2', error: 'timeout' },
        ],
      }),
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
  })

  it('treats 502 with non-JSON body as transport error (all failed)', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 502,
      json: async () => {
        throw new Error('not json')
      },
      text: async () => 'Bad Gateway',
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toEqual([{ messageId: 'msg-1', error: 'HTTP 502: Bad Gateway' }])
  })

  it('treats transport error (fetch throws) as all failed', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockRejectedValueOnce(new Error('network failure'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0].error).toContain('network failure')
  })

  it('treats non-2xx/non-502 as transport error (all failed)', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(errorResponse(500, 'Internal Server Error'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toEqual([
      { messageId: 'msg-1', error: 'HTTP 500: Internal Server Error' },
    ])
  })

  it('fires all chunks concurrently and merges results', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // Two chunks of 2
    mockFetch
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(4)
    expect(result.failed).toHaveLength(0)
  })

  it('handles mixed success and failure across chunks', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // Chunk 1: success
    mockFetch
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))
      // Chunk 2: partial
      .mockResolvedValueOnce({
        ok: true,
        status: 207,
        json: async () => ({
          status: 'partial',
          data: [{ messageId: 'msg-3' }],
          errors: [{ messageId: 'msg-4', error: 'rate limited' }],
        }),
      })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(3)
    expect(result.failed).toHaveLength(1)
  })

  it('returns empty fetched and failed when messageIds is empty', async () => {
    const result = await fetchEmails([], new Map(), makeOpts())
    expect(result).toEqual({ fetched: [], failed: [] })
    expect(mockFetch).not.toHaveBeenCalled()
  })

  it('logs chunk request and response details', async () => {
    const spy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    const logs = spy.mock.calls.map((c) => c[0])
    expect(logs.some((l) => l.includes('[fetchEmails]') && l.includes('requesting'))).toBe(true)
    expect(logs.some((l) => l.includes('[fetchEmails]') && l.includes('fetched'))).toBe(true)

    spy.mockRestore()
  })
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/emails.test.js --no-cache`
Expected: FAIL — existing tests expect array return, new tests expect `{ fetched, failed }` shape

**Step 3: Rewrite fetchEmails() implementation**

Replace `src/lib/emails.js:190-285` with:

```javascript
/**
 * Single-shot fetch of email content from content-fetcher service.
 * Fires all chunks concurrently. No retry logic — caller handles retries.
 *
 * Handles HTTP 200 (success), 207 (partial), 502 (total failure),
 * and transport errors (timeout, connection reset).
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

  // Fire all chunks concurrently
  const chunkResults = await Promise.allSettled(
    chunks.map(async (chunk, idx) => {
      const chunkIndex = idx + 1
      const totalChunks = chunks.length

      console.log(
        `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: requesting ${chunk.length} messageIds` +
          (format ? ` (format=${format})` : ''),
      )

      const startMs = Date.now()
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

        const elapsedMs = Date.now() - startMs

        // --- 200 success ---
        if (resp.status === 200) {
          const result = await resp.json()
          const emails = result.data || result
          console.log(
            `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: HTTP 200 — ${emails.length} fetched (${elapsedMs}ms)`,
          )
          return { fetched: emails, failed: [] }
        }

        // --- 207 partial ---
        if (resp.status === 207) {
          const result = await resp.json()
          const emails = Array.isArray(result.data) ? result.data : []
          const errors = Array.isArray(result.errors) ? result.errors : []
          console.log(
            `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: HTTP 207 partial — ${emails.length} fetched, ${errors.length} failed (${elapsedMs}ms)`,
          )
          if (errors.length > 0) {
            const errorSummary = errors.map((e) => `${e.messageId}: ${e.error}`).join(', ')
            console.log(
              `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: failed messageIds: ${errorSummary}`,
            )
          }
          return { fetched: emails, failed: errors }
        }

        // --- 502 failure (parse JSON body if possible) ---
        if (resp.status === 502) {
          try {
            const result = await resp.json()
            if (result.status === 'failure' && Array.isArray(result.errors)) {
              console.log(
                `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: HTTP 502 total failure — ${result.errors.length} failed (${elapsedMs}ms)`,
              )
              return { fetched: [], failed: result.errors }
            }
          } catch {
            // Non-JSON 502 — fall through to transport error handling
          }
          const errText = await resp.text().catch(() => 'Bad Gateway')
          console.log(
            `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: HTTP 502 non-JSON — treating as transport error (${elapsedMs}ms)`,
          )
          return {
            fetched: [],
            failed: chunk.map((id) => ({ messageId: id, error: `HTTP 502: ${errText}` })),
          }
        }

        // --- Other HTTP errors ---
        const errText = await resp.text().catch(() => '')
        console.log(
          `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: HTTP ${resp.status} — treating as transport error (${elapsedMs}ms)`,
        )
        return {
          fetched: [],
          failed: chunk.map((id) => ({ messageId: id, error: `HTTP ${resp.status}: ${errText}` })),
        }
      } catch (err) {
        clear()
        console.log(
          `[fetchEmails] chunk ${chunkIndex}/${totalChunks}: transport error — ${err.message}`,
        )
        return {
          fetched: [],
          failed: chunk.map((id) => ({ messageId: id, error: err.message })),
        }
      }
    }),
  )

  // Merge all chunk results
  const allFetched = []
  const allFailed = []

  for (const result of chunkResults) {
    if (result.status === 'fulfilled') {
      allFetched.push(...result.value.fetched)
      allFailed.push(...result.value.failed)
    }
  }

  // Enrich fetched emails with metadata
  for (const email of allFetched) {
    const meta = metaByMessageId.get(email.messageId)
    if (meta) {
      email.id = meta.EMAIL_METADATA_ID
      email.threadId = meta.THREAD_ID
      if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
    }
  }

  return { fetched: allFetched, failed: allFailed }
}
```

**Step 4: Update existing tests to match new return type**

All existing tests that check `result[0]`, `result.toHaveLength(N)`, etc. need to change to `result.fetched[0]`, `result.fetched.toHaveLength(N)`, etc. Remove tests for retry/backoff behavior since `fetchEmails()` no longer retries. Remove the 429 handling tests. Remove the `maxRetries` parameter from `makeOpts()`.

**Step 5: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/emails.test.js --no-cache`
Expected: PASS

**Step 6: Commit**

```bash
git add src/lib/emails.js __tests__/emails.test.js
git commit -m "refactor: make fetchEmails single-shot with 200/207/502 parsing"
```

---

### Task 2: Add fetchThreadEmails() — thread-aware retry layer

**Files:**

- Create: `src/lib/fetch-threads.js`
- Test: `__tests__/fetch-threads.test.js`

**Step 1: Write failing tests for fetchThreadEmails()**

Create `__tests__/fetch-threads.test.js`:

```javascript
import { jest } from '@jest/globals'

// Mock fetchEmails
const mockFetchEmails = jest.fn()
jest.unstable_mockModule('../src/lib/emails.js', () => ({
  fetchEmails: mockFetchEmails,
}))

// Mock sleep to avoid real delays in tests
jest.unstable_mockModule('../src/lib/retry.js', () => ({
  sleep: jest.fn().mockResolvedValue(undefined),
  backoffMs: jest.fn((attempt, opts) => {
    const base = opts?.base || 1000
    const max = opts?.max || 60000
    return Math.min(base * Math.pow(2, attempt), max)
  }),
}))

const { fetchThreadEmails } = await import('../src/lib/fetch-threads.js')
const retrySleep = (await import('../src/lib/retry.js')).sleep

beforeEach(() => {
  jest.clearAllMocks()
})

// Helpers
function makeMeta(entries) {
  // entries: [{ messageId, threadId }]
  const map = new Map()
  for (const e of entries) {
    map.set(e.messageId, {
      EMAIL_METADATA_ID: `meta-${e.messageId}`,
      THREAD_ID: e.threadId,
    })
  }
  return map
}

function makeOpts(overrides = {}) {
  return {
    contentFetcherUrl: 'https://fetcher.test',
    userId: 'user-1',
    chunkSize: 50,
    fetchTimeoutMs: 5000,
    deadlineMs: 200000,
    maxFetchAttempts: 10,
    ...overrides,
  }
}

describe('fetchThreadEmails', () => {
  it('returns all threads as complete when everything succeeds', async () => {
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-A' },
      { messageId: 'msg-3', threadId: 'thread-B' },
    ])

    mockFetchEmails.mockResolvedValueOnce({
      fetched: [
        { messageId: 'msg-1', threadId: 'thread-A' },
        { messageId: 'msg-2', threadId: 'thread-A' },
        { messageId: 'msg-3', threadId: 'thread-B' },
      ],
      failed: [],
    })

    const result = await fetchThreadEmails(['msg-1', 'msg-2', 'msg-3'], meta, makeOpts())

    expect(result.completedThreads).toHaveLength(3) // 3 emails across 2 threads
    expect(result.unfetchableThreadIds).toHaveLength(0)
  })

  it('retries only failed messageIds on partial failure', async () => {
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-A' },
    ])

    // Round 1: msg-2 fails
    mockFetchEmails.mockResolvedValueOnce({
      fetched: [{ messageId: 'msg-1', threadId: 'thread-A' }],
      failed: [{ messageId: 'msg-2', error: 'rate limited' }],
    })
    // Round 2: msg-2 succeeds
    mockFetchEmails.mockResolvedValueOnce({
      fetched: [{ messageId: 'msg-2', threadId: 'thread-A' }],
      failed: [],
    })

    const result = await fetchThreadEmails(['msg-1', 'msg-2'], meta, makeOpts())

    expect(result.completedThreads).toHaveLength(2) // both emails
    expect(result.unfetchableThreadIds).toHaveLength(0)

    // Second call should only have msg-2
    const secondCallIds = mockFetchEmails.mock.calls[1][0]
    expect(secondCallIds).toEqual(['msg-2'])
  })

  it('skips entire thread when any message is unfetchable after max attempts', async () => {
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-A' },
    ])

    // All attempts fail for msg-2
    mockFetchEmails.mockResolvedValue({
      fetched: [{ messageId: 'msg-1', threadId: 'thread-A' }],
      failed: [{ messageId: 'msg-2', error: 'permanently broken' }],
    })

    const result = await fetchThreadEmails(
      ['msg-1', 'msg-2'],
      meta,
      makeOpts({ maxFetchAttempts: 3 }),
    )

    expect(result.completedThreads).toHaveLength(0)
    expect(result.unfetchableThreadIds).toEqual(['thread-A'])
    // 3 attempts total (initial + 2 retries)
    expect(mockFetchEmails).toHaveBeenCalledTimes(3)
  })

  it('applies exponential backoff between retry rounds', async () => {
    const meta = makeMeta([{ messageId: 'msg-1', threadId: 'thread-A' }])

    // Fail 3 times then succeed
    mockFetchEmails
      .mockResolvedValueOnce({ fetched: [], failed: [{ messageId: 'msg-1', error: 'fail' }] })
      .mockResolvedValueOnce({ fetched: [], failed: [{ messageId: 'msg-1', error: 'fail' }] })
      .mockResolvedValueOnce({
        fetched: [{ messageId: 'msg-1', threadId: 'thread-A' }],
        failed: [],
      })

    await fetchThreadEmails(['msg-1'], meta, makeOpts())

    // sleep called between rounds: round 1->2, round 2->3
    expect(retrySleep).toHaveBeenCalledTimes(2)
  })

  it('stops retrying when deadline is reached', async () => {
    const meta = makeMeta([{ messageId: 'msg-1', threadId: 'thread-A' }])

    mockFetchEmails.mockResolvedValue({
      fetched: [],
      failed: [{ messageId: 'msg-1', error: 'fail' }],
    })

    // Use a very short deadline (already passed)
    const result = await fetchThreadEmails(['msg-1'], meta, makeOpts({ deadlineMs: 0 }))

    expect(result.unfetchableThreadIds).toEqual(['thread-A'])
    // Should have only done 1 round (no retries since deadline already passed)
    expect(mockFetchEmails).toHaveBeenCalledTimes(1)
  })

  it('keeps threads intact when packing into chunks', async () => {
    // Thread A has 3 msgs, thread B has 2 msgs, chunkSize is 4
    // Thread A (3) fits in chunk 1, thread B (2) would exceed 4, so new chunk
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-A' },
      { messageId: 'msg-3', threadId: 'thread-A' },
      { messageId: 'msg-4', threadId: 'thread-B' },
      { messageId: 'msg-5', threadId: 'thread-B' },
    ])

    mockFetchEmails.mockResolvedValue({
      fetched: [
        { messageId: 'msg-1', threadId: 'thread-A' },
        { messageId: 'msg-2', threadId: 'thread-A' },
        { messageId: 'msg-3', threadId: 'thread-A' },
        { messageId: 'msg-4', threadId: 'thread-B' },
        { messageId: 'msg-5', threadId: 'thread-B' },
      ],
      failed: [],
    })

    await fetchThreadEmails(
      ['msg-1', 'msg-2', 'msg-3', 'msg-4', 'msg-5'],
      meta,
      makeOpts({ chunkSize: 4 }),
    )

    expect(mockFetchEmails).toHaveBeenCalledTimes(1)
    // All 5 in one call since we pass all messageIds
    expect(mockFetchEmails.mock.calls[0][0]).toHaveLength(5)
  })

  it('handles oversized thread spanning multiple chunks', async () => {
    // Thread with 5 msgs, chunkSize is 2 — must span 3 chunks
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-A' },
      { messageId: 'msg-3', threadId: 'thread-A' },
      { messageId: 'msg-4', threadId: 'thread-A' },
      { messageId: 'msg-5', threadId: 'thread-A' },
    ])

    mockFetchEmails.mockResolvedValueOnce({
      fetched: [
        { messageId: 'msg-1', threadId: 'thread-A' },
        { messageId: 'msg-2', threadId: 'thread-A' },
        { messageId: 'msg-3', threadId: 'thread-A' },
        { messageId: 'msg-4', threadId: 'thread-A' },
        { messageId: 'msg-5', threadId: 'thread-A' },
      ],
      failed: [],
    })

    const result = await fetchThreadEmails(
      ['msg-1', 'msg-2', 'msg-3', 'msg-4', 'msg-5'],
      meta,
      makeOpts({ chunkSize: 2 }),
    )

    expect(result.completedThreads).toHaveLength(5)
    expect(result.unfetchableThreadIds).toHaveLength(0)
  })

  it('releases completed thread data from fetchedMap (no duplication)', async () => {
    const meta = makeMeta([
      { messageId: 'msg-1', threadId: 'thread-A' },
      { messageId: 'msg-2', threadId: 'thread-B' },
    ])

    // Round 1: thread-A complete, thread-B fails
    mockFetchEmails.mockResolvedValueOnce({
      fetched: [{ messageId: 'msg-1', threadId: 'thread-A' }],
      failed: [{ messageId: 'msg-2', error: 'fail' }],
    })
    // Round 2: thread-B succeeds
    mockFetchEmails.mockResolvedValueOnce({
      fetched: [{ messageId: 'msg-2', threadId: 'thread-B' }],
      failed: [],
    })

    const result = await fetchThreadEmails(['msg-1', 'msg-2'], meta, makeOpts())

    expect(result.completedThreads).toHaveLength(2)
    // msg-1 should not be re-requested in round 2
    expect(mockFetchEmails.mock.calls[1][0]).toEqual(['msg-2'])
  })

  it('returns empty results when messageIds is empty', async () => {
    const result = await fetchThreadEmails([], new Map(), makeOpts())
    expect(result.completedThreads).toEqual([])
    expect(result.unfetchableThreadIds).toEqual([])
    expect(mockFetchEmails).not.toHaveBeenCalled()
  })
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/fetch-threads.test.js --no-cache`
Expected: FAIL — module `src/lib/fetch-threads.js` does not exist

**Step 3: Implement fetchThreadEmails()**

Create `src/lib/fetch-threads.js`:

```javascript
/**
 * Thread-aware email fetching with retry queue and re-batching.
 *
 * Sits on top of fetchEmails() (single-shot, no retry).
 * Groups emails by thread, retries only failed messageIds,
 * and ensures incomplete threads are never returned as complete.
 */

import { fetchEmails } from './emails.js'
import { sleep, backoffMs } from './retry.js'

const DEFAULT_MAX_FETCH_ATTEMPTS = 10
const DEFAULT_DEADLINE_MS = 200000 // 200s of the 240s workflow timeout
const BACKOFF_BASE = 1000
const BACKOFF_MAX = 60000

/**
 * Fetch emails with thread-aware retry logic.
 *
 * @param {string[]} messageIds - all message IDs to fetch
 * @param {Map} metaByMessageId - Map<messageId, { EMAIL_METADATA_ID, THREAD_ID, ... }>
 * @param {object} opts - same as fetchEmails opts plus:
 * @param {number} [opts.deadlineMs=200000] - wall-clock budget for retries
 * @param {number} [opts.maxFetchAttempts=10] - max attempts per messageId
 * @returns {Promise<{ completedThreads: object[], unfetchableThreadIds: string[] }>}
 */
export async function fetchThreadEmails(messageIds, metaByMessageId, opts) {
  const {
    deadlineMs = DEFAULT_DEADLINE_MS,
    maxFetchAttempts = DEFAULT_MAX_FETCH_ATTEMPTS,
    ...fetchOpts
  } = opts

  if (!messageIds || messageIds.length === 0) {
    return { completedThreads: [], unfetchableThreadIds: [] }
  }

  // Build thread map: threadId -> [messageId, ...]
  const threadMap = new Map()
  for (const msgId of messageIds) {
    const meta = metaByMessageId.get(msgId)
    const threadId = meta?.THREAD_ID || msgId
    if (!threadMap.has(threadId)) threadMap.set(threadId, [])
    threadMap.get(threadId).push(msgId)
  }

  // State
  const fetchedMap = new Map() // messageId -> EmailContent
  const attemptCounts = new Map() // messageId -> number
  const completedThreads = [] // emails from completed threads
  const deadline = Date.now() + deadlineMs

  // Initialize attempt counts
  for (const msgId of messageIds) {
    attemptCounts.set(msgId, 0)
  }

  let round = 0
  let pendingMessageIds = [...messageIds]

  while (pendingMessageIds.length > 0) {
    round++

    if (round > 1) {
      // Check deadline before retrying
      if (Date.now() >= deadline) {
        console.log(
          `[fetchThreadEmails] deadline reached after round ${round - 1} — ` +
            `${pendingMessageIds.length} messageIds still pending`,
        )
        break
      }

      // Apply backoff
      const waitMs = backoffMs(round - 2, { base: BACKOFF_BASE, max: BACKOFF_MAX })
      console.log(
        `[fetchThreadEmails] round ${round}: retrying ${pendingMessageIds.length} messageIds, ` +
          `backoff ${waitMs}ms`,
      )
      await sleep(waitMs)
    } else {
      console.log(
        `[fetchThreadEmails] round ${round}: fetching ${pendingMessageIds.length} messageIds ` +
          `across ${threadMap.size} threads`,
      )
    }

    // Fire fetch
    const { fetched, failed } = await fetchEmails(pendingMessageIds, metaByMessageId, fetchOpts)

    // Store successes
    for (const email of fetched) {
      fetchedMap.set(email.messageId, email)
    }

    // Increment attempt counts for failures
    for (const { messageId } of failed) {
      attemptCounts.set(messageId, (attemptCounts.get(messageId) || 0) + 1)
    }

    // Also count successful fetches (they used an attempt)
    for (const email of fetched) {
      attemptCounts.set(email.messageId, (attemptCounts.get(email.messageId) || 0) + 1)
    }

    // Check thread completeness
    for (const [threadId, threadMsgIds] of threadMap.entries()) {
      const allFetched = threadMsgIds.every((id) => fetchedMap.has(id))
      if (allFetched) {
        // Move to completed, delete from fetchedMap
        for (const id of threadMsgIds) {
          completedThreads.push(fetchedMap.get(id))
          fetchedMap.delete(id)
        }
        threadMap.delete(threadId)
      }
    }

    // Collect messageIds still needing retry
    pendingMessageIds = []
    for (const [threadId, threadMsgIds] of threadMap.entries()) {
      for (const id of threadMsgIds) {
        if (!fetchedMap.has(id) && attemptCounts.get(id) < maxFetchAttempts) {
          pendingMessageIds.push(id)
        }
      }
    }

    // Check if any threads are permanently stuck (all attempts exhausted)
    for (const [threadId, threadMsgIds] of threadMap.entries()) {
      const hasUnfetchable = threadMsgIds.some(
        (id) => !fetchedMap.has(id) && attemptCounts.get(id) >= maxFetchAttempts,
      )
      if (hasUnfetchable) {
        // Remove from threadMap — will be reported as unfetchable
        threadMap.delete(threadId)
        // Clean up any partially fetched emails for this thread
        for (const id of threadMsgIds) {
          fetchedMap.delete(id)
        }
      }
    }

    if (pendingMessageIds.length === 0) break
  }

  // Remaining threads in threadMap are unfetchable (deadline or attempts exhausted)
  const unfetchableThreadIds = [...threadMap.keys()]

  if (unfetchableThreadIds.length > 0) {
    console.log(
      `[fetchThreadEmails] ${unfetchableThreadIds.length} threads unfetchable after ${round} rounds`,
    )
  }

  console.log(
    `[fetchThreadEmails] done: ${completedThreads.length} emails from completed threads, ` +
      `${unfetchableThreadIds.length} unfetchable threads`,
  )

  return { completedThreads, unfetchableThreadIds }
}
```

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/fetch-threads.test.js --no-cache`
Expected: PASS

**Step 5: Commit**

```bash
git add src/lib/fetch-threads.js __tests__/fetch-threads.test.js
git commit -m "feat: add fetchThreadEmails — thread-aware retry layer"
```

---

### Task 3: Update filter pipeline to use fetchThreadEmails()

**Files:**

- Modify: `src/commands/run-filter-pipeline.js:5,124-143`
- Test: `__tests__/run-filter-pipeline.test.js`

**Step 1: Write/update failing tests**

In `__tests__/run-filter-pipeline.test.js`, update the mock to use `fetchThreadEmails` instead of `fetchEmails`, and add a test for the unfetchable thread case:

```javascript
// Update the mock module — mock fetch-threads.js instead of emails.js fetchEmails
jest.unstable_mockModule('../src/lib/fetch-threads.js', () => ({
  fetchThreadEmails: mockFetchThreadEmails,
}))
```

Add test:

```javascript
it('throws when threads are unfetchable (triggers batch-level retry)', async () => {
  // Setup: fetchThreadEmails returns some unfetchable threads
  mockFetchThreadEmails.mockResolvedValueOnce({
    completedThreads: [],
    unfetchableThreadIds: ['thread-1'],
  })

  await expect(processFilterBatch(batch)).rejects.toThrow(/unfetchable/)
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js --no-cache`
Expected: FAIL

**Step 3: Update filter pipeline implementation**

In `src/commands/run-filter-pipeline.js`:

1. Change import:

```javascript
// Remove:
import { isRejected, fetchEmails } from '../lib/emails.js'
// Add:
import { isRejected } from '../lib/emails.js'
import { fetchThreadEmails } from '../lib/fetch-threads.js'
```

2. Replace `fetchEmails()` call in `processFilterBatch()` (lines 124-131) with:

```javascript
// b. Call fetchThreadEmails() with format: 'metadata'
const { completedThreads, unfetchableThreadIds } = await fetchThreadEmails(
  messageIds,
  metaByMessageId,
  {
    contentFetcherUrl,
    userId,
    syncStateId,
    chunkSize,
    fetchTimeoutMs,
    format: 'metadata',
  },
)

if (unfetchableThreadIds.length > 0) {
  console.log(
    `[run-filter-pipeline] ${unfetchableThreadIds.length} unfetchable threads — ` +
      `throwing to trigger batch-level retry`,
  )
  throw new Error(`${unfetchableThreadIds.length} threads unfetchable after content fetch retries`)
}

const emails = completedThreads
```

3. The rest of the function (`isRejected` loop, deal state updates) stays the same since `emails` is still an array of email objects.

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js --no-cache`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/run-filter-pipeline.js __tests__/run-filter-pipeline.test.js
git commit -m "feat: filter pipeline uses fetchThreadEmails for thread-aware retry"
```

---

### Task 4: Update classify pipeline to use fetchThreadEmails()

**Files:**

- Modify: `src/commands/run-classify-pipeline.js:5,179-196`
- Test: `__tests__/run-classify-pipeline.test.js`

**Step 1: Write/update failing tests**

In `__tests__/run-classify-pipeline.test.js`, update the mock to use `fetchThreadEmails` instead of `fetchEmails`:

```javascript
// Update mock module — mock fetch-threads.js instead of emails.js fetchEmails
jest.unstable_mockModule('../src/lib/fetch-threads.js', () => ({
  fetchThreadEmails: mockFetchThreadEmails,
}))
```

The mock return shape changes from an array to `{ completedThreads: [...], unfetchableThreadIds: [] }`.

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js --no-cache`
Expected: FAIL

**Step 3: Update classify pipeline implementation**

In `src/commands/run-classify-pipeline.js`:

1. Change import:

```javascript
// Remove:
import { fetchEmails } from '../lib/emails.js'
// Add:
import { fetchThreadEmails } from '../lib/fetch-threads.js'
```

2. Replace the `fetchEmails()` call block (lines 179-191) with:

```javascript
let allEmails
try {
  const { completedThreads, unfetchableThreadIds } = await fetchThreadEmails(
    messageIds,
    metaByMessageId,
    {
      contentFetcherUrl,
      userId,
      syncStateId,
      chunkSize,
      fetchTimeoutMs,
    },
  )
  allEmails = completedThreads

  if (unfetchableThreadIds.length > 0) {
    console.log(
      `[run-classify-pipeline] ${unfetchableThreadIds.length} unfetchable threads — ` +
        `will be retried on next batch attempt`,
    )
  }
} catch {
  allEmails = []
}
```

3. The existing unfetchable thread handling (lines 193-238) stays. It already detects threads with zero emails and handles them via previous evals. Now it will also catch threads that were unfetchable at the content-fetcher level.

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js --no-cache`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/run-classify-pipeline.js __tests__/run-classify-pipeline.test.js
git commit -m "feat: classify pipeline uses fetchThreadEmails for thread-aware retry"
```

---

### Task 5: Run full test suite and package

**Step 1: Run all tests**

Run: `npm test`
Expected: All tests PASS

**Step 2: Fix any broken tests**

If any tests fail, check:

- `__tests__/main.test.js` — may import the old `fetchEmails` path
- `__tests__/emails.test.js` — verify all updated to `{ fetched, failed }` return type
- Any test that mocks `fetchEmails` directly needs to return `{ fetched, failed }` instead of array

**Step 3: Package the bundle**

Run: `npm run package`
Expected: `dist/index.js` regenerated successfully

**Step 4: Run full suite again post-package**

Run: `npm run all`
Expected: format + test + package all pass

**Step 5: Commit the bundle**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist bundle"
```

---

### Task 6: Create PR

**Step 1: Create feature branch and push**

```bash
git checkout -b feat/content-fetcher-partial-failure
git push -u origin feat/content-fetcher-partial-failure
```

**Step 2: Create PR**

```bash
gh pr create --title "feat: handle content fetcher partial failures with thread-aware retry" --body "$(cat <<'EOF'
## Summary

- Refactored `fetchEmails()` to single-shot (no internal retry) with HTTP 200/207/502 parsing
- Added `fetchThreadEmails()` — thread-aware retry layer with exponential backoff, wall-clock deadline, and efficient re-batching of only failed messageIds
- Updated filter and classify pipelines to use `fetchThreadEmails()`
- Incomplete threads (any email unfetchable) are never processed — they trigger batch-level retry via existing `runPool()` mechanism

## Context

Companion to creatorland/backend#1184 and creatorland/dealsync-v2#349 (PROJ-5820) which add HTTP 207 partial and 502 total failure semantics to the content fetcher API.

## Design

See `docs/plans/2026-04-01-content-fetcher-design.md` for full design document and `docs/plans/2026-04-01-content-fetcher-flows.md` for mermaid flow diagrams.

## Test plan

- [ ] Unit tests for `fetchEmails()` — 200/207/502/transport error parsing
- [ ] Unit tests for `fetchThreadEmails()` — retry, backoff, thread completeness, deadline
- [ ] Unit tests for filter pipeline — fetchThreadEmails integration
- [ ] Unit tests for classify pipeline — fetchThreadEmails integration
- [ ] Full suite passes (`npm run all`)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
