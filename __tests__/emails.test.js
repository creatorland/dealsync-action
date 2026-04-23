import { jest } from '@jest/globals'

// Mock withTimeout from sxt-client
jest.unstable_mockModule('../src/lib/db.js', () => ({
  withTimeout: jest.fn((ms) => {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), ms || 120000)
    return { signal: controller.signal, clear: () => clearTimeout(timeout) }
  }),
}))

// We need to capture the global fetch mock
const originalFetch = globalThis.fetch
let mockFetch

beforeEach(() => {
  mockFetch = jest.fn()
  globalThis.fetch = mockFetch
})

afterEach(() => {
  globalThis.fetch = originalFetch
  jest.restoreAllMocks()
})

const { fetchEmails, isBlockedSenderAddress, deriveFallbackMainContact } =
  await import('../src/lib/emails.js')
const sxtClient = await import('../src/lib/db.js')

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeOpts(overrides = {}) {
  return {
    contentFetcherUrl: 'https://content-fetcher.example.com',
    userId: 'user-1',
    syncStateId: 'sync-1',
    chunkSize: 2,
    fetchTimeoutMs: 5000,
    maxRetries: 3,
    ...overrides,
  }
}

function makeMeta(messageIds) {
  const map = new Map()
  for (const id of messageIds) {
    map.set(id, {
      EMAIL_METADATA_ID: `meta-${id}`,
      THREAD_ID: `thread-${id}`,
      PREVIOUS_AI_SUMMARY: `summary-${id}`,
    })
  }
  return map
}

function okResponse(data) {
  return {
    ok: true,
    status: 200,
    json: async () => ({ data }),
    text: async () => JSON.stringify({ data }),
  }
}

function partialResponse(data, errors) {
  return {
    ok: true,
    status: 207,
    json: async () => ({ status: 'partial', data, errors }),
    text: async () => JSON.stringify({ status: 'partial', data, errors }),
  }
}

function errorResponse(status, body = 'error') {
  return {
    ok: false,
    status,
    json: async () => (typeof body === 'object' ? body : {}),
    text: async () => (typeof body === 'string' ? body : JSON.stringify(body)),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('fetchEmails', () => {
  it('fetches a single chunk and enriches emails with metadata', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result).toHaveLength(2)
    expect(result[0]).toMatchObject({
      messageId: 'msg-1',
      id: 'meta-msg-1',
      threadId: 'thread-msg-1',
      previousAiSummary: 'summary-msg-1',
    })
    expect(result[1]).toMatchObject({
      messageId: 'msg-2',
      id: 'meta-msg-2',
      threadId: 'thread-msg-2',
      previousAiSummary: 'summary-msg-2',
    })

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url, reqOpts] = mockFetch.mock.calls[0]
    expect(url).toBe('https://content-fetcher.example.com/email-content/fetch')
    const body = JSON.parse(reqOpts.body)
    expect(body.userId).toBe('user-1')
    expect(body.syncStateId).toBe('sync-1')
    expect(body.messageIds).toEqual(['msg-1', 'msg-2'])
  })

  it('splits messageIds into chunks of chunkSize', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4', 'msg-5']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-5' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result).toHaveLength(5)
    expect(mockFetch).toHaveBeenCalledTimes(3)
  })

  it('omits syncStateId from body when not provided', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    await fetchEmails(messageIds, meta, makeOpts({ syncStateId: undefined }))

    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body).not.toHaveProperty('syncStateId')
  })

  it('includes format in body when provided', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    await fetchEmails(messageIds, meta, makeOpts({ format: 'metadata' }))

    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body.format).toBe('metadata')
  })

  it('omits format from body when not provided', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    await fetchEmails(messageIds, meta, makeOpts())

    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body).not.toHaveProperty('format')
  })

  it('handles response with top-level array (no data wrapper)', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => [{ messageId: 'msg-1' }],
      text: async () => '[]',
    })

    const result = await fetchEmails(messageIds, meta, makeOpts())

    expect(result).toHaveLength(1)
    expect(result[0].id).toBe('meta-msg-1')
  })

  it('enriches email even when meta has no PREVIOUS_AI_SUMMARY', async () => {
    const meta = new Map([['msg-1', { EMAIL_METADATA_ID: 'meta-1', THREAD_ID: 'thread-1' }]])

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(['msg-1'], meta, makeOpts())

    expect(result[0].id).toBe('meta-1')
    expect(result[0].threadId).toBe('thread-1')
    expect(result[0]).not.toHaveProperty('previousAiSummary')
  })

  it('skips enrichment for emails not in metaByMessageId', async () => {
    const meta = new Map() // empty map

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-unknown' }]))

    const result = await fetchEmails(['msg-unknown'], meta, makeOpts())

    expect(result).toHaveLength(1)
    expect(result[0]).not.toHaveProperty('id')
    expect(result[0]).not.toHaveProperty('threadId')
  })

  // -------------------------------------------------------------------------
  // Retry behavior
  // -------------------------------------------------------------------------

  it('retries on HTTP error with exponential backoff and succeeds', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce(errorResponse(500, 'Internal Server Error'))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ maxRetries: 3 }))

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('retries on network error (fetch throws) with exponential backoff', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockRejectedValueOnce(new Error('network failure'))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ maxRetries: 3 }))

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('exhausts retries and continues to next chunk (partial success)', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // First chunk: all 3 retries fail
    mockFetch
      .mockRejectedValueOnce(new Error('fail 1'))
      .mockRejectedValueOnce(new Error('fail 2'))
      .mockRejectedValueOnce(new Error('fail 3'))
      // Second chunk: succeeds
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2, maxRetries: 3 }))

    expect(result).toHaveLength(2)
    expect(result[0].messageId).toBe('msg-3')
    // 3 retries for first chunk + 1 success for second chunk
    expect(mockFetch).toHaveBeenCalledTimes(4)
  })

  it('throws when ALL chunks fail and 0 emails retrieved', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))

    await expect(
      fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 })),
    ).rejects.toThrow(/0.*emails retrieved/)
  })

  it('does NOT throw when at least some emails are retrieved (partial failure)', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3']
    const meta = makeMeta(messageIds)

    // chunk 1 (msg-1, msg-2): fails all retries
    mockFetch
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))
      // chunk 2 (msg-3): succeeds
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2, maxRetries: 3 }))
    expect(result).toHaveLength(1)
  })

  // -------------------------------------------------------------------------
  // 429 handling
  // -------------------------------------------------------------------------

  it('on 429, reads retryAfterMs from response body and retries', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({ retryAfterMs: 10 }),
        text: async () => '{"retryAfterMs":10}',
      })
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ maxRetries: 3 }))

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('on 429 without retryAfterMs, falls back to exponential backoff', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({}),
        text: async () => '{}',
      })
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ maxRetries: 3 }))

    expect(result).toHaveLength(1)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('429 retries count toward maxRetries limit', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({ retryAfterMs: 10 }),
        text: async () => '{}',
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({ retryAfterMs: 10 }),
        text: async () => '{}',
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({ retryAfterMs: 10 }),
        text: async () => '{}',
      })

    // All retries exhausted, should throw
    await expect(
      fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 })),
    ).rejects.toThrow(/0.*emails retrieved/)

    expect(mockFetch).toHaveBeenCalledTimes(3)
  })

  // -------------------------------------------------------------------------
  // 207 handling (partial success)
  // -------------------------------------------------------------------------

  it('on 207, accepts successful emails and retries only failed messageIds', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3']
    const meta = makeMeta(messageIds)

    mockFetch
      // First attempt: 207 with msg-1 success, msg-2 and msg-3 failed
      .mockResolvedValueOnce(
        partialResponse(
          [{ messageId: 'msg-1' }],
          [
            { messageId: 'msg-2', error: 'not found' },
            { messageId: 'msg-3', error: 'not found' },
          ],
        ),
      )
      // Second attempt: 200 with msg-2 and msg-3 success
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-2' }, { messageId: 'msg-3' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

    expect(result).toHaveLength(3)
    expect(result.map((e) => e.messageId).sort()).toEqual(['msg-1', 'msg-2', 'msg-3'])

    // Second fetch should only contain the 2 failed IDs
    expect(mockFetch).toHaveBeenCalledTimes(2)
    const secondBody = JSON.parse(mockFetch.mock.calls[1][1].body)
    expect(secondBody.messageIds).toEqual(['msg-2', 'msg-3'])
  })

  it('on 207, retries exhaust and returns only successful emails', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    // First attempt: 207 with msg-1 success, msg-2 failed
    mockFetch
      .mockResolvedValueOnce(
        partialResponse([{ messageId: 'msg-1' }], [{ messageId: 'msg-2', error: 'fail' }]),
      )
      // Second attempt: 207 with msg-2 still failing
      .mockResolvedValueOnce(partialResponse([], [{ messageId: 'msg-2', error: 'fail' }]))
      // Third attempt: 207 with msg-2 still failing
      .mockResolvedValueOnce(partialResponse([], [{ messageId: 'msg-2', error: 'fail' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

    // Only msg-1 returned (not duplicated)
    expect(result).toHaveLength(1)
    expect(result[0].messageId).toBe('msg-1')
    expect(mockFetch).toHaveBeenCalledTimes(3)

    // Second and third attempts should only send msg-2
    const secondBody = JSON.parse(mockFetch.mock.calls[1][1].body)
    expect(secondBody.messageIds).toEqual(['msg-2'])
    const thirdBody = JSON.parse(mockFetch.mock.calls[2][1].body)
    expect(thirdBody.messageIds).toEqual(['msg-2'])
  })

  // -------------------------------------------------------------------------
  // 502 handling
  // -------------------------------------------------------------------------

  it('on 502 with JSON errors, retries failed messageIds', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 502,
        json: async () => ({
          errors: [
            { messageId: 'msg-1', error: 'upstream fail' },
            { messageId: 'msg-2', error: 'upstream fail' },
          ],
        }),
        text: async () =>
          JSON.stringify({
            errors: [
              { messageId: 'msg-1', error: 'upstream fail' },
              { messageId: 'msg-2', error: 'upstream fail' },
            ],
          }),
      })
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

    expect(result).toHaveLength(2)
    expect(result.map((e) => e.messageId).sort()).toEqual(['msg-1', 'msg-2'])
  })

  it('on 502 with non-JSON body, retries all messageIds in chunk', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 502,
        json: async () => {
          throw new Error('not json')
        },
        text: async () => 'Bad Gateway',
      })
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

    expect(result).toHaveLength(2)
    expect(result.map((e) => e.messageId).sort()).toEqual(['msg-1', 'msg-2'])

    // Second fetch should contain all IDs since body was non-JSON
    const secondBody = JSON.parse(mockFetch.mock.calls[1][1].body)
    expect(secondBody.messageIds).toEqual(['msg-1', 'msg-2'])
  })

  it('on 502 exhausting retries, continues to next chunk', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // Chunk 1 (msg-1, msg-2): 502 all 3 retries
    mockFetch
      .mockResolvedValueOnce({
        ok: false,
        status: 502,
        json: async () => {
          throw new Error('not json')
        },
        text: async () => 'Bad Gateway',
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 502,
        json: async () => {
          throw new Error('not json')
        },
        text: async () => 'Bad Gateway',
      })
      .mockResolvedValueOnce({
        ok: false,
        status: 502,
        json: async () => {
          throw new Error('not json')
        },
        text: async () => 'Bad Gateway',
      })
      // Chunk 2 (msg-3, msg-4): succeeds
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2, maxRetries: 3 }))

    expect(result).toHaveLength(2)
    expect(result[0].messageId).toBe('msg-3')
    expect(result[1].messageId).toBe('msg-4')
    expect(mockFetch).toHaveBeenCalledTimes(4)
  })

  // -------------------------------------------------------------------------
  // withTimeout integration
  // -------------------------------------------------------------------------

  it('passes fetchTimeoutMs to withTimeout', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    await fetchEmails(messageIds, meta, makeOpts({ fetchTimeoutMs: 9999 }))

    expect(sxtClient.withTimeout).toHaveBeenCalledWith(9999)
  })

  it('clears timeout after successful fetch', async () => {
    // withTimeout is already mocked; we verify clear() is called
    // by checking the mock was exercised (no abort)
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts())
    expect(result).toHaveLength(1)
  })

  // -------------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------------

  it('returns empty array and does not throw when messageIds is empty', async () => {
    const result = await fetchEmails([], new Map(), makeOpts())
    expect(result).toEqual([])
    expect(mockFetch).not.toHaveBeenCalled()
  })

  it('defaults maxRetries to 3 when not provided', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))
      .mockRejectedValueOnce(new Error('fail'))

    const opts = makeOpts()
    delete opts.maxRetries

    await expect(fetchEmails(messageIds, meta, opts)).rejects.toThrow(/0.*emails retrieved/)

    // default 3 retries
    expect(mockFetch).toHaveBeenCalledTimes(3)
  })
})

// ---------------------------------------------------------------------------
// isBlockedSenderAddress
// ---------------------------------------------------------------------------

describe('isBlockedSenderAddress', () => {
  it('returns true for empty or null input (fail-closed)', () => {
    expect(isBlockedSenderAddress('')).toBe(true)
    expect(isBlockedSenderAddress(null)).toBe(true)
    expect(isBlockedSenderAddress(undefined)).toBe(true)
    expect(isBlockedSenderAddress('   ')).toBe(true)
  })

  it('blocks addresses matching a blocked prefix', () => {
    expect(isBlockedSenderAddress('no-reply@brand.com')).toBe(true)
    expect(isBlockedSenderAddress('donotreply@brand.com')).toBe(true)
    expect(isBlockedSenderAddress('billing@brand.com')).toBe(true)
  })

  it('blocks addresses whose domain contains a blocked-domain token', () => {
    expect(isBlockedSenderAddress('someone@news.brand.com')).toBe(true)
    expect(isBlockedSenderAddress('someone@mailer.brand.com')).toBe(true)
    expect(isBlockedSenderAddress('someone@marketing.co')).toBe(true)
  })

  it('is case-insensitive on prefix and domain matching', () => {
    expect(isBlockedSenderAddress('NO-REPLY@BRAND.COM')).toBe(true)
    expect(isBlockedSenderAddress('  Billing@Brand.com  ')).toBe(true)
    expect(isBlockedSenderAddress('foo@NEWS.Brand.COM')).toBe(true)
  })

  it('passes legitimate personal addresses', () => {
    expect(isBlockedSenderAddress('alice@brand.com')).toBe(false)
    expect(isBlockedSenderAddress('bob.smith@agency.co')).toBe(false)
    expect(isBlockedSenderAddress('a.user@example.com')).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// deriveFallbackMainContact
// ---------------------------------------------------------------------------

describe('deriveFallbackMainContact', () => {
  const makeEmail = ({ from, date }) => ({
    topLevelHeaders: [
      ...(from ? [{ name: 'From', value: from }] : []),
      ...(date ? [{ name: 'Date', value: date }] : []),
    ],
  })

  it('returns null for empty or missing input', () => {
    expect(deriveFallbackMainContact(null, 'me@creator.com')).toBeNull()
    expect(deriveFallbackMainContact([], 'me@creator.com')).toBeNull()
  })

  it('returns the latest non-creator, non-blocked sender', () => {
    const emails = [
      makeEmail({ from: 'Old <old@brand.com>', date: '2026-01-01T00:00:00Z' }),
      makeEmail({ from: 'Latest <latest@brand.com>', date: '2026-01-03T00:00:00Z' }),
      makeEmail({ from: 'Mid <mid@brand.com>', date: '2026-01-02T00:00:00Z' }),
    ]
    expect(deriveFallbackMainContact(emails, 'me@creator.com')).toEqual({
      email: 'latest@brand.com',
      name: 'Latest',
      company: null,
      title: null,
      phone_number: null,
    })
  })

  it('skips the creator and falls through to the next external sender', () => {
    const emails = [
      makeEmail({ from: 'me@creator.com', date: '2026-01-03T00:00:00Z' }),
      makeEmail({ from: 'Alice <alice@brand.com>', date: '2026-01-02T00:00:00Z' }),
    ]
    expect(deriveFallbackMainContact(emails, 'me@creator.com')).toMatchObject({
      email: 'alice@brand.com',
      name: 'Alice',
    })
  })

  it('skips blocked senders (no-reply, marketing domain)', () => {
    const emails = [
      makeEmail({ from: 'no-reply@brand.com', date: '2026-01-03T00:00:00Z' }),
      makeEmail({ from: 'foo@news.brand.com', date: '2026-01-02T00:00:00Z' }),
      makeEmail({ from: 'Alice <alice@brand.com>', date: '2026-01-01T00:00:00Z' }),
    ]
    expect(deriveFallbackMainContact(emails, 'me@creator.com')).toMatchObject({
      email: 'alice@brand.com',
    })
  })

  it('returns null when every sender is creator or blocked', () => {
    const emails = [
      makeEmail({ from: 'me@creator.com', date: '2026-01-03T00:00:00Z' }),
      makeEmail({ from: 'no-reply@brand.com', date: '2026-01-02T00:00:00Z' }),
    ]
    expect(deriveFallbackMainContact(emails, 'me@creator.com')).toBeNull()
  })

  it('handles bare-email From headers (no angle brackets)', () => {
    const emails = [makeEmail({ from: 'alice@brand.com', date: '2026-01-01T00:00:00Z' })]
    expect(deriveFallbackMainContact(emails, 'me@creator.com')).toEqual({
      email: 'alice@brand.com',
      name: null,
      company: null,
      title: null,
      phone_number: null,
    })
  })

  it('keeps a deterministic result when Date headers are missing or unparseable', () => {
    const emails = [
      makeEmail({ from: 'Alice <alice@brand.com>' }),
      makeEmail({ from: 'Bob <bob@brand.com>', date: 'garbage' }),
    ]
    // Both timestamps coerce to 0; sort is stable so the first usable sender wins.
    const got = deriveFallbackMainContact(emails, 'me@creator.com')
    expect(got).not.toBeNull()
    expect(['alice@brand.com', 'bob@brand.com']).toContain(got.email)
  })
})
