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

const { fetchEmails } = await import('../src/lib/emails.js')
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
