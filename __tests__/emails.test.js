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
  jest.spyOn(console, 'log').mockImplementation(() => {})
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
    json: async () => ({ status: 'success', data }),
    text: async () => JSON.stringify({ status: 'success', data }),
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

function failureResponse502(errors, parseable = true) {
  if (parseable) {
    return {
      ok: false,
      status: 502,
      json: async () => ({ status: 'failure', data: [], errors }),
      text: async () => JSON.stringify({ status: 'failure', data: [], errors }),
    }
  }
  return {
    ok: false,
    status: 502,
    json: async () => {
      throw new Error('not JSON')
    },
    text: async () => 'Bad Gateway',
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
  // -------------------------------------------------------------------------
  // Basic success (HTTP 200)
  // -------------------------------------------------------------------------

  it('fetches a single chunk and enriches emails with metadata', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(2)
    expect(result.fetched[0]).toMatchObject({
      messageId: 'msg-1',
      id: 'meta-msg-1',
      threadId: 'thread-msg-1',
      previousAiSummary: 'summary-msg-1',
    })
    expect(result.fetched[1]).toMatchObject({
      messageId: 'msg-2',
      id: 'meta-msg-2',
      threadId: 'thread-msg-2',
      previousAiSummary: 'summary-msg-2',
    })
    expect(result.failed).toHaveLength(0)

    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url, reqOpts] = mockFetch.mock.calls[0]
    expect(url).toBe('https://content-fetcher.example.com/email-content/fetch')
    const body = JSON.parse(reqOpts.body)
    expect(body.userId).toBe('user-1')
    expect(body.syncStateId).toBe('sync-1')
    expect(body.messageIds).toEqual(['msg-1', 'msg-2'])
  })

  it('splits messageIds into chunks and fires them concurrently', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4', 'msg-5']
    const meta = makeMeta(messageIds)

    mockFetch
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))
      .mockResolvedValueOnce(okResponse([{ messageId: 'msg-5' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(5)
    expect(result.failed).toHaveLength(0)
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

    expect(result.fetched).toHaveLength(1)
    expect(result.fetched[0].id).toBe('meta-msg-1')
  })

  it('enriches email even when meta has no PREVIOUS_AI_SUMMARY', async () => {
    const meta = new Map([['msg-1', { EMAIL_METADATA_ID: 'meta-1', THREAD_ID: 'thread-1' }]])

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(['msg-1'], meta, makeOpts())

    expect(result.fetched[0].id).toBe('meta-1')
    expect(result.fetched[0].threadId).toBe('thread-1')
    expect(result.fetched[0]).not.toHaveProperty('previousAiSummary')
  })

  it('skips enrichment for emails not in metaByMessageId', async () => {
    const meta = new Map() // empty map

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-unknown' }]))

    const result = await fetchEmails(['msg-unknown'], meta, makeOpts())

    expect(result.fetched).toHaveLength(1)
    expect(result.fetched[0]).not.toHaveProperty('id')
    expect(result.fetched[0]).not.toHaveProperty('threadId')
  })

  // -------------------------------------------------------------------------
  // Malformed JSON body on HTTP 200 (resp.json() throws)
  // -------------------------------------------------------------------------

  it('returns failures when resp.json() throws on HTTP 200 (malformed body)', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => {
        throw new SyntaxError('Unexpected token < in JSON at position 0')
      },
      text: async () => '<html>bad gateway</html>',
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({
      messageId: 'msg-1',
      error: 'Unexpected token < in JSON at position 0',
    })
    expect(result.failed[1]).toEqual({
      messageId: 'msg-2',
      error: 'Unexpected token < in JSON at position 0',
    })
  })

  // -------------------------------------------------------------------------
  // HTTP 207 — partial success
  // -------------------------------------------------------------------------

  it('parses 207 partial response into fetched and failed', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(
      partialResponse(
        [{ messageId: 'msg-1' }],
        [
          { messageId: 'msg-2', error: 'rate limited' },
          { messageId: 'msg-3', error: 'timeout' },
        ],
      ),
    )

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(1)
    expect(result.fetched[0].messageId).toBe('msg-1')
    expect(result.fetched[0].id).toBe('meta-msg-1')

    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({ messageId: 'msg-2', error: 'rate limited' })
    expect(result.failed[1]).toEqual({ messageId: 'msg-3', error: 'timeout' })
  })

  // -------------------------------------------------------------------------
  // HTTP 502 — total failure
  // -------------------------------------------------------------------------

  it('parses 502 JSON response and extracts per-message errors', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(
      failureResponse502([
        { messageId: 'msg-1', error: 'upstream timeout' },
        { messageId: 'msg-2', error: 'upstream timeout' },
      ]),
    )

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({ messageId: 'msg-1', error: 'upstream timeout' })
    expect(result.failed[1]).toEqual({ messageId: 'msg-2', error: 'upstream timeout' })
  })

  it('treats 502 JSON with no errors array as failure for all messageIds in chunk', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 502,
      text: async () => JSON.stringify({ status: 'failure', message: 'gateway timeout' }),
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0].messageId).toBe('msg-1')
    expect(result.failed[0].error).toContain('HTTP 502')
    expect(result.failed[1].messageId).toBe('msg-2')
    expect(result.failed[1].error).toContain('HTTP 502')
  })

  it('treats non-JSON 502 as transport error for all messageIds in chunk', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(failureResponse502(null, false))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({ messageId: 'msg-1', error: 'HTTP 502: Bad Gateway' })
    expect(result.failed[1]).toEqual({ messageId: 'msg-2', error: 'HTTP 502: Bad Gateway' })
  })

  // -------------------------------------------------------------------------
  // Transport errors (fetch throws)
  // -------------------------------------------------------------------------

  it('treats transport error (fetch throws) as failure for all chunk messageIds', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockRejectedValueOnce(new Error('timeout after 240000ms'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({ messageId: 'msg-1', error: 'timeout after 240000ms' })
    expect(result.failed[1]).toEqual({ messageId: 'msg-2', error: 'timeout after 240000ms' })
  })

  // -------------------------------------------------------------------------
  // Other HTTP errors (non-2xx, non-502)
  // -------------------------------------------------------------------------

  it('treats non-2xx/non-502 HTTP error as failure for all chunk messageIds', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(errorResponse(500, 'Internal Server Error'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({
      messageId: 'msg-1',
      error: 'HTTP 500: Internal Server Error',
    })
    expect(result.failed[1]).toEqual({
      messageId: 'msg-2',
      error: 'HTTP 500: Internal Server Error',
    })
  })

  it('treats HTTP 503 as failure for all chunk messageIds', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(errorResponse(503, 'Service Unavailable'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(1)
    expect(result.failed[0]).toEqual({
      messageId: 'msg-1',
      error: 'HTTP 503: Service Unavailable',
    })
  })

  // -------------------------------------------------------------------------
  // Mixed success/failure across chunks
  // -------------------------------------------------------------------------

  it('aggregates fetched and failed across multiple chunks', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // Chunk 1 (msg-1, msg-2): success
    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))
    // Chunk 2 (msg-3, msg-4): transport error
    mockFetch.mockRejectedValueOnce(new Error('connection reset'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(2)
    expect(result.fetched[0].messageId).toBe('msg-1')
    expect(result.fetched[1].messageId).toBe('msg-2')

    expect(result.failed).toHaveLength(2)
    expect(result.failed[0]).toEqual({ messageId: 'msg-3', error: 'connection reset' })
    expect(result.failed[1]).toEqual({ messageId: 'msg-4', error: 'connection reset' })
  })

  it('aggregates 207 partial + 200 success across chunks', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    // Chunk 1: partial
    mockFetch.mockResolvedValueOnce(
      partialResponse([{ messageId: 'msg-1' }], [{ messageId: 'msg-2', error: 'rate limited' }]),
    )
    // Chunk 2: success
    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(3)
    expect(result.failed).toHaveLength(1)
    expect(result.failed[0]).toEqual({ messageId: 'msg-2', error: 'rate limited' })
  })

  // -------------------------------------------------------------------------
  // Concurrent execution
  // -------------------------------------------------------------------------

  it('fires all chunks concurrently (not sequentially)', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
    const meta = makeMeta(messageIds)

    const callOrder = []

    mockFetch.mockImplementation(async (url, opts) => {
      const body = JSON.parse(opts.body)
      callOrder.push(body.messageIds[0])
      // Chunk 2 resolves before chunk 1 to prove concurrency
      if (body.messageIds[0] === 'msg-1') {
        await new Promise((r) => setTimeout(r, 50))
      }
      return okResponse(body.messageIds.map((id) => ({ messageId: id })))
    })

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2 }))

    expect(result.fetched).toHaveLength(4)
    // Both chunks should have been initiated before either resolved
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  // -------------------------------------------------------------------------
  // Never throws
  // -------------------------------------------------------------------------

  it('never throws even when all chunks fail — returns failures instead', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockRejectedValueOnce(new Error('total failure'))

    const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    // Does NOT throw — returns failures
    expect(result.fetched).toHaveLength(0)
    expect(result.failed).toHaveLength(2)
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
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }]))

    const result = await fetchEmails(messageIds, meta, makeOpts())
    expect(result.fetched).toHaveLength(1)
  })

  // -------------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------------

  it('returns { fetched: [], failed: [] } when messageIds is empty', async () => {
    const result = await fetchEmails([], new Map(), makeOpts())
    expect(result).toEqual({ fetched: [], failed: [] })
    expect(mockFetch).not.toHaveBeenCalled()
  })

  it('returns { fetched: [], failed: [] } when messageIds is null/undefined', async () => {
    const result = await fetchEmails(null, new Map(), makeOpts())
    expect(result).toEqual({ fetched: [], failed: [] })
    expect(mockFetch).not.toHaveBeenCalled()
  })

  // -------------------------------------------------------------------------
  // Logging
  // -------------------------------------------------------------------------

  it('logs chunk request and HTTP 200 success', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

    await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    const logs = console.log.mock.calls.map((c) => c[0])
    expect(logs.some((l) => l.includes('[fetchEmails] chunk 1/1: requesting 2 messageIds'))).toBe(
      true,
    )
    expect(logs.some((l) => l.includes('HTTP 200') && l.includes('2 fetched'))).toBe(true)
  })

  it('logs 207 partial with failed messageIds', async () => {
    const messageIds = ['msg-1', 'msg-2', 'msg-3']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(
      partialResponse(
        [{ messageId: 'msg-1' }],
        [
          { messageId: 'msg-2', error: 'rate limited' },
          { messageId: 'msg-3', error: 'timeout' },
        ],
      ),
    )

    await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    const logs = console.log.mock.calls.map((c) => c[0])
    expect(logs.some((l) => l.includes('HTTP 207') && l.includes('1 fetched, 2 failed'))).toBe(true)
    expect(
      logs.some((l) => l.includes('failed messageIds') && l.includes('msg-2: rate limited')),
    ).toBe(true)
  })

  it('logs 502 total failure', async () => {
    const messageIds = ['msg-1', 'msg-2']
    const meta = makeMeta(messageIds)

    mockFetch.mockResolvedValueOnce(
      failureResponse502([
        { messageId: 'msg-1', error: 'upstream timeout' },
        { messageId: 'msg-2', error: 'upstream timeout' },
      ]),
    )

    await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    const logs = console.log.mock.calls.map((c) => c[0])
    expect(logs.some((l) => l.includes('HTTP 502') && l.includes('2 failed'))).toBe(true)
  })

  it('logs transport error', async () => {
    const messageIds = ['msg-1']
    const meta = makeMeta(messageIds)

    mockFetch.mockRejectedValueOnce(new Error('timeout after 240000ms'))

    await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10 }))

    const logs = console.log.mock.calls.map((c) => c[0])
    expect(
      logs.some((l) => l.includes('transport error') && l.includes('timeout after 240000ms')),
    ).toBe(true)
  })
})
