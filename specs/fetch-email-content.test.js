import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest'

const mockOutputs = {}

vi.mock('@actions/core', () => {
  return {
    getInput: vi.fn(),
    setOutput: vi.fn((name, value) => {
      mockOutputs[name] = value
    }),
    setFailed: vi.fn(),
  }
})

import * as core from '@actions/core'
import { run } from '../fetch-email-content/src/main.js'

function makeMetadataRow(overrides = {}) {
  return {
    ID: 'sxt-id-1',
    MESSAGE_ID: 'gmail-msg-1',
    USER_ID: 'user-1',
    THREAD_ID: 'thread-1',
    PREVIOUS_AI_SUMMARY: null,
    EXISTING_DEAL_ID: null,
    ...overrides,
  }
}

function makeContentResponse(overrides = {}) {
  return {
    messageId: 'gmail-msg-1',
    topLevelHeaders: [
      { name: 'From', value: 'Jane <jane@example.com>' },
      { name: 'Subject', value: 'Deal Opportunity' },
    ],
    labelIds: ['INBOX'],
    body: '<p>Hello</p>',
    replyBody: 'Hello',
    ...overrides,
  }
}

function mockFetchSuccess(data) {
  return vi.fn().mockResolvedValue({
    ok: true,
    json: async () => ({ data }),
  })
}

describe('fetch-email-content', () => {
  let originalFetch

  beforeEach(() => {
    vi.clearAllMocks()
    Object.keys(mockOutputs).forEach((key) => delete mockOutputs[key])
    originalFetch = globalThis.fetch
  })

  afterEach(() => {
    globalThis.fetch = originalFetch
  })

  function setupInputs({ metadata, url, secret, fields }) {
    core.getInput.mockImplementation((name) => {
      const inputs = {
        metadata: metadata !== undefined ? metadata : '[]',
        'content-fetcher-url':
          url !== undefined ? url : 'https://content.example.com',
        'm2m-secret': secret !== undefined ? secret : 'test-m2m-token',
        fields: fields !== undefined ? fields : 'topLevelHeaders,body',
      }
      return inputs[name] || ''
    })
  }

  describe('single-user batch', () => {
    it('calls content fetcher and merges output with SxT fields', async () => {
      const row = makeMetadataRow({
        THREAD_ID: 'thread-abc',
        PREVIOUS_AI_SUMMARY: 'Previous summary',
        EXISTING_DEAL_ID: 'deal-xyz',
      })
      setupInputs({ metadata: JSON.stringify([row]) })

      const contentItem = makeContentResponse({ messageId: 'gmail-msg-1' })
      globalThis.fetch = mockFetchSuccess([contentItem])

      await run()

      // Verify fetch was called correctly
      expect(globalThis.fetch).toHaveBeenCalledTimes(1)
      const [fetchUrl, fetchOpts] = globalThis.fetch.mock.calls[0]
      expect(fetchUrl).toBe(
        'https://content.example.com/email-content/fetch-m2m',
      )
      expect(fetchOpts.method).toBe('POST')
      expect(fetchOpts.headers['Authorization']).toBe(
        'Bearer test-m2m-token',
      )
      expect(fetchOpts.headers['Content-Type']).toBe('application/json')
      const body = JSON.parse(fetchOpts.body)
      expect(body.userId).toBe('user-1')
      expect(body.messageIds).toEqual(['gmail-msg-1'])

      // Verify output
      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
      expect(emails[0].id).toBe('sxt-id-1')
      expect(emails[0].messageId).toBe('gmail-msg-1')
      expect(emails[0].userId).toBe('user-1')
      expect(emails[0].threadId).toBe('thread-abc')
      expect(emails[0].previousAiSummary).toBe('Previous summary')
      expect(emails[0].existingDealId).toBe('deal-xyz')
      expect(emails[0].topLevelHeaders).toEqual(contentItem.topLevelHeaders)
      expect(emails[0].labelIds).toEqual(['INBOX'])
      expect(emails[0].body).toBe('<p>Hello</p>')
      expect(emails[0].replyBody).toBe('Hello')
      expect(mockOutputs.failed_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })
  })

  describe('multi-user batch', () => {
    it('makes separate calls per user and merges results', async () => {
      const row1 = makeMetadataRow({
        ID: 'sxt-1',
        MESSAGE_ID: 'msg-a',
        USER_ID: 'user-alpha',
      })
      const row2 = makeMetadataRow({
        ID: 'sxt-2',
        MESSAGE_ID: 'msg-b',
        USER_ID: 'user-beta',
      })
      const row3 = makeMetadataRow({
        ID: 'sxt-3',
        MESSAGE_ID: 'msg-c',
        USER_ID: 'user-alpha',
      })
      setupInputs({ metadata: JSON.stringify([row1, row2, row3]) })

      const contentAlpha = [
        makeContentResponse({ messageId: 'msg-a' }),
        makeContentResponse({ messageId: 'msg-c' }),
      ]
      const contentBeta = [makeContentResponse({ messageId: 'msg-b' })]

      let callCount = 0
      globalThis.fetch = vi.fn().mockImplementation(async (_url, opts) => {
        callCount++
        const body = JSON.parse(opts.body)
        const data = body.userId === 'user-alpha' ? contentAlpha : contentBeta
        return { ok: true, json: async () => ({ data }) }
      })

      await run()

      expect(globalThis.fetch).toHaveBeenCalledTimes(2)

      // Verify first call is for user-alpha (appears first)
      const call1Body = JSON.parse(globalThis.fetch.mock.calls[0][1].body)
      expect(call1Body.userId).toBe('user-alpha')
      expect(call1Body.messageIds).toEqual(['msg-a', 'msg-c'])

      // Verify second call is for user-beta
      const call2Body = JSON.parse(globalThis.fetch.mock.calls[1][1].body)
      expect(call2Body.userId).toBe('user-beta')
      expect(call2Body.messageIds).toEqual(['msg-b'])

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(3)
      expect(emails.map((e) => e.id)).toEqual(['sxt-1', 'sxt-3', 'sxt-2'])
      expect(mockOutputs.success).toBe('true')
    })
  })

  describe('join on MESSAGE_ID', () => {
    it('joins on MESSAGE_ID not on any other field', async () => {
      const row = makeMetadataRow({
        ID: 'sxt-row-id',
        MESSAGE_ID: 'the-gmail-id',
      })
      setupInputs({ metadata: JSON.stringify([row]) })

      // Content fetcher returns messageId matching MESSAGE_ID
      const contentItem = makeContentResponse({ messageId: 'the-gmail-id' })
      globalThis.fetch = mockFetchSuccess([contentItem])

      await run()

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
      // SxT ID is carried through as id
      expect(emails[0].id).toBe('sxt-row-id')
      // Gmail messageId is preserved
      expect(emails[0].messageId).toBe('the-gmail-id')
    })
  })

  describe('missing content for a messageId', () => {
    it('adds ID to failed_ids as SQL-quoted', async () => {
      const row1 = makeMetadataRow({ ID: 'found-id', MESSAGE_ID: 'msg-found' })
      const row2 = makeMetadataRow({
        ID: 'missing-id',
        MESSAGE_ID: 'msg-missing',
      })
      setupInputs({ metadata: JSON.stringify([row1, row2]) })

      // Content fetcher only returns data for msg-found
      const contentItem = makeContentResponse({ messageId: 'msg-found' })
      globalThis.fetch = mockFetchSuccess([contentItem])

      await run()

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
      expect(emails[0].id).toBe('found-id')
      expect(mockOutputs.failed_ids).toBe("'missing-id'")
      expect(mockOutputs.success).toBe('true')
    })

    it('handles multiple missing IDs', async () => {
      const rows = [
        makeMetadataRow({ ID: 'ok-1', MESSAGE_ID: 'msg-ok' }),
        makeMetadataRow({ ID: 'fail-1', MESSAGE_ID: 'msg-fail-1' }),
        makeMetadataRow({ ID: 'fail-2', MESSAGE_ID: 'msg-fail-2' }),
      ]
      setupInputs({ metadata: JSON.stringify(rows) })

      globalThis.fetch = mockFetchSuccess([
        makeContentResponse({ messageId: 'msg-ok' }),
      ])

      await run()

      expect(mockOutputs.failed_ids).toBe("'fail-1','fail-2'")
      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
    })
  })

  describe('empty metadata', () => {
    it('returns empty output with success true for empty array', async () => {
      setupInputs({ metadata: '[]' })

      await run()

      expect(mockOutputs.emails).toBe('[]')
      expect(mockOutputs.failed_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })

    it('returns empty output with success true for empty string', async () => {
      setupInputs({ metadata: '' })

      await run()

      expect(mockOutputs.emails).toBe('[]')
      expect(mockOutputs.failed_ids).toBe('')
      expect(mockOutputs.success).toBe('true')
    })
  })

  describe('content fetcher 500 error', () => {
    it('retries then fails on persistent 500', async () => {
      const row = makeMetadataRow()
      setupInputs({ metadata: JSON.stringify([row]) })

      globalThis.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        text: async () => 'Internal Server Error',
      })

      await run()

      // 1 initial + 3 retries = 4 calls
      expect(globalThis.fetch).toHaveBeenCalledTimes(4)
      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalledWith(
        expect.stringContaining('Content fetcher HTTP 500'),
      )
    }, 30000)

    it('does not retry on 400 error', async () => {
      const row = makeMetadataRow()
      setupInputs({ metadata: JSON.stringify([row]) })

      globalThis.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        text: async () => 'Bad Request',
      })

      await run()

      expect(globalThis.fetch).toHaveBeenCalledTimes(1)
      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalledWith(
        expect.stringContaining('Content fetcher HTTP 400'),
      )
    })
  })

  describe('content fetcher response wrapper', () => {
    it('unwraps { data: [...] } wrapper correctly', async () => {
      const row = makeMetadataRow()
      setupInputs({ metadata: JSON.stringify([row]) })

      const contentItem = makeContentResponse()
      globalThis.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ data: [contentItem] }),
      })

      await run()

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
      expect(emails[0].messageId).toBe('gmail-msg-1')
    })

    it('handles bare array response (no wrapper)', async () => {
      const row = makeMetadataRow()
      setupInputs({ metadata: JSON.stringify([row]) })

      const contentItem = makeContentResponse()
      globalThis.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: async () => [contentItem],
      })

      await run()

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails).toHaveLength(1)
      expect(emails[0].messageId).toBe('gmail-msg-1')
    })
  })

  describe('fields passed correctly', () => {
    it('sends parsed fields array in request body', async () => {
      const row = makeMetadataRow()
      setupInputs({
        metadata: JSON.stringify([row]),
        fields: 'topLevelHeaders, body, replyBody, labelIds',
      })

      globalThis.fetch = mockFetchSuccess([makeContentResponse()])

      await run()

      const body = JSON.parse(globalThis.fetch.mock.calls[0][1].body)
      expect(body.fields).toEqual([
        'topLevelHeaders',
        'body',
        'replyBody',
        'labelIds',
      ])
    })
  })

  describe('ID sanitization', () => {
    it('allows valid alphanumeric IDs with hyphens and underscores', async () => {
      const row = makeMetadataRow({
        ID: 'abc-123_DEF',
        MESSAGE_ID: 'msg-1',
      })
      setupInputs({ metadata: JSON.stringify([row]) })

      // Return no content so ID goes to failed_ids (exercises sanitizeId)
      globalThis.fetch = mockFetchSuccess([])

      await run()

      expect(mockOutputs.failed_ids).toBe("'abc-123_DEF'")
      expect(mockOutputs.success).toBe('true')
    })

    it('rejects IDs with SQL injection characters', async () => {
      const row = makeMetadataRow({
        ID: "'; DROP TABLE",
        MESSAGE_ID: 'msg-1',
      })
      setupInputs({ metadata: JSON.stringify([row]) })

      globalThis.fetch = mockFetchSuccess([])

      await run()

      expect(mockOutputs.success).toBe('false')
      expect(core.setFailed).toHaveBeenCalledWith(
        expect.stringContaining('Invalid ID'),
      )
    })
  })

  describe('optional SxT fields', () => {
    it('omits threadId, previousAiSummary, existingDealId when null', async () => {
      const row = makeMetadataRow({
        THREAD_ID: null,
        PREVIOUS_AI_SUMMARY: null,
        EXISTING_DEAL_ID: null,
      })
      setupInputs({ metadata: JSON.stringify([row]) })

      globalThis.fetch = mockFetchSuccess([makeContentResponse()])

      await run()

      const emails = JSON.parse(mockOutputs.emails)
      expect(emails[0].threadId).toBeUndefined()
      expect(emails[0].previousAiSummary).toBeUndefined()
      expect(emails[0].existingDealId).toBeUndefined()
    })
  })
})
