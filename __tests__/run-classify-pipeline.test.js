import { jest } from '@jest/globals'

// ============================================================
// Mocks
// ============================================================

const outputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

// Mock uuid so we get deterministic IDs
let uuidCallCount = 0
jest.unstable_mockModule('uuid', () => ({
  v7: jest.fn(() => {
    uuidCallCount++
    return `test-uuid-${uuidCallCount}`
  }),
}))

// Mock sxt-client
const mockAuthenticate = jest.fn()
const mockExecuteSql = jest.fn()
const mockAcquireRateLimitToken = jest.fn().mockResolvedValue(undefined)
jest.unstable_mockModule('../src/lib/db.js', () => ({
  authenticate: mockAuthenticate,
  executeSql: mockExecuteSql,
  acquireRateLimitToken: mockAcquireRateLimitToken,
  logSqlStats: jest.fn(),
  getSqlStats: jest.fn(() => ({ calls: 0, totalMs: 0, slowest: 0, slowestSql: '', avgMs: 0 })),
  withTimeout: jest.fn(() => ({
    signal: new AbortController().signal,
    clear: jest.fn(),
  })),
}))

// Mock emails
const mockFetchEmails = jest.fn()
const mockDeriveFallbackMainContact = jest.fn(() => null)
const mockIsBlockedSenderAddress = jest.fn(() => false)
jest.unstable_mockModule('../src/lib/emails.js', () => ({
  fetchEmails: mockFetchEmails,
  deriveFallbackMainContact: mockDeriveFallbackMainContact,
  isBlockedSenderAddress: mockIsBlockedSenderAddress,
}))

// Mock ai
const mockCallModel = jest.fn()
const mockParseAndValidate = jest.fn()
const mockBuildPrompt = jest.fn()
jest.unstable_mockModule('../src/lib/ai.js', () => ({
  callModel: mockCallModel,
  parseAndValidate: mockParseAndValidate,
  buildPrompt: mockBuildPrompt,
  VALID_CATEGORIES: new Set([
    'new',
    'in_progress',
    'completed',
    'not_interested',
    'likely_scam',
    'low_confidence',
  ]),
  VALID_DEAL_TYPES: new Set([
    'brand_collaboration',
    'sponsorship',
    'affiliate',
    'product_seeding',
    'ambassador',
    'content_partnership',
    'paid_placement',
    'other_business',
  ]),
}))

// Mock pipeline
const mockRunPool = jest.fn()
const mockInsertBatchEvent = jest.fn()
const mockSweepStuckRows = jest.fn().mockResolvedValue(0)
const mockSweepOrphanedRows = jest.fn().mockResolvedValue(0)
jest.unstable_mockModule('../src/lib/pipeline.js', () => ({
  runPool: mockRunPool,
  insertBatchEvent: mockInsertBatchEvent,
  sweepStuckRows: mockSweepStuckRows,
  sweepOrphanedRows: mockSweepOrphanedRows,
}))

// Mock WriteBatcher
const mockBatcherInstance = {
  pushEvals: jest.fn().mockResolvedValue(undefined),
  pushDealDeletes: jest.fn().mockResolvedValue(undefined),
  pushDeals: jest.fn().mockResolvedValue(undefined),
  pushContactDeletes: jest.fn().mockResolvedValue(undefined),
  pushContacts: jest.fn().mockResolvedValue(undefined),
  pushCoreContacts: jest.fn().mockResolvedValue(undefined),
  pushStateUpdates: jest.fn().mockResolvedValue(undefined),
  pushBatchEvents: jest.fn().mockResolvedValue(undefined),
  drain: jest.fn().mockResolvedValue(undefined),
  stop: jest.fn(),
}
const MockWriteBatcher = jest.fn(() => mockBatcherInstance)
jest.unstable_mockModule('../src/lib/batcher.js', () => ({
  WriteBatcher: MockWriteBatcher,
}))

const core = await import('@actions/core')
const { runClassifyPipeline } = await import('../src/commands/run-classify-pipeline.js')

// ============================================================
// Helpers
// ============================================================

function mockInputs(overrides = {}) {
  const defaults = {
    'sxt-auth-url': 'https://auth.example.com/token',
    'sxt-auth-secret': 'test-secret',
    'sxt-api-url': 'https://sxt.example.com',
    'sxt-biscuit': 'test-biscuit',
    'sxt-schema': 'dealsync_stg_v1',
    'email-content-fetcher-url': 'https://fetcher.example.com',
    'ai-api-key': 'test-hyp-key',
    'ai-primary-model': 'TestPrimary/Model',
    'ai-fallback-model': 'TestFallback/Model',
    'ai-api-url': 'https://ai.example.com/v1/chat/completions',
    'pipeline-max-concurrent': '3',
    'pipeline-classify-batch-size': '5',
    'pipeline-max-retries': '3',
    'pipeline-fetch-chunk-size': '10',
    'pipeline-fetch-timeout-ms': '120000',
    'pipeline-flush-interval-ms': '5000',
    'pipeline-flush-threshold': '10',
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

function makeBatchRows(count = 2) {
  return Array.from({ length: count }, (_, i) => ({
    EMAIL_METADATA_ID: `em-${i + 1}`,
    MESSAGE_ID: `msg-${i + 1}`,
    USER_ID: 'user-1',
    THREAD_ID: `thread-${i + 1}`,
    CREATOR_EMAIL: 'creator@test.com',
    SYNC_STATE_ID: 'ss-1',
  }))
}

function makeThreads(rows, { allDeals = false } = {}) {
  return rows.reduce((acc, r) => {
    if (!acc.find((t) => t.thread_id === r.THREAD_ID)) {
      acc.push({
        thread_id: r.THREAD_ID,
        is_deal: allDeals || r.THREAD_ID === 'thread-1',
        ai_score: 8,
        ai_summary: `Summary for ${r.THREAD_ID}`,
        category: allDeals || r.THREAD_ID === 'thread-1' ? 'new' : null,
        deal_name: allDeals || r.THREAD_ID === 'thread-1' ? 'Test Deal' : null,
        deal_type: 'brand_collaboration',
        deal_value: '1000',
        currency: 'USD',
        main_contact:
          allDeals || r.THREAD_ID === 'thread-1'
            ? {
                name: 'Alice',
                email: 'alice@co.com',
                company: 'TestCo',
                title: 'CEO',
                phone_number: '555-1234',
              }
            : null,
      })
    }
    return acc
  }, [])
}

// ============================================================
// Tests
// ============================================================

describe('run-classify-pipeline command', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockExecuteSql.mockReset()
    uuidCallCount = 0
    for (const key of Object.keys(outputs)) delete outputs[key]
    mockAuthenticate.mockResolvedValue('test-jwt')
    mockInsertBatchEvent.mockResolvedValue(undefined)
    // Reset emails.js mocks so prior-test mockReturnValue cannot leak
    // into the next test (jest.clearAllMocks only clears call history).
    mockDeriveFallbackMainContact.mockReset().mockReturnValue(null)
    mockIsBlockedSenderAddress.mockReset().mockReturnValue(false)
    // Reset batcher mock
    mockBatcherInstance.pushEvals.mockResolvedValue(undefined)
    mockBatcherInstance.pushDealDeletes.mockResolvedValue(undefined)
    mockBatcherInstance.pushDeals.mockResolvedValue(undefined)
    mockBatcherInstance.pushContactDeletes.mockResolvedValue(undefined)
    mockBatcherInstance.pushContacts.mockResolvedValue(undefined)
    mockBatcherInstance.pushCoreContacts.mockResolvedValue(undefined)
    mockBatcherInstance.pushStateUpdates.mockResolvedValue(undefined)
    mockBatcherInstance.pushBatchEvents.mockResolvedValue(undefined)
    mockBatcherInstance.drain.mockResolvedValue(undefined)
    mockSweepStuckRows.mockResolvedValue(0)
    mockSweepOrphanedRows.mockResolvedValue(0)
  })

  // ----------------------------------------------------------
  // WriteBatcher is created with correct params
  // ----------------------------------------------------------

  it('creates WriteBatcher with correct params', async () => {
    mockInputs({ 'pipeline-flush-interval-ms': '3000', 'pipeline-flush-threshold': '20' })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    expect(MockWriteBatcher).toHaveBeenCalledTimes(1)
    expect(MockWriteBatcher).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
      flushIntervalMs: 3000,
      flushThreshold: 20,
      coreSchema: 'EMAIL_CORE_STAGING',
    })
  })

  // ----------------------------------------------------------
  // drain() called after runPool
  // ----------------------------------------------------------

  it('calls batcher.drain() after runPool completes', async () => {
    mockInputs()

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    expect(mockBatcherInstance.drain).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // Full pipeline flow (claim -> classify -> save chain)
  // ----------------------------------------------------------

  it('runs the full pipeline: claim, classify, save evals, save deals, save contacts, update states, complete', async () => {
    mockInputs()

    const rows = makeBatchRows(3)
    const threads = makeThreads(rows)

    mockRunPool.mockImplementation(async (claimFn, workerFn, opts) => {
      // --- Claim phase ---
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()
      expect(batch).not.toBeNull()
      expect(batch.batch_id).toBe('test-uuid-1')
      expect(batch.count).toBe(3)
      expect(batch.rows).toEqual(rows)

      // --- Worker phase ---
      // Step 2: Check existing audit (none)
      mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

      // Step 3: AI classification
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test email body',
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[{"thread_id":"thread-1"}]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      // Save audit checkpoint
      mockExecuteSql.mockResolvedValueOnce([]) // insertAudit

      await workerFn(batch, { attempt: 0 })

      // Verify batcher was used for all save operations
      expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
      expect(mockBatcherInstance.pushDealDeletes).toHaveBeenCalledTimes(1) // non-deal threads
      expect(mockBatcherInstance.pushDeals).toHaveBeenCalledTimes(1) // deal threads
      expect(mockBatcherInstance.pushCoreContacts).toHaveBeenCalledTimes(1) // core contacts
      expect(mockBatcherInstance.pushContacts).toHaveBeenCalledTimes(1) // deal contacts
      expect(mockBatcherInstance.pushContactDeletes).not.toHaveBeenCalled() // removed: ON CONFLICT handles it
      expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)

      return { processed: 1, failed: 0 }
    })

    const result = await runClassifyPipeline()

    expect(result).toEqual({
      batches_processed: 1,
      batches_failed: 0,
      stuck_failed: 0,
      orphan_failed: 0,
    })

    // Verify authentication
    expect(mockAuthenticate).toHaveBeenCalledTimes(1)
    expect(mockAuthenticate).toHaveBeenCalledWith('https://auth.example.com/token', 'test-secret')

    // Verify batch event for claim
    expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
      triggerHash: 'test-uuid-1',
      batchId: 'test-uuid-1',
      batchType: 'classify',
      eventType: 'new',
    })
  })

  // ----------------------------------------------------------
  // Retry case: existing audit in DB -> skip AI, use in-memory
  // ----------------------------------------------------------

  it('skips AI when existing audit found and uses cached threads', async () => {
    mockInputs()

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows)
    const cachedAudit = { threads }

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Claim phase
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()

      // Step 2: Check existing audit (found!)
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify(cachedAudit) }])

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI should NOT have been called
    expect(mockFetchEmails).not.toHaveBeenCalled()
    expect(mockCallModel).not.toHaveBeenCalled()
    expect(mockBuildPrompt).not.toHaveBeenCalled()
    expect(mockParseAndValidate).not.toHaveBeenCalled()

    // But batcher should still have been used
    expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // Step 6a: main_contact fallback (Copilot review coverage)
  // ----------------------------------------------------------

  it('Step 6a Pass 1: calls deriveFallbackMainContact and writes deal_contacts when AI main_contact is null', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 8,
        ai_summary: 'Deal without AI contact',
        category: 'new',
        deal_name: 'Outreach',
        deal_type: 'brand_collaboration',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
    ]

    const fallbackMc = {
      email: 'gatsby@protonmail.com',
      name: 'Gatsby',
      company: null,
      title: null,
      phone_number: null,
    }
    mockDeriveFallbackMainContact.mockReturnValue(fallbackMc)
    mockIsBlockedSenderAddress.mockReturnValue(false)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // audit empty
      const emails = [
        {
          messageId: rows[0].MESSAGE_ID,
          threadId: 'thread-1',
          body: 'Thanks for reaching out',
          topLevelHeaders: [
            { name: 'From', value: 'Gatsby <gatsby@protonmail.com>' },
            { name: 'Date', value: '2026-01-02T00:00:00Z' },
          ],
        },
      ]
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds — no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValueOnce([]) // insert audit

      await workerFn(batch, { attempt: 0 })

      expect(mockDeriveFallbackMainContact).toHaveBeenCalled()
      const derivedArg = mockDeriveFallbackMainContact.mock.calls[0][0]
      expect(Array.isArray(derivedArg)).toBe(true)
      expect(derivedArg.length).toBeGreaterThan(0)

      expect(mockBatcherInstance.pushContacts).toHaveBeenCalled()
      const contactTuples = mockBatcherInstance.pushContacts.mock.calls[0][0]
      expect(contactTuples.some((t) => t.includes('gatsby@protonmail.com'))).toBe(true)

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  it('Step 6a Pass 2: EMAIL_SENDERS lookup when audit cache skips fetch (allEmails null)', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 7,
        ai_summary: 'Cached deal',
        category: 'new',
        deal_name: 'Cached',
        deal_type: 'sponsorship',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
    ]
    const cachedAudit = { threads }

    mockDeriveFallbackMainContact.mockClear()
    mockIsBlockedSenderAddress.mockReturnValue(false)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // claim UPDATE
        .mockResolvedValueOnce(rows) // claim SELECT
      const batch = await claimFn()

      mockExecuteSql
        .mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify(cachedAudit) }]) // selectByBatch audit
        .mockResolvedValueOnce([
          {
            THREAD_ID: 'thread-1',
            RECEIVED_AT: '2026-01-03T00:00:00Z',
            SENDER_EMAIL: 'fromdb@example.com',
            SENDER_NAME: 'Db Sender',
          },
        ]) // EMAIL_METADATA + EMAIL_SENDERS fallback
        .mockResolvedValueOnce([]) // updateStatusByIds → deal

      await workerFn(batch, { attempt: 0 })

      expect(mockFetchEmails).not.toHaveBeenCalled()
      expect(mockDeriveFallbackMainContact).not.toHaveBeenCalled()

      const senderSqlCalls = mockExecuteSql.mock.calls.filter(
        (c) => typeof c[3] === 'string' && c[3].includes('EMAIL_SENDERS'),
      )
      expect(senderSqlCalls.length).toBeGreaterThanOrEqual(1)
      expect(senderSqlCalls[0][3]).toContain("em.USER_ID = 'user-1'")
      expect(senderSqlCalls[0][3]).toMatch(/LIMIT 500\b/)

      expect(mockBatcherInstance.pushContacts).toHaveBeenCalled()
      const contactTuples = mockBatcherInstance.pushContacts.mock.calls[0][0]
      expect(contactTuples.some((t) => t.includes('fromdb@example.com'))).toBe(true)

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  it('Step 6a mixed batch: Pass 1 resolves one thread, Pass 2 resolves another', async () => {
    mockInputs()

    const rows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-1',
        CREATOR_EMAIL: 'creator@test.com',
        SYNC_STATE_ID: 'ss-1',
      },
      {
        EMAIL_METADATA_ID: 'em-2',
        MESSAGE_ID: 'msg-2',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-2',
        CREATOR_EMAIL: 'creator@test.com',
        SYNC_STATE_ID: 'ss-1',
      },
    ]
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 8,
        ai_summary: 'Fresh',
        category: 'new',
        deal_name: 'D1',
        deal_type: 'brand_collaboration',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
      {
        thread_id: 'thread-2',
        is_deal: true,
        ai_score: 8,
        ai_summary: 'Fresh',
        category: 'new',
        deal_name: 'D2',
        deal_type: 'brand_collaboration',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
    ]

    // Pass 1 resolves thread-1 only; thread-2 returns null and must fall through to Pass 2.
    mockDeriveFallbackMainContact.mockImplementation((threadEmails) => {
      const first = threadEmails?.[0]
      const threadId = first?.threadId
      if (threadId === 'thread-1') {
        return {
          email: 'alice@brand.com',
          name: 'Alice',
          company: null,
          title: null,
          phone_number: null,
        }
      }
      return null
    })

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // audit empty → fresh classification
      mockFetchEmails.mockResolvedValueOnce([
        {
          messageId: 'msg-1',
          threadId: 'thread-1',
          body: 'b',
          topLevelHeaders: [{ name: 'From', value: 'Alice <alice@brand.com>' }],
        },
        {
          messageId: 'msg-2',
          threadId: 'thread-2',
          body: 'b',
          topLevelHeaders: [{ name: 'From', value: 'creator@test.com' }],
        },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 's', userPrompt: 'u' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValueOnce([]) // insert audit
      // Pass 2 runs for thread-2 only (one EMAIL_SENDERS query).
      mockExecuteSql.mockResolvedValueOnce([
        {
          THREAD_ID: 'thread-2',
          RECEIVED_AT: '2026-01-03T00:00:00Z',
          SENDER_EMAIL: 'bob@brand.com',
          SENDER_NAME: 'Bob',
        },
      ])

      await workerFn(batch, { attempt: 0 })

      expect(mockDeriveFallbackMainContact).toHaveBeenCalledTimes(2)

      const senderSqlCalls = mockExecuteSql.mock.calls.filter(
        (c) => typeof c[3] === 'string' && c[3].includes('EMAIL_SENDERS'),
      )
      expect(senderSqlCalls.length).toBe(1)
      expect(senderSqlCalls[0][3]).toContain("em.THREAD_ID = 'thread-2'")
      expect(senderSqlCalls[0][3]).toContain("em.USER_ID = 'user-1'")
      expect(senderSqlCalls[0][3]).not.toContain("'thread-1'")

      expect(mockBatcherInstance.pushContacts).toHaveBeenCalled()
      const contactTuples = mockBatcherInstance.pushContacts.mock.calls[0][0]
      expect(contactTuples.some((t) => t.includes('alice@brand.com'))).toBe(true)
      expect(contactTuples.some((t) => t.includes('bob@brand.com'))).toBe(true)

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  it('Step 6a both passes fail: main_contact stays unusable, no deal contact persisted', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 7,
        ai_summary: 'Cached deal',
        category: 'new',
        deal_name: 'Cached',
        deal_type: 'sponsorship',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
    ]
    const cachedAudit = { threads }

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()

      mockExecuteSql
        .mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify(cachedAudit) }]) // audit cache hit
        .mockResolvedValueOnce([]) // EMAIL_SENDERS returns nothing
        .mockResolvedValueOnce([]) // updateStatusByIds

      await workerFn(batch, { attempt: 0 })

      // Pass 1 skipped (allEmails null on cache-hit). Pass 2 ran and found no sender.
      expect(mockFetchEmails).not.toHaveBeenCalled()
      expect(mockDeriveFallbackMainContact).not.toHaveBeenCalled()
      const senderSqlCalls = mockExecuteSql.mock.calls.filter(
        (c) => typeof c[3] === 'string' && c[3].includes('EMAIL_SENDERS'),
      )
      expect(senderSqlCalls.length).toBe(1)

      // Deals upsert still runs (Step 5 is unconditional) but no contact row is pushed.
      expect(mockBatcherInstance.pushDeals).toHaveBeenCalled()
      expect(mockBatcherInstance.pushContacts).not.toHaveBeenCalled()
      expect(mockBatcherInstance.pushCoreContacts).not.toHaveBeenCalled()

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  it('Step 6a creator-only senders: Pass 2 filters them, contact not persisted', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 7,
        ai_summary: 'Self thread',
        category: 'new',
        deal_name: 'Self',
        deal_type: 'sponsorship',
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
    ]
    const cachedAudit = { threads }

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()

      mockExecuteSql
        .mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify(cachedAudit) }]) // audit cache hit
        .mockResolvedValueOnce([
          {
            THREAD_ID: 'thread-1',
            RECEIVED_AT: '2026-01-03T00:00:00Z',
            SENDER_EMAIL: 'CREATOR@test.com', // uppercase — match must be case-insensitive
            SENDER_NAME: 'Creator',
          },
          {
            THREAD_ID: 'thread-1',
            RECEIVED_AT: '2026-01-02T00:00:00Z',
            SENDER_EMAIL: 'creator@test.com',
            SENDER_NAME: 'Creator',
          },
        ])
        .mockResolvedValueOnce([]) // updateStatusByIds

      await workerFn(batch, { attempt: 0 })

      expect(mockBatcherInstance.pushContacts).not.toHaveBeenCalled()
      expect(mockBatcherInstance.pushCoreContacts).not.toHaveBeenCalled()

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Empty batch (claim returns null)
  // ----------------------------------------------------------

  it('handles empty batches when claim returns null immediately', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Claim returns null: no pending rows and no stuck batches
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce([]) // SELECT claimed — empty
        .mockResolvedValueOnce([]) // stuck batches — empty

      const batch = await claimFn()
      expect(batch).toBeNull()

      return { processed: 0, failed: 0 }
    })

    const result = await runClassifyPipeline()

    expect(result).toEqual({
      batches_processed: 0,
      batches_failed: 0,
      stuck_failed: 0,
      orphan_failed: 0,
    })

    // No pipeline work should have been done
    expect(mockFetchEmails).not.toHaveBeenCalled()
    expect(mockCallModel).not.toHaveBeenCalled()
  })

  // ----------------------------------------------------------
  // Correct params passed to each step
  // ----------------------------------------------------------

  it('passes correct params to fetchEmails (no format = full content)', async () => {
    mockInputs({ 'pipeline-fetch-chunk-size': '15', 'pipeline-fetch-timeout-ms': '60000' })

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // No existing audit
      mockExecuteSql.mockResolvedValueOnce([])

      // Set up AI pipeline
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test',
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      // Rest of pipeline
      mockExecuteSql.mockResolvedValue([]) // all remaining SQL calls

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockFetchEmails).toHaveBeenCalledTimes(1)
    const [messageIds, metaMap, opts] = mockFetchEmails.mock.calls[0]

    expect(messageIds).toEqual(['msg-1', 'msg-2'])
    expect(metaMap).toBeInstanceOf(Map)
    expect(metaMap.get('msg-1')).toEqual(rows[0])
    expect(opts).toEqual({
      contentFetcherUrl: 'https://fetcher.example.com',
      emailProvider: '',
      emailServiceUrl: '',
      userId: 'user-1',
      syncStateId: 'ss-1',
      chunkSize: 15,
      fetchTimeoutMs: 60000,
    })
    // No format param (full content for classify)
    expect(opts.format).toBeUndefined()
  })

  it('passes correct params to callModel for primary model', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = makeThreads(rows, { allDeals: true })

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // no existing audit

      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({
        systemPrompt: 'system-prompt',
        userPrompt: 'user-prompt',
      })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      mockExecuteSql.mockResolvedValue([]) // all remaining

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockCallModel).toHaveBeenCalledTimes(1)
    expect(mockCallModel).toHaveBeenCalledWith(
      'TestPrimary/Model',
      [
        { role: 'system', content: 'system-prompt' },
        { role: 'user', content: 'user-prompt' },
      ],
      {
        temperature: 0,
        apiUrl: 'https://ai.example.com/v1/chat/completions',
        apiKey: 'test-hyp-key',
      },
    )
  })

  it('passes correct options to runPool', async () => {
    mockInputs({ 'pipeline-max-concurrent': '5', 'pipeline-max-retries': '7' })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    expect(mockRunPool).toHaveBeenCalledTimes(1)
    const [claimFn, workerFn, opts] = mockRunPool.mock.calls[0]
    expect(typeof claimFn).toBe('function')
    expect(typeof workerFn).toBe('function')
    expect(opts).toEqual({
      maxConcurrent: 5,
      maxRetries: 7,
      onDeadLetter: expect.any(Function),
    })
  })

  // ----------------------------------------------------------
  // AI resilience: fallback model
  // ----------------------------------------------------------

  it('falls back to fallback model when primary fails', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      { thread_id: 'thread-1', is_deal: false, ai_score: 5, ai_summary: 'test', category: null },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // no existing audit

      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })

      // Primary model fails completely
      mockCallModel.mockRejectedValueOnce(new Error('Primary API down'))

      // Fallback model succeeds
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      mockExecuteSql.mockResolvedValue([]) // all remaining

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockCallModel).toHaveBeenCalledTimes(2)
    // Second call should be the fallback model with temperature 0.6
    expect(mockCallModel.mock.calls[1][0]).toBe('TestFallback/Model')
    expect(mockCallModel.mock.calls[1][2].temperature).toBe(0.6)
  })

  // ----------------------------------------------------------
  // AI resilience: corrective retry (Layer 2)
  // ----------------------------------------------------------

  it('uses corrective retry when primary parse fails', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      { thread_id: 'thread-1', is_deal: false, ai_score: 5, ai_summary: 'test', category: null },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // no existing audit

      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })

      // Primary model returns content but it can't be parsed
      mockCallModel.mockResolvedValueOnce({ content: 'broken json' })
      mockParseAndValidate.mockImplementationOnce(() => {
        throw new Error('No valid JSON found')
      })

      // Corrective retry succeeds
      mockCallModel.mockResolvedValueOnce({ content: '[{"thread_id":"thread-1"}]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      mockExecuteSql.mockResolvedValue([]) // all remaining

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockCallModel).toHaveBeenCalledTimes(2)
    // The corrective retry should include the broken output as assistant message
    const correctiveCall = mockCallModel.mock.calls[1]
    expect(correctiveCall[1]).toHaveLength(4) // system, user, assistant (broken), user (fix prompt)
    expect(correctiveCall[1][2].role).toBe('assistant')
    expect(correctiveCall[1][2].content).toBe('broken json')
    expect(correctiveCall[1][3].content).toContain('No valid JSON found')
  })

  // ----------------------------------------------------------
  // Claim uses thread-aware SQL
  // ----------------------------------------------------------

  it('claim uses thread-aware SQL with NOT EXISTS for pending/filtering', async () => {
    mockInputs({ 'pipeline-classify-batch-size': '10' })

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce([]) // SELECT — empty
        .mockResolvedValueOnce([]) // stuck — empty

      await claimFn()

      // Verify UPDATE SQL
      const updateSql = mockExecuteSql.mock.calls[0][3]
      expect(updateSql).toContain('UPDATE dealsync_stg_v1.DEAL_STATES')
      expect(updateSql).toContain("STATUS = 'classifying'")
      expect(updateSql).toContain("STATUS = 'pending_classification'")
      expect(updateSql).toContain('NOT EXISTS')
      expect(updateSql).toContain("'pending'")
      expect(updateSql).toContain("'filtering'")
      expect(updateSql).toContain('LIMIT 10')

      return { processed: 0, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Claim stuck batch with retrigger event
  // ----------------------------------------------------------

  it('claims stuck batch and inserts retrigger event', async () => {
    mockInputs()

    const stuckRows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce([]) // SELECT claimed — empty
        .mockResolvedValueOnce([{ BATCH_ID: 'stuck-batch-abc', ATTEMPTS: 1 }]) // stuck batches
        .mockResolvedValueOnce(stuckRows) // SELECT stuck rows
        .mockResolvedValueOnce([]) // UPDATE UPDATED_AT

      const batch = await claimFn()

      expect(batch).toEqual({
        batch_id: 'stuck-batch-abc',
        count: 2,
        attempts: 1,
        rows: stuckRows,
      })

      // Should have called insertBatchEvent with retrigger
      expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
        triggerHash: 'test-uuid-2', // second uuid call
        batchId: 'stuck-batch-abc',
        batchType: 'classify',
        eventType: 'retrigger',
      })

      return { processed: 0, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Save evals uses batcher
  // ----------------------------------------------------------

  it('pushes evals to batcher with correct values', async () => {
    mockInputs()

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      // Verify pushEvals was called with correct values
      expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
      const evalValues = mockBatcherInstance.pushEvals.mock.calls[0][0]
      expect(evalValues).toHaveLength(2)
      expect(evalValues[0]).toContain('thread-1')
      expect(evalValues[1]).toContain('thread-2')

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Save deals: deal vs non-deal separation
  // ----------------------------------------------------------

  it('separates deal and non-deal threads for save-deals step', async () => {
    mockInputs()

    const rows = makeBatchRows(2) // thread-1 = deal, thread-2 = not deal
    const threads = makeThreads(rows) // default: thread-1 is deal

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      // Verify non-deal deletes via batcher
      expect(mockBatcherInstance.pushDealDeletes).toHaveBeenCalledTimes(1)
      const deleteArgs = mockBatcherInstance.pushDealDeletes.mock.calls[0][0]
      expect(deleteArgs).toEqual(["'thread-2'"])

      // Verify deal upserts via batcher
      expect(mockBatcherInstance.pushDeals).toHaveBeenCalledTimes(1)
      const dealValues = mockBatcherInstance.pushDeals.mock.calls[0][0]
      expect(dealValues).toHaveLength(1)
      expect(dealValues[0]).toContain('thread-1')
      expect(dealValues[0]).toContain('Test Deal')

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Save deal contacts uses thread_id as deal_id
  // ----------------------------------------------------------

  it('saves deal contacts using thread_id as deal_id (no DEALS lookup)', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = makeThreads(rows, { allDeals: true })

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      // Verify two-table contact upsert
      expect(mockBatcherInstance.pushContactDeletes).not.toHaveBeenCalled() // ON CONFLICT handles it
      expect(mockBatcherInstance.pushCoreContacts).toHaveBeenCalledTimes(1)
      expect(mockBatcherInstance.pushContacts).toHaveBeenCalledTimes(1)

      // Core contacts contain user, email, name, company, title, phone
      const coreValues = mockBatcherInstance.pushCoreContacts.mock.calls[0][0]
      expect(coreValues).toHaveLength(1)
      expect(coreValues[0]).toContain('alice@co.com')
      expect(coreValues[0]).toContain("'Alice'")
      expect(coreValues[0]).toContain("'TestCo'")
      expect(coreValues[0]).toContain("'CEO'")
      expect(coreValues[0]).toContain("'555-1234'")

      // Deal contacts contain thread_id, user_id, email, role
      const contactValues = mockBatcherInstance.pushContacts.mock.calls[0][0]
      expect(contactValues).toHaveLength(1)
      expect(contactValues[0]).toContain("'thread-1'")
      expect(contactValues[0]).toContain('alice@co.com')
      expect(contactValues[0]).toContain("'primary'")

      // No DEALS SELECT query should have been made during worker
      // (only audit query and claim queries)
      const workerExecCalls = mockExecuteSql.mock.calls.filter(
        (c) => typeof c[3] === 'string' && c[3].includes('SELECT ID, THREAD_ID FROM'),
      )
      expect(workerExecCalls).toHaveLength(0)

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Update deal states via batcher
  // ----------------------------------------------------------

  it('updates deal states via batcher with correct IDs', async () => {
    mockInputs()

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows) // thread-1 = deal, thread-2 = not_deal

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Batch completion event via batcher
  // ----------------------------------------------------------

  it('records batch completion via batcher', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      { thread_id: 'thread-1', is_deal: false, ai_score: 5, ai_summary: 'test', category: null },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      // Verify batch event via batcher
      expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)
      const eventValues = mockBatcherInstance.pushBatchEvents.mock.calls[0][0]
      expect(eventValues).toHaveLength(1)
      expect(eventValues[0]).toContain('classify')
      expect(eventValues[0]).toContain('complete')
      expect(eventValues[0]).toContain('test-uuid-1') // batchId

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Audit duplicate handled gracefully
  // ----------------------------------------------------------

  it('handles duplicate audit insert gracefully (integrity constraint)', async () => {
    mockInputs()

    const rows = makeBatchRows(1)
    const threads = [
      { thread_id: 'thread-1', is_deal: false, ai_score: 5, ai_summary: 'test', category: null },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // No existing audit
      mockExecuteSql.mockResolvedValueOnce([])

      // AI
      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])

      // selectByThreadIds for already-evaluated skip check
      mockExecuteSql.mockResolvedValueOnce([])

      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      // Audit insert fails with integrity constraint (duplicate)
      mockExecuteSql.mockRejectedValueOnce(new Error('integrity constraint violation'))

      // Should NOT throw — handles gracefully
      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    // Should not throw
    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Default input values
  // ----------------------------------------------------------

  it('uses default values when inputs are not specified', async () => {
    mockInputs({
      'ai-primary-model': '',
      'ai-fallback-model': '',
      'ai-api-url': '',
      'pipeline-max-concurrent': '',
      'pipeline-classify-batch-size': '',
      'pipeline-max-retries': '',
      'pipeline-fetch-chunk-size': '',
      'pipeline-fetch-timeout-ms': '',
      'pipeline-flush-interval-ms': '',
      'pipeline-flush-threshold': '',
    })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    const [, , opts] = mockRunPool.mock.calls[0]
    expect(opts).toEqual({
      maxConcurrent: 70,
      maxRetries: 6,
      onDeadLetter: expect.any(Function),
    })

    // WriteBatcher created with defaults
    expect(MockWriteBatcher).toHaveBeenCalledWith(expect.any(Function), expect.any(String), {
      flushIntervalMs: 5000,
      flushThreshold: 5,
      coreSchema: 'EMAIL_CORE_STAGING',
    })
  })

  // ----------------------------------------------------------
  // Schema validation
  // ----------------------------------------------------------

  it('rejects invalid schema', async () => {
    mockInputs({ 'sxt-schema': 'schema; DROP TABLE' })
    await expect(runClassifyPipeline()).rejects.toThrow('Invalid schema')
  })

  // ----------------------------------------------------------
  // Authenticates once
  // ----------------------------------------------------------

  it('authenticates once at start and reuses JWT', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])
      await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])
      await claimFn()

      return { processed: 0, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockAuthenticate).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // exec helper passes correct args
  // ----------------------------------------------------------

  it('exec helper passes apiUrl, jwt, biscuit to executeSql', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])

      await claimFn()

      const firstCall = mockExecuteSql.mock.calls[0]
      expect(firstCall[0]).toBe('https://sxt.example.com')
      expect(firstCall[1]).toBe('test-jwt')
      expect(firstCall[2]).toBe('test-biscuit')
      expect(typeof firstCall[3]).toBe('string')

      return { processed: 0, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Reports failed batches
  // ----------------------------------------------------------

  it('reports failed batches from pool', async () => {
    mockInputs()

    mockRunPool.mockResolvedValue({ processed: 3, failed: 2 })

    const result = await runClassifyPipeline()

    expect(result).toEqual({
      batches_processed: 3,
      batches_failed: 2,
      stuck_failed: 0,
      orphan_failed: 0,
    })
  })

  // ----------------------------------------------------------
  // All classification layers fail -> throws
  // ----------------------------------------------------------

  it('throws when all AI layers fail', async () => {
    mockInputs()

    const rows = makeBatchRows(1)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // no existing audit

      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])

      // selectByThreadIds for already-evaluated skip check
      mockExecuteSql.mockResolvedValueOnce([])

      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })

      // Primary fails
      mockCallModel.mockRejectedValueOnce(new Error('Primary down'))

      // Fallback also fails
      mockCallModel.mockRejectedValueOnce(new Error('Fallback down'))

      await expect(workerFn(batch, { attempt: 0 })).rejects.toThrow('Classification failed')

      return { processed: 0, failed: 1 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // likely_scam category sets LIKELY_SCAM flag
  // ----------------------------------------------------------

  it('sets LIKELY_SCAM flag for likely_scam category in evals', async () => {
    mockInputs()

    const rows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-scam',
        SYNC_STATE_ID: 'ss-1',
      },
    ]
    const threads = [
      {
        thread_id: 'thread-scam',
        is_deal: false,
        ai_score: 2,
        ai_summary: 'Scam email',
        category: 'likely_scam',
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Existing audit with scam thread
      mockExecuteSql.mockResolvedValueOnce([{ AI_EVALUATION: JSON.stringify({ threads }) }])

      await workerFn(batch, { attempt: 0 })

      // Find the evals push and verify LIKELY_SCAM is true
      const evalValues = mockBatcherInstance.pushEvals.mock.calls[0][0]
      expect(evalValues).toHaveLength(1)
      // IS_DEAL=false, LIKELY_SCAM=true
      expect(evalValues[0]).toMatch(/false, true/)

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Core contacts: pushCoreContacts with correct values
  // ----------------------------------------------------------

  it('calls pushCoreContacts with (userId, email, name, company, title, phone) values', async () => {
    mockInputs()
    const rows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-42',
        THREAD_ID: 'thread-xyz',
        SYNC_STATE_ID: 'ss-1',
        CREATOR_EMAIL: 'creator@test.com',
      },
    ]
    const threads = [
      {
        thread_id: 'thread-xyz',
        is_deal: true,
        ai_score: 9,
        ai_summary: 'Great deal',
        category: 'new',
        deal_name: 'Big Deal',
        deal_type: 'sponsorship',
        deal_value: '1000',
        currency: 'USD',
        main_contact: {
          name: 'Alice',
          email: 'alice@brand.com',
          company: 'BrandCo',
          title: 'Manager',
          phone_number: '555-0101',
        },
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()
      mockExecuteSql.mockResolvedValueOnce([])
      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-xyz', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValue([])
      await workerFn(batch, { attempt: 0 })
      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockBatcherInstance.pushCoreContacts).toHaveBeenCalledTimes(1)
    const coreValues = mockBatcherInstance.pushCoreContacts.mock.calls[0][0]
    expect(coreValues).toHaveLength(1)
    expect(coreValues[0]).toContain('user-42')
    expect(coreValues[0]).toContain('alice@brand.com')
    expect(coreValues[0]).toContain("'Alice'")
    expect(coreValues[0]).toContain("'BrandCo'")
  })

  // ----------------------------------------------------------
  // Core contacts: NULL for missing fields
  // ----------------------------------------------------------

  it('uses NULL literal for missing contact fields in coreContacts', async () => {
    mockInputs()
    const rows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-1',
        SYNC_STATE_ID: 'ss-1',
        CREATOR_EMAIL: '',
      },
    ]
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 7,
        ai_summary: 'Deal',
        category: 'new',
        deal_name: 'Deal',
        deal_type: 'sponsorship',
        deal_value: '100',
        currency: 'USD',
        main_contact: {
          name: '',
          email: 'contact@co.com',
          company: '',
          title: '',
          phone_number: '',
        },
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()
      mockExecuteSql.mockResolvedValueOnce([])
      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValue([])
      await workerFn(batch, { attempt: 0 })
      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    const coreValues = mockBatcherInstance.pushCoreContacts.mock.calls[0][0]
    expect(coreValues[0]).toMatch(/NULL/)
  })

  // ----------------------------------------------------------
  // Already-evaluated skip: all threads skipped
  // ----------------------------------------------------------

  it('skips classification for threads with existing deals and older emails', async () => {
    mockInputs()

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Claim phase
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()

      // Worker phase
      mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

      // Fetch emails with dates OLDER than existing deals
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test email body',
        date: '2025-01-01T00:00:00Z',
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // Query existing deals — both threads have deals with NEWER UPDATED_AT
      mockExecuteSql.mockResolvedValueOnce([
        { THREAD_ID: 'thread-1', UPDATED_AT: '2026-01-01T00:00:00Z' },
        { THREAD_ID: 'thread-2', UPDATED_AT: '2026-01-01T00:00:00Z' },
      ])

      // updateStatusByIds for skipped threads
      mockExecuteSql.mockResolvedValueOnce([])

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI should NOT have been called — all threads skipped
    expect(mockCallModel).not.toHaveBeenCalled()
    expect(mockBuildPrompt).not.toHaveBeenCalled()

    // Batch completion event should still be recorded
    expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // Already-evaluated skip: mixed batch (some skipped, some classified)
  // ----------------------------------------------------------

  it('classifies only threads without existing deals or with newer emails', async () => {
    mockInputs()

    // 3 rows: thread-1, thread-2, thread-3
    const rows = makeBatchRows(3)

    // Only thread-2 and thread-3 will be classified
    const classifiedThreads = [
      {
        thread_id: 'thread-2',
        is_deal: false,
        ai_score: 3,
        ai_summary: 'Not a deal',
        category: null,
        deal_name: null,
        deal_type: null,
        deal_value: '0',
        currency: 'USD',
        main_contact: null,
      },
      {
        thread_id: 'thread-3',
        is_deal: true,
        ai_score: 9,
        ai_summary: 'New deal',
        category: 'new',
        deal_name: 'New Deal',
        deal_type: 'brand_collaboration',
        deal_value: '500',
        currency: 'USD',
        main_contact: {
          name: 'Bob',
          email: 'bob@co.com',
          company: 'BobCo',
          title: 'CTO',
          phone_number: null,
        },
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Claim
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()

      // Worker
      mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

      // Fetch emails - all 3 threads
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test email body',
        date: '2025-06-01T00:00:00Z',
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // Only thread-1 has existing deal with newer UPDATED_AT
      mockExecuteSql.mockResolvedValueOnce([
        { THREAD_ID: 'thread-1', UPDATED_AT: '2026-01-01T00:00:00Z' },
      ])

      // updateStatusByIds for skipped thread-1
      mockExecuteSql.mockResolvedValueOnce([])

      // AI classification for thread-2 and thread-3
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(classifiedThreads)

      // Save audit
      mockExecuteSql.mockResolvedValueOnce([])

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI should have been called for non-skipped threads
    expect(mockCallModel).toHaveBeenCalledTimes(1)

    // buildPrompt should have received only thread-2 and thread-3 emails
    const promptEmails = mockBuildPrompt.mock.calls[0][0]
    const promptThreadIds = [...new Set(promptEmails.map((e) => e.threadId))]
    expect(promptThreadIds).not.toContain('thread-1')
    expect(promptThreadIds).toContain('thread-2')
    expect(promptThreadIds).toContain('thread-3')

    // Evals/deals should have been saved for classified threads
    expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // Already-evaluated skip: newer emails -> classify normally
  // ----------------------------------------------------------

  it('classifies threads with existing deals when emails are newer than deal', async () => {
    mockInputs()

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

      // Emails with dates NEWER than existing deals
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test email body',
        date: '2026-06-01T00:00:00Z',
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // Both threads have deals but with OLDER UPDATED_AT
      mockExecuteSql.mockResolvedValueOnce([
        { THREAD_ID: 'thread-1', UPDATED_AT: '2025-01-01T00:00:00Z' },
        { THREAD_ID: 'thread-2', UPDATED_AT: '2025-01-01T00:00:00Z' },
      ])

      // No skipping — proceed to AI classification
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValueOnce([]) // audit insert

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI SHOULD have been called — emails are newer
    expect(mockCallModel).toHaveBeenCalledTimes(1)

    // All emails should be in the prompt
    const promptEmails = mockBuildPrompt.mock.calls[0][0]
    expect(promptEmails.length).toBe(2)
  })

  // ----------------------------------------------------------
  // Already-evaluated skip: missing/unparseable dates -> classify normally
  // ----------------------------------------------------------

  it('does not skip threads when email dates are missing or unparseable', async () => {
    mockInputs()

    const rows = makeBatchRows(2)
    const threads = makeThreads(rows)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const batch = await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

      // Emails with NO date field
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        threadId: r.THREAD_ID,
        body: 'test email body',
        // no date field
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // Both threads have existing deals
      mockExecuteSql.mockResolvedValueOnce([
        { THREAD_ID: 'thread-1', UPDATED_AT: '2026-01-01T00:00:00Z' },
        { THREAD_ID: 'thread-2', UPDATED_AT: '2026-01-01T00:00:00Z' },
      ])

      // No skipping — proceed to AI because dates are unparseable
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValueOnce([]) // audit insert

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI SHOULD have been called — missing dates means we can't determine, so classify
    expect(mockCallModel).toHaveBeenCalledTimes(1)
    expect(mockBuildPrompt).toHaveBeenCalledTimes(1)

    // All emails should be in the prompt
    const promptEmails = mockBuildPrompt.mock.calls[0][0]
    expect(promptEmails.length).toBe(2)
  })

  // ----------------------------------------------------------
  // No pushContactDeletes (ON CONFLICT handles idempotency)
  // ----------------------------------------------------------

  it('does NOT call pushContactDeletes (ON CONFLICT handles idempotency)', async () => {
    mockInputs()
    const rows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-1',
        SYNC_STATE_ID: 'ss-1',
        CREATOR_EMAIL: '',
      },
    ]
    const threads = [
      {
        thread_id: 'thread-1',
        is_deal: true,
        ai_score: 7,
        ai_summary: 'Deal',
        category: 'new',
        deal_name: 'Deal',
        deal_type: 'sponsorship',
        deal_value: '100',
        currency: 'USD',
        main_contact: { name: 'X', email: 'x@co.com', company: '', title: '', phone_number: '' },
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
      const batch = await claimFn()
      mockExecuteSql.mockResolvedValueOnce([])
      mockFetchEmails.mockResolvedValueOnce([
        { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
      ])
      mockExecuteSql.mockResolvedValueOnce([]) // selectByThreadIds -> no existing deals
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)
      mockExecuteSql.mockResolvedValue([])
      await workerFn(batch, { attempt: 0 })
      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    expect(mockBatcherInstance.pushContactDeletes).not.toHaveBeenCalled()
  })

  // ----------------------------------------------------------
  // Mega-claim: returns array of sub-batches when claimSize > classifyBatchSize
  // ----------------------------------------------------------

  it('mega-claim: claims claimSize threads and splits into sub-batches', async () => {
    mockInputs({ 'pipeline-claim-size': '10', 'pipeline-classify-batch-size': '3' })

    // 7 rows across 7 threads → should split into 3 sub-batches (3+3+1)
    const rows = Array.from({ length: 7 }, (_, i) => ({
      EMAIL_METADATA_ID: `em-${i + 1}`,
      MESSAGE_ID: `msg-${i + 1}`,
      USER_ID: 'user-1',
      THREAD_ID: `thread-${i + 1}`,
      CREATOR_EMAIL: 'creator@test.com',
      SYNC_STATE_ID: 'ss-1',
    }))

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE mega-claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows
        .mockResolvedValueOnce([]) // restampSubBatches

      const result = await claimFn()

      // Should return an array of sub-batches
      expect(Array.isArray(result)).toBe(true)
      expect(result.length).toBe(3) // ceil(7/3) = 3

      // First sub-batch: 3 threads
      expect(result[0].count).toBe(3)
      expect(result[0].attempts).toBe(0)
      expect(result[0].rows.length).toBe(3)

      // Second sub-batch: 3 threads
      expect(result[1].count).toBe(3)
      expect(result[1].rows.length).toBe(3)

      // Third sub-batch: 1 thread
      expect(result[2].count).toBe(1)
      expect(result[2].rows.length).toBe(1)

      // Each sub-batch should have a unique batch_id
      const ids = new Set(result.map((b) => b.batch_id))
      expect(ids.size).toBe(3)

      // Verify claimClassifyBatch was called with claimSize (10), not classifyBatchSize (3)
      const claimSql = mockExecuteSql.mock.calls[0][3]
      expect(claimSql).toContain('LIMIT 10')

      // Verify restampSubBatches was called
      const restampSql = mockExecuteSql.mock.calls[2][3]
      expect(restampSql).toContain('CASE')
      expect(restampSql).toContain('WHEN THREAD_ID IN')

      // Verify batch events were pushed via batcher
      expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)
      const eventValues = mockBatcherInstance.pushBatchEvents.mock.calls[0][0]
      expect(eventValues.length).toBe(3)

      return { processed: 3, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Mega-claim: backward compatibility when claimSize <= classifyBatchSize
  // ----------------------------------------------------------

  it('mega-claim: backward compatible when pipeline-claim-size not set (defaults to single batch)', async () => {
    mockInputs() // pipeline-claim-size defaults to 5, pipeline-classify-batch-size defaults to 5

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows

      const result = await claimFn()

      // Should return a single batch object (not an array)
      expect(Array.isArray(result)).toBe(false)
      expect(result).not.toBeNull()
      expect(result.batch_id).toBeDefined()
      expect(result.count).toBe(2)
      expect(result.rows).toEqual(rows)

      // Verify claimClassifyBatch was called with classifyBatchSize (5)
      const claimSql = mockExecuteSql.mock.calls[0][3]
      expect(claimSql).toContain('LIMIT 5')

      // Verify insertBatchEvent was called (not batcher.pushBatchEvents)
      expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
        triggerHash: expect.any(String),
        batchId: expect.any(String),
        batchType: 'classify',
        eventType: 'new',
      })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Mega-claim: stuck mega batch recovery re-splits
  // ----------------------------------------------------------

  it('mega-claim: re-splits stuck mega batch into sub-batches', async () => {
    mockInputs({ 'pipeline-claim-size': '10', 'pipeline-classify-batch-size': '2' })

    // 4 rows across 4 threads for the stuck mega batch
    const stuckRows = Array.from({ length: 4 }, (_, i) => ({
      EMAIL_METADATA_ID: `em-${i + 1}`,
      MESSAGE_ID: `msg-${i + 1}`,
      USER_ID: 'user-1',
      THREAD_ID: `thread-${i + 1}`,
      CREATOR_EMAIL: 'creator@test.com',
      SYNC_STATE_ID: 'ss-1',
    }))

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE mega-claim
        .mockResolvedValueOnce([]) // SELECT claimed rows — empty (nothing pending)
        .mockResolvedValueOnce([{ BATCH_ID: 'mega:stuck-uuid', ATTEMPTS: 2 }]) // stuck batches
        .mockResolvedValueOnce(stuckRows) // SELECT stuck mega batch rows
        .mockResolvedValueOnce([]) // refreshBatchTimestamp
        .mockResolvedValueOnce([]) // restampSubBatches

      const result = await claimFn()

      // Should return an array of sub-batches
      expect(Array.isArray(result)).toBe(true)
      expect(result.length).toBe(2) // ceil(4/2) = 2

      // Each sub-batch should have attempts from the stuck batch
      expect(result[0].attempts).toBe(2)
      expect(result[1].attempts).toBe(2)

      // Each sub-batch should have 2 rows
      expect(result[0].count).toBe(2)
      expect(result[1].count).toBe(2)

      return { processed: 2, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Mega-claim: stuck non-mega batch still works normally
  // ----------------------------------------------------------

  it('mega-claim: stuck non-mega batch uses standard recovery', async () => {
    mockInputs({ 'pipeline-claim-size': '10', 'pipeline-classify-batch-size': '5' })

    const stuckRows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE mega-claim
        .mockResolvedValueOnce([]) // SELECT claimed rows — empty
        .mockResolvedValueOnce([{ BATCH_ID: 'normal-batch-abc', ATTEMPTS: 1 }]) // stuck batches (non-mega)
        .mockResolvedValueOnce(stuckRows) // SELECT stuck rows
        .mockResolvedValueOnce([]) // refreshBatchTimestamp

      const result = await claimFn()

      // Should return a single batch object (not an array)
      expect(Array.isArray(result)).toBe(false)
      expect(result).toEqual({
        batch_id: 'normal-batch-abc',
        count: 2,
        attempts: 1,
        rows: stuckRows,
      })

      // Should have called insertBatchEvent with retrigger
      expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
        triggerHash: expect.any(String),
        batchId: 'normal-batch-abc',
        batchType: 'classify',
        eventType: 'retrigger',
      })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Mega-claim: multi-row threads are kept together in sub-batches
  // ----------------------------------------------------------

  it('mega-claim: groups multi-row threads correctly in sub-batches', async () => {
    mockInputs({ 'pipeline-claim-size': '10', 'pipeline-classify-batch-size': '2' })

    // 6 rows across 3 threads (2 rows per thread) → 2 sub-batches (2 threads + 1 thread)
    const rows = [
      { EMAIL_METADATA_ID: 'em-1', MESSAGE_ID: 'msg-1', USER_ID: 'user-1', THREAD_ID: 'thread-A', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
      { EMAIL_METADATA_ID: 'em-2', MESSAGE_ID: 'msg-2', USER_ID: 'user-1', THREAD_ID: 'thread-A', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
      { EMAIL_METADATA_ID: 'em-3', MESSAGE_ID: 'msg-3', USER_ID: 'user-1', THREAD_ID: 'thread-B', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
      { EMAIL_METADATA_ID: 'em-4', MESSAGE_ID: 'msg-4', USER_ID: 'user-1', THREAD_ID: 'thread-B', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
      { EMAIL_METADATA_ID: 'em-5', MESSAGE_ID: 'msg-5', USER_ID: 'user-1', THREAD_ID: 'thread-C', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
      { EMAIL_METADATA_ID: 'em-6', MESSAGE_ID: 'msg-6', USER_ID: 'user-1', THREAD_ID: 'thread-C', CREATOR_EMAIL: 'c@t.com', SYNC_STATE_ID: 'ss-1' },
    ]

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE mega-claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows
        .mockResolvedValueOnce([]) // restampSubBatches

      const result = await claimFn()

      expect(Array.isArray(result)).toBe(true)
      expect(result.length).toBe(2) // 3 threads / 2 per batch = 2 sub-batches

      // First sub-batch: 2 threads × 2 rows = 4 rows
      expect(result[0].count).toBe(4)
      expect(result[0].rows.length).toBe(4)

      // Second sub-batch: 1 thread × 2 rows = 2 rows
      expect(result[1].count).toBe(2)
      expect(result[1].rows.length).toBe(2)

      // All rows from a thread should be in the same sub-batch
      const sub0ThreadIds = new Set(result[0].rows.map((r) => r.THREAD_ID))
      const sub1ThreadIds = new Set(result[1].rows.map((r) => r.THREAD_ID))
      expect(sub0ThreadIds.size).toBe(2)
      expect(sub1ThreadIds.size).toBe(1)

      return { processed: 2, failed: 0 }
    })

    await runClassifyPipeline()
  })
})
