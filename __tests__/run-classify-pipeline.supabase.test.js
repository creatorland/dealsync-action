import { jest } from '@jest/globals'

// ===========================================================================
// Integration test for the INGEST_WRITE_TARGET wiring in run-classify-pipeline.
// Verifies the consolidated Supabase block (Step 6.5) calls the writer with
// correctly-linked data, and that the switch gates SxT vs Supabase writes.
// ===========================================================================

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

let uuidCallCount = 0
jest.unstable_mockModule('uuid', () => ({
  v7: jest.fn(() => {
    uuidCallCount++
    return `test-uuid-${uuidCallCount}`
  }),
}))

const mockAuthenticate = jest.fn()
const mockExecuteSql = jest.fn()
const mockAcquireRateLimitToken = jest.fn().mockResolvedValue(undefined)
jest.unstable_mockModule('../src/lib/db.js', () => ({
  authenticate: mockAuthenticate,
  executeSql: mockExecuteSql,
  acquireRateLimitToken: mockAcquireRateLimitToken,
  logSqlStats: jest.fn(),
  getSqlStats: jest.fn(() => ({ calls: 0, totalMs: 0, slowest: 0, slowestSql: '', avgMs: 0 })),
  withTimeout: jest.fn(() => ({ signal: new AbortController().signal, clear: jest.fn() })),
}))

const mockFetchEmails = jest.fn()
const mockDeriveFallbackMainContact = jest.fn(() => null)
const mockIsBlockedSenderAddress = jest.fn(() => false)
jest.unstable_mockModule('../src/lib/emails.js', () => ({
  fetchEmails: mockFetchEmails,
  deriveFallbackMainContact: mockDeriveFallbackMainContact,
  isBlockedSenderAddress: mockIsBlockedSenderAddress,
}))

const mockCallModel = jest.fn()
const mockParseAndValidate = jest.fn()
const mockBuildPrompt = jest.fn()
jest.unstable_mockModule('../src/lib/ai.js', () => ({
  callModel: mockCallModel,
  parseAndValidate: mockParseAndValidate,
  buildPrompt: mockBuildPrompt,
  VALID_CATEGORIES: new Set(['new', 'likely_scam']),
  VALID_DEAL_TYPES: new Set(['brand_collaboration']),
}))

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
jest.unstable_mockModule('../src/lib/batcher.js', () => ({
  WriteBatcher: jest.fn(() => mockBatcherInstance),
}))

// The module under test for the wiring assertions.
const mockWriteAudit = jest.fn().mockResolvedValue(undefined)
const mockWriteEvals = jest.fn().mockResolvedValue(undefined)
const mockWriteDeals = jest.fn().mockResolvedValue(undefined)
const mockDeleteDeals = jest.fn().mockResolvedValue(undefined)
const mockWriteContacts = jest.fn().mockResolvedValue(undefined)
jest.unstable_mockModule('../src/lib/supabase-writer.js', () => ({
  writeAudit: mockWriteAudit,
  writeEvals: mockWriteEvals,
  writeDeals: mockWriteDeals,
  deleteDeals: mockDeleteDeals,
  writeContacts: mockWriteContacts,
}))

const core = await import('@actions/core')
const { runClassifyPipeline } = await import('../src/commands/run-classify-pipeline.js')

// ===========================================================================
// Helpers
// ===========================================================================

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
    'pipeline-claim-size': '5',
    'pipeline-max-retries': '3',
    'pipeline-fetch-chunk-size': '10',
    'pipeline-fetch-timeout-ms': '120000',
    'pipeline-flush-interval-ms': '5000',
    'pipeline-flush-threshold': '10',
    'supabase-url': 'https://test.supabase.co',
    'supabase-service-role-key': 'svc-key',
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

// Two threads: thread-1 is a deal with a usable main_contact; thread-2 is non-deal.
const ROWS = [
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

const THREADS = [
  {
    thread_id: 'thread-1',
    is_deal: true,
    ai_score: 8,
    ai_summary: 'A real deal',
    category: 'new',
    deal_name: 'Test Deal',
    deal_type: 'brand_collaboration',
    deal_value: '5000',
    currency: 'USD',
    main_contact: {
      name: 'Alice',
      email: 'Alice@CO.com',
      company: 'TestCo',
      title: 'CEO',
      phone_number: '555-1234',
    },
  },
  {
    thread_id: 'thread-2',
    is_deal: false,
    ai_score: 2,
    ai_summary: 'Not a deal',
    category: 'likely_scam',
    deal_name: null,
    deal_type: null,
    deal_value: '0',
    currency: 'USD',
    main_contact: null,
  },
]

/** Drives one fresh-classification batch through runPool with the given write target. */
function driveFreshBatch() {
  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    mockExecuteSql
      .mockResolvedValueOnce([]) // claim UPDATE
      .mockResolvedValueOnce(ROWS) // claim SELECT
    const batch = await claimFn()

    mockExecuteSql
      .mockResolvedValueOnce([]) // audit selectByBatch → empty (fresh)
      .mockResolvedValueOnce([]) // selectByThreadIds → no existing deals
      .mockResolvedValueOnce([]) // insert audit (SxT)
      .mockResolvedValueOnce([]) // updateStatusByIds → deal
      .mockResolvedValueOnce([]) // updateStatusByIds → not_deal

    mockFetchEmails.mockResolvedValueOnce([
      { messageId: 'msg-1', threadId: 'thread-1', body: 'hi', topLevelHeaders: [] },
      { messageId: 'msg-2', threadId: 'thread-2', body: 'spam', topLevelHeaders: [] },
    ])
    mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 's', userPrompt: 'u', threadOrder: [] })
    mockCallModel.mockResolvedValueOnce({ content: '[]' })
    mockParseAndValidate.mockReturnValueOnce(THREADS)

    await workerFn(batch, { attempt: 0 })
    return { processed: 1, failed: 0 }
  })
}

beforeEach(() => {
  jest.clearAllMocks()
  mockExecuteSql.mockReset()
  uuidCallCount = 0
  for (const key of Object.keys(outputs)) delete outputs[key]
  mockAuthenticate.mockResolvedValue('test-jwt')
  mockInsertBatchEvent.mockResolvedValue(undefined)
  mockDeriveFallbackMainContact.mockReset().mockReturnValue(null)
  mockIsBlockedSenderAddress.mockReset().mockReturnValue(false)
  mockAcquireRateLimitToken.mockResolvedValue(undefined)
  for (const fn of [
    mockWriteAudit,
    mockWriteEvals,
    mockWriteDeals,
    mockDeleteDeals,
    mockWriteContacts,
  ]) {
    fn.mockReset().mockResolvedValue(undefined)
  }
  for (const fn of Object.values(mockBatcherInstance)) {
    if (typeof fn.mockResolvedValue === 'function') fn.mockResolvedValue(undefined)
  }
  mockSweepStuckRows.mockResolvedValue(0)
  mockSweepOrphanedRows.mockResolvedValue(0)
})

// ===========================================================================
// Tests
// ===========================================================================

describe('INGEST_WRITE_TARGET wiring', () => {
  it('default (no target) writes SxT only, never Supabase', async () => {
    mockInputs() // ingest-write-target defaults to sxt
    driveFreshBatch()
    await runClassifyPipeline()

    expect(mockBatcherInstance.pushDeals).toHaveBeenCalled()
    expect(mockWriteAudit).not.toHaveBeenCalled()
    expect(mockWriteEvals).not.toHaveBeenCalled()
    expect(mockWriteDeals).not.toHaveBeenCalled()
    expect(mockWriteContacts).not.toHaveBeenCalled()
  })

  it('target=supabase writes Supabase, skips SxT business writes', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    // SxT business writes skipped
    expect(mockBatcherInstance.pushEvals).not.toHaveBeenCalled()
    expect(mockBatcherInstance.pushDeals).not.toHaveBeenCalled()
    expect(mockBatcherInstance.pushContacts).not.toHaveBeenCalled()
    expect(mockBatcherInstance.pushDealDeletes).not.toHaveBeenCalled()

    // Supabase writes happened
    expect(mockWriteAudit).toHaveBeenCalledTimes(1)
    expect(mockWriteEvals).toHaveBeenCalledTimes(1)
    expect(mockWriteDeals).toHaveBeenCalledTimes(1)
    expect(mockDeleteDeals).toHaveBeenCalledTimes(1)
    expect(mockWriteContacts).toHaveBeenCalledTimes(1)
  })

  it('target=both dual-writes SxT and Supabase', async () => {
    mockInputs({ 'ingest-write-target': 'both' })
    driveFreshBatch()
    await runClassifyPipeline()

    expect(mockBatcherInstance.pushDeals).toHaveBeenCalled()
    expect(mockBatcherInstance.pushEvals).toHaveBeenCalled()
    expect(mockWriteDeals).toHaveBeenCalledTimes(1)
    expect(mockWriteEvals).toHaveBeenCalledTimes(1)
  })

  it('Supabase audit carries batchId linkage + jsonb evaluation object', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    const audit = mockWriteAudit.mock.calls[0][0]
    expect(audit.threadCount).toBe(2)
    expect(audit.emailCount).toBe(2)
    expect(audit.aiEvaluation).toEqual({ threads: THREADS })
    expect(typeof audit.batchId).toBe('string')
  })

  it('Supabase evals: all threads, main_contact only on the deal thread, audit linkage = batchId', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    const evals = mockWriteEvals.mock.calls[0][0]
    expect(evals).toHaveLength(2)

    const e1 = evals.find((e) => e.threadId === 'thread-1')
    const e2 = evals.find((e) => e.threadId === 'thread-2')

    // audit linkage: ete.ai_evaluation_audit_id = audits.id = batchId
    const audit = mockWriteAudit.mock.calls[0][0]
    expect(e1.auditId).toBe(audit.batchId)

    expect(e1.isDeal).toBe(true)
    expect(e1.mainContact).toMatchObject({ email: 'Alice@CO.com', company: 'TestCo' })

    expect(e2.isDeal).toBe(false)
    expect(e2.likelyScam).toBe(true) // category 'likely_scam'
    expect(e2.mainContact).toBeNull()
  })

  it('Supabase deals: only the deal thread, user_id + brand from contact, threadId linkage', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    const deals = mockWriteDeals.mock.calls[0][0]
    expect(deals).toHaveLength(1)
    expect(deals[0].threadId).toBe('thread-1')
    expect(deals[0].userId).toBe('user-1')
    expect(deals[0].category).toBe('new')
    expect(deals[0].value).toBe(5000)
    expect(deals[0].brand).toBe('TestCo') // from main_contact.company
  })

  it('Supabase deleteDeals targets the non-deal thread', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    expect(mockDeleteDeals).toHaveBeenCalledWith(['thread-2'])
  })

  it('Supabase contacts: one row for the deal thread with its resolved contact', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    driveFreshBatch()
    await runClassifyPipeline()

    const contacts = mockWriteContacts.mock.calls[0][0]
    expect(contacts).toHaveLength(1)
    expect(contacts[0].userId).toBe('user-1')
    expect(contacts[0].email).toBe('Alice@CO.com') // writer lowercases
    expect(contacts[0].company).toBe('TestCo')
  })

  it('a Supabase write failure does not throw out of the batch (non-fatal)', async () => {
    mockInputs({ 'ingest-write-target': 'supabase' })
    mockWriteDeals.mockRejectedValueOnce(new Error('supabase 500'))
    driveFreshBatch()
    await expect(runClassifyPipeline()).resolves.toBeDefined()
  })
})
