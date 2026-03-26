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
jest.unstable_mockModule('../src/lib/sxt-client.js', () => ({
  authenticate: mockAuthenticate,
  executeSql: mockExecuteSql,
  withTimeout: jest.fn(() => ({
    signal: new AbortController().signal,
    clear: jest.fn(),
  })),
}))

// Mock email-client
const mockFetchEmails = jest.fn()
jest.unstable_mockModule('../src/lib/email-client.js', () => ({
  fetchEmails: mockFetchEmails,
}))

// Mock ai-client
const mockCallModel = jest.fn()
const mockParseAndValidate = jest.fn()
jest.unstable_mockModule('../src/lib/ai-client.js', () => ({
  callModel: mockCallModel,
  parseAndValidate: mockParseAndValidate,
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

// Mock build-prompt
const mockBuildPrompt = jest.fn()
jest.unstable_mockModule('../src/lib/build-prompt.js', () => ({
  buildPrompt: mockBuildPrompt,
}))

// Mock pipeline
const mockRunPool = jest.fn()
const mockInsertBatchEvent = jest.fn()
jest.unstable_mockModule('../src/lib/pipeline.js', () => ({
  runPool: mockRunPool,
  insertBatchEvent: mockInsertBatchEvent,
}))

const core = await import('@actions/core')
const { runClassifyPipeline } = await import('../src/commands/run-classify-pipeline.js')

// ============================================================
// Helpers
// ============================================================

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'content-fetcher-url': 'https://fetcher.example.com',
    'hyperbolic-key': 'test-hyp-key',
    'primary-model': 'TestPrimary/Model',
    'fallback-model': 'TestFallback/Model',
    'ai-api-url': 'https://ai.example.com/v1/chat/completions',
    'max-concurrent': '3',
    'classify-batch-size': '5',
    'max-retries': '3',
    'chunk-size': '10',
    'fetch-timeout-ms': '120000',
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
    uuidCallCount = 0
    for (const key of Object.keys(outputs)) delete outputs[key]
    mockAuthenticate.mockResolvedValue('test-jwt')
    mockInsertBatchEvent.mockResolvedValue(undefined)
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
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[{"thread_id":"thread-1"}]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      // Save audit checkpoint
      mockExecuteSql.mockResolvedValueOnce([]) // insertAudit

      // Step 4: Save evals
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT evals

      // Step 5: Save deals
      // DELETE non-deals
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE non-deal from DEALS
      // INSERT deals
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT deals

      // Step 6: Save deal contacts
      // SELECT deals by thread_id
      mockExecuteSql.mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1' }])
      // DELETE existing contacts
      mockExecuteSql.mockResolvedValueOnce([])
      // INSERT contacts
      mockExecuteSql.mockResolvedValueOnce([])

      // Step 7: Update deal states
      mockExecuteSql.mockResolvedValueOnce([]) // updateDeals
      mockExecuteSql.mockResolvedValueOnce([]) // updateNotDeal

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    const result = await runClassifyPipeline()

    expect(result).toEqual({
      batches_processed: 1,
      batches_failed: 0,
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

    // Verify batch event for completion
    expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
      triggerHash: 'test-uuid-1',
      batchId: 'test-uuid-1',
      batchType: 'classify',
      eventType: 'complete',
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

      // Step 4: Save evals (no AI calls needed)
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT evals

      // Step 5: Save deals
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE non-deal from DEALS
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT deals

      // Step 6: Save deal contacts
      mockExecuteSql.mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1' }]) // SELECT deals
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE contacts
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT contacts

      // Step 7: Update deal states
      mockExecuteSql.mockResolvedValueOnce([]) // updateDeals
      mockExecuteSql.mockResolvedValueOnce([]) // updateNotDeal

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()

    // AI should NOT have been called
    expect(mockFetchEmails).not.toHaveBeenCalled()
    expect(mockCallModel).not.toHaveBeenCalled()
    expect(mockBuildPrompt).not.toHaveBeenCalled()
    expect(mockParseAndValidate).not.toHaveBeenCalled()
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
    })

    // No pipeline work should have been done
    expect(mockFetchEmails).not.toHaveBeenCalled()
    expect(mockCallModel).not.toHaveBeenCalled()
  })

  // ----------------------------------------------------------
  // Correct params passed to each step
  // ----------------------------------------------------------

  it('passes correct params to fetchEmails (no format = full content)', async () => {
    mockInputs({ 'chunk-size': '15', 'fetch-timeout-ms': '60000' })

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
    mockInputs({ 'max-concurrent': '5', 'max-retries': '7' })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    expect(mockRunPool).toHaveBeenCalledTimes(1)
    const [claimFn, workerFn, opts] = mockRunPool.mock.calls[0]
    expect(typeof claimFn).toBe('function')
    expect(typeof workerFn).toBe('function')
    expect(opts).toEqual({ maxConcurrent: 5, maxRetries: 7 })
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
    mockInputs({ 'classify-batch-size': '10' })

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
  // Save evals uses correct SQL
  // ----------------------------------------------------------

  it('saves evals with correct ON CONFLICT upsert', async () => {
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

      // Capture evals SQL
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT evals

      // Rest of pipeline
      mockExecuteSql.mockResolvedValue([])

      await workerFn(batch, { attempt: 0 })

      // Find the evals INSERT call
      const evalCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('EMAIL_THREAD_EVALUATIONS'),
      )
      expect(evalCall).toBeTruthy()
      expect(evalCall[3]).toContain('INSERT INTO dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS')
      expect(evalCall[3]).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
      expect(evalCall[3]).toContain('thread-1')
      expect(evalCall[3]).toContain('thread-2')

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

      // Evals
      mockExecuteSql.mockResolvedValueOnce([])

      // Deals: DELETE non-deal + INSERT deals
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE non-deal
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT deals

      // Contacts
      mockExecuteSql.mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1' }])
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE contacts
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT contacts

      // States
      mockExecuteSql.mockResolvedValueOnce([]) // updateDeals
      mockExecuteSql.mockResolvedValueOnce([]) // updateNotDeal

      await workerFn(batch, { attempt: 0 })

      // Verify DELETE non-deal was called with thread-2
      const deleteCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('DELETE FROM dealsync_stg_v1.DEALS'),
      )
      expect(deleteCall).toBeTruthy()
      expect(deleteCall[3]).toContain("'thread-2'")

      // Verify INSERT deals was called with thread-1
      const insertDealsCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('INSERT INTO dealsync_stg_v1.DEALS'),
      )
      expect(insertDealsCall).toBeTruthy()
      expect(insertDealsCall[3]).toContain("'thread-1'")
      expect(insertDealsCall[3]).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Save deal contacts
  // ----------------------------------------------------------

  it('saves deal contacts with correct data from main_contact', async () => {
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

      // Evals
      mockExecuteSql.mockResolvedValueOnce([])

      // No non-deal threads to delete, so INSERT deals
      mockExecuteSql.mockResolvedValueOnce([])

      // Contacts: SELECT deals
      mockExecuteSql.mockResolvedValueOnce([{ ID: 'deal-abc', THREAD_ID: 'thread-1' }])
      // DELETE existing contacts
      mockExecuteSql.mockResolvedValueOnce([])
      // INSERT contacts
      mockExecuteSql.mockResolvedValueOnce([])

      // States
      mockExecuteSql.mockResolvedValueOnce([])

      await workerFn(batch, { attempt: 0 })

      // Verify INSERT contacts
      const contactInsert = mockExecuteSql.mock.calls.find(
        (c) =>
          typeof c[3] === 'string' && c[3].includes('INSERT INTO dealsync_stg_v1.DEAL_CONTACTS'),
      )
      expect(contactInsert).toBeTruthy()
      expect(contactInsert[3]).toContain('alice@co.com')
      expect(contactInsert[3]).toContain('Alice')
      expect(contactInsert[3]).toContain('TestCo')
      expect(contactInsert[3]).toContain('CEO')
      expect(contactInsert[3]).toContain('555-1234')

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })

  // ----------------------------------------------------------
  // Update deal states to terminal
  // ----------------------------------------------------------

  it('updates deal states to terminal status using detection queries', async () => {
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

      // Evals
      mockExecuteSql.mockResolvedValueOnce([])

      // Deals
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE non-deal
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT deals

      // Contacts
      mockExecuteSql.mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1' }])
      mockExecuteSql.mockResolvedValueOnce([]) // DELETE contacts
      mockExecuteSql.mockResolvedValueOnce([]) // INSERT contacts

      // States
      mockExecuteSql.mockResolvedValueOnce([]) // updateDeals
      mockExecuteSql.mockResolvedValueOnce([]) // updateNotDeal

      await workerFn(batch, { attempt: 0 })

      // Find deal state update calls
      const dealUpdateCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes("STATUS = 'deal'"),
      )
      const notDealUpdateCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes("STATUS = 'not_deal'"),
      )

      expect(dealUpdateCall).toBeTruthy()
      expect(dealUpdateCall[3]).toContain("'em-1'") // thread-1's email
      expect(notDealUpdateCall).toBeTruthy()
      expect(notDealUpdateCall[3]).toContain("'em-2'") // thread-2's email

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
      mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
      mockCallModel.mockResolvedValueOnce({ content: '[]' })
      mockParseAndValidate.mockReturnValueOnce(threads)

      // Audit insert fails with integrity constraint (duplicate)
      mockExecuteSql.mockRejectedValueOnce(new Error('integrity constraint violation'))

      // Rest of pipeline should still proceed
      mockExecuteSql.mockResolvedValue([]) // all remaining

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
      'primary-model': '',
      'fallback-model': '',
      'ai-api-url': '',
      'max-concurrent': '',
      'classify-batch-size': '',
      'max-retries': '',
      'chunk-size': '',
      'fetch-timeout-ms': '',
    })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runClassifyPipeline()

    const [, , opts] = mockRunPool.mock.calls[0]
    expect(opts).toEqual({ maxConcurrent: 3, maxRetries: 3 })
  })

  // ----------------------------------------------------------
  // Schema validation
  // ----------------------------------------------------------

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
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

      // Evals
      mockExecuteSql.mockResolvedValueOnce([])

      // No deals — skip deal INSERT, just states
      mockExecuteSql.mockResolvedValueOnce([]) // updateNotDeal

      await workerFn(batch, { attempt: 0 })

      // Find the evals INSERT call
      const evalCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('EMAIL_THREAD_EVALUATIONS'),
      )
      expect(evalCall).toBeTruthy()
      // LIKELY_SCAM should be true
      expect(evalCall[3]).toContain('true') // both IS_DEAL=false and LIKELY_SCAM=true are present
      // Check the specific value tuple has likely_scam = true
      expect(evalCall[3]).toMatch(/false, true/) // IS_DEAL=false, LIKELY_SCAM=true

      return { processed: 1, failed: 0 }
    })

    await runClassifyPipeline()
  })
})
