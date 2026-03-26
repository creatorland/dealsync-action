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

// Mock filter-rules
const mockIsRejected = jest.fn()
jest.unstable_mockModule('../src/lib/filter-rules.js', () => ({
  isRejected: mockIsRejected,
}))

// Mock pipeline — use real insertBatchEvent but mock runPool
const mockRunPool = jest.fn()
const mockInsertBatchEvent = jest.fn()
jest.unstable_mockModule('../src/lib/pipeline.js', () => ({
  runPool: mockRunPool,
  insertBatchEvent: mockInsertBatchEvent,
}))

const core = await import('@actions/core')
const { runFilterPipeline } = await import('../src/commands/run-filter-pipeline.js')

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
    'max-concurrent': '5',
    'filter-batch-size': '200',
    'max-retries': '3',
    'chunk-size': '50',
    'fetch-timeout-ms': '30000',
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

// ============================================================
// Tests
// ============================================================

describe('run-filter-pipeline command', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    uuidCallCount = 0
    for (const key of Object.keys(outputs)) delete outputs[key]
    mockAuthenticate.mockResolvedValue('test-jwt')
    mockInsertBatchEvent.mockResolvedValue(undefined)
  })

  // ----------------------------------------------------------
  // Claims batches and processes them
  // ----------------------------------------------------------

  it('claims batches and processes them end-to-end', async () => {
    mockInputs()

    const rows = makeBatchRows(3)

    // Mock runPool to capture claimFn and workerFn, then simulate one batch
    mockRunPool.mockImplementation(async (claimFn, workerFn, opts) => {
      // Simulate: claim returns a batch, then null
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed rows
      // processFilterBatch will call exec for UPDATE passed + UPDATE rejected + insertBatchEvent via mock

      const batch = await claimFn()
      expect(batch).not.toBeNull()
      expect(batch.batch_id).toBe('test-uuid-1')
      expect(batch.count).toBe(3)
      expect(batch.rows).toEqual(rows)

      // Set up fetchEmails to return emails
      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [{ name: 'From', value: 'alice@company.com' }],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // First email passes, second rejected, third passes
      mockIsRejected.mockReturnValueOnce(false).mockReturnValueOnce(true).mockReturnValueOnce(false)

      // Mock exec calls for status updates
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected

      await workerFn(batch, { attempt: 0 })

      // Now claim returns null
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce([]) // SELECT — empty
        .mockResolvedValueOnce([]) // stuck batches — empty

      const batch2 = await claimFn()
      expect(batch2).toBeNull()

      return { processed: 1, failed: 0 }
    })

    const result = await runFilterPipeline()

    expect(result).toEqual({
      batches_processed: 1,
      batches_failed: 0,
      total_filtered: 2,
      total_rejected: 1,
    })

    // Verify authentication was called once
    expect(mockAuthenticate).toHaveBeenCalledTimes(1)
    expect(mockAuthenticate).toHaveBeenCalledWith('https://auth.example.com/token', 'test-secret')
  })

  // ----------------------------------------------------------
  // Calls fetchEmails with correct params (format: 'metadata')
  // ----------------------------------------------------------

  it('calls fetchEmails with correct params including format metadata', async () => {
    mockInputs({ 'chunk-size': '25', 'fetch-timeout-ms': '15000' })

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Set up claim
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      mockFetchEmails.mockResolvedValueOnce(
        rows.map((r) => ({
          messageId: r.MESSAGE_ID,
          id: r.EMAIL_METADATA_ID,
          topLevelHeaders: [],
        })),
      )
      mockIsRejected.mockReturnValue(false)
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()

    expect(mockFetchEmails).toHaveBeenCalledTimes(1)
    const [messageIds, metaMap, opts] = mockFetchEmails.mock.calls[0]

    expect(messageIds).toEqual(['msg-1', 'msg-2'])
    expect(metaMap).toBeInstanceOf(Map)
    expect(metaMap.get('msg-1')).toEqual(rows[0])
    expect(opts).toEqual({
      contentFetcherUrl: 'https://fetcher.example.com',
      userId: 'user-1',
      syncStateId: 'ss-1',
      chunkSize: 25,
      fetchTimeoutMs: 15000,
      format: 'metadata',
    })
  })

  // ----------------------------------------------------------
  // Applies filter rules correctly
  // ----------------------------------------------------------

  it('applies filter rules correctly separating passed and rejected', async () => {
    mockInputs()

    const rows = makeBatchRows(4)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // em-1 passes, em-2 rejected, em-3 rejected, em-4 passes
      mockIsRejected
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(false)

      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected

      await workerFn(batch, { attempt: 0 })

      return { processed: 1, failed: 0 }
    })

    const result = await runFilterPipeline()

    expect(mockIsRejected).toHaveBeenCalledTimes(4)
    expect(result.total_filtered).toBe(2)
    expect(result.total_rejected).toBe(2)
  })

  // ----------------------------------------------------------
  // Saves results (UPDATEs for passed/rejected)
  // ----------------------------------------------------------

  it('saves results with correct SQL UPDATEs for passed and rejected', async () => {
    mockInputs()

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)

      // em-1 passes, em-2 rejected
      mockIsRejected.mockReturnValueOnce(false).mockReturnValueOnce(true)

      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected

      await workerFn(batch, { attempt: 0 })

      // Verify the UPDATE calls made by the worker
      // exec calls executeSql(apiUrl, jwt, biscuit, sql) — SQL is at index 3
      const calls = mockExecuteSql.mock.calls
      const passedUpdateCall = calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('pending_classification'),
      )
      const rejectedUpdateCall = calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('filter_rejected'),
      )

      expect(passedUpdateCall).toBeTruthy()
      expect(passedUpdateCall[3]).toContain("STATUS = 'pending_classification'")
      expect(passedUpdateCall[3]).toContain("'em-1'")
      expect(passedUpdateCall[3]).toContain('UPDATED_AT = CURRENT_TIMESTAMP')

      expect(rejectedUpdateCall).toBeTruthy()
      expect(rejectedUpdateCall[3]).toContain("STATUS = 'filter_rejected'")
      expect(rejectedUpdateCall[3]).toContain("'em-2'")
      expect(rejectedUpdateCall[3]).toContain('UPDATED_AT = CURRENT_TIMESTAMP')

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()
  })

  // ----------------------------------------------------------
  // Records batch events (new + complete)
  // ----------------------------------------------------------

  it('records batch events for new claim and completion', async () => {
    mockInputs()

    const rows = makeBatchRows(1)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      // Verify 'new' batch event was inserted during claim
      expect(mockInsertBatchEvent).toHaveBeenCalledTimes(1)
      expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
        triggerHash: 'test-uuid-1',
        batchId: 'test-uuid-1',
        batchType: 'filter',
        eventType: 'new',
      })

      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockIsRejected.mockReturnValueOnce(false)
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed

      await workerFn(batch, { attempt: 0 })

      // Verify 'complete' batch event was inserted during processing
      expect(mockInsertBatchEvent).toHaveBeenCalledTimes(2)
      expect(mockInsertBatchEvent).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
        triggerHash: 'test-uuid-1',
        batchId: 'test-uuid-1',
        batchType: 'filter',
        eventType: 'complete',
      })

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()
  })

  // ----------------------------------------------------------
  // Returns correct totals
  // ----------------------------------------------------------

  it('returns correct totals from multiple batches', async () => {
    mockInputs()

    const rows1 = makeBatchRows(3)
    const rows2 = [
      {
        EMAIL_METADATA_ID: 'em-10',
        MESSAGE_ID: 'msg-10',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-10',
        SYNC_STATE_ID: 'ss-1',
      },
      {
        EMAIL_METADATA_ID: 'em-11',
        MESSAGE_ID: 'msg-11',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-11',
        SYNC_STATE_ID: 'ss-1',
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Batch 1
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE
        .mockResolvedValueOnce(rows1) // SELECT

      const batch1 = await claimFn()

      mockFetchEmails.mockResolvedValueOnce(
        rows1.map((r) => ({
          messageId: r.MESSAGE_ID,
          id: r.EMAIL_METADATA_ID,
          topLevelHeaders: [],
        })),
      )
      // Batch 1: 2 passed, 1 rejected
      mockIsRejected.mockReturnValueOnce(false).mockReturnValueOnce(true).mockReturnValueOnce(false)
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected

      await workerFn(batch1, { attempt: 0 })

      // Batch 2
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE
        .mockResolvedValueOnce(rows2) // SELECT

      const batch2 = await claimFn()

      mockFetchEmails.mockResolvedValueOnce(
        rows2.map((r) => ({
          messageId: r.MESSAGE_ID,
          id: r.EMAIL_METADATA_ID,
          topLevelHeaders: [],
        })),
      )
      // Batch 2: 1 passed, 1 rejected
      mockIsRejected.mockReturnValueOnce(false).mockReturnValueOnce(true)
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed
      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected

      await workerFn(batch2, { attempt: 0 })

      return { processed: 2, failed: 0 }
    })

    const result = await runFilterPipeline()

    expect(result).toEqual({
      batches_processed: 2,
      batches_failed: 0,
      total_filtered: 3,
      total_rejected: 2,
    })
  })

  // ----------------------------------------------------------
  // Handles empty batches (claim returns null immediately)
  // ----------------------------------------------------------

  it('handles empty batches when claim returns null immediately', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      // Claim returns null: no pending rows and no stuck batches
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim — 0 affected
        .mockResolvedValueOnce([]) // SELECT claimed — empty
        .mockResolvedValueOnce([]) // stuck batches — empty

      const batch = await claimFn()
      expect(batch).toBeNull()

      return { processed: 0, failed: 0 }
    })

    const result = await runFilterPipeline()

    expect(result).toEqual({
      batches_processed: 0,
      batches_failed: 0,
      total_filtered: 0,
      total_rejected: 0,
    })

    // No emails should have been fetched
    expect(mockFetchEmails).not.toHaveBeenCalled()
    expect(mockIsRejected).not.toHaveBeenCalled()
  })

  // ----------------------------------------------------------
  // Passes runPool correct options
  // ----------------------------------------------------------

  it('passes correct options to runPool', async () => {
    mockInputs({ 'max-concurrent': '3', 'max-retries': '5' })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runFilterPipeline()

    expect(mockRunPool).toHaveBeenCalledTimes(1)
    const [claimFn, workerFn, opts] = mockRunPool.mock.calls[0]
    expect(typeof claimFn).toBe('function')
    expect(typeof workerFn).toBe('function')
    expect(opts).toEqual({ maxConcurrent: 3, maxRetries: 5 })
  })

  // ----------------------------------------------------------
  // Authenticates once at start
  // ----------------------------------------------------------

  it('authenticates once at start and reuses JWT', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn) => {
      // Claim twice to verify auth is not called again
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])
      await claimFn()

      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])
      await claimFn()

      return { processed: 0, failed: 0 }
    })

    await runFilterPipeline()

    expect(mockAuthenticate).toHaveBeenCalledTimes(1)
  })

  // ----------------------------------------------------------
  // Default input values
  // ----------------------------------------------------------

  it('uses default values when inputs are not specified', async () => {
    mockInputs({
      'max-concurrent': '',
      'filter-batch-size': '',
      'max-retries': '',
      'chunk-size': '',
      'fetch-timeout-ms': '',
    })

    mockRunPool.mockResolvedValue({ processed: 0, failed: 0 })

    await runFilterPipeline()

    const [, , opts] = mockRunPool.mock.calls[0]
    expect(opts).toEqual({ maxConcurrent: 5, maxRetries: 3 })
  })

  // ----------------------------------------------------------
  // Schema validation
  // ----------------------------------------------------------

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runFilterPipeline()).rejects.toThrow('Invalid schema')
  })

  // ----------------------------------------------------------
  // Claim uses correct SQL for pending rows
  // ----------------------------------------------------------

  it('claim uses correct SQL for pending rows', async () => {
    mockInputs({ 'filter-batch-size': '100' })

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE
        .mockResolvedValueOnce([]) // SELECT — empty
        .mockResolvedValueOnce([]) // stuck — empty

      await claimFn()

      // Verify UPDATE SQL
      const updateSql = mockExecuteSql.mock.calls[0][3]
      expect(updateSql).toContain('UPDATE dealsync_stg_v1.DEAL_STATES')
      expect(updateSql).toContain("STATUS = 'filtering'")
      expect(updateSql).toContain("BATCH_ID = 'test-uuid-1'")
      expect(updateSql).toContain("STATUS = 'pending'")
      expect(updateSql).toContain('LIMIT 100')

      return { processed: 0, failed: 0 }
    })

    await runFilterPipeline()
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
        batchType: 'filter',
        eventType: 'retrigger',
      })

      return { processed: 0, failed: 0 }
    })

    await runFilterPipeline()
  })

  // ----------------------------------------------------------
  // Worker skips UPDATE when no passed or rejected IDs
  // ----------------------------------------------------------

  it('skips UPDATE when all emails pass (no rejected)', async () => {
    mockInputs()

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockIsRejected.mockReturnValue(false)

      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed (only)

      await workerFn(batch, { attempt: 0 })

      // No rejected UPDATE should exist
      const rejectedUpdate = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('filter_rejected'),
      )
      expect(rejectedUpdate).toBeUndefined()

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()
  })

  it('skips UPDATE when all emails are rejected (no passed)', async () => {
    mockInputs()

    const rows = makeBatchRows(2)

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      const emails = rows.map((r) => ({
        messageId: r.MESSAGE_ID,
        id: r.EMAIL_METADATA_ID,
        topLevelHeaders: [],
      }))
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockIsRejected.mockReturnValue(true)

      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE rejected (only)

      await workerFn(batch, { attempt: 0 })

      // No pending_classification update
      const passedUpdate = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('pending_classification'),
      )
      expect(passedUpdate).toBeUndefined()

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()
  })

  // ----------------------------------------------------------
  // Uses sanitizeId when building quoted ID lists
  // ----------------------------------------------------------

  it('uses sanitizeId when building SQL-quoted ID lists', async () => {
    mockInputs()

    const rows = [
      {
        EMAIL_METADATA_ID: 'em-valid-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-1',
        SYNC_STATE_ID: 'ss-1',
      },
    ]

    mockRunPool.mockImplementation(async (claimFn, workerFn) => {
      mockExecuteSql
        .mockResolvedValueOnce([]) // UPDATE claim
        .mockResolvedValueOnce(rows) // SELECT claimed

      const batch = await claimFn()

      const emails = [
        {
          messageId: 'msg-1',
          id: 'em-valid-1',
          topLevelHeaders: [],
        },
      ]
      mockFetchEmails.mockResolvedValueOnce(emails)
      mockIsRejected.mockReturnValueOnce(false)

      mockExecuteSql.mockResolvedValueOnce([]) // UPDATE passed

      await workerFn(batch, { attempt: 0 })

      // Find the UPDATE call for passed IDs
      const passedCall = mockExecuteSql.mock.calls.find(
        (c) => typeof c[3] === 'string' && c[3].includes('pending_classification'),
      )
      expect(passedCall).toBeTruthy()
      // sanitizeId wraps in quotes: 'em-valid-1'
      expect(passedCall[3]).toContain("'em-valid-1'")

      return { processed: 1, failed: 0 }
    })

    await runFilterPipeline()
  })

  // ----------------------------------------------------------
  // Handles pool reporting failed batches
  // ----------------------------------------------------------

  it('reports failed batches from pool', async () => {
    mockInputs()

    mockRunPool.mockResolvedValue({ processed: 3, failed: 2 })

    const result = await runFilterPipeline()

    expect(result).toEqual({
      batches_processed: 3,
      batches_failed: 2,
      total_filtered: 0,
      total_rejected: 0,
    })
  })

  // ----------------------------------------------------------
  // exec helper passes correct args to executeSql
  // ----------------------------------------------------------

  it('exec helper passes apiUrl, jwt, biscuit to executeSql', async () => {
    mockInputs()

    mockRunPool.mockImplementation(async (claimFn) => {
      mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce([]).mockResolvedValueOnce([])

      await claimFn()

      // Verify executeSql was called with correct args
      const firstCall = mockExecuteSql.mock.calls[0]
      expect(firstCall[0]).toBe('https://sxt.example.com') // apiUrl
      expect(firstCall[1]).toBe('test-jwt') // jwt
      expect(firstCall[2]).toBe('test-biscuit') // biscuit
      expect(typeof firstCall[3]).toBe('string') // sql

      return { processed: 0, failed: 0 }
    })

    await runFilterPipeline()
  })
})
