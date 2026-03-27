import { jest } from '@jest/globals'

// Mock @actions/core before importing the module under test
const mockCore = {
  error: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
}

jest.unstable_mockModule('@actions/core', () => mockCore)

const { runPool, insertBatchEvent, sweepStuckRows, sweepOrphanedRows } =
  await import('../src/lib/pipeline.js')

beforeEach(() => {
  jest.clearAllMocks()
})

// ============================================================
// runPool
// ============================================================

describe('runPool', () => {
  it('processes all batches claimed by claimFn', async () => {
    const batches = [
      { batch_id: 'b1', attempts: 0 },
      { batch_id: 'b2', attempts: 0 },
      { batch_id: 'b3', attempts: 0 },
    ]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {})

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })

    expect(results).toEqual({ processed: 3, failed: 0 })
    expect(workerFn).toHaveBeenCalledTimes(3)
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 0 })
    expect(workerFn).toHaveBeenCalledWith(batches[1], { attempt: 0 })
    expect(workerFn).toHaveBeenCalledWith(batches[2], { attempt: 0 })
  })

  it('returns immediately when claimFn returns null on first call', async () => {
    const claimFn = jest.fn(async () => null)
    const workerFn = jest.fn(async () => {})

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })

    expect(results).toEqual({ processed: 0, failed: 0 })
    expect(claimFn).toHaveBeenCalledTimes(1)
    expect(workerFn).not.toHaveBeenCalled()
  })

  it('retries a failing worker up to maxRetries then dead-letters', async () => {
    const batches = [{ batch_id: 'b-fail', attempts: 0 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {
      throw new Error('boom')
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 2 })

    expect(results).toEqual({ processed: 0, failed: 1 })
    expect(workerFn).toHaveBeenCalledTimes(2)
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 0 })
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 1 })
    // Should log error messages
    expect(mockCore.error).toHaveBeenCalled()
    // Should log dead-lettered message
    const deadLetteredCall = mockCore.error.mock.calls.find((c) => c[0].includes('dead-lettered'))
    expect(deadLetteredCall).toBeTruthy()
  })

  it('retries and eventually succeeds', async () => {
    const batches = [{ batch_id: 'b-flaky', attempts: 0 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    let callCount = 0
    const workerFn = jest.fn(async () => {
      callCount++
      if (callCount < 2) throw new Error('transient')
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 5 })

    expect(results).toEqual({ processed: 1, failed: 0 })
    expect(workerFn).toHaveBeenCalledTimes(2)
    // First call fails (attempt 0), second call succeeds (attempt 1)
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 0 })
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 1 })
  }, 10000)

  it('respects retriggered batch attempt count', async () => {
    // Batch already attempted once (retriggered)
    const batches = [{ batch_id: 'b-retrigger', attempts: 1 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {
      throw new Error('still failing')
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3 })

    expect(results).toEqual({ processed: 0, failed: 1 })
    // Should only attempt from 1 to maxRetries (2 more attempts)
    expect(workerFn).toHaveBeenCalledTimes(2)
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 1 })
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 2 })
  }, 10000)

  it('failed workers do not stop the pool', async () => {
    const batches = [
      { batch_id: 'b-fail', attempts: 0 },
      { batch_id: 'b-ok', attempts: 0 },
    ]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async (batch) => {
      if (batch.batch_id === 'b-fail') throw new Error('oops')
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 1 })

    expect(results).toEqual({ processed: 1, failed: 1 })
  })

  it('respects maxConcurrent limit', async () => {
    let maxSeen = 0
    let currentActive = 0
    const batches = [
      { batch_id: 'b1', attempts: 0 },
      { batch_id: 'b2', attempts: 0 },
      { batch_id: 'b3', attempts: 0 },
      { batch_id: 'b4', attempts: 0 },
    ]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {
      currentActive++
      maxSeen = Math.max(maxSeen, currentActive)
      await new Promise((r) => setTimeout(r, 50))
      currentActive--
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })

    expect(results).toEqual({ processed: 4, failed: 0 })
    expect(maxSeen).toBeLessThanOrEqual(2)
  })

  it('waits for active workers when claim returns null then tries claiming again', async () => {
    // Simulate: first claim returns a batch, second returns null (while worker active),
    // worker finishes, third claim returns another batch, fourth returns null (done)
    let callNum = 0

    const claimFn = jest.fn(async () => {
      callNum++
      if (callNum === 1) return { batch_id: 'b1', attempts: 0 }
      if (callNum === 2) return null // worker still active
      if (callNum === 3) return { batch_id: 'b2', attempts: 0 }
      return null
    })

    const workerFn = jest.fn(async (batch) => {
      if (batch.batch_id === 'b1') {
        // Simulate slow worker — resolve after a delay
        await new Promise((r) => setTimeout(r, 50))
      }
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })

    expect(results).toEqual({ processed: 2, failed: 0 })
    // claimFn should have been called at least 3 times (to re-claim after null)
    expect(claimFn.mock.calls.length).toBeGreaterThanOrEqual(3)
  })

  it('handles batch.attempts undefined (defaults to 0)', async () => {
    const batches = [{ batch_id: 'b-no-attempts' }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {})

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3 })

    expect(results).toEqual({ processed: 1, failed: 0 })
    expect(workerFn).toHaveBeenCalledWith(batches[0], { attempt: 0 })
  })

  it('batch with attempts >= maxRetries is immediately dead-lettered', async () => {
    const batches = [{ batch_id: 'b-exhausted', attempts: 3 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {
      throw new Error('should not run')
    })

    const results = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3 })

    expect(workerFn).not.toHaveBeenCalled()
    expect(results).toEqual({ processed: 0, failed: 1 })
  })

  it('invokes onDeadLetter when batch is already exhausted', async () => {
    const onDeadLetter = jest.fn().mockResolvedValue(undefined)
    const batches = [{ batch_id: 'b-exhausted', attempts: 3 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn()

    await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3, onDeadLetter })

    expect(onDeadLetter).toHaveBeenCalledTimes(1)
    expect(onDeadLetter).toHaveBeenCalledWith(batches[0])
    expect(workerFn).not.toHaveBeenCalled()
  })

  it('invokes onDeadLetter after worker exhausts retries', async () => {
    const onDeadLetter = jest.fn().mockResolvedValue(undefined)
    const batches = [{ batch_id: 'b-fail', attempts: 0 }]
    let idx = 0
    const claimFn = jest.fn(async () => (idx < batches.length ? batches[idx++] : null))
    const workerFn = jest.fn(async () => {
      throw new Error('boom')
    })

    await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 2, onDeadLetter })

    expect(onDeadLetter).toHaveBeenCalledTimes(1)
    expect(onDeadLetter).toHaveBeenCalledWith(batches[0])
  })
})

// ============================================================
// insertBatchEvent
// ============================================================

describe('insertBatchEvent', () => {
  it('generates correct INSERT SQL with sanitized inputs', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await insertBatchEvent(executeSqlFn, 'MY_SCHEMA', {
      triggerHash: 'abc-123',
      batchId: 'batch-456',
      batchType: 'filter',
      eventType: 'claimed',
    })

    expect(executeSqlFn).toHaveBeenCalledTimes(1)
    const sql = executeSqlFn.mock.calls[0][0]
    expect(sql).toContain('MY_SCHEMA.BATCH_EVENTS')
    expect(sql).toContain("'abc-123'")
    expect(sql).toContain("'batch-456'")
    expect(sql).toContain("'filter'")
    expect(sql).toContain("'claimed'")
    expect(sql).toContain('CURRENT_TIMESTAMP')
  })

  it('sanitizes string values (escapes single quotes)', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await insertBatchEvent(executeSqlFn, 'MY_SCHEMA', {
      triggerHash: 'abc-123',
      batchId: 'batch-456',
      batchType: "filter's",
      eventType: "it's done",
    })

    const sql = executeSqlFn.mock.calls[0][0]
    expect(sql).toContain("'filter''s'")
    expect(sql).toContain("'it''s done'")
  })

  it('rejects invalid schema', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await expect(
      insertBatchEvent(executeSqlFn, 'bad schema!', {
        triggerHash: 'abc',
        batchId: 'def',
        batchType: 'filter',
        eventType: 'claimed',
      }),
    ).rejects.toThrow('Invalid schema')
  })

  it('rejects invalid triggerHash', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await expect(
      insertBatchEvent(executeSqlFn, 'MY_SCHEMA', {
        triggerHash: 'bad hash!',
        batchId: 'def',
        batchType: 'filter',
        eventType: 'claimed',
      }),
    ).rejects.toThrow('Invalid ID')
  })

  it('rejects invalid batchId', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await expect(
      insertBatchEvent(executeSqlFn, 'MY_SCHEMA', {
        triggerHash: 'abc',
        batchId: 'bad id!',
        batchType: 'filter',
        eventType: 'claimed',
      }),
    ).rejects.toThrow('Invalid ID')
  })

  it('produces the expected full SQL statement', async () => {
    const executeSqlFn = jest.fn(async () => {})

    await insertBatchEvent(executeSqlFn, 'TEST_SCHEMA', {
      triggerHash: 'trigger-abc',
      batchId: 'batch-xyz',
      batchType: 'classify',
      eventType: 'completed',
    })

    const expectedSql =
      "INSERT INTO TEST_SCHEMA.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ('trigger-abc', 'batch-xyz', 'classify', 'completed', CURRENT_TIMESTAMP) ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP"
    expect(executeSqlFn).toHaveBeenCalledWith(expectedSql)
  })
})

// ============================================================
// sweepStuckRows / sweepOrphanedRows
// ============================================================

describe('sweepStuckRows', () => {
  it('returns 0 when no exhausted batches', async () => {
    const exec = jest.fn().mockResolvedValueOnce([])
    const n = await sweepStuckRows(exec, 'dealsync_stg_v1', {
      activeStatus: 'filtering',
      batchType: 'filter',
      maxRetries: 6,
    })
    expect(n).toBe(0)
    expect(exec).toHaveBeenCalledTimes(1)
    expect(exec.mock.calls[0][0]).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= 6')
  })

  it('updates rows to failed and inserts dead_letter event per exhausted batch', async () => {
    const exec = jest
      .fn()
      .mockResolvedValueOnce([{ BATCH_ID: 'batch-a' }])
      .mockResolvedValueOnce([{ C: 2 }])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
    const n = await sweepStuckRows(exec, 'dealsync_stg_v1', {
      activeStatus: 'filtering',
      batchType: 'filter',
      maxRetries: 3,
    })
    expect(n).toBe(2)
    expect(exec).toHaveBeenCalledTimes(4)
    expect(exec.mock.calls[2][0]).toContain("STATUS = 'failed'")
    expect(exec.mock.calls[3][0]).toContain("'dead_letter'")
  })

  it('skips update and dead_letter event when no rows remain in active status', async () => {
    const exec = jest
      .fn()
      .mockResolvedValueOnce([{ BATCH_ID: 'batch-z' }])
      .mockResolvedValueOnce([{ C: 0 }])
    const n = await sweepStuckRows(exec, 'dealsync_stg_v1', {
      activeStatus: 'filtering',
      batchType: 'filter',
      maxRetries: 3,
    })
    expect(n).toBe(0)
    expect(exec).toHaveBeenCalledTimes(2)
    expect(exec.mock.calls[1][0]).toContain('COUNT(*)')
    expect(exec.mock.calls[1][0]).toContain("STATUS = 'filtering'")
  })
})

describe('sweepOrphanedRows', () => {
  it('rejects invalid staleMinutes', async () => {
    await expect(
      sweepOrphanedRows(jest.fn(), 'dealsync_stg_v1', {
        statuses: ['pending_classification'],
        staleMinutes: '30x',
      }),
    ).rejects.toThrow(/staleMinutes must be a non-negative integer/)
  })

  it('returns 0 when count is 0', async () => {
    const exec = jest.fn().mockResolvedValueOnce([{ C: 0 }])
    const n = await sweepOrphanedRows(exec, 'dealsync_stg_v1', {
      statuses: ['pending_classification'],
      staleMinutes: 30,
    })
    expect(n).toBe(0)
    expect(exec).toHaveBeenCalledTimes(1)
  })

  it('updates orphaned rows to failed when count > 0', async () => {
    const exec = jest
      .fn()
      .mockResolvedValueOnce([{ C: 3 }])
      .mockResolvedValueOnce([])
    const n = await sweepOrphanedRows(exec, 'dealsync_stg_v1', {
      statuses: ['pending_classification'],
      staleMinutes: 30,
    })
    expect(n).toBe(3)
    expect(exec).toHaveBeenCalledTimes(2)
    expect(exec.mock.calls[1][0]).toContain("STATUS = 'failed'")
    expect(exec.mock.calls[1][0]).toContain('BATCH_ID IS NULL')
  })
})
