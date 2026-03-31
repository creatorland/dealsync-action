import { jest } from '@jest/globals'
import { WriteBatcher } from '../src/lib/batcher.js'

// ============================================================
// Helpers
// ============================================================

function makeBatcher(execFn, opts = {}) {
  return new WriteBatcher(execFn, 'TEST_SCHEMA', {
    flushIntervalMs: 60000, // high so timer doesn't fire during tests
    flushThreshold: 3,
    ...opts,
  })
}

// ============================================================
// Tests
// ============================================================

describe('WriteBatcher', () => {
  let mockExec

  beforeEach(() => {
    mockExec = jest.fn().mockResolvedValue([])
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  // ----------------------------------------------------------
  // Constructor
  // ----------------------------------------------------------

  it('creates queues and starts timer', () => {
    const batcher = makeBatcher(mockExec)
    expect(batcher._queues).toBeDefined()
    expect(batcher._queues.evals).toBeDefined()
    expect(batcher._queues.dealDeletes).toBeDefined()
    expect(batcher._queues.deals).toBeDefined()
    expect(batcher._queues.contactDeletes).toBeDefined()
    expect(batcher._queues.contacts).toBeDefined()
    expect(batcher._queues.stateUpdates).toBeDefined()
    expect(batcher._queues.batchEvents).toBeDefined()
    expect(batcher._queues.coreContacts).toBeDefined()
    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushEvals — threshold flush
  // ----------------------------------------------------------

  it('flushes evals when threshold is reached', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 2 })

    const p = batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      "('id2', 'th-2', '', 'cat', 'sum', false, false, 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO TEST_SCHEMA.EMAIL_THREAD_EVALUATIONS')
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
    expect(sql).toContain('th-1')
    expect(sql).toContain('th-2')

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushDealDeletes
  // ----------------------------------------------------------

  it('flushes deal deletes with correct SQL', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 2 })

    const p = batcher.pushDealDeletes(["'th-1'", "'th-2'"])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('DELETE FROM TEST_SCHEMA.DEALS WHERE THREAD_ID IN')
    expect(sql).toContain("'th-1'")
    expect(sql).toContain("'th-2'")

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushDeals
  // ----------------------------------------------------------

  it('flushes deals with correct upsert SQL', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushDeals([
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'new', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    await p

    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO TEST_SCHEMA.DEALS')
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
    expect(sql).toContain('d1')

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushContactDeletes
  // ----------------------------------------------------------

  it('flushes contact deletes with correct SQL', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 2 })

    const p = batcher.pushContactDeletes(["'th-1'", "'th-2'"])
    await p

    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('DELETE FROM TEST_SCHEMA.DEAL_CONTACTS WHERE DEAL_ID IN')
    expect(sql).toContain("'th-1'")

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushContacts
  // ----------------------------------------------------------

  it('flushes contacts with simplified 4-column ON CONFLICT upsert', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushContacts([
      "('thread-1', 'user-1', 'email@co.com', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    await p

    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO TEST_SCHEMA.DEAL_CONTACTS')
    expect(sql).toContain('DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE')
    expect(sql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET')
    expect(sql).toContain('CONTACT_TYPE = EXCLUDED.CONTACT_TYPE')
    expect(sql).not.toContain('CONTACT_ID')
    expect(sql).not.toContain('IS_FAVORITE')

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushStateUpdates
  // ----------------------------------------------------------

  it('flushes state updates with deal and not_deal SQL', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushStateUpdates(["'em-1'"], ["'em-2'"])
    await p

    expect(mockExec).toHaveBeenCalledTimes(2)

    const dealSql = mockExec.mock.calls[0][0]
    expect(dealSql).toContain("SET STATUS = 'deal'")
    expect(dealSql).toContain("'em-1'")

    const notDealSql = mockExec.mock.calls[1][0]
    expect(notDealSql).toContain("SET STATUS = 'not_deal'")
    expect(notDealSql).toContain("'em-2'")

    batcher.stop()
  })

  it('skips deal update when no deal IDs provided', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushStateUpdates([], ["'em-2'"])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain("SET STATUS = 'not_deal'")

    batcher.stop()
  })

  it('skips not_deal update when no not_deal IDs provided', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushStateUpdates(["'em-1'"], [])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain("SET STATUS = 'deal'")

    batcher.stop()
  })

  it('skips both state updates when both arrays are empty', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushStateUpdates([], [])
    await p

    expect(mockExec).not.toHaveBeenCalled()

    batcher.stop()
  })

  // ----------------------------------------------------------
  // pushBatchEvents
  // ----------------------------------------------------------

  it('flushes batch events with correct upsert SQL', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    const p = batcher.pushBatchEvents([
      "('hash1', 'batch1', 'classify', 'complete', CURRENT_TIMESTAMP)",
    ])
    await p

    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO TEST_SCHEMA.BATCH_EVENTS')
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH) DO UPDATE SET')
    expect(sql).toContain('hash1')

    batcher.stop()
  })

  // ----------------------------------------------------------
  // Accumulation below threshold
  // ----------------------------------------------------------

  it('does not flush until threshold is reached', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 3 })

    // Push 2 items, below threshold of 3
    batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    batcher.pushEvals([
      "('id2', 'th-2', '', 'cat', 'sum', false, false, 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    // Not flushed yet
    expect(mockExec).not.toHaveBeenCalled()
    expect(batcher._queues.evals.items).toHaveLength(2)

    // Push third item to trigger threshold
    const p = batcher.pushEvals([
      "('id3', 'th-3', '', 'cat', 'sum', false, false, 5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)

    batcher.stop()
  })

  // ----------------------------------------------------------
  // drain() flushes all pending queues
  // ----------------------------------------------------------

  it('drain() flushes all pending queues and clears timer', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 100 }) // very high threshold

    // Push to multiple queues (below threshold)
    batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    batcher.pushDeals([
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'new', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    expect(mockExec).not.toHaveBeenCalled()

    await batcher.drain()

    // Both queues should have been flushed
    expect(mockExec).toHaveBeenCalledTimes(2)
    const sqls = mockExec.mock.calls.map((c) => c[0])
    expect(sqls.some((s) => s.includes('EMAIL_THREAD_EVALUATIONS'))).toBe(true)
    expect(sqls.some((s) => s.includes('TEST_SCHEMA.DEALS'))).toBe(true)
  })

  // ----------------------------------------------------------
  // stop() clears timer without flushing
  // ----------------------------------------------------------

  it('stop() clears timer and rejects pending waiters', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 100 })

    const p = batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    batcher.stop()

    expect(mockExec).not.toHaveBeenCalled()
    await expect(p).rejects.toThrow('WriteBatcher stopped')
  })

  // ----------------------------------------------------------
  // Error handling — rejects waiters on flush error
  // ----------------------------------------------------------

  it('rejects all waiters when flush fails', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 2 })

    mockExec.mockRejectedValueOnce(new Error('SQL execution failed'))

    const p1 = batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    const p2 = batcher.pushEvals([
      "('id2', 'th-2', '', 'cat', 'sum', false, false, 3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    await expect(p1).rejects.toThrow('SQL execution failed')
    await expect(p2).rejects.toThrow('SQL execution failed')

    batcher.stop()
  })

  // ----------------------------------------------------------
  // Multiple pushes to same queue combine items
  // ----------------------------------------------------------

  it('combines items from multiple pushes into single flush', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 3 })

    batcher.pushDealDeletes(["'th-1'"])
    batcher.pushDealDeletes(["'th-2'"])
    const p = batcher.pushDealDeletes(["'th-3'"])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain("'th-1'")
    expect(sql).toContain("'th-2'")
    expect(sql).toContain("'th-3'")

    batcher.stop()
  })

  // ----------------------------------------------------------
  // State updates merge multiple pushes
  // ----------------------------------------------------------

  it('merges multiple state update pushes into combined flush', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 100 })

    batcher.pushStateUpdates(["'em-1'"], ["'em-3'"])
    batcher.pushStateUpdates(["'em-2'"], ["'em-4'"])

    await batcher.drain()

    // Should produce two SQL calls (deal + not_deal)
    expect(mockExec).toHaveBeenCalledTimes(2)

    const dealSql = mockExec.mock.calls[0][0]
    expect(dealSql).toContain("'em-1'")
    expect(dealSql).toContain("'em-2'")

    const notDealSql = mockExec.mock.calls[1][0]
    expect(notDealSql).toContain("'em-3'")
    expect(notDealSql).toContain("'em-4'")
  })

  // ----------------------------------------------------------
  // Independent queues
  // ----------------------------------------------------------

  it('queues are independent — flushing one does not affect others', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 2 })

    // Push to evals (below threshold) — capture promise to handle rejection on stop
    const evalPromise = batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    // Push to dealDeletes (at threshold) — should flush only dealDeletes
    const p = batcher.pushDealDeletes(["'th-1'", "'th-2'"])
    await p

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('DELETE FROM TEST_SCHEMA.DEALS')

    // Evals should still be pending
    expect(batcher._queues.evals.items).toHaveLength(1)

    batcher.stop()
    // The eval promise is rejected by stop() — catch to avoid unhandled rejection
    await expect(evalPromise).rejects.toThrow('WriteBatcher stopped')
  })

  // ----------------------------------------------------------
  // Timer-based flush
  // ----------------------------------------------------------

  it('timer-based flush fires on interval', async () => {
    jest.useFakeTimers()

    const batcher = new WriteBatcher(mockExec, 'TEST_SCHEMA', {
      flushIntervalMs: 1000,
      flushThreshold: 100, // high so threshold doesn't trigger
    })

    batcher.pushEvals([
      "('id1', 'th-1', '', 'cat', 'sum', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    expect(mockExec).not.toHaveBeenCalled()

    // Advance past the interval
    jest.advanceTimersByTime(1100)

    // _flushAll is async — we need to let the microtask queue drain
    await Promise.resolve()

    expect(mockExec).toHaveBeenCalledTimes(1)

    batcher.stop()
    jest.useRealTimers()
  })

  // ----------------------------------------------------------
  // Flush empty queue is a no-op
  // ----------------------------------------------------------

  it('flushing an empty queue is a no-op', async () => {
    const batcher = makeBatcher(mockExec)

    await batcher.drain()

    expect(mockExec).not.toHaveBeenCalled()
  })

  // ----------------------------------------------------------
  // Log output on flush
  // ----------------------------------------------------------

  it('logs flush info with queue name and item count', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    await batcher.pushDeals([
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'new', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('[batcher] flushing deals: 1 items'),
    )

    consoleSpy.mockRestore()
    batcher.stop()
  })

  // ----------------------------------------------------------
  // coreContacts queue
  // ----------------------------------------------------------

  describe('coreContacts queue', () => {
    it('has coreContacts queue in _queues', () => {
      const batcher = makeBatcher(mockExec)
      expect(batcher._queues.coreContacts).toBeDefined()
      batcher.stop()
    })

    it('flushes coreContacts with COALESCE ON CONFLICT SQL using coreSchema', async () => {
      const batcher = new WriteBatcher(mockExec, 'TEST_SCHEMA', {
        flushIntervalMs: 60000,
        flushThreshold: 1,
        coreSchema: 'MY_CORE_SCHEMA',
      })

      await batcher.pushCoreContacts([
        "('user-1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      ])

      expect(mockExec).toHaveBeenCalledTimes(1)
      const sql = mockExec.mock.calls[0][0]
      expect(sql).toContain('INSERT INTO MY_CORE_SCHEMA.CONTACTS')
      expect(sql).toContain('USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER')
      expect(sql).toContain('ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET')
      expect(sql).toContain('NAME = EXCLUDED.NAME')
      expect(sql).toContain('COMPANY_NAME = EXCLUDED.COMPANY_NAME')
      expect(sql).toContain('TITLE = EXCLUDED.TITLE')
      expect(sql).toContain('PHONE_NUMBER = EXCLUDED.PHONE_NUMBER')

      batcher.stop()
    })

    it('defaults coreSchema to EMAIL_CORE_STAGING', async () => {
      const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

      await batcher.pushCoreContacts([
        "('user-1', 'bob@co.com', 'Bob', NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      ])

      const sql = mockExec.mock.calls[0][0]
      expect(sql).toContain('INSERT INTO EMAIL_CORE_STAGING.CONTACTS')

      batcher.stop()
    })

    it('deduplicates by (USER_ID, EMAIL) within a flush batch', async () => {
      const batcher = makeBatcher(mockExec, { flushThreshold: 100 })

      // Push duplicate (user-1, alice@co.com) — second push has updated name
      batcher.pushCoreContacts([
        "('user-1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "('user-1', 'bob@co.com', 'Bob', NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      ])
      batcher.pushCoreContacts([
        "('user-1', 'alice@co.com', 'Alice K', 'Acme Corp', 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
      ])

      await batcher.drain()

      expect(mockExec).toHaveBeenCalledTimes(1)
      const sql = mockExec.mock.calls[0][0]
      // Should contain only 2 rows (deduped), not 3
      expect(sql).toContain('bob@co.com')
      expect(sql).toContain('Alice K') // last write wins
      expect(sql).not.toContain("'Alice',") // first version replaced
    })
  })
})
