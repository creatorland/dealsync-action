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

const core = await import('@actions/core')
const { runClaimFilterBatch } = await import('../src/commands/claim-filter-batch.js')

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
    'filter-batch-size': '200',
    'max-retries': '3',
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

function authResponse() {
  return new Response(JSON.stringify({ data: 'test-jwt' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function sxtResponse(data = []) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function getSqlCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => c[0].includes('/v1/sql'))
}

function getSqlText(call) {
  return JSON.parse(call[1].body).sqlText
}

// ============================================================
// Tests
// ============================================================

describe('claim-filter-batch command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    uuidCallCount = 0
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  // ----------------------------------------------------------
  // Happy path: pending rows found
  // ----------------------------------------------------------

  it('claims pending rows and returns batch with count', async () => {
    mockInputs({ 'filter-batch-size': '100' })

    const claimedRows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'user-1',
        THREAD_ID: 'thread-1',
        SYNC_STATE_ID: 'ss-1',
      },
      {
        EMAIL_METADATA_ID: 'em-2',
        MESSAGE_ID: 'msg-2',
        USER_ID: 'user-2',
        THREAD_ID: 'thread-2',
        SYNC_STATE_ID: 'ss-2',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // UPDATE (claim)
      .mockResolvedValueOnce(sxtResponse(claimedRows)) // SELECT claimed
      .mockResolvedValueOnce(sxtResponse()) // INSERT batch event

    const result = await runClaimFilterBatch()

    expect(result).toEqual({
      batch_id: 'test-uuid-1',
      count: 2,
      attempts: 0,
      rows: claimedRows,
    })

    // Verify SQL calls
    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // UPDATE + SELECT + INSERT batch event

    // UPDATE claim query
    const updateSql = getSqlText(sqlCalls[0])
    expect(updateSql).toContain('UPDATE dealsync_stg_v1.DEAL_STATES')
    expect(updateSql).toContain("STATUS = 'filtering'")
    expect(updateSql).toContain("BATCH_ID = 'test-uuid-1'")
    expect(updateSql).toContain('UPDATED_AT = CURRENT_TIMESTAMP')
    expect(updateSql).toContain("STATUS = 'pending'")
    expect(updateSql).toContain('LIMIT 100')

    // SELECT query
    const selectSql = getSqlText(sqlCalls[1])
    expect(selectSql).toContain(
      'SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID',
    )
    expect(selectSql).toContain("BATCH_ID = 'test-uuid-1'")

    // INSERT batch event
    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain('BATCH_EVENTS')
    expect(insertSql).toContain("'test-uuid-1'") // triggerHash
    expect(insertSql).toContain("'filter'")
    expect(insertSql).toContain("'new'")
  })

  // ----------------------------------------------------------
  // No pending rows, no stuck batches
  // ----------------------------------------------------------

  it('returns null batch_id when no pending rows and no stuck batches', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // UPDATE (claim — 0 affected)
      .mockResolvedValueOnce(sxtResponse([])) // SELECT claimed — empty
      .mockResolvedValueOnce(sxtResponse([])) // stuck batches query — empty

    const result = await runClaimFilterBatch()

    expect(result).toEqual({ batch_id: null, count: 0 })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // UPDATE + SELECT + stuck query
  })

  // ----------------------------------------------------------
  // Stuck batch re-claim
  // ----------------------------------------------------------

  it('re-claims a stuck batch when no pending rows exist', async () => {
    mockInputs({ 'max-retries': '5' })

    const stuckBatchResult = [{ BATCH_ID: 'stuck-batch-abc', ATTEMPTS: 2 }]
    const stuckRows = [
      {
        EMAIL_METADATA_ID: 'em-10',
        MESSAGE_ID: 'msg-10',
        USER_ID: 'user-10',
        THREAD_ID: 'thread-10',
        SYNC_STATE_ID: 'ss-10',
      },
      {
        EMAIL_METADATA_ID: 'em-11',
        MESSAGE_ID: 'msg-11',
        USER_ID: 'user-11',
        THREAD_ID: 'thread-11',
        SYNC_STATE_ID: 'ss-11',
      },
      {
        EMAIL_METADATA_ID: 'em-12',
        MESSAGE_ID: 'msg-12',
        USER_ID: 'user-12',
        THREAD_ID: 'thread-12',
        SYNC_STATE_ID: 'ss-12',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // UPDATE (claim — 0 affected)
      .mockResolvedValueOnce(sxtResponse([])) // SELECT claimed — empty
      .mockResolvedValueOnce(sxtResponse(stuckBatchResult)) // stuck batches query
      .mockResolvedValueOnce(sxtResponse(stuckRows)) // SELECT stuck rows
      .mockResolvedValueOnce(sxtResponse()) // UPDATE UPDATED_AT
      .mockResolvedValueOnce(sxtResponse()) // INSERT batch event

    const result = await runClaimFilterBatch()

    expect(result).toEqual({
      batch_id: 'stuck-batch-abc',
      count: 3,
      attempts: 2,
      rows: stuckRows,
    })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(6) // UPDATE claim + SELECT claimed + stuck query + SELECT stuck rows + UPDATE UPDATED_AT + INSERT batch event

    // Stuck batch query uses maxRetries
    const stuckSql = getSqlText(sqlCalls[2])
    expect(stuckSql).toContain("ds.STATUS = 'filtering'")
    expect(stuckSql).toContain('ds.BATCH_ID IS NOT NULL')
    expect(stuckSql).toContain("INTERVAL '5' MINUTE")
    expect(stuckSql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 5')
    expect(stuckSql).toContain('LIMIT 1')

    // SELECT stuck rows by batch ID
    const selectStuckSql = getSqlText(sqlCalls[3])
    expect(selectStuckSql).toContain("BATCH_ID = 'stuck-batch-abc'")

    // UPDATE UPDATED_AT to prevent other grabbers
    const updateTimeSql = getSqlText(sqlCalls[4])
    expect(updateTimeSql).toContain('UPDATED_AT = CURRENT_TIMESTAMP')
    expect(updateTimeSql).toContain("BATCH_ID = 'stuck-batch-abc'")

    // INSERT batch event with retrigger
    const insertSql = getSqlText(sqlCalls[5])
    expect(insertSql).toContain('BATCH_EVENTS')
    expect(insertSql).toContain("'test-uuid-2'") // new triggerHash (2nd uuid call)
    expect(insertSql).toContain("'stuck-batch-abc'") // batchId
    expect(insertSql).toContain("'filter'")
    expect(insertSql).toContain("'retrigger'")
  })

  // ----------------------------------------------------------
  // Default input values
  // ----------------------------------------------------------

  it('uses default batch size of 200 when not specified', async () => {
    mockInputs({ 'filter-batch-size': '' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const updateSql = getSqlText(sqlCalls[0])
    expect(updateSql).toContain('LIMIT 200')
  })

  it('uses default max-retries of 3 when not specified', async () => {
    mockInputs({ 'max-retries': '' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const stuckSql = getSqlText(sqlCalls[2])
    expect(stuckSql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 3')
  })

  // ----------------------------------------------------------
  // Schema validation
  // ----------------------------------------------------------

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runClaimFilterBatch()).rejects.toThrow('Invalid schema')
  })

  // ----------------------------------------------------------
  // Authentication
  // ----------------------------------------------------------

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  // ----------------------------------------------------------
  // SQL uses correct STATUS constants
  // ----------------------------------------------------------

  it('uses STATUS constants from queries.js in SQL', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const updateSql = getSqlText(sqlCalls[0])

    // UPDATE sets status to 'filtering'
    expect(updateSql).toContain("STATUS = 'filtering'")
    // Subquery WHERE uses 'pending'
    expect(updateSql).toContain("STATUS = 'pending'")
  })

  // ----------------------------------------------------------
  // JWT is passed to SQL calls
  // ----------------------------------------------------------

  it('passes JWT from auth to all SQL calls', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth returns 'test-jwt'
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    for (const call of sqlCalls) {
      expect(call[1].headers.Authorization).toBe('Bearer test-jwt')
    }
  })

  // ----------------------------------------------------------
  // Biscuit is passed to SQL calls
  // ----------------------------------------------------------

  it('passes biscuit to all SQL calls', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    for (const call of sqlCalls) {
      const body = JSON.parse(call[1].body)
      expect(body.biscuits).toEqual(['test-biscuit'])
    }
  })

  // ----------------------------------------------------------
  // Custom batch size
  // ----------------------------------------------------------

  it('respects custom filter-batch-size', async () => {
    mockInputs({ 'filter-batch-size': '50' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const updateSql = getSqlText(sqlCalls[0])
    expect(updateSql).toContain('LIMIT 50')
  })

  // ----------------------------------------------------------
  // Atomic claim pattern: UPDATE...WHERE subquery
  // ----------------------------------------------------------

  it('uses atomic UPDATE...WHERE IN (SELECT ... LIMIT) pattern', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimFilterBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const updateSql = getSqlText(sqlCalls[0])

    // Must use the subquery pattern for atomicity
    expect(updateSql).toMatch(
      /UPDATE.*SET.*WHERE EMAIL_METADATA_ID IN \(SELECT EMAIL_METADATA_ID FROM/,
    )
  })

  // ----------------------------------------------------------
  // Stuck batch: generates new triggerHash for retrigger
  // ----------------------------------------------------------

  it('generates a new triggerHash for retrigger events', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ BATCH_ID: 'old-batch-id', ATTEMPTS: 1 }]))
      .mockResolvedValueOnce(
        sxtResponse([
          {
            EMAIL_METADATA_ID: 'em-1',
            MESSAGE_ID: 'msg-1',
            USER_ID: 'u-1',
            THREAD_ID: 't-1',
            SYNC_STATE_ID: 'ss-1',
          },
        ]),
      )
      .mockResolvedValueOnce(sxtResponse()) // UPDATE UPDATED_AT
      .mockResolvedValueOnce(sxtResponse()) // INSERT batch event

    const result = await runClaimFilterBatch()

    // batch_id should be the stuck batch's ID, not a new UUID
    expect(result.batch_id).toBe('old-batch-id')
    expect(result.attempts).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const insertSql = getSqlText(sqlCalls[5])
    // triggerHash is a new UUID (test-uuid-2), different from batchId
    expect(insertSql).toContain("'test-uuid-2'")
    expect(insertSql).toContain("'old-batch-id'")
  })
})
