import { jest } from '@jest/globals'

// ---------------------------------------------------------------------------
// Mocks — must be set up before importing the module under test
// ---------------------------------------------------------------------------

const mockCore = {
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
  warning: jest.fn(),
}

jest.unstable_mockModule('@actions/core', () => mockCore)

// Deterministic UUIDs for testing
let uuidCallCount = 0
jest.unstable_mockModule('uuid', () => ({
  v7: jest.fn(() => {
    uuidCallCount++
    return `test-uuid-${uuidCallCount}`
  }),
}))

const { runClaimClassifyBatch } = await import('../src/commands/claim-classify-batch.js')

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'classify-batch-size': '5',
    'max-retries': '3',
    'rate-limiter-url': '',
    'rate-limiter-api-key': '',
    ...overrides,
  }
  mockCore.getInput.mockImplementation((name) => defaults[name] ?? '')
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('claim-classify-batch', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    uuidCallCount = 0
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  // -----------------------------------------------------------------------
  // Happy path: new batch claimed
  // -----------------------------------------------------------------------
  it('claims pending_classification rows and returns batch with rows', async () => {
    mockInputs()

    const claimedRows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'u-1',
        THREAD_ID: 't-1',
        SYNC_STATE_ID: 'ss-1',
      },
      {
        EMAIL_METADATA_ID: 'em-2',
        MESSAGE_ID: 'msg-2',
        USER_ID: 'u-1',
        THREAD_ID: 't-1',
        SYNC_STATE_ID: 'ss-1',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // authenticate
      .mockResolvedValueOnce(sxtResponse()) // UPDATE claim
      .mockResolvedValueOnce(sxtResponse(claimedRows)) // SELECT claimed
      .mockResolvedValueOnce(sxtResponse()) // INSERT batch event

    const result = await runClaimClassifyBatch()

    expect(result.batch_id).toBe('test-uuid-1')
    expect(result.count).toBe(2)
    expect(result.attempts).toBe(0)
    expect(result.rows).toEqual(claimedRows)

    // Verify SQL calls
    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // UPDATE + SELECT + INSERT batch event

    // Verify claim UPDATE
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain("SET STATUS = 'classifying'")
    expect(claimSql).toContain("BATCH_ID = 'test-uuid-1'")
    expect(claimSql).toContain('THREAD_ID IN')
    expect(claimSql).toContain("ds.STATUS = 'pending_classification'")
    expect(claimSql).toContain('NOT EXISTS')
    expect(claimSql).toContain("ds2.STATUS IN ('pending', 'filtering')")
    expect(claimSql).toContain('ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID')
    expect(claimSql).toContain('LIMIT 5')
    expect(claimSql).toContain("AND STATUS = 'pending_classification'")

    // Verify SELECT claimed rows
    const selectSql = getSqlText(sqlCalls[1])
    expect(selectSql).toContain('SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID')
    expect(selectSql).toContain("BATCH_ID = 'test-uuid-1'")

    // Verify batch event INSERT
    const eventSql = getSqlText(sqlCalls[2])
    expect(eventSql).toContain('BATCH_EVENTS')
    expect(eventSql).toContain("'test-uuid-1'") // triggerHash = batchId for new
    expect(eventSql).toContain("'classify'")
    expect(eventSql).toContain("'new'")
  })

  // -----------------------------------------------------------------------
  // No rows claimed, no stuck — returns null
  // -----------------------------------------------------------------------
  it('returns batch_id: null when no rows to claim and no stuck batches', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // authenticate
      .mockResolvedValueOnce(sxtResponse()) // UPDATE claim (no-op)
      .mockResolvedValueOnce(sxtResponse([])) // SELECT claimed (empty)
      .mockResolvedValueOnce(sxtResponse([])) // stuck query (empty)

    const result = await runClaimClassifyBatch()

    expect(result).toEqual({ batch_id: null, count: 0 })
  })

  // -----------------------------------------------------------------------
  // Stuck batch retrigger
  // -----------------------------------------------------------------------
  it('retriggers a stuck batch when no new rows to claim', async () => {
    mockInputs()

    const stuckQueryResult = [{ BATCH_ID: 'stuck-batch-abc', ATTEMPTS: '1' }]
    const stuckRows = [
      {
        EMAIL_METADATA_ID: 'em-10',
        MESSAGE_ID: 'msg-10',
        USER_ID: 'u-2',
        THREAD_ID: 't-5',
        SYNC_STATE_ID: 'ss-2',
      },
      {
        EMAIL_METADATA_ID: 'em-11',
        MESSAGE_ID: 'msg-11',
        USER_ID: 'u-2',
        THREAD_ID: 't-5',
        SYNC_STATE_ID: 'ss-2',
      },
      {
        EMAIL_METADATA_ID: 'em-12',
        MESSAGE_ID: 'msg-12',
        USER_ID: 'u-2',
        THREAD_ID: 't-6',
        SYNC_STATE_ID: 'ss-2',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // authenticate
      .mockResolvedValueOnce(sxtResponse()) // UPDATE claim (no-op)
      .mockResolvedValueOnce(sxtResponse([])) // SELECT claimed (empty)
      .mockResolvedValueOnce(sxtResponse(stuckQueryResult)) // stuck query
      .mockResolvedValueOnce(sxtResponse(stuckRows)) // SELECT stuck batch rows
      .mockResolvedValueOnce(sxtResponse()) // UPDATE UPDATED_AT
      .mockResolvedValueOnce(sxtResponse()) // INSERT batch event

    const result = await runClaimClassifyBatch()

    expect(result.batch_id).toBe('stuck-batch-abc')
    expect(result.count).toBe(3)
    expect(result.attempts).toBe(1)
    expect(result.rows).toEqual(stuckRows)

    const sqlCalls = getSqlCalls(fetchSpy)

    // Verify stuck query
    const stuckSql = getSqlText(sqlCalls[2])
    expect(stuckSql).toContain("ds.STATUS = 'classifying'")
    expect(stuckSql).toContain('ds.BATCH_ID IS NOT NULL')
    expect(stuckSql).toContain("INTERVAL '5' MINUTE")
    expect(stuckSql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 3')
    expect(stuckSql).toContain('LIMIT 1')

    // Verify SELECT stuck rows
    const stuckSelectSql = getSqlText(sqlCalls[3])
    expect(stuckSelectSql).toContain("BATCH_ID = 'stuck-batch-abc'")

    // Verify UPDATED_AT touch
    const touchSql = getSqlText(sqlCalls[4])
    expect(touchSql).toContain('SET UPDATED_AT = CURRENT_TIMESTAMP')
    expect(touchSql).toContain("BATCH_ID = 'stuck-batch-abc'")

    // Verify batch event INSERT for retrigger
    const eventSql = getSqlText(sqlCalls[5])
    expect(eventSql).toContain('BATCH_EVENTS')
    expect(eventSql).toContain("'test-uuid-2'") // new triggerHash (2nd uuid call)
    expect(eventSql).toContain("'stuck-batch-abc'") // original batchId
    expect(eventSql).toContain("'classify'")
    expect(eventSql).toContain("'retrigger'")
  })

  // -----------------------------------------------------------------------
  // Custom batch size and max-retries
  // -----------------------------------------------------------------------
  it('respects custom classify-batch-size and max-retries inputs', async () => {
    mockInputs({ 'classify-batch-size': '10', 'max-retries': '5' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)

    // Claim SQL should use LIMIT 10
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain('LIMIT 10')

    // Stuck query should use < 5
    const stuckSql = getSqlText(sqlCalls[2])
    expect(stuckSql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 5')
  })

  // -----------------------------------------------------------------------
  // Default batch size and max-retries
  // -----------------------------------------------------------------------
  it('uses default batch-size=5 and max-retries=3 when inputs are empty', async () => {
    mockInputs({ 'classify-batch-size': '', 'max-retries': '' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)

    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain('LIMIT 5')

    const stuckSql = getSqlText(sqlCalls[2])
    expect(stuckSql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 3')
  })

  // -----------------------------------------------------------------------
  // Schema validation
  // -----------------------------------------------------------------------
  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'bad schema; DROP TABLE' })

    await expect(runClaimClassifyBatch()).rejects.toThrow('Invalid schema')
  })

  // -----------------------------------------------------------------------
  // Schema used correctly in all queries
  // -----------------------------------------------------------------------
  it('uses the schema in all SQL queries', async () => {
    mockInputs({ schema: 'MY_CUSTOM_SCHEMA' })

    const claimedRows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'u-1',
        THREAD_ID: 't-1',
        SYNC_STATE_ID: 'ss-1',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse(claimedRows))
      .mockResolvedValueOnce(sxtResponse())

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    for (const call of sqlCalls) {
      const sql = getSqlText(call)
      expect(sql).toContain('MY_CUSTOM_SCHEMA.')
    }
  })

  // -----------------------------------------------------------------------
  // Authentication
  // -----------------------------------------------------------------------
  it('authenticates with correct credentials', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  // -----------------------------------------------------------------------
  // SQL uses correct status constants
  // -----------------------------------------------------------------------
  it('uses STATUS constants from queries.js', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])

    // Uses classifying and pending_classification status values
    expect(claimSql).toContain("'classifying'")
    expect(claimSql).toContain("'pending_classification'")
    expect(claimSql).toContain("'pending'")
    expect(claimSql).toContain("'filtering'")
  })

  // -----------------------------------------------------------------------
  // Thread-aware claim includes NOT EXISTS with SYNC_STATE_ID
  // -----------------------------------------------------------------------
  it('claim SQL is thread-aware with SYNC_STATE_ID join in NOT EXISTS', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])

    // Must use THREAD_ID (not EMAIL_METADATA_ID) in outer WHERE
    expect(claimSql).toContain('WHERE THREAD_ID IN')
    // NOT EXISTS subquery must join on both THREAD_ID and SYNC_STATE_ID
    expect(claimSql).toContain('ds2.THREAD_ID = ds.THREAD_ID')
    expect(claimSql).toContain('ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID')
  })

  // -----------------------------------------------------------------------
  // Stuck batch attempts parsed as integer
  // -----------------------------------------------------------------------
  it('parses stuck batch ATTEMPTS as integer', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([{ BATCH_ID: 'stuck-1', ATTEMPTS: '2' }]))
      .mockResolvedValueOnce(sxtResponse([{ EMAIL_METADATA_ID: 'em-1', MESSAGE_ID: 'msg-1', USER_ID: 'u-1', THREAD_ID: 't-1', SYNC_STATE_ID: 'ss-1' }]))
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse())

    const result = await runClaimClassifyBatch()

    expect(result.attempts).toBe(2)
    expect(typeof result.attempts).toBe('number')
  })

  // -----------------------------------------------------------------------
  // JWT used in SQL calls
  // -----------------------------------------------------------------------
  it('passes JWT from auth to SQL calls', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // returns jwt='test-jwt'
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([]))
      .mockResolvedValueOnce(sxtResponse([]))

    await runClaimClassifyBatch()

    // All SQL calls (not auth) should use Bearer test-jwt
    const sqlCalls = getSqlCalls(fetchSpy)
    for (const call of sqlCalls) {
      expect(call[1].headers.Authorization).toBe('Bearer test-jwt')
    }
  })

  // -----------------------------------------------------------------------
  // Batch event uses batchType='classify'
  // -----------------------------------------------------------------------
  it('uses batchType classify for new batch event', async () => {
    mockInputs()

    const claimedRows = [
      {
        EMAIL_METADATA_ID: 'em-1',
        MESSAGE_ID: 'msg-1',
        USER_ID: 'u-1',
        THREAD_ID: 't-1',
        SYNC_STATE_ID: 'ss-1',
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse(claimedRows))
      .mockResolvedValueOnce(sxtResponse())

    await runClaimClassifyBatch()

    const sqlCalls = getSqlCalls(fetchSpy)
    const eventSql = getSqlText(sqlCalls[2])
    expect(eventSql).toContain("'classify'")
    expect(eventSql).not.toContain("'filter'")
  })
})
