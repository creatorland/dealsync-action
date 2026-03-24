import { jest } from '@jest/globals'

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

const core = await import('@actions/core')
const { runSyncDealStates } = await import('../src/sync-deal-states.js')

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    offset: '0',
    limit: '1000',
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

function sxtResponse(data = []) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function authResponse() {
  return new Response(JSON.stringify({ data: 'test-jwt' }), {
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

describe('sync-deal-states command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('queries diff, inserts 2 rows, returns synced_count=2', async () => {
    mockInputs({ limit: '500', offset: '10' })

    const diffRows = [
      { ID: 'em-1', USER_ID: 'user-1', THREAD_ID: 'thread-1', MESSAGE_ID: 'msg-1' },
      { ID: 'em-2', USER_ID: 'user-2', THREAD_ID: 'thread-2', MESSAGE_ID: 'msg-2' },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse(diffRows)) // diff query
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // count existing (0 conflicts)
      .mockResolvedValueOnce(sxtResponse()) // insert

    const result = await runSyncDealStates()

    expect(result).toEqual({ synced_count: 2, conflict_count: 0 })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // diff + count + insert

    // Verify diff query
    const diffSql = getSqlText(sqlCalls[0])
    expect(diffSql).toContain('EMAIL_CORE_STAGING.EMAIL_METADATA')
    expect(diffSql).toContain('dealsync_stg_v1.DEAL_STATES')
    expect(diffSql).toContain('LIMIT 500')
    expect(diffSql).toContain('OFFSET 10')
    expect(diffSql).toContain('NOT IN (SELECT EMAIL_METADATA_ID FROM')

    // Verify insert query
    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain('INSERT INTO dealsync_stg_v1.DEAL_STATES')
    expect(insertSql).toContain('ON CONFLICT (EMAIL_METADATA_ID) DO NOTHING')
    expect(insertSql).toContain("'em-1'")
    expect(insertSql).toContain("'em-2'")
    expect(insertSql).toContain("'pending'")
    expect(insertSql).toContain('CURRENT_TIMESTAMP')
  })

  it('returns synced_count=0 when no diff rows found', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse([])) // diff query returns empty

    const result = await runSyncDealStates()

    expect(result).toEqual({ synced_count: 0, conflict_count: 0 })

    // Should only have the diff query, no insert
    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(1)
  })

  it('inserts all rows in a single INSERT statement', async () => {
    mockInputs()

    const diffRows = Array.from({ length: 60 }, (_, i) => ({
      ID: `em-${i}`,
      USER_ID: `user-${i}`,
      THREAD_ID: `thread-${i}`,
      MESSAGE_ID: `msg-${i}`,
    }))

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse(diffRows)) // diff query
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // count existing
      .mockResolvedValueOnce(sxtResponse()) // single insert

    const result = await runSyncDealStates()

    expect(result).toEqual({ synced_count: 60, conflict_count: 0 })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // diff + count + 1 insert

    // Verify single insert contains all rows
    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain("'em-0'")
    expect(insertSql).toContain("'em-59'")
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runSyncDealStates()).rejects.toThrow('Invalid schema')
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([]))

    await runSyncDealStates()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  it('uses correct SQL column names in diff and insert', async () => {
    mockInputs()

    const diffRows = [
      { ID: 'em-abc', USER_ID: 'u-1', THREAD_ID: 't-1', MESSAGE_ID: 'm-1' },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(diffRows))
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // count existing
      .mockResolvedValueOnce(sxtResponse())

    await runSyncDealStates()

    const sqlCalls = getSqlCalls(fetchSpy)

    // Diff query selects correct columns
    const diffSql = getSqlText(sqlCalls[0])
    expect(diffSql).toContain('em.ID')
    expect(diffSql).toContain('em.USER_ID')
    expect(diffSql).toContain('em.THREAD_ID')
    expect(diffSql).toContain('em.MESSAGE_ID')

    // Insert uses correct column order (index 2: after diff + count)
    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain(
      'ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT',
    )
    expect(insertSql).toContain("'em-abc'")
    expect(insertSql).toContain("'u-1'")
    expect(insertSql).toContain("'t-1'")
    expect(insertSql).toContain("'m-1'")
  })
})
