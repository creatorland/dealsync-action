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
const { runSyncDealStates } = await import('../src/commands/sync-deal-states.js')

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

  it('runs single INSERT...SELECT and returns synced_count from row count', async () => {
    mockInputs()

    const insertedRows = [{}, {}]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse(insertedRows)) // insert result

    const result = await runSyncDealStates()

    expect(result).toEqual({ synced_count: 2 })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(1)

    const sql = getSqlText(sqlCalls[0])
    expect(sql).toContain('INSERT INTO dealsync_stg_v1.DEAL_STATES')
    expect(sql).toContain('EMAIL_CORE_STAGING.EMAIL_METADATA')
    expect(sql).toContain('NOT EXISTS')
    expect(sql).toContain('ON CONFLICT (EMAIL_METADATA_ID)')
  })

  it('returns synced_count=0 when insert returns no rows', async () => {
    mockInputs()

    fetchSpy.mockResolvedValueOnce(authResponse()).mockResolvedValueOnce(sxtResponse([]))

    const result = await runSyncDealStates()

    expect(result).toEqual({ synced_count: 0 })

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(1)
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runSyncDealStates()).rejects.toThrow('Invalid schema')
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()

    fetchSpy.mockResolvedValueOnce(authResponse()).mockResolvedValueOnce(sxtResponse([]))

    await runSyncDealStates()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  it('uses email-core-schema input in SQL', async () => {
    mockInputs({ 'email-core-schema': 'MY_CORE' })

    fetchSpy.mockResolvedValueOnce(authResponse()).mockResolvedValueOnce(sxtResponse([]))

    await runSyncDealStates()

    const sql = getSqlText(getSqlCalls(fetchSpy)[0])
    expect(sql).toContain('MY_CORE.EMAIL_METADATA')
  })
})
