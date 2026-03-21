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
const { runDispatch } = await import('../src/dispatch.js')

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'w3-rpc-url': 'https://w3.example.com',
    'processor-name': 'Dealsync Processor',
    'max-filter': '30000',
    'max-classify': '750',
    'filter-batch-size': '200',
    'classify-batch-size': '5',
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

function triggerSuccess() {
  return new Response(JSON.stringify({ triggerHash: 'w3-hash-123' }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function triggerError() {
  return new Response('workflow not found', { status: 404 })
}

function getSqlCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => c[0].includes('/v1/sql'))
}

function getSqlText(call) {
  return JSON.parse(call[1].body).sqlText
}

function getTriggerCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => c[0].includes('/workflow/'))
}

describe('dispatch command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('claims filter batch, verifies in-flight count, then triggers', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // countInFlight
      .mockResolvedValueOnce(triggerSuccess()) // trigger
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // countClaimed = 0, stop
      // classify loop
      .mockResolvedValueOnce(sxtResponse()) // claim classify
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // countClaimed = 0, stop

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain("STATUS = 'filtering'")
    expect(claimSql).toContain('BATCH_ID')

    // Verify in-flight check happened
    const inflightSql = getSqlText(sqlCalls[2])
    expect(inflightSql).toContain("STATUS = 'filtering'")
    expect(inflightSql).toContain('COUNT')

    const triggerCalls = getTriggerCalls(fetchSpy)
    const body = JSON.parse(triggerCalls[0][1].body)
    expect(body.inputs.batch_type).toBe('filter')
    expect(body.inputs.batch_id).toMatch(/^[0-9a-f]{8}-/)
  })

  it('releases batch if over max-filter limit', async () => {
    mockInputs({ 'max-filter': '100' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 500 }])) // countInFlight = 500 > 100
      .mockResolvedValueOnce(sxtResponse()) // resetClaimed
      // classify loop
      .mockResolvedValueOnce(sxtResponse()) // claim classify
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // stop

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[3])
    expect(resetSql).toContain("STATUS = 'pending'")
    expect(resetSql).toContain('BATCH_ID')
  })

  it('claims classify batch by thread with sync-level guard', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      // filter loop: nothing to claim
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // classify loop
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // countInFlight
      .mockResolvedValueOnce(triggerSuccess()) // trigger
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // stop

    const result = await runDispatch()

    expect(result.dispatched_classify_count).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    // Find the classify claim SQL
    const classifyClaim = sqlCalls.find((c) => getSqlText(c).includes("'classifying'"))
    expect(getSqlText(classifyClaim)).toContain('DISTINCT')
    expect(getSqlText(classifyClaim)).toContain('THREAD_ID')
    expect(getSqlText(classifyClaim)).toContain('SYNC_STATE_ID')
  })

  it('resets filter batch on trigger failure', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // countInFlight
      .mockResolvedValueOnce(triggerError()) // trigger fails
      .mockResolvedValueOnce(sxtResponse()) // resetClaimed
      // classify loop
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[sqlCalls.length - 3]) // before classify loop
    expect(resetSql).toContain("STATUS = 'pending'")
  })

  it('stops when claim returns 0 emails', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // nothing claimed
      // classify loop
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)
    const triggerCalls = getTriggerCalls(fetchSpy)
    expect(triggerCalls).toHaveLength(0)
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runDispatch()).rejects.toThrow('Invalid schema')
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    await runDispatch()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })
})
