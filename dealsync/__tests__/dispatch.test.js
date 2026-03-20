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
    'active-filter': '400',
    'active-detect': '200',
    'pending-filter': '50',
    'pending-detect': '30',
    'max-filter': '600',
    'max-detect': '300',
    'filter-batch-size': '200',
    'detect-batch-size': '50',
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
  return new Response(JSON.stringify({ triggerHash: 'hash-123' }), {
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

  it('early exits when no pending emails', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-detect': '0' })

    const result = await runDispatch()

    expect(fetchSpy).not.toHaveBeenCalled()
    expect(result.dispatched_filter_count).toBe(0)
    expect(result.dispatched_detect_count).toBe(0)
    expect(core.info).toHaveBeenCalledWith('No pending emails to dispatch')
  })

  it('claims filter batch and triggers processor', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-detect': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }])) // verify
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0
      .mockResolvedValueOnce(triggerSuccess()) // trigger

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(1)
    expect(result.dispatched_detect_count).toBe(0)

    // Verify claim SQL
    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain('SET STAGE = 1001')
    expect(claimSql).toContain('WHERE STAGE = 2')
  })

  it('claims detection batch with thread-completeness check', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-detect': '10' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // verify
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0
      .mockResolvedValueOnce(triggerSuccess()) // trigger

    const result = await runDispatch()

    expect(result.dispatched_detect_count).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain('SET STAGE = 11001')
    expect(claimSql).toContain('NOT EXISTS')
    expect(claimSql).toContain('ds2.STAGE IN (1, 2)')
  })

  it('sends REST trigger with correct payload', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-detect': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      .mockResolvedValueOnce(triggerSuccess())

    await runDispatch()

    const triggerCalls = getTriggerCalls(fetchSpy)
    expect(triggerCalls).toHaveLength(1)
    expect(triggerCalls[0][0]).toBe('https://w3.example.com/workflow/Dealsync%20Processor/trigger')
    const body = JSON.parse(triggerCalls[0][1].body)
    expect(body).toEqual({
      inputs: { batch_type: 'filter', transition_stage: '1001', reset_stage: '2' },
    })
  })

  it('resets claimed emails on trigger failure', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-detect': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }]))
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      .mockResolvedValueOnce(triggerError()) // trigger fails
      .mockResolvedValueOnce(sxtResponse()) // reset

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)
    expect(core.error).toHaveBeenCalledWith(expect.stringContaining('Filter trigger failed'))

    // Last SQL call should reset to stage 2
    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(resetSql).toContain('SET STAGE = 2')
    expect(resetSql).toContain('WHERE STAGE = 1001')
  })

  it('resets detection emails to stage 3 on trigger failure', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-detect': '10' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }]))
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      .mockResolvedValueOnce(triggerError())
      .mockResolvedValueOnce(sxtResponse())

    const result = await runDispatch()

    expect(result.dispatched_detect_count).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(resetSql).toContain('SET STAGE = 3')
    expect(resetSql).toContain('WHERE STAGE = 11001')
  })

  it('claims multiple batches per run', async () => {
    mockInputs({
      'pending-filter': '100',
      'pending-detect': '0',
      'filter-batch-size': '25',
    })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      // Batch 0: claim + verify
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Batch 1: claim + verify
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      // Batch 2: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))
      // Trigger batch 0
      .mockResolvedValueOnce(triggerSuccess())
      // Trigger batch 1
      .mockResolvedValueOnce(triggerSuccess())

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(2)

    // Verify stages increment
    const sqlCalls = getSqlCalls(fetchSpy)
    expect(getSqlText(sqlCalls[0])).toContain('SET STAGE = 1001')
    expect(getSqlText(sqlCalls[2])).toContain('SET STAGE = 1002')
  })

  it('stops when claim returns 0 emails', async () => {
    mockInputs({ 'pending-filter': '50', 'pending-detect': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)
    const triggerCalls = getTriggerCalls(fetchSpy)
    expect(triggerCalls).toHaveLength(0)
  })

  it('rejects negative numbers in inputs', async () => {
    mockInputs({ 'active-filter': '-5' })

    await expect(runDispatch()).rejects.toThrow('active-filter must be non-negative integer')
  })

  it('rejects non-numeric inputs', async () => {
    mockInputs({ 'max-filter': 'abc' })

    await expect(runDispatch()).rejects.toThrow('max-filter must be non-negative integer')
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })

    await expect(runDispatch()).rejects.toThrow('Invalid schema')
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-detect': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    await runDispatch()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })
})
