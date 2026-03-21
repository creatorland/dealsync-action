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
    'active-classify': '200',
    'pending-filter': '50',
    'pending-classify': '30',
    'max-filter': '600',
    'max-classify': '300',
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

  it('early exits when no pending emails', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-classify': '0' })

    const result = await runDispatch()

    expect(fetchSpy).not.toHaveBeenCalled()
    expect(result.dispatched_filter_count).toBe(0)
    expect(result.dispatched_classify_count).toBe(0)
    expect(core.info).toHaveBeenCalledWith('No pending emails to dispatch')
  })

  it('claims filter batch first with UUIDv7 batch_id, then triggers processor', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-classify': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse()) // claim with batch_id
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }])) // verify claim
      .mockResolvedValueOnce(triggerSuccess()) // trigger processor
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0, stop

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(1)

    // Verify claim SQL uses BATCH_ID with UUID format
    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain("STATUS = 'filtering'")
    expect(claimSql).toContain('BATCH_ID')
    // UUID v7 format: 8-4-4-4-12 hex chars
    expect(claimSql).toMatch(/BATCH_ID = '[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}'/)

    // Verify trigger payload includes batch_id
    const triggerCalls = getTriggerCalls(fetchSpy)
    const body = JSON.parse(triggerCalls[0][1].body)
    expect(body.inputs.batch_type).toBe('filter')
    expect(body.inputs.batch_id).toMatch(/^[0-9a-f]{8}-/)
    // No transition_stage or trigger_hash in payload
    expect(body.inputs.transition_stage).toBeUndefined()
    expect(body.inputs.trigger_hash).toBeUndefined()
  })

  it('claims classify batch with thread-completeness check', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-classify': '10' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // verify
      .mockResolvedValueOnce(triggerSuccess()) // trigger
      .mockResolvedValueOnce(sxtResponse()) // claim batch 2
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0

    const result = await runDispatch()

    expect(result.dispatched_classify_count).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const claimSql = getSqlText(sqlCalls[0])
    expect(claimSql).toContain("STATUS = 'classifying'")
    expect(claimSql).toContain('NOT EXISTS')
    expect(claimSql).toContain('SYNC_STATE_ID')
    expect(claimSql).toContain("ds2.STATUS IN ('pending', 'filtering')")
  })

  it('resets claimed emails on trigger failure', async () => {
    mockInputs({ 'pending-filter': '10', 'pending-classify': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 10 }])) // verify
      .mockResolvedValueOnce(triggerError()) // trigger fails
      .mockResolvedValueOnce(sxtResponse()) // reset

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)
    expect(core.error).toHaveBeenCalledWith(expect.stringContaining('Filter trigger failed'))

    // Last SQL call should reset
    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(resetSql).toContain("STATUS = 'pending'")
    expect(resetSql).toContain('BATCH_ID')
  })

  it('resets classify to pending_classification on trigger failure', async () => {
    mockInputs({ 'pending-filter': '0', 'pending-classify': '10' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // verify
      .mockResolvedValueOnce(triggerError()) // trigger fails
      .mockResolvedValueOnce(sxtResponse()) // reset

    const result = await runDispatch()

    expect(result.dispatched_classify_count).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    const resetSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(resetSql).toContain("STATUS = 'pending_classification'")
  })

  it('claims multiple batches per run', async () => {
    mockInputs({
      'pending-filter': '100',
      'pending-classify': '0',
      'filter-batch-size': '25',
    })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      // Batch 0: claim + verify + trigger
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      .mockResolvedValueOnce(triggerSuccess())
      // Batch 1: claim + verify + trigger
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 25 }]))
      .mockResolvedValueOnce(triggerSuccess())
      // Batch 2: claim + verify = 0
      .mockResolvedValueOnce(sxtResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(2)

    // Verify different batch IDs used
    const sqlCalls = getSqlCalls(fetchSpy)
    const batch0Id = getSqlText(sqlCalls[0]).match(/BATCH_ID = '([^']+)'/)[1]
    const batch1Id = getSqlText(sqlCalls[2]).match(/BATCH_ID = '([^']+)'/)[1]
    expect(batch0Id).not.toBe(batch1Id)
  })

  it('stops when claim returns 0 emails', async () => {
    mockInputs({ 'pending-filter': '50', 'pending-classify': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)
    // No trigger calls since nothing was claimed
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
    mockInputs({ 'pending-filter': '10', 'pending-classify': '0' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse()) // claim
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // verify = 0

    await runDispatch()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })
})
