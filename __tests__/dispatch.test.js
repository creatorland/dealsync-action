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
const { runDispatch } = await import('../src/commands/dispatch.js')

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
      .mockResolvedValueOnce(authResponse()) // 1. auth
      // Iteration 1 - claim filter
      .mockResolvedValueOnce(sxtResponse()) // 2. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // 3. countClaimed (filter)
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // 4. countInFlight (filter)
      // Iteration 1 - claim classify
      .mockResolvedValueOnce(sxtResponse()) // 5. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 6. countClaimed = 0 -> exhausted
      // trigger (filter only)
      .mockResolvedValueOnce(triggerSuccess()) // 7. trigger
      .mockResolvedValueOnce(sxtResponse()) // 8. batch_events insert
      // Iteration 2 - claim filter again
      .mockResolvedValueOnce(sxtResponse()) // 9. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 10. countClaimed = 0 -> exhausted, break

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
    expect(body.inputs.filter_batch_id).toMatch(/^[0-9a-f]{8}-/)
  })

  it('releases batch if over max-filter limit', async () => {
    mockInputs({ 'max-filter': '100' })

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse()) // 2. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // 3. countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 500 }])) // 4. countInFlight = 500 > 100
      .mockResolvedValueOnce(sxtResponse()) // 5. resetClaimed -> filterExhausted
      // classify claim
      .mockResolvedValueOnce(sxtResponse()) // 6. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 7. countClaimed = 0 -> exhausted, break

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
      .mockResolvedValueOnce(authResponse()) // 1. auth
      // Iteration 1 - claim filter: nothing
      .mockResolvedValueOnce(sxtResponse()) // 2. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 3. countClaimed = 0 -> exhausted
      // Iteration 1 - claim classify
      .mockResolvedValueOnce(sxtResponse()) // 4. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // 5. countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 5 }])) // 6. countInFlight
      // trigger (classify only)
      .mockResolvedValueOnce(triggerSuccess()) // 7. trigger
      .mockResolvedValueOnce(sxtResponse()) // 8. batch_events insert
      // Iteration 2 - claim classify again
      .mockResolvedValueOnce(sxtResponse()) // 9. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 10. countClaimed = 0 -> exhausted, break

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
      .mockResolvedValueOnce(authResponse()) // 1. auth
      // claim filter
      .mockResolvedValueOnce(sxtResponse()) // 2. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // 3. countClaimed
      .mockResolvedValueOnce(sxtResponse([{ CNT: 200 }])) // 4. countInFlight
      // claim classify
      .mockResolvedValueOnce(sxtResponse()) // 5. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 6. countClaimed = 0 -> exhausted
      // trigger fails
      .mockResolvedValueOnce(triggerError()) // 7. trigger
      .mockResolvedValueOnce(sxtResponse()) // 8. resetClaimed (filter) -> break

    const result = await runDispatch()

    expect(result.dispatched_filter_count).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    // Find the reset SQL
    const resetSql = sqlCalls.find((c) => getSqlText(c).includes("STATUS = 'pending'") && getSqlText(c).includes('BATCH_ID'))
    expect(getSqlText(resetSql)).toContain("STATUS = 'pending'")
  })

  it('stops when claim returns 0 emails', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse()) // 2. claimFilterBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 3. countClaimed = 0 -> exhausted
      // classify claim
      .mockResolvedValueOnce(sxtResponse()) // 4. claimClassifyBatch
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // 5. countClaimed = 0 -> exhausted, break

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
