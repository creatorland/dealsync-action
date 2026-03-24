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
const { runDispatchDealStates } = await import('../src/dispatch-deal-states.js')

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'w3-rpc-url': 'https://w3.example.com/rpc',
    'creator-name': 'DealStateWorker',
    'deal-state-batch-size': '500',
    'deal-state-max-emails': '5000',
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

function triggerSuccess(hash = 'w3-hash-abc123') {
  return new Response(JSON.stringify({ result: { triggerHash: hash } }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function triggerError() {
  return new Response('trigger failed', { status: 500 })
}

function getSqlCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => c[0].includes('/v1/sql'))
}

function getSqlText(call) {
  return JSON.parse(call[1].body).sqlText
}

function getRpcCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter(
    (c) => c[0].includes('/rpc') && !c[0].includes('/v1/sql'),
  )
}

function getRpcPayload(call) {
  return JSON.parse(call[1].body)
}

describe('dispatch-deal-states command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('dispatches 3 workers for 1200 diff with batch=500', async () => {
    mockInputs({ 'deal-state-batch-size': '500' })

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse([{ CNT: 1200 }])) // count diff
      .mockResolvedValueOnce(triggerSuccess('hash-0')) // worker 0
      .mockResolvedValueOnce(triggerSuccess('hash-1')) // worker 1
      .mockResolvedValueOnce(triggerSuccess('hash-2')) // worker 2

    const result = await runDispatchDealStates()

    expect(result.workers_triggered).toBe(3)
    expect(result.total_emails).toBe(1200)

    // Verify the count SQL
    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(1)
    const countSql = getSqlText(sqlCalls[0])
    expect(countSql).toContain('EMAIL_CORE_STAGING.EMAIL_METADATA')
    expect(countSql).toContain('DEAL_STATES')
    expect(countSql).toContain('NOT IN (SELECT EMAIL_METADATA_ID FROM')

    // Verify 3 W3 RPC calls with correct offsets
    const rpcCalls = getRpcCalls(fetchSpy)
    expect(rpcCalls).toHaveLength(3)

    const payload0 = getRpcPayload(rpcCalls[0])
    expect(payload0.jsonrpc).toBe('2.0')
    expect(payload0.method).toBe('w3_triggerWorkflow')
    expect(payload0.params.workflowName).toBe('DealStateWorker')
    expect(payload0.params.body.offset).toBe('0')
    expect(payload0.params.body.limit).toBe('500')
    expect(payload0.id).toBe(1)

    const payload1 = getRpcPayload(rpcCalls[1])
    expect(payload1.params.body.offset).toBe('500')
    expect(payload1.params.body.limit).toBe('500')
    expect(payload1.id).toBe(2)

    const payload2 = getRpcPayload(rpcCalls[2])
    expect(payload2.params.body.offset).toBe('1000')
    expect(payload2.params.body.limit).toBe('500')
    expect(payload2.id).toBe(3)
  })

  it('caps at max-emails: 8000 diff with max=1000 → 2 workers', async () => {
    mockInputs({
      'deal-state-batch-size': '500',
      'deal-state-max-emails': '1000',
    })

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse([{ CNT: 8000 }])) // count diff
      .mockResolvedValueOnce(triggerSuccess('hash-0')) // worker 0
      .mockResolvedValueOnce(triggerSuccess('hash-1')) // worker 1

    const result = await runDispatchDealStates()

    expect(result.workers_triggered).toBe(2)
    expect(result.total_emails).toBe(1000)

    const rpcCalls = getRpcCalls(fetchSpy)
    expect(rpcCalls).toHaveLength(2)

    const payload0 = getRpcPayload(rpcCalls[0])
    expect(payload0.params.body.offset).toBe('0')

    const payload1 = getRpcPayload(rpcCalls[1])
    expect(payload1.params.body.offset).toBe('500')
  })

  it('returns 0 workers when diff is 0, no trigger calls', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // auth
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }])) // count diff = 0

    const result = await runDispatchDealStates()

    expect(result.workers_triggered).toBe(0)
    expect(result.total_emails).toBe(0)

    const rpcCalls = getRpcCalls(fetchSpy)
    expect(rpcCalls).toHaveLength(0)
  })

  it('verifies W3 RPC payload structure', async () => {
    mockInputs({ 'deal-state-batch-size': '500', 'deal-state-max-emails': '500' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 100 }]))
      .mockResolvedValueOnce(triggerSuccess('hash-single'))

    await runDispatchDealStates()

    const rpcCalls = getRpcCalls(fetchSpy)
    expect(rpcCalls).toHaveLength(1)

    const call = rpcCalls[0]
    expect(call[0]).toBe('https://w3.example.com/rpc')
    expect(call[1].method).toBe('POST')
    expect(call[1].headers['Content-Type']).toBe('application/json')

    const payload = getRpcPayload(call)
    expect(payload).toEqual({
      jsonrpc: '2.0',
      method: 'w3_triggerWorkflow',
      params: {
        workflowName: 'DealStateWorker',
        body: { offset: '0', limit: '500' },
      },
      id: 1,
    })
  })

  it('warns on trigger failure but continues dispatching', async () => {
    mockInputs({ 'deal-state-batch-size': '500' })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 1500 }]))
      .mockResolvedValueOnce(triggerError()) // worker 0 fails
      .mockResolvedValueOnce(triggerSuccess('hash-1')) // worker 1 succeeds
      .mockResolvedValueOnce(triggerSuccess('hash-2')) // worker 2 succeeds

    const result = await runDispatchDealStates()

    // 2 succeeded, 1 failed
    expect(result.workers_triggered).toBe(2)
    expect(result.total_emails).toBe(1500)

    // All 3 triggers were attempted
    const rpcCalls = getRpcCalls(fetchSpy)
    expect(rpcCalls).toHaveLength(3)
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 0 }]))

    await runDispatchDealStates()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })
    await expect(runDispatchDealStates()).rejects.toThrow('Invalid schema')
  })

  it('uses default batch-size and max-emails when not provided', async () => {
    mockInputs({
      'deal-state-batch-size': '',
      'deal-state-max-emails': '',
    })

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([{ CNT: 1200 }]))
      .mockResolvedValueOnce(triggerSuccess('hash-0'))
      .mockResolvedValueOnce(triggerSuccess('hash-1'))
      .mockResolvedValueOnce(triggerSuccess('hash-2'))

    const result = await runDispatchDealStates()

    // Default batch=500, max=5000 → 1200 emails, 3 workers
    expect(result.workers_triggered).toBe(3)
    expect(result.total_emails).toBe(1200)
  })
})
