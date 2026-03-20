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
const { runClassify } = await import('../src/classify.js')

function makeThread(overrides = {}) {
  return {
    thread_id: 'thread-abc-123',
    is_deal: true,
    category: 'new',
    ai_summary: 'A potential partnership deal',
    ai_score: 8,
    language: 'en',
    deal_name: 'Big Deal',
    deal_type: 'sponsorship',
    deal_value: '50000',
    currency: 'USD',
    main_contact: {
      email: 'jane@example.com',
      name: 'Jane Smith',
      company: 'Acme Corp',
      title: 'VP Sales',
    },
    ...overrides,
  }
}

function makeMetadata(overrides = {}) {
  return {
    EMAIL_METADATA_ID: 'email-id-001',
    MESSAGE_ID: 'msg-001',
    USER_ID: 'user-001',
    THREAD_ID: 'thread-abc-123',
    ...overrides,
  }
}

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'ai-response': JSON.stringify({ threads: [makeThread()] }),
    metadata: JSON.stringify([makeMetadata()]),
    'encryption-key': '',
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

function mockAuthAndSql(fetchSpy, sqlCount) {
  fetchSpy.mockResolvedValueOnce(authResponse())
  for (let i = 0; i < sqlCount; i++) {
    fetchSpy.mockResolvedValueOnce(sxtResponse())
  }
}

function getSqlCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => c[0].includes('/v1/sql'))
}

function getSqlText(call) {
  return JSON.parse(call[1].body).sqlText
}

describe('classify command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('creates all records for a deal and sets stage to 4', async () => {
    mockInputs()
    // Deal: audit(1) + eval_del(2) + eval_ins(3) + contact_del(4) + contact_ins(5) + deal_del(6) + deal_ins(7) + dc_del(8) + dc_ins(9) + stage(10) = 10
    mockAuthAndSql(fetchSpy, 10)

    const result = await runClassify()

    expect(result.deals_created).toBe(1)
    expect(result.emails_classified).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls.length).toBe(10)

    // Audit INSERT
    expect(getSqlText(sqlCalls[0])).toContain('INSERT INTO dealsync_stg_v1.AI_EVALUATION_AUDITS')

    // Eval DELETE + INSERT
    expect(getSqlText(sqlCalls[1])).toBe(
      "DELETE FROM dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID = 'thread-abc-123'",
    )
    expect(getSqlText(sqlCalls[2])).toContain(
      'INSERT INTO dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS',
    )

    // Contact DELETE + INSERT
    expect(getSqlText(sqlCalls[3])).toBe(
      "DELETE FROM dealsync_stg_v1.CONTACTS WHERE EMAIL = 'jane@example.com'",
    )
    expect(getSqlText(sqlCalls[4])).toContain('Jane Smith')
    expect(getSqlText(sqlCalls[4])).toContain('Acme Corp')

    // Deal DELETE + INSERT
    expect(getSqlText(sqlCalls[5])).toBe(
      "DELETE FROM dealsync_stg_v1.DEALS WHERE THREAD_ID = 'thread-abc-123' AND USER_ID = 'user-001'",
    )
    expect(getSqlText(sqlCalls[6])).toContain('Big Deal')
    expect(getSqlText(sqlCalls[6])).toContain('50000')

    // Stage UPDATE to 4
    expect(getSqlText(sqlCalls[9])).toContain('SET STAGE = 4')
    expect(getSqlText(sqlCalls[9])).toContain("'email-id-001'")
  })

  it('sets stage to 106 for non-deal', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [makeThread({ is_deal: false, main_contact: null, category: 'not_a_deal' })],
      }),
    })
    mockAuthAndSql(fetchSpy, 4) // audit + eval_del + eval_ins + stage

    const result = await runClassify()

    expect(result.deals_created).toBe(0)
    expect(result.emails_classified).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(getSqlText(sqlCalls[3])).toContain('SET STAGE = 106')
  })

  it('sets stage to 107 for non-English', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [makeThread({ is_deal: false, main_contact: null, language: 'es' })],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    const result = await runClassify()

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(getSqlText(sqlCalls[3])).toContain('SET STAGE = 107')
  })

  it('authenticates via proxy with x-shared-secret', async () => {
    mockInputs()
    mockAuthAndSql(fetchSpy, 10)

    await runClassify()

    const authCall = fetchSpy.mock.calls[0]
    expect(authCall[0]).toBe('https://auth.example.com/token')
    expect(authCall[1].method).toBe('GET')
    expect(authCall[1].headers['x-shared-secret']).toBe('test-secret')
  })

  it('uses pre-generated biscuit in SQL calls', async () => {
    mockInputs()
    mockAuthAndSql(fetchSpy, 10)

    await runClassify()

    const sqlCalls = getSqlCalls(fetchSpy)
    const body = JSON.parse(sqlCalls[0][1].body)
    expect(body.biscuits).toEqual(['test-biscuit'])
  })

  it('escapes single quotes in SQL values', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [
          makeThread({
            ai_summary: "It's a great deal",
            deal_name: "O'Brien's Offer",
            main_contact: {
              email: "o'brien@example.com",
              name: "Tim O'Brien",
              company: "O'Reilly",
              title: 'CEO',
            },
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 10)

    await runClassify()

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(getSqlText(sqlCalls[2])).toContain("It''s a great deal")
    expect(getSqlText(sqlCalls[4])).toContain("Tim O''Brien")
    expect(getSqlText(sqlCalls[6])).toContain("O''Brien''s Offer")
  })

  it('continues processing when one thread fails', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [
          makeThread({ thread_id: 'thread-good-1', is_deal: false, main_contact: null }),
          makeThread({ thread_id: 'thread-bad-1', is_deal: false, main_contact: null }),
          makeThread({ thread_id: 'thread-good-2', is_deal: false, main_contact: null }),
        ],
      }),
      metadata: JSON.stringify([
        makeMetadata({ EMAIL_METADATA_ID: 'e1', THREAD_ID: 'thread-good-1' }),
        makeMetadata({ EMAIL_METADATA_ID: 'e2', THREAD_ID: 'thread-bad-1' }),
        makeMetadata({ EMAIL_METADATA_ID: 'e3', THREAD_ID: 'thread-good-2' }),
      ]),
    })

    fetchSpy.mockResolvedValueOnce(authResponse())
    // Thread 1: 4 SQL calls
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    // Thread 2: first SQL fails
    fetchSpy.mockResolvedValueOnce(new Response('Internal Server Error', { status: 500 }))
    // Thread 3: 4 SQL calls
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())
    fetchSpy.mockResolvedValueOnce(sxtResponse())

    const result = await runClassify()

    expect(result.emails_classified).toBe(2)
    expect(core.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to process thread thread-bad-1'),
    )
  })

  it('returns 0 counts for empty threads', async () => {
    mockInputs({ 'ai-response': JSON.stringify({ threads: [] }) })

    const result = await runClassify()

    expect(fetchSpy).not.toHaveBeenCalled()
    expect(result.deals_created).toBe(0)
    expect(result.emails_classified).toBe(0)
    expect(core.info).toHaveBeenCalledWith('No threads to process')
  })

  it('sets LIKELY_SCAM to true for likely_scam category', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [makeThread({ is_deal: false, main_contact: null, category: 'likely_scam' })],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await runClassify()

    const sqlCalls = getSqlCalls(fetchSpy)
    const evalInsert = getSqlText(sqlCalls[2])
    expect(evalInsert).toMatch(/false, true/)
  })

  it('rejects invalid schema', async () => {
    mockInputs({ schema: 'schema; DROP TABLE' })

    await expect(runClassify()).rejects.toThrow('Invalid schema')
  })

  it('stage 107 for non-English even if is_deal is true', async () => {
    mockInputs({
      'ai-response': JSON.stringify({
        threads: [makeThread({ is_deal: true, language: 'zh' })],
      }),
    })
    mockAuthAndSql(fetchSpy, 10)

    const result = await runClassify()

    const sqlCalls = getSqlCalls(fetchSpy)
    const stageSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(stageSql).toContain('SET STAGE = 107')
  })
})
