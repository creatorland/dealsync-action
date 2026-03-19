import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest'

// --- Mock @actions/core ---
const mockOutputs = {}
const core = {
  getInput: vi.fn(),
  setOutput: vi.fn((name, value) => {
    mockOutputs[name] = value
  }),
  setFailed: vi.fn(),
  info: vi.fn(),
  error: vi.fn(),
}
vi.mock('@actions/core', () => core)

const { run } = await import('../save-detection-results/src/main.js')

// --- Helpers ---

function makeThread(overrides = {}) {
  return {
    thread_id: 'thread-abc-123',
    is_deal: true,
    category: 'business_opportunity',
    ai_summary: 'A potential partnership deal',
    ai_score: 0.92,
    language: 'en',
    deal_title: 'Big Deal',
    deal_value: 50000,
    main_contact: {
      email: 'jane@example.com',
      name: 'Jane Smith',
      company: 'Acme Corp',
      role: 'VP Sales',
    },
    ...overrides,
  }
}

function makeMetadata(overrides = {}) {
  return {
    ID: 'email-id-001',
    MESSAGE_ID: 'msg-001',
    USER_ID: 'user-001',
    THREAD_ID: 'thread-abc-123',
    ...overrides,
  }
}

function mockInputs(overrides = {}) {
  const defaults = {
    'sxt-api-url': 'https://sxt.example.com',
    'sxt-schema': 'dealsync_stg_v1',
    'sxt-user-id': 'test-user',
    'sxt-password': 'test-pass',
    'ai-output': JSON.stringify({ threads: [makeThread()] }),
    'ai-model': 'meta-llama/llama-3.1-70b',
    'ai-prompt-tokens': '1000',
    'ai-completion-tokens': '500',
    metadata: JSON.stringify([makeMetadata()]),
    'transition-stage': '11000',
    'encryption-key': '',
    ...overrides,
  }
  core.getInput.mockImplementation((name) => defaults[name] ?? '')
}

function sxtResponse(data) {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

function authLoginResponse() {
  return new Response(
    JSON.stringify([{ ACCESSTOKEN: 'test-jwt', SESSIONID: 'test-session' }]),
    { status: 200, headers: { 'content-type': 'application/json' } },
  )
}

function biscuitResponse() {
  return new Response(JSON.stringify([{ BISCUIT: 'test-biscuit' }]), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  })
}

/**
 * Mock fetch to handle auth (login + biscuit) then N subsequent SxT DML/SQL calls.
 * Each SQL call gets its own fresh Response object.
 */
function mockAuthAndSql(fetchSpy, sqlCount) {
  fetchSpy.mockResolvedValueOnce(authLoginResponse())
  fetchSpy.mockResolvedValueOnce(biscuitResponse())
  for (let i = 0; i < sqlCount; i++) {
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
  }
}

/**
 * Get all SxT SQL calls (exclude auth calls).
 */
function getSqlCalls(fetchSpy) {
  return fetchSpy.mock.calls.filter((c) => {
    const url = c[0]
    return url.includes('/v1/sql')
  })
}

/**
 * Parse the SQL text from a fetch call.
 */
function getSqlText(call) {
  return JSON.parse(call[1].body).sqlText
}

describe('save-detection-results', () => {
  let fetchSpy

  beforeEach(() => {
    vi.clearAllMocks()
    for (const key of Object.keys(mockOutputs)) delete mockOutputs[key]
    fetchSpy = vi.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  // 1. Deal classification: creates audit, evaluation, contact, deal, deal_contact, updates stage to 4
  // SQL calls for a deal: audit(1) + eval_del(2) + eval_ins(3) + contact_del(4) + contact_ins(5)
  //   + deal_del(6) + deal_ins(7) + dc_del(8) + dc_ins(9) + stage_upd(10) = 10 SQL calls
  it('creates all records for a deal classification and sets stage to 4', async () => {
    mockInputs()
    mockAuthAndSql(fetchSpy, 10)

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.deals_created).toBe('1')
    expect(mockOutputs.emails_classified).toBe('1')

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls.length).toBe(10)

    // Audit INSERT
    const auditSql = getSqlText(sqlCalls[0])
    expect(auditSql).toContain(
      'INSERT INTO dealsync_stg_v1.AI_EVALUATION_AUDITS',
    )
    expect(auditSql).toContain('meta-llama/llama-3.1-70b')

    // Evaluation DELETE + INSERT
    const evalDeleteSql = getSqlText(sqlCalls[1])
    expect(evalDeleteSql).toBe(
      "DELETE FROM dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID = 'thread-abc-123'",
    )
    const evalInsertSql = getSqlText(sqlCalls[2])
    expect(evalInsertSql).toContain(
      'INSERT INTO dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS',
    )
    expect(evalInsertSql).toContain('IS_DEAL')
    expect(evalInsertSql).toContain('true')

    // Contact DELETE + INSERT
    const contactDeleteSql = getSqlText(sqlCalls[3])
    expect(contactDeleteSql).toBe(
      "DELETE FROM dealsync_stg_v1.CONTACTS WHERE EMAIL = 'jane@example.com'",
    )
    const contactInsertSql = getSqlText(sqlCalls[4])
    expect(contactInsertSql).toContain('INSERT INTO dealsync_stg_v1.CONTACTS')
    expect(contactInsertSql).toContain('Jane Smith')
    expect(contactInsertSql).toContain('Acme Corp')

    // Deal DELETE + INSERT
    const dealDeleteSql = getSqlText(sqlCalls[5])
    expect(dealDeleteSql).toBe(
      "DELETE FROM dealsync_stg_v1.DEALS WHERE THREAD_ID = 'thread-abc-123' AND USER_ID = 'user-001'",
    )
    const dealInsertSql = getSqlText(sqlCalls[6])
    expect(dealInsertSql).toContain('INSERT INTO dealsync_stg_v1.DEALS')
    expect(dealInsertSql).toContain('Big Deal')
    expect(dealInsertSql).toContain('50000')

    // Deal_Contact DELETE + INSERT
    const dcDeleteSql = getSqlText(sqlCalls[7])
    expect(dcDeleteSql).toContain('DELETE FROM dealsync_stg_v1.DEAL_CONTACTS')
    const dcInsertSql = getSqlText(sqlCalls[8])
    expect(dcInsertSql).toContain('INSERT INTO dealsync_stg_v1.DEAL_CONTACTS')

    // Stage UPDATE to 4
    const stageSql = getSqlText(sqlCalls[9])
    expect(stageSql).toBe(
      "UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 4 WHERE ID IN ('email-id-001')",
    )
  })

  // 2. Non-deal classification: creates audit, evaluation, updates stage to 106
  // SQL calls: audit(1) + eval_del(2) + eval_ins(3) + stage_upd(4) = 4
  it('sets stage to 106 for non-deal classification', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: false,
            main_contact: null,
            category: 'not_a_deal',
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.deals_created).toBe('0')
    expect(mockOutputs.emails_classified).toBe('1')

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls.length).toBe(4)

    const stageSql = getSqlText(sqlCalls[3])
    expect(stageSql).toBe(
      "UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 106 WHERE ID IN ('email-id-001')",
    )
  })

  // 3. Non-English: updates stage to 107
  it('sets stage to 107 for non-English language thread', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: false,
            main_contact: null,
            language: 'es',
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.emails_classified).toBe('1')

    const sqlCalls = getSqlCalls(fetchSpy)
    const stageSql = getSqlText(sqlCalls[3])
    expect(stageSql).toBe(
      "UPDATE dealsync_stg_v1.EMAIL_METADATA SET STAGE = 107 WHERE ID IN ('email-id-001')",
    )
  })

  // 4. Re-authentication to SxT (login + biscuit fetch)
  it('authenticates to SxT with login and biscuit fetch', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [makeThread({ is_deal: false, main_contact: null })],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    // First call: login
    const loginCall = fetchSpy.mock.calls[0]
    expect(loginCall[0]).toBe('https://sxt.example.com/v1/auth/login')
    const loginBody = JSON.parse(loginCall[1].body)
    expect(loginBody).toEqual({ userId: 'test-user', password: 'test-pass' })

    // Second call: biscuit
    const biscuitCall = fetchSpy.mock.calls[1]
    expect(biscuitCall[0]).toBe(
      'https://sxt.example.com/v1/biscuits/generated/dealsync-dml',
    )
    expect(biscuitCall[1].headers.sessionId).toBe('test-session')

    // SQL calls use the access token
    const sqlCall = fetchSpy.mock.calls[2]
    expect(sqlCall[1].headers.Authorization).toBe('Bearer test-jwt')
  })

  // 5. SQL escape for strings with single quotes
  it('escapes single quotes in SQL string values', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: true,
            ai_summary: "It's a great deal",
            deal_title: "O'Brien's Offer",
            main_contact: {
              email: "o'brien@example.com",
              name: "Tim O'Brien",
              company: "O'Reilly",
              role: "CEO's assistant",
            },
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 10)

    await run()

    const sqlCalls = getSqlCalls(fetchSpy)

    // Check eval INSERT for escaped summary
    const evalInsertSql = getSqlText(sqlCalls[2])
    expect(evalInsertSql).toContain("It''s a great deal")

    // Check contact INSERT for escaped name
    const contactInsertSql = getSqlText(sqlCalls[4])
    expect(contactInsertSql).toContain("Tim O''Brien")
    expect(contactInsertSql).toContain("O''Reilly")

    // Check deal INSERT for escaped title
    const dealInsertSql = getSqlText(sqlCalls[6])
    expect(dealInsertSql).toContain("O''Brien''s Offer")

    expect(mockOutputs.success).toBe('true')
  })

  // 6. Partial failure (one thread fails, others still processed)
  it('continues processing when one thread fails', async () => {
    const threads = [
      makeThread({
        thread_id: 'thread-good-1',
        is_deal: false,
        main_contact: null,
      }),
      makeThread({
        thread_id: 'thread-bad-1',
        is_deal: false,
        main_contact: null,
      }),
      makeThread({
        thread_id: 'thread-good-2',
        is_deal: false,
        main_contact: null,
      }),
    ]
    mockInputs({
      'ai-output': JSON.stringify({ threads }),
      metadata: JSON.stringify([
        makeMetadata({ ID: 'e1', THREAD_ID: 'thread-good-1' }),
        makeMetadata({ ID: 'e2', THREAD_ID: 'thread-bad-1' }),
        makeMetadata({ ID: 'e3', THREAD_ID: 'thread-good-2' }),
      ]),
    })

    // Auth (2 calls)
    fetchSpy.mockResolvedValueOnce(authLoginResponse())
    fetchSpy.mockResolvedValueOnce(biscuitResponse())

    // Thread 1 (good): audit INSERT + eval DELETE + eval INSERT + stage UPDATE
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))

    // Thread 2 (bad): audit INSERT fails
    fetchSpy.mockResolvedValueOnce(
      new Response('Internal Server Error', { status: 500 }),
    )

    // Thread 3 (good): audit INSERT + eval DELETE + eval INSERT + stage UPDATE
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))
    fetchSpy.mockResolvedValueOnce(sxtResponse([]))

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.emails_classified).toBe('2')
    expect(core.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to process thread thread-bad-1'),
    )
  })

  // 7. Empty ai-output.threads: success with 0 counts
  it('returns success with 0 counts for empty threads', async () => {
    mockInputs({
      'ai-output': JSON.stringify({ threads: [] }),
    })

    await run()

    // No fetch calls at all (not even auth since we return early)
    expect(fetchSpy).not.toHaveBeenCalled()
    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.deals_created).toBe('0')
    expect(mockOutputs.emails_classified).toBe('0')
    expect(core.info).toHaveBeenCalledWith('No threads to process')
  })

  // 8. Multiple threads in one batch
  it('processes multiple threads in a single batch', async () => {
    const threads = [
      makeThread({ thread_id: 'thread-1', is_deal: true }),
      makeThread({
        thread_id: 'thread-2',
        is_deal: false,
        main_contact: null,
        category: 'spam',
      }),
      makeThread({
        thread_id: 'thread-3',
        is_deal: true,
        main_contact: {
          email: 'bob@corp.com',
          name: 'Bob',
          company: 'Corp',
          role: 'CTO',
        },
      }),
    ]
    mockInputs({
      'ai-output': JSON.stringify({ threads }),
      metadata: JSON.stringify([
        makeMetadata({ ID: 'e1', THREAD_ID: 'thread-1', USER_ID: 'u1' }),
        makeMetadata({ ID: 'e2', THREAD_ID: 'thread-1', USER_ID: 'u1' }),
        makeMetadata({ ID: 'e3', THREAD_ID: 'thread-2', USER_ID: 'u1' }),
        makeMetadata({ ID: 'e4', THREAD_ID: 'thread-3', USER_ID: 'u2' }),
      ]),
    })

    // thread-1 deal: 10 SQL, thread-2 non-deal: 4 SQL, thread-3 deal: 10 SQL = 24
    mockAuthAndSql(fetchSpy, 24)

    await run()

    expect(mockOutputs.success).toBe('true')
    expect(mockOutputs.deals_created).toBe('2')
    expect(mockOutputs.emails_classified).toBe('4')
  })

  // 9. likely_scam category: LIKELY_SCAM = true
  it('sets LIKELY_SCAM to true when category is likely_scam', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: false,
            main_contact: null,
            category: 'likely_scam',
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    const sqlCalls = getSqlCalls(fetchSpy)
    const evalInsertSql = getSqlText(sqlCalls[2])
    // Check LIKELY_SCAM is true in the VALUES
    expect(evalInsertSql).toContain(
      'INSERT INTO dealsync_stg_v1.EMAIL_THREAD_EVALUATIONS',
    )
    // The VALUES should have: ..., IS_DEAL, LIKELY_SCAM, ...
    // false (not a deal), true (is a scam)
    expect(evalInsertSql).toMatch(/false, true/)

    expect(mockOutputs.success).toBe('true')
  })

  // Additional: auth failure
  it('handles SxT auth failure', async () => {
    mockInputs()
    fetchSpy.mockResolvedValueOnce(
      new Response('Unauthorized', { status: 401 }),
    )

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('SxT auth login 401'),
    )
  })

  // Additional: invalid schema
  it('rejects invalid schema names', async () => {
    mockInputs({ 'sxt-schema': 'schema; DROP TABLE' })

    await run()

    expect(mockOutputs.success).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining('Invalid schema'),
    )
  })

  // Additional: non-English deal still gets stage 107 (language takes priority)
  it('sets stage to 107 for non-English even if is_deal is true', async () => {
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: true,
            language: 'zh',
          }),
        ],
      }),
    })
    // Deal with non-English: audit + eval(2) + contact(2) + deal(2) + dc(2) + stage = 10
    mockAuthAndSql(fetchSpy, 10)

    await run()

    const sqlCalls = getSqlCalls(fetchSpy)
    const stageSql = getSqlText(sqlCalls[sqlCalls.length - 1])
    expect(stageSql).toContain('SET STAGE = 107')
    expect(mockOutputs.success).toBe('true')
  })

  // Additional: AI_EVALUATION truncated to 6400 chars
  it('truncates AI_EVALUATION to 6400 characters', async () => {
    const longSummary = 'x'.repeat(7000)
    mockInputs({
      'ai-output': JSON.stringify({
        threads: [
          makeThread({
            is_deal: false,
            main_contact: null,
            ai_summary: longSummary,
          }),
        ],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    const sqlCalls = getSqlCalls(fetchSpy)
    const auditSql = getSqlText(sqlCalls[0])
    // The raw JSON is truncated to 6400 chars before SQL escaping
    expect(auditSql).toContain(
      'INSERT INTO dealsync_stg_v1.AI_EVALUATION_AUDITS',
    )
    // Verify truncation happened (the raw JSON.stringify is cut at 6400)
    const valuesMatch = auditSql.match(/AI_EVALUATION, CREATED_AT/)
    expect(valuesMatch).not.toBeNull()

    expect(mockOutputs.success).toBe('true')
  })

  // Additional: inference cost calculation
  it('calculates inference cost from tokens', async () => {
    mockInputs({
      'ai-prompt-tokens': '2000',
      'ai-completion-tokens': '1000',
      'ai-output': JSON.stringify({
        threads: [makeThread({ is_deal: false, main_contact: null })],
      }),
    })
    mockAuthAndSql(fetchSpy, 4)

    await run()

    const sqlCalls = getSqlCalls(fetchSpy)
    const auditSql = getSqlText(sqlCalls[0])
    // (2000 + 1000) / 1000 * 0.001 = 0.003
    expect(auditSql).toContain('0.003')
  })
})
