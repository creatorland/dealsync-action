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
const { runSaveDealContacts } = await import('../src/commands/save-deal-contacts.js')

function mockInputs(overrides = {}) {
  const defaults = {
    'auth-url': 'https://auth.example.com/token',
    'auth-secret': 'test-secret',
    'api-url': 'https://sxt.example.com',
    biscuit: 'test-biscuit',
    schema: 'dealsync_stg_v1',
    'email-core-schema': 'EMAIL_CORE_STAGING',
    'batch-id': 'batch-001',
    'rate-limiter-url': '',
    'rate-limiter-api-key': '',
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

function makeAudit(threads) {
  return [{ AI_EVALUATION: JSON.stringify({ threads }) }]
}

function makeDeals(entries) {
  return entries.map(([threadId, dealId, userId]) => ({
    THREAD_ID: threadId,
    ID: dealId,
    USER_ID: userId || 'user-001',
  }))
}

describe('save-deal-contacts command', () => {
  let fetchSpy

  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
    fetchSpy = jest.spyOn(globalThis, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  it('should upsert core contacts and deal contacts from AI response', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-abc',
        is_deal: true,
        deal_name: 'Brand X Collab',
        main_contact: {
          name: 'Sarah Kim',
          email: 'sarah@beautybrandx.com',
          company: 'Beauty Brand X',
          title: 'Partnerships Manager',
          phone_number: '+1-555-0100',
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-abc', 'deal-001', 'user-abc']]))) // 3. SELECT deals
      .mockResolvedValueOnce(sxtResponse()) // 4. core contacts upsert
      .mockResolvedValueOnce(sxtResponse()) // 5. deal contacts upsert

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(4) // audit + deals lookup + core upsert + deal upsert

    // Core contacts upsert — enrichment fields with COALESCE
    const coreSql = getSqlText(sqlCalls[2])
    expect(coreSql).toContain('EMAIL_CORE_STAGING.CONTACTS')
    expect(coreSql).toContain('ON CONFLICT (USER_ID, EMAIL)')
    expect(coreSql).toContain('EXCLUDED.NAME')
    expect(coreSql).toContain('Sarah Kim')
    expect(coreSql).toContain('Beauty Brand X')
    expect(coreSql).toContain('Partnerships Manager')
    expect(coreSql).toContain('+1-555-0100')

    // Deal contacts upsert — simplified 4-column
    const dealSql = getSqlText(sqlCalls[3])
    expect(dealSql).toContain('DEAL_CONTACTS')
    expect(dealSql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL)')
    expect(dealSql).toContain('sarah@beautybrandx.com')
    expect(dealSql).toContain('thread-abc') // dealId = threadId
    expect(dealSql).toContain('user-abc')
    expect(dealSql).toContain('primary')
    // Should NOT contain old enrichment columns in deal contacts
    expect(dealSql).not.toContain('NAME')
    expect(dealSql).not.toContain('COMPANY')
  })

  it('should lowercase email addresses', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-upper',
        is_deal: true,
        deal_name: 'Upper Deal',
        main_contact: {
          name: 'Test',
          email: 'Sarah.Kim@BrandX.COM',
          company: 'BrandX',
          title: null,
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-upper', 'deal-upper', 'user-u']])))
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    await runSaveDealContacts()

    const sqlCalls = getSqlCalls(fetchSpy)
    const coreSql = getSqlText(sqlCalls[2])
    const dealSql = getSqlText(sqlCalls[3])

    expect(coreSql).toContain('sarah.kim@brandx.com')
    expect(coreSql).not.toContain('Sarah.Kim@BrandX.COM')
    expect(dealSql).toContain('sarah.kim@brandx.com')
  })

  it('should skip contacts without email', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-no-email',
        is_deal: true,
        deal_name: 'Some Deal',
        main_contact: { name: 'John', email: '', company: 'Acme' },
      },
      {
        thread_id: 'thread-no-contact',
        is_deal: true,
        deal_name: 'Another Deal',
        main_contact: null,
      },
      {
        thread_id: 'thread-whitespace',
        is_deal: true,
        deal_name: 'WS Deal',
        main_contact: { name: 'Jane', email: '   ', company: 'Ws Inc' },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(
        sxtResponse(
          makeDeals([
            ['thread-no-email', 'deal-a'],
            ['thread-no-contact', 'deal-b'],
            ['thread-whitespace', 'deal-c'],
          ]),
        ),
      )

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(0)

    // Only audit + deals lookup — no insert calls
    const sqlCalls = getSqlCalls(fetchSpy)
    const contactInserts = sqlCalls.filter(
      (c) => getSqlText(c).includes('INSERT') && getSqlText(c).includes('DEAL_CONTACTS'),
    )
    expect(contactInserts).toHaveLength(0)
  })

  it('should use ON CONFLICT upsert instead of delete-then-insert', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-xyz',
        is_deal: true,
        deal_name: 'Deal XYZ',
        main_contact: {
          name: 'Jane Doe',
          email: 'jane@example.com',
          company: 'ExampleCo',
          title: 'CEO',
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-xyz', 'deal-xyz', 'user-xyz']])))
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    await runSaveDealContacts()

    const sqlCalls = getSqlCalls(fetchSpy)
    // No DELETE statements
    const deleteCalls = sqlCalls.filter((c) => getSqlText(c).includes('DELETE'))
    expect(deleteCalls).toHaveLength(0)

    // Both inserts use ON CONFLICT
    const dealSql = getSqlText(sqlCalls[3])
    expect(dealSql).toContain('ON CONFLICT')
  })

  it('should return 0 when no audit found', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse([]))

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(0)
  })

  it('should return 0 when no deal threads in audit', async () => {
    mockInputs()

    const threads = [
      { thread_id: 'thread-not-deal', is_deal: false, deal_name: '', main_contact: null },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(0)
  })

  it('should handle multiple deal threads in a single batch', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-001',
        is_deal: true,
        deal_name: 'Deal A',
        main_contact: {
          name: 'Alice',
          email: 'alice@a.com',
          company: 'A Inc',
          title: 'VP',
          phone_number: null,
        },
      },
      {
        thread_id: 'thread-002',
        is_deal: true,
        deal_name: 'Deal B',
        main_contact: {
          name: 'Bob',
          email: 'bob@b.com',
          company: 'B Corp',
          title: 'Director',
          phone_number: '+1-555-0200',
        },
      },
      {
        thread_id: 'thread-003',
        is_deal: false,
        deal_name: '',
        main_contact: null,
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(
        sxtResponse(
          makeDeals([
            ['thread-001', 'deal-a', 'user-001'],
            ['thread-002', 'deal-b', 'user-002'],
          ]),
        ),
      )
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(2)

    const sqlCalls = getSqlCalls(fetchSpy)
    const dealSql = getSqlText(sqlCalls[3])
    expect(dealSql).toContain('alice@a.com')
    expect(dealSql).toContain('bob@b.com')
  })

  it('should skip contacts when deal not found for thread', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-has-deal',
        is_deal: true,
        deal_name: 'Deal OK',
        main_contact: {
          name: 'Alice',
          email: 'alice@a.com',
          company: 'A Inc',
          title: 'VP',
          phone_number: null,
        },
      },
      {
        thread_id: 'thread-no-deal',
        is_deal: true,
        deal_name: 'Deal Missing',
        main_contact: {
          name: 'Bob',
          email: 'bob@b.com',
          company: 'B Corp',
          title: 'CEO',
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-has-deal', 'deal-ok', 'user-ok']])))
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(1)
  })

  it('should throw on missing batch-id', async () => {
    mockInputs({ 'batch-id': '' })
    await expect(runSaveDealContacts()).rejects.toThrow()
  })

  it('should sanitize contact strings with single quotes', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-quote',
        is_deal: true,
        deal_name: 'Quote Deal',
        main_contact: {
          name: "O'Brien",
          email: 'obrien@test.com',
          company: "Mc'Donald's Inc",
          title: "Head of Int'l",
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-quote', 'deal-quote', 'user-q']])))
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const coreSql = getSqlText(sqlCalls[2])
    expect(coreSql).toContain("O''Brien")
    expect(coreSql).toContain("Mc''Donald''s Inc")
  })

  it('should use NULL for missing optional contact fields in core contacts', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-sparse',
        is_deal: true,
        deal_name: 'Sparse Deal',
        main_contact: {
          name: null,
          email: 'sparse@test.com',
          company: null,
          title: null,
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-sparse', 'deal-sparse', 'user-s']])))
      .mockResolvedValueOnce(sxtResponse()) // core contacts
      .mockResolvedValueOnce(sxtResponse()) // deal contacts

    await runSaveDealContacts()

    const sqlCalls = getSqlCalls(fetchSpy)
    const coreSql = getSqlText(sqlCalls[2])
    // All optional fields should be NULL literal (not empty strings)
    const valuesMatch = coreSql.match(/VALUES\s*\((.+)\)/s)
    expect(valuesMatch).toBeTruthy()
    const values = valuesMatch[1]
    // Should contain 4 NULL literals for name, company, title, phone
    const nullCount = (values.match(/NULL/g) || []).length
    expect(nullCount).toBe(4)
  })

  it('should continue if core contacts upsert fails', async () => {
    mockInputs()

    const threads = [
      {
        thread_id: 'thread-core-fail',
        is_deal: true,
        deal_name: 'Core Fail Deal',
        main_contact: {
          name: 'Test',
          email: 'test@fail.com',
          company: 'FailCo',
          title: null,
          phone_number: null,
        },
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse())
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads)))
      .mockResolvedValueOnce(sxtResponse(makeDeals([['thread-core-fail', 'deal-cf', 'user-cf']])))
      .mockResolvedValueOnce(new Response('Internal Server Error', { status: 500 })) // core contacts fails
      .mockResolvedValueOnce(sxtResponse()) // deal contacts succeeds

    const result = await runSaveDealContacts()

    // Should still succeed — core contacts failure is non-fatal
    expect(result.contacts_created).toBe(1)
  })
})
