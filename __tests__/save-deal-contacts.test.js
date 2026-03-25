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

  it('should insert deal contacts with enrichment fields from AI response', async () => {
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
      .mockResolvedValueOnce(sxtResponse()) // 3. DELETE contacts
      .mockResolvedValueOnce(sxtResponse()) // 4. INSERT contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    expect(sqlCalls).toHaveLength(3) // audit read + delete + insert

    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain('INSERT INTO')
    expect(insertSql).toContain('DEAL_CONTACTS')
    expect(insertSql).toContain('NAME')
    expect(insertSql).toContain('EMAIL')
    expect(insertSql).toContain('COMPANY')
    expect(insertSql).toContain('TITLE')
    expect(insertSql).toContain('PHONE_NUMBER')
    expect(insertSql).toContain('Sarah Kim')
    expect(insertSql).toContain('sarah@beautybrandx.com')
    expect(insertSql).toContain('Beauty Brand X')
    expect(insertSql).toContain('Partnerships Manager')
    expect(insertSql).toContain('+1-555-0100')
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
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId
      .mockResolvedValueOnce(sxtResponse()) // 3. DELETE contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(0)

    const sqlCalls = getSqlCalls(fetchSpy)
    // Should only have audit read + delete, no INSERT
    expect(sqlCalls).toHaveLength(2)
    const contactInserts = sqlCalls.filter((c) => getSqlText(c).includes('INSERT') && getSqlText(c).includes('DEAL_CONTACTS'))
    expect(contactInserts).toHaveLength(0)
  })

  it('should delete existing contacts before inserting new ones', async () => {
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
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId
      .mockResolvedValueOnce(sxtResponse()) // 3. DELETE contacts
      .mockResolvedValueOnce(sxtResponse()) // 4. INSERT contacts

    await runSaveDealContacts()

    const sqlCalls = getSqlCalls(fetchSpy)
    const deleteIdx = sqlCalls.findIndex((c) => getSqlText(c).includes('DELETE') && getSqlText(c).includes('DEAL_CONTACTS'))
    const insertIdx = sqlCalls.findIndex((c) => getSqlText(c).includes('INSERT') && getSqlText(c).includes('DEAL_CONTACTS'))
    expect(deleteIdx).toBeGreaterThanOrEqual(0)
    expect(insertIdx).toBeGreaterThan(deleteIdx)
  })

  it('should return 0 when no audit found', async () => {
    mockInputs()

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse([])) // 2. getAuditByBatchId — empty

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(0)
  })

  it('should return 0 when no deal threads in audit', async () => {
    mockInputs()

    const threads = [
      { thread_id: 'thread-not-deal', is_deal: false, deal_name: '', main_contact: null },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId

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
        main_contact: { name: 'Alice', email: 'alice@a.com', company: 'A Inc', title: 'VP', phone_number: null },
      },
      {
        thread_id: 'thread-002',
        is_deal: true,
        deal_name: 'Deal B',
        main_contact: { name: 'Bob', email: 'bob@b.com', company: 'B Corp', title: 'Director', phone_number: '+1-555-0200' },
      },
      {
        thread_id: 'thread-003',
        is_deal: false,
        deal_name: '',
        main_contact: null,
      },
    ]

    fetchSpy
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId
      .mockResolvedValueOnce(sxtResponse()) // 3. DELETE contacts
      .mockResolvedValueOnce(sxtResponse()) // 4. INSERT contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(2)

    const sqlCalls = getSqlCalls(fetchSpy)
    const insertSql = getSqlText(sqlCalls[2])
    expect(insertSql).toContain('alice@a.com')
    expect(insertSql).toContain('bob@b.com')
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
      .mockResolvedValueOnce(authResponse()) // 1. auth
      .mockResolvedValueOnce(sxtResponse(makeAudit(threads))) // 2. getAuditByBatchId
      .mockResolvedValueOnce(sxtResponse()) // 3. DELETE contacts
      .mockResolvedValueOnce(sxtResponse()) // 4. INSERT contacts

    const result = await runSaveDealContacts()

    expect(result.contacts_created).toBe(1)

    const sqlCalls = getSqlCalls(fetchSpy)
    const insertSql = getSqlText(sqlCalls[2])
    // Single quotes should be escaped as double single quotes
    expect(insertSql).toContain("O''Brien")
    expect(insertSql).toContain("Mc''Donald''s Inc")
  })
})
