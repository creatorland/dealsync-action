import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const executeSql = jest.fn()
const authenticate = jest.fn().mockResolvedValue('jwt-stub')

jest.unstable_mockModule('../src/lib/db.js', () => ({
  authenticate,
  executeSql: (apiUrl, jwt, biscuit, sql) => executeSql(sql),
  acquireRateLimitToken: jest.fn().mockResolvedValue(true),
  withTimeout: jest.fn(),
}))

jest.unstable_mockModule('../prompts/system.md', () => ({ default: 's' }))
jest.unstable_mockModule('../prompts/user.md', () => ({ default: 'u {{THREAD_DATA}}' }))
jest.unstable_mockModule('../prompts/system-llama.md', () => ({ default: 'sl' }))

const core = await import('@actions/core')
const { runSyncDealValues } = await import('../src/commands/sync-deal-values.js')

function setInputs(inputs) {
  core.getInput.mockImplementation((name) => inputs[name] ?? '')
}

const DEFAULTS = {
  'sxt-auth-url': 'https://auth',
  'sxt-auth-secret': 'secret',
  'sxt-api-url': 'https://api',
  'sxt-biscuit': 'bisc',
  'sxt-schema': 'dealsync_stg_v1',
  'backfill-start-date': '2026-03-31',
  'backfill-batch-size': '500',
  'backfill-audit-page-size': '500',
  'backfill-dry-run': 'false',
}

const auditJson = JSON.stringify([
  {
    thread_id: 'thread-1',
    is_deal: true,
    category: 'in_progress',
    deal_type: 'brand_collaboration',
    deal_name: 'Acme',
    deal_value: 2500,
    deal_currency: 'EUR',
    ai_score: 8,
  },
  {
    thread_id: 'thread-2',
    is_deal: true,
    category: 'in_progress',
    deal_type: 'sponsorship',
    deal_name: 'Beta',
    deal_value: null,
    deal_currency: null,
    ai_score: 5,
  },
])

beforeEach(() => {
  executeSql.mockReset()
  setInputs(DEFAULTS)
})

test('happy path: builds map, finds deal, bulk-updates', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1)
  expect(result.skipped.auditMissing).toBe(0)
  expect(result.totalScanned).toBe(1)
  expect(result.threadEntries).toBe(2)

  const updateCall = executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))
  expect(updateCall[0]).toContain('CASE ID')
  expect(updateCall[0]).toContain("WHEN 'deal-1' THEN 2500")
  expect(updateCall[0]).toContain("WHEN 'deal-1' THEN 'EUR'")
  expect(updateCall[0]).toContain("WHERE ID IN ('deal-1')")
  expect(updateCall[0]).toContain('VALUE = 0 OR VALUE IS NULL')
})

test('skips deal whose thread is not in any audit', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([{ ID: 'deal-x', THREAD_ID: 'thread-missing', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.auditMissing).toBe(1)
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('skips deal whose audit entry has null deal_value', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([{ ID: 'deal-2', THREAD_ID: 'thread-2', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.valueNull).toBe(1)
})

test('counts unparsable audits but continues', async () => {
  executeSql
    .mockResolvedValueOnce([
      { ID: 'audit-1', AI_EVALUATION: 'not valid json' },
      { ID: 'audit-2', AI_EVALUATION: auditJson },
    ])
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.auditsParseFailed).toBe(1)
  expect(result.recovered).toBe(1)
})

test('dry-run does not issue UPDATE', async () => {
  setInputs({ ...DEFAULTS, 'backfill-dry-run': 'true' })
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1)
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('paginates deals, single bulk update per page', async () => {
  setInputs({ ...DEFAULTS, 'backfill-batch-size': '2' })
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([
      { ID: 'deal-a', THREAD_ID: 'thread-1', USER_ID: 'u1' },
      { ID: 'deal-b', THREAD_ID: 'thread-1', USER_ID: 'u1' },
    ])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(2)
  expect(result.pages).toBe(1)

  const updateCalls = executeSql.mock.calls.filter(([sql]) => sql.startsWith('UPDATE'))
  expect(updateCalls).toHaveLength(1)
  expect(updateCalls[0][0]).toContain("WHERE ID IN ('deal-a', 'deal-b')")
})

test('paginates audits via cursor', async () => {
  setInputs({ ...DEFAULTS, 'backfill-audit-page-size': '1' })
  executeSql
    .mockResolvedValueOnce([{ ID: 'audit-1', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([{ ID: 'audit-2', AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.auditsScanned).toBe(2)
  const auditSelects = executeSql.mock.calls.filter(([sql]) =>
    sql.includes('AI_EVALUATION_AUDITS'),
  )
  expect(auditSelects.length).toBe(3)
  expect(auditSelects[1][0]).toContain("ID > 'audit-1'")
})
