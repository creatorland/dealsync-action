import { jest } from '@jest/globals'
import { fallbackReattemptEligibility } from '../src/lib/sql/fallback-reattempt-eligibility.js'
import {
  extractRowFields,
  parsePositiveIntegerInput,
  postFallbackReattempt,
} from '../src/commands/run-fallback-reattempt-pipeline.js'

describe('fallbackReattemptEligibility.selectUnreattemptedFallbacks', () => {
  it('builds an SQL string scoped to LOOKBACK + failed + fallback_reason set', () => {
    const sql = fallbackReattemptEligibility.selectUnreattemptedFallbacks(
      'EMAIL_CORE_STAGING',
      200,
    )
    expect(sql).toContain('EMAIL_CORE_STAGING.sync_states')
    expect(sql).toMatch(/sync_strategy\s*=\s*'LOOKBACK'/)
    expect(sql).toMatch(/status\s*=\s*'failed'/)
    expect(sql).toMatch(/fallback_reason IS NOT NULL/)
  })

  it('enforces the one-shot guarantee via originating_sync_state_id IS NULL + NOT EXISTS successor', () => {
    const sql = fallbackReattemptEligibility.selectUnreattemptedFallbacks(
      'EMAIL_CORE_STAGING',
      50,
    )
    expect(sql).toMatch(/originating_sync_state_id IS NULL/)
    expect(sql).toMatch(/NOT EXISTS\s*\(/)
    expect(sql).toMatch(/originating_sync_state_id\s*=\s*ss\.id/)
  })

  it('bounds history to 7 days so ancient failures do not get re-attempted', () => {
    const sql = fallbackReattemptEligibility.selectUnreattemptedFallbacks(
      'EMAIL_CORE_STAGING',
      50,
    )
    // Uses INTERVAL 'N' MINUTE (proven against SxT in deal-states.js) rather
    // than the unverified DAY unit; 7 days = 10080 minutes.
    expect(sql).toMatch(/INTERVAL\s+'10080'\s+MINUTE/i)
  })

  it('LIMITs by the requested batch size', () => {
    const sql = fallbackReattemptEligibility.selectUnreattemptedFallbacks(
      'EMAIL_CORE_STAGING',
      77,
    )
    expect(sql).toMatch(/LIMIT 77\s*$/)
  })

  it('rejects non-positive batchSize', () => {
    expect(() =>
      fallbackReattemptEligibility.selectUnreattemptedFallbacks('EMAIL_CORE_STAGING', 0),
    ).toThrow(/batchSize/)
    expect(() =>
      fallbackReattemptEligibility.selectUnreattemptedFallbacks('EMAIL_CORE_STAGING', -1),
    ).toThrow(/batchSize/)
    expect(() =>
      fallbackReattemptEligibility.selectUnreattemptedFallbacks('EMAIL_CORE_STAGING', 1.5),
    ).toThrow(/batchSize/)
  })

  it('sanitizes the schema identifier (defends against injection)', () => {
    expect(() =>
      fallbackReattemptEligibility.selectUnreattemptedFallbacks('bad; DROP TABLE x;--', 10),
    ).toThrow()
  })
})

describe('extractRowFields', () => {
  it('reads UPPERCASE keys (SxT default)', () => {
    expect(
      extractRowFields({
        USER_ID: 'u1',
        SYNC_STATE_ID: 's1',
        FALLBACK_REASON: 'gmail_quota',
      }),
    ).toEqual({ userId: 'u1', syncStateId: 's1' })
  })

  it('reads snake_case keys', () => {
    expect(extractRowFields({ user_id: 'u1', sync_state_id: 's1' })).toEqual({
      userId: 'u1',
      syncStateId: 's1',
    })
  })

  it('reads camelCase keys', () => {
    expect(extractRowFields({ userId: 'u1', syncStateId: 's1' })).toEqual({
      userId: 'u1',
      syncStateId: 's1',
    })
  })

  it('throws on missing user_id', () => {
    expect(() => extractRowFields({ SYNC_STATE_ID: 's1' })).toThrow(/user_id/)
  })

  it('throws on missing sync_state_id', () => {
    expect(() => extractRowFields({ USER_ID: 'u1' })).toThrow(/sync_state_id/)
  })

  it('throws on null row', () => {
    expect(() => extractRowFields(null)).toThrow()
  })
})

describe('parsePositiveIntegerInput', () => {
  it('parses positive integers', () => {
    expect(parsePositiveIntegerInput('5', 'x')).toBe(5)
    expect(parsePositiveIntegerInput('200', 'x')).toBe(200)
  })

  it('rejects zero, negatives, decimals, and non-numerics', () => {
    expect(() => parsePositiveIntegerInput('0', 'x')).toThrow(/positive integer/)
    expect(() => parsePositiveIntegerInput('-5', 'x')).toThrow(/positive integer/)
    expect(() => parsePositiveIntegerInput('1.5', 'x')).toThrow(/positive integer/)
    expect(() => parsePositiveIntegerInput('abc', 'x')).toThrow(/positive integer/)
  })
})

describe('postFallbackReattempt', () => {
  let originalFetch
  beforeEach(() => {
    originalFetch = global.fetch
  })
  afterEach(() => {
    global.fetch = originalFetch
  })

  it('POSTs the override payload + LOOKBACK strategy + 45-day fallback', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
    })
    global.fetch = fetchMock

    const res = await postFallbackReattempt(
      'https://backend.example/api',
      's3cret',
      { userId: 'user-1', originatingSyncStateId: 'sync-60d' },
    )

    expect(res).toEqual({ ok: true, status: 200 })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('https://backend.example/api/v1/dealsync-v2/sync/ingestion-trigger')
    expect(init.method).toBe('POST')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(init.headers['x-shared-secret']).toBe('s3cret')
    expect(JSON.parse(init.body)).toEqual({
      userId: 'user-1',
      syncStrategy: 'LOOKBACK',
      lookbackDaysOverride: 45,
      originatingSyncStateId: 'sync-60d',
    })
  })

  it('treats 409 as success (already in progress)', async () => {
    global.fetch = jest.fn().mockResolvedValue({ status: 409, ok: false })

    const res = await postFallbackReattempt(
      'https://backend.example/api',
      's',
      { userId: 'u', originatingSyncStateId: 'orig' },
    )

    expect(res).toEqual({ ok: true, status: 409 })
  })

  it('returns ok=false on 5xx with body text', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 500,
      ok: false,
      text: jest.fn().mockResolvedValue('boom'),
    })

    const res = await postFallbackReattempt(
      'https://backend.example/api',
      's',
      { userId: 'u', originatingSyncStateId: 'orig' },
    )

    expect(res.ok).toBe(false)
    expect(res.status).toBe(500)
    expect(res.text).toBe('boom')
  })

  it('trims trailing slashes from backend base URL', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ status: 200, ok: true })
    global.fetch = fetchMock

    await postFallbackReattempt(
      'https://backend.example/api///',
      's',
      { userId: 'u', originatingSyncStateId: 'orig' },
    )

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('https://backend.example/api/v1/dealsync-v2/sync/ingestion-trigger')
  })

  it('forwards extra headers (e.g. x-correlation-id)', async () => {
    const fetchMock = jest.fn().mockResolvedValue({ status: 200, ok: true })
    global.fetch = fetchMock

    await postFallbackReattempt(
      'https://backend.example/api',
      's',
      { userId: 'u', originatingSyncStateId: 'orig' },
      { 'x-correlation-id': 'cid-123' },
    )

    const [, init] = fetchMock.mock.calls[0]
    expect(init.headers['x-correlation-id']).toBe('cid-123')
  })
})
