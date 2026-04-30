import {
  UEI_LOOKBACK_DAYS_DEFAULT,
  UEI_LOOKBACK_DAYS_FALLBACK,
  UEI_LOOKBACK_DAYS_MAX,
  UEI_LOOKBACK_FALLBACK_REASONS,
  buildUeiLookbackFallbackPayload,
  createLookbackDateRange,
  emitUeiLookbackFallbackLog,
  parseUeiLookbackDaysArg,
  resolveUeiLookbackFallbackReason,
} from '../src/lib/uei-lookback.js'

describe('uei-lookback', () => {
  it('uses 60 as default window and 45 as fallback target (#471)', () => {
    expect(UEI_LOOKBACK_DAYS_DEFAULT).toBe(60)
    expect(UEI_LOOKBACK_DAYS_FALLBACK).toBe(45)
  })

  it('createLookbackDateRange spans exactly N days', () => {
    const now = Date.UTC(2026, 3, 30, 12, 0, 0)
    const { rangeStart, rangeEnd } = createLookbackDateRange(now, 60)
    expect(rangeEnd.getTime()).toBe(now)
    const deltaDays = (rangeEnd.getTime() - rangeStart.getTime()) / (24 * 60 * 60 * 1000)
    expect(deltaDays).toBe(60)
  })

  it('createLookbackDateRange falls back to default window when days invalid', () => {
    const now = Date.UTC(2026, 3, 30, 12, 0, 0)
    for (const bad of [Number.NaN, 0, -5, 45.7]) {
      const { rangeStart, rangeEnd } = createLookbackDateRange(now, bad)
      const deltaDays = (rangeEnd.getTime() - rangeStart.getTime()) / (24 * 60 * 60 * 1000)
      expect(deltaDays).toBe(UEI_LOOKBACK_DAYS_DEFAULT)
    }
  })

  it('createLookbackDateRange clamps days to UEI_LOOKBACK_DAYS_MAX', () => {
    const now = Date.UTC(2026, 3, 30, 12, 0, 0)
    const { rangeStart, rangeEnd } = createLookbackDateRange(now, 9000)
    const deltaDays = (rangeEnd.getTime() - rangeStart.getTime()) / (24 * 60 * 60 * 1000)
    expect(deltaDays).toBe(UEI_LOOKBACK_DAYS_MAX)
  })

  it('parseUeiLookbackDaysArg accepts digit-only strings and rejects partial parses', () => {
    expect(parseUeiLookbackDaysArg(undefined)).toBe(UEI_LOOKBACK_DAYS_DEFAULT)
    expect(parseUeiLookbackDaysArg('')).toBe(UEI_LOOKBACK_DAYS_DEFAULT)
    expect(parseUeiLookbackDaysArg('  45 ')).toBe(45)
    expect(parseUeiLookbackDaysArg('60')).toBe(60)
    expect(parseUeiLookbackDaysArg('60days')).toBe(UEI_LOOKBACK_DAYS_DEFAULT)
    expect(parseUeiLookbackDaysArg('abc')).toBe(UEI_LOOKBACK_DAYS_DEFAULT)
  })

  it('parseUeiLookbackDaysArg clamps to UEI_LOOKBACK_DAYS_MAX', () => {
    expect(parseUeiLookbackDaysArg(String(UEI_LOOKBACK_DAYS_MAX + 1))).toBe(UEI_LOOKBACK_DAYS_MAX)
  })

  it('createLookbackDateRange uses Date.now when nowMs is not finite', () => {
    const before = Date.now()
    const { rangeEnd } = createLookbackDateRange(Number.NaN, 1)
    const after = Date.now()
    expect(rangeEnd.getTime()).toBeGreaterThanOrEqual(before)
    expect(rangeEnd.getTime()).toBeLessThanOrEqual(after)
  })

  it('buildUeiLookbackFallbackPayload matches NFR-3 log contract', () => {
    expect(buildUeiLookbackFallbackPayload('u1', 45, 'gmail_quota')).toEqual({
      userId: 'u1',
      fellBackTo: 45,
      reason: 'gmail_quota',
    })
  })

  it('exports frozen fallback reason allowlist', () => {
    expect(Object.isFrozen(UEI_LOOKBACK_FALLBACK_REASONS)).toBe(true)
    expect(UEI_LOOKBACK_FALLBACK_REASONS).toHaveLength(4)
    expect([...UEI_LOOKBACK_FALLBACK_REASONS]).toEqual([
      'gmail_quota',
      'gmail_rate_limit',
      'batch_processing_constraint',
      'operational_window_exceeded',
    ])
  })

  it('emitUeiLookbackFallbackLog rejects unknown reason values', () => {
    expect(() => emitUeiLookbackFallbackLog('u', 45, 'typo_quota', { log: () => {} })).toThrow(
      /Invalid UEI lookback fallback reason/,
    )
  })

  it('emitUeiLookbackFallbackLog writes JSON with event + AC fields', () => {
    const lines = []
    emitUeiLookbackFallbackLog('user-2', 45, 'operational_window_exceeded', {
      log: (s) => lines.push(s),
    })
    expect(lines).toHaveLength(1)
    const row = JSON.parse(lines[0])
    expect(row).toMatchObject({
      event: 'uei_lookback_fallback',
      userId: 'user-2',
      fellBackTo: 45,
      reason: 'operational_window_exceeded',
    })
  })

  it('resolveUeiLookbackFallbackReason returns null when not on 60-day attempt', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 45,
        syncComplete: false,
        elapsedMs: 999,
        operationalBudgetMs: 1,
      }),
    ).toBeNull()
  })

  it('resolveUeiLookbackFallbackReason returns null when sync already complete', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: true,
        elapsedMs: 999,
        operationalBudgetMs: 1,
        gmailQuotaExceeded: true,
      }),
    ).toBeNull()
  })

  it('resolveUeiLookbackFallbackReason ignores truthy non-boolean signal flags', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 10_000,
        operationalBudgetMs: 10_000,
        gmailQuotaExceeded: 1,
        sustainedGmailRateLimit: 'yes',
        batchProcessingBlocked: {},
      }),
    ).toBe('operational_window_exceeded')
  })

  it('resolveUeiLookbackFallbackReason does not treat truthy syncComplete as complete', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: 1,
        elapsedMs: 10_000,
        operationalBudgetMs: 10_000,
      }),
    ).toBe('operational_window_exceeded')
  })

  it('prioritizes gmail_quota over operational window', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 0,
        operationalBudgetMs: 999999,
        gmailQuotaExceeded: true,
      }),
    ).toBe('gmail_quota')
  })

  it('returns operational_window_exceeded when budget exhausted and no stronger signal', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 10_000,
        operationalBudgetMs: 10_000,
      }),
    ).toBe('operational_window_exceeded')
  })

  it('does not return operational_window_exceeded when budget is zero or non-finite', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 999_999,
        operationalBudgetMs: 0,
      }),
    ).toBeNull()
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 999_999,
        operationalBudgetMs: Number.NaN,
      }),
    ).toBeNull()
  })

  it('skips operational fallback when budget undefined, negative, or elapsedMs non-finite', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 10_000,
      }),
    ).toBeNull()
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 999_999,
        operationalBudgetMs: -1,
      }),
    ).toBeNull()
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: Number.NaN,
        operationalBudgetMs: 10_000,
      }),
    ).toBeNull()
  })

  it('returns gmail_rate_limit when sustained rate limit flag set', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 0,
        operationalBudgetMs: 999999,
        sustainedGmailRateLimit: true,
      }),
    ).toBe('gmail_rate_limit')
  })

  it('returns batch_processing_constraint when flagged', () => {
    expect(
      resolveUeiLookbackFallbackReason({
        lookbackDaysRequested: 60,
        syncComplete: false,
        elapsedMs: 0,
        operationalBudgetMs: 999999,
        batchProcessingBlocked: true,
      }),
    ).toBe('batch_processing_constraint')
  })
})
