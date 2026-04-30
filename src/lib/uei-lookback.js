/**
 * UEI LOOKBACK window (FR1 / Story dealsync-v2#471).
 * Default 60 days of Gmail history with graceful fallback to 45 days when
 * quota, rate limits, or batch/operational-window constraints apply.
 *
 * **Day semantics:** “N days” means N × 86_400_000 ms anchored at `rangeEnd` (UTC instant).
 * Not calendar-local midnight boundaries; aligns with ISO timestamps passed to SQL `TIMESTAMP`.
 */

export const UEI_LOOKBACK_DAYS_DEFAULT = 60
export const UEI_LOOKBACK_DAYS_FALLBACK = 45
/** Sanity cap for lookback window (avoids pathological Date arithmetic). */
export const UEI_LOOKBACK_DAYS_MAX = 3650

/** @typedef {'gmail_quota' | 'gmail_rate_limit' | 'batch_processing_constraint' | 'operational_window_exceeded'} UeiLookbackFallbackReason */

/** @type {readonly UeiLookbackFallbackReason[]} */
export const UEI_LOOKBACK_FALLBACK_REASONS = Object.freeze([
  'gmail_quota',
  'gmail_rate_limit',
  'batch_processing_constraint',
  'operational_window_exceeded',
])

/**
 * Parse a lookback day count from CLI or untrusted string input.
 * Accepts only an optional trimmed all-digit string; rejects partial parses (e.g. `60days`).
 *
 * @param {string | undefined} raw
 * @returns {number} validated days in (0, UEI_LOOKBACK_DAYS_MAX], or default when invalid
 */
export function parseUeiLookbackDaysArg(raw) {
  if (raw === undefined || raw === '') {
    return UEI_LOOKBACK_DAYS_DEFAULT
  }
  const trimmed = String(raw).trim()
  if (trimmed === '' || !/^\d+$/.test(trimmed)) {
    return UEI_LOOKBACK_DAYS_DEFAULT
  }
  const n = Number(trimmed)
  if (!Number.isSafeInteger(n) || n <= 0) {
    return UEI_LOOKBACK_DAYS_DEFAULT
  }
  return Math.min(n, UEI_LOOKBACK_DAYS_MAX)
}

/**
 * @param {number} [nowMs=Date.now()] — UTC epoch ms for range end when finite
 * @param {number} [days=UEI_LOOKBACK_DAYS_DEFAULT] — whole-day count only (finite positive integers); each day is 24h wall in UTC ms
 * @returns {{ rangeStart: Date; rangeEnd: Date }}
 */
export function createLookbackDateRange(nowMs = Date.now(), days = UEI_LOOKBACK_DAYS_DEFAULT) {
  const endMs = Number.isFinite(nowMs) ? nowMs : Date.now()
  let safeDays =
    Number.isFinite(days) && Number.isInteger(days) && days > 0 ? days : UEI_LOOKBACK_DAYS_DEFAULT
  safeDays = Math.min(safeDays, UEI_LOOKBACK_DAYS_MAX)
  const rangeEnd = new Date(endMs)
  const rangeStart = new Date(endMs - safeDays * 24 * 60 * 60 * 1000)
  return { rangeStart, rangeEnd }
}

/**
 * Structured payload subset for the NFR-3 / interim 10% guard-rail fallback log.
 * Excludes the top-level `event` field, which is added by {@link emitUeiLookbackFallbackLog}.
 * @param {string} userId
 * @param {number} fellBackTo - expected 45 per §A1
 * @param {UeiLookbackFallbackReason | string} reason — {@link emitUeiLookbackFallbackLog} rejects values outside {@link UEI_LOOKBACK_FALLBACK_REASONS}
 * @returns {{ userId: string, fellBackTo: number, reason: string }} payload fields only; excludes `event`
 */
export function buildUeiLookbackFallbackPayload(userId, fellBackTo, reason) {
  return { userId, fellBackTo, reason }
}

/**
 * Emit a single structured JSON log line (Cloud Logging–friendly).
 * @param {string} userId
 * @param {number} fellBackTo
 * @param {UeiLookbackFallbackReason} reason
 * @param {{ log?: (s: string) => void }} [opts]
 */
export function emitUeiLookbackFallbackLog(userId, fellBackTo, reason, opts = {}) {
  if (!UEI_LOOKBACK_FALLBACK_REASONS.includes(reason)) {
    throw new Error(`Invalid UEI lookback fallback reason: ${String(reason)}`)
  }
  const log = opts.log ?? console.log
  const payload = {
    event: 'uei_lookback_fallback',
    ...buildUeiLookbackFallbackPayload(userId, fellBackTo, reason),
  }
  log(JSON.stringify(payload))
}

/**
 * Recommend fallback when a 60-day LOOKBACK cannot complete within policy.
 * Call from metadata ingestion (or equivalent) when aborting the wider window.
 *
 * @param {object} input
 * @param {number} input.lookbackDaysRequested
 * @param {boolean} input.syncComplete — only `true` skips fallback (truthy non-booleans ignored)
 * @param {number} input.elapsedMs — must be finite when evaluating operational window
 * @param {number} input.operationalBudgetMs — max wall time for the 60-day attempt;
 *   must be finite and **> 0** for `operational_window_exceeded`; `0`, negative, NaN,
 *   or omission skips operational fallback (signals only; caller provides budget when applicable)
 * @param {boolean} [input.gmailQuotaExceeded] — only literal `true` counts
 * @param {boolean} [input.sustainedGmailRateLimit] — only literal `true` counts
 * @param {boolean} [input.batchProcessingBlocked] — only literal `true` counts
 * @returns {UeiLookbackFallbackReason | null} reason to fall back, or null if no fallback
 */
export function resolveUeiLookbackFallbackReason(input) {
  const {
    lookbackDaysRequested,
    syncComplete,
    elapsedMs,
    operationalBudgetMs,
    gmailQuotaExceeded = false,
    sustainedGmailRateLimit = false,
    batchProcessingBlocked = false,
  } = input

  if (lookbackDaysRequested !== UEI_LOOKBACK_DAYS_DEFAULT) {
    return null
  }
  if (syncComplete === true) {
    return null
  }
  if (gmailQuotaExceeded === true) {
    return 'gmail_quota'
  }
  if (sustainedGmailRateLimit === true) {
    return 'gmail_rate_limit'
  }
  if (batchProcessingBlocked === true) {
    return 'batch_processing_constraint'
  }
  const budgetOk =
    Number.isFinite(operationalBudgetMs) && Number.isFinite(elapsedMs) && operationalBudgetMs > 0
  if (budgetOk && elapsedMs >= operationalBudgetMs) {
    return 'operational_window_exceeded'
  }
  return null
}
