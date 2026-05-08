/**
 * POST helper for Brand Contacts backfill → backend /api/v1/ingestion/trigger.
 * Mirrors postFallbackReattempt (run-fallback-reattempt-pipeline.js) shape but with
 * the backfill body: { userId, syncStrategy, origin, attributionTag, dryRun }.
 * Does NOT send lookbackDaysOverride — backend defaults to 60 days via INITIAL_LOOKBACK_DATE_RANGE_DAYS.
 */

function normalizeBaseUrl(url) {
  return String(url ?? '').replace(/\/+$/, '')
}

/**
 * @param {{ backendBaseUrl: string, sharedSecret: string, userId: string, attributionTag: string, dryRun: boolean, extraHeaders?: Record<string, string> }} opts
 * @returns {Promise<{ ok: boolean, status: number, alreadyInProgress: boolean, text?: string }>}
 */
export async function postBackfillIngestionTrigger({
  backendBaseUrl,
  sharedSecret,
  userId,
  attributionTag,
  dryRun,
  extraHeaders = {},
}) {
  const url = `${normalizeBaseUrl(backendBaseUrl)}/api/v1/ingestion/trigger`
  const body = {
    userId,
    syncStrategy: 'LOOKBACK',
    origin: 'backfill',
    attributionTag,
    dryRun,
  }

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-shared-secret': sharedSecret,
      ...extraHeaders,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  })

  if (resp.status === 409) {
    await resp.body?.cancel().catch(() => {})
    return { ok: true, status: 409, alreadyInProgress: true }
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => '<unreadable>')
    return { ok: false, status: resp.status, alreadyInProgress: false, text }
  }

  await resp.body?.cancel().catch(() => {})
  return { ok: true, status: resp.status, alreadyInProgress: false }
}
