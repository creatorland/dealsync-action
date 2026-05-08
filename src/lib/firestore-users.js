/**
 * Firestore REST helpers for Brand Contacts backfill: tier-eligibility pagination + legacy token presence check.
 * Uses the same service-account JWT from makeGoogleDatastoreTokenProvider (scan-complete.js).
 */

import * as core from '@actions/core'
import { backoffMs, sleep } from './retry.js'

const FIRESTORE_BASE = 'https://firestore.googleapis.com/v1'

// Retry policy for transient network failures on Firestore REST GETs:
// 3 attempts (initial + 2 retries), exponential backoff 500ms → 2s.
// Sized for sub-5s p99 wall-clock per user under typical Firestore latency,
// keeping the per-batch fan-out within the action's 30-min job timeout
// even at concurrency=5 with worst-case retries on every user.
const TOKEN_CHECK_MAX_ATTEMPTS = 3
const TOKEN_CHECK_BACKOFF_BASE_MS = 500
const TOKEN_CHECK_BACKOFF_MAX_MS = 2000

/**
 * Async generator yielding pages of tier-eligible users from Firestore REST runQuery.
 * Filter: permissionTier.tier == 'readonly' (single equality — does NOT require a
 * composite index). The caller applies in-memory filters for `tierRevokedAt`,
 * `backfillCircuitBrokenAt`, and `backfillDispatchedAt`.
 *
 * Why single-equality only on the wire: a composite filter combining
 * `tier == 'readonly'` AND `tierRevokedAt IS_NULL` requires a Firestore composite
 * index `(permissionTier.tier, permissionTier.tierRevokedAt, __name__)`, which is
 * a deploy-time gate (must be provisioned via Firestore Console / Terraform before
 * the first run, otherwise the API returns FAILED_PRECONDITION). Filtering
 * `tierRevokedAt` in-memory eliminates the index dependency at the cost of a
 * larger raw fetch — `revokedRate` of the readonly cohort scales the over-fetch.
 * For the BC backlog of ~4,116 users with very-low revoke rate this is trivial.
 *
 * Orders by __name__ for stable cursor pagination.
 *
 * Pages are fetched in chunks of `max(batchSize * 4, 200)` raw documents; the
 * caller is responsible for `break`ing once it has accumulated enough eligible
 * candidates. The generator paginates until Firestore returns a short page
 * (natural end).
 *
 * @param {{ tokenProvider: () => Promise<string>, gcpProjectId: string, batchSize: number }} opts
 * @yields {{ userId: string, permissionTier: object }[]}
 */
export async function* paginateTierEligibleUsers({ tokenProvider, gcpProjectId, batchSize }) {
  const fetchChunk = Math.max(batchSize * 4, 200)
  let startAfter = null

  while (true) {
    const accessToken = await tokenProvider()
    const url = `${FIRESTORE_BASE}/projects/${encodeURIComponent(gcpProjectId)}/databases/(default)/documents:runQuery`

    const structuredQuery = {
      from: [{ collectionId: 'users' }],
      where: {
        fieldFilter: {
          field: { fieldPath: 'permissionTier.tier' },
          op: 'EQUAL',
          value: { stringValue: 'readonly' },
        },
      },
      orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
      // Project only the field we actually read so Firestore stops shipping the
      // entire user document (email, settings, OAuth metadata, etc.) over the wire
      // to the runner. Document name (__name__) is always returned regardless.
      select: { fields: [{ fieldPath: 'permissionTier' }] },
      limit: fetchChunk,
    }

    if (startAfter) {
      structuredQuery.startAt = {
        values: [{ referenceValue: startAfter }],
        before: false,
      }
    }

    let resp
    try {
      resp = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ structuredQuery }),
        signal: AbortSignal.timeout(60_000),
      })
    } catch (err) {
      // Network-level failure (DNS, connection refused, AbortSignal timeout).
      // Without this catch the unhandled rejection crashes the generator AND
      // the caller's for-await loop with no actionable diagnostic context.
      // Re-throw with the URL + cursor position so triage can correlate to
      // exactly which page failed mid-pagination.
      throw new Error(
        `Firestore runQuery network failure (startAfter=${startAfter ?? '<none>'}): ${err.message}`,
      )
    }

    if (!resp.ok) {
      const text = await resp.text().catch(() => '<unreadable>')
      throw new Error(`Firestore runQuery ${resp.status}: ${text.slice(0, 500)}`)
    }

    const results = await resp.json()
    if (!Array.isArray(results) || results.length === 0) break

    const docs = results.filter((r) => r.document).map((r) => r.document)

    if (docs.length === 0) break

    const page = []
    for (const doc of docs) {
      const userId = doc.name.split('/').pop()
      const permissionTier = extractPermissionTier(doc)
      if (!permissionTier) {
        core.error(
          `[brand-contacts-backfill] user=${userId} missing permissionTier field group — migration gap`,
        )
        throw new Error(
          `User ${userId} lacks permissionTier field group; Story 1.5 migration must land first`,
        )
      }
      page.push({ userId, permissionTier })
    }

    yield page

    const lastDoc = docs[docs.length - 1]
    startAfter = lastDoc.name

    if (docs.length < fetchChunk) break
  }
}

/**
 * Extract permissionTier fields from a Firestore document.
 * @param {object} doc
 * @returns {{ tier: string, tierRevokedAt: string|null, backfillCircuitBrokenAt: string|null, backfillDispatchedAt: string|null } | null}
 */
function extractPermissionTier(doc) {
  const fields = doc?.fields?.permissionTier?.mapValue?.fields
  if (!fields) return null

  const tier = fields.tier?.stringValue
  if (!tier) return null

  const tierRevokedAt = fields.tierRevokedAt?.timestampValue ?? null
  const backfillCircuitBrokenAt = fields.backfillCircuitBrokenAt?.timestampValue ?? null
  const backfillDispatchedAt = fields.backfillDispatchedAt?.timestampValue ?? null

  return { tier, tierRevokedAt, backfillCircuitBrokenAt, backfillDispatchedAt }
}

/**
 * Batched token-presence check via Firestore REST `:batchGet`. Returns a Map
 * keyed by userId with `{ present: boolean }` for each entry. Mask is pinned
 * to `__name__` so Firestore returns only document references — NEVER the
 * plaintext OAuth token contents.
 *
 * Wraps the same retry policy as the single-user `checkLegacyTokenPresence`.
 * On retry exhaustion: throws (does NOT degrade to per-user fallback —
 * caller decides how to handle batch failure; default behavior is to mark
 * the whole batch as `dispatchFailedTokenCheck` and continue with the next
 * user-set on the next cron firing).
 *
 * Firestore batchGet limits to 500 documents per request; caller is
 * responsible for chunking when batchSize exceeds this. For Brand Contacts
 * the daily batch is 75 users, well below the limit.
 *
 * @param {{ tokenProvider: () => Promise<string>, gcpProjectId: string, userIds: string[] }} opts
 * @returns {Promise<Map<string, { present: boolean }>>}
 */
export async function batchCheckLegacyTokenPresence({ tokenProvider, gcpProjectId, userIds }) {
  if (userIds.length === 0) return new Map()

  const databasePath = `projects/${encodeURIComponent(gcpProjectId)}/databases/(default)`
  const url = `${FIRESTORE_BASE}/${databasePath}/documents:batchGet`
  const documents = userIds.map(
    (userId) =>
      `${databasePath}/documents/users-sensitive-data/${encodeURIComponent(userId)}/oauth-token/youtube`,
  )

  let lastErr
  for (let attempt = 0; attempt < TOKEN_CHECK_MAX_ATTEMPTS; attempt++) {
    if (attempt > 0) {
      await sleep(
        backoffMs(attempt - 1, {
          base: TOKEN_CHECK_BACKOFF_BASE_MS,
          max: TOKEN_CHECK_BACKOFF_MAX_MS,
          jitter: true,
        }),
      )
    }

    let resp
    try {
      const accessToken = await tokenProvider()
      resp = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          documents,
          mask: { fieldPaths: ['__name__'] },
        }),
        signal: AbortSignal.timeout(60_000),
      })
    } catch (err) {
      lastErr = new Error(`Firestore batchGet network failure: ${err.message}`)
      continue
    }

    if (resp.ok) {
      const results = await resp.json()
      const out = new Map()
      // Firestore batchGet returns one entry per requested document, in order.
      // Each entry is either `{ found: { name, fields }, ... }` (present) or
      // `{ missing: <doc-name>, ... }` (absent — the 404 equivalent).
      // Document order matches the request, so we can pair by index.
      for (let i = 0; i < results.length; i++) {
        const entry = results[i]
        const userId = userIds[i]
        const present = entry?.found != null
        out.set(userId, { present })
      }
      return out
    }

    const text = await resp.text().catch(() => '<unreadable>')
    const truncated = text.slice(0, 500)
    lastErr = new Error(`Firestore batchGet ${resp.status}: ${truncated}`)
    if (resp.status < 500) break
  }

  throw lastErr
}

/**
 * Check whether the legacy plaintext OAuth token document exists at
 * users-sensitive-data/{userId}/oauth-token/youtube.
 * Treats 404 as absent (not an error). Read-only.
 *
 * Uses `mask.fieldPaths=__name__` so Firestore returns only the document name —
 * NEVER the plaintext OAuth token contents (`refreshToken`, `scope`). BC11.1's
 * legacy plaintext path must stay server-side; the W3 caller only needs to know
 * the document exists so `core-email-metadata-ingestion` can fetch it itself.
 *
 * Retries on transient network failures + Firestore 5xx (3 attempts total,
 * exponential backoff 500ms → 2s). 4xx (other than 404) and the 404-as-absent
 * fast-path do NOT retry — they are deterministic outcomes. Without retry, a
 * single dropped TCP connection drops the user from this run and (combined with
 * the persistent dispatch marker) means the user has to wait for the NEXT
 * cron firing's natural retry — slower than necessary for transient blips.
 *
 * @param {{ tokenProvider: () => Promise<string>, gcpProjectId: string, userId: string }} opts
 * @returns {Promise<{ present: boolean }>}
 */
export async function checkLegacyTokenPresence({ tokenProvider, gcpProjectId, userId }) {
  const path = `projects/${encodeURIComponent(gcpProjectId)}/databases/(default)/documents/users-sensitive-data/${encodeURIComponent(userId)}/oauth-token/youtube`
  const url = new URL(`${FIRESTORE_BASE}/${path}`)
  url.searchParams.set('mask.fieldPaths', '__name__')

  let lastErr
  for (let attempt = 0; attempt < TOKEN_CHECK_MAX_ATTEMPTS; attempt++) {
    if (attempt > 0) {
      await sleep(
        backoffMs(attempt - 1, {
          base: TOKEN_CHECK_BACKOFF_BASE_MS,
          max: TOKEN_CHECK_BACKOFF_MAX_MS,
          jitter: true,
        }),
      )
    }

    let resp
    try {
      const accessToken = await tokenProvider()
      resp = await fetch(url.toString(), {
        method: 'GET',
        headers: { Authorization: `Bearer ${accessToken}` },
        signal: AbortSignal.timeout(30_000),
      })
    } catch (err) {
      // Network-level failure (DNS, connection refused, AbortSignal timeout).
      // These are exactly the transient cases retry exists for.
      lastErr = new Error(
        `Firestore token check network failure for userId=${userId}: ${err.message}`,
      )
      continue
    }

    if (resp.status === 404) {
      await resp.body?.cancel().catch(() => {})
      return { present: false }
    }

    if (resp.ok) {
      await resp.body?.cancel().catch(() => {})
      return { present: true }
    }

    // Non-2xx, non-404 response. Retry only on 5xx (transient); 4xx is the
    // caller's fault (auth, permissions, malformed request) — don't burn retries.
    const text = await resp.text().catch(() => '<unreadable>')
    const truncated = text.slice(0, 500)
    lastErr = new Error(`Firestore token check ${resp.status} for userId=${userId}: ${truncated}`)
    if (resp.status < 500) break
  }

  throw lastErr
}

/**
 * Persist `permissionTier.backfillDispatchedAt = <REQUEST_TIME>` on the user doc
 * via Firestore REST `:commit` with a `setToServerValue: REQUEST_TIME` transform.
 * Server-time avoids per-runner clock-skew drift across the 4,116-user campaign.
 *
 * Required for backfill correctness (NFR-1): without this marker, the read-side
 * tier-eligibility query (`tier === 'readonly' AND tierRevokedAt IS NULL`)
 * keeps returning already-dispatched users every cron firing, starving the
 * orchestrator's daily candidate pool and re-dispatching the same first-N users
 * every day forever. The orchestrator's read-side filter excludes users with
 * `backfillDispatchedAt != null`.
 *
 * Idempotent at the application layer — caller only invokes after a successful
 * dispatch (HTTP 200/201). 409 (already-in-progress) does NOT mark; the prior
 * dispatcher's caller owns the eventual mark on its 200 response.
 *
 * @param {{ tokenProvider: () => Promise<string>, gcpProjectId: string, userId: string }} opts
 * @returns {Promise<void>}
 */
export async function markBackfillDispatched({ tokenProvider, gcpProjectId, userId }) {
  const accessToken = await tokenProvider()
  const docName = `projects/${encodeURIComponent(gcpProjectId)}/databases/(default)/documents/users/${encodeURIComponent(userId)}`
  const url = `${FIRESTORE_BASE}/projects/${encodeURIComponent(gcpProjectId)}/databases/(default)/documents:commit`

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      writes: [
        {
          transform: {
            document: docName,
            fieldTransforms: [
              {
                fieldPath: 'permissionTier.backfillDispatchedAt',
                setToServerValue: 'REQUEST_TIME',
              },
            ],
          },
        },
      ],
    }),
    signal: AbortSignal.timeout(30_000),
  })

  if (!resp.ok) {
    const text = await resp.text().catch(() => '<unreadable>')
    throw new Error(
      `Firestore mark-dispatched ${resp.status} for userId=${userId}: ${text.slice(0, 500)}`,
    )
  }

  await resp.body?.cancel().catch(() => {})
}
