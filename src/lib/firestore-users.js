/**
 * Firestore REST helpers for Brand Contacts backfill: tier-eligibility pagination + legacy token presence check.
 * Uses the same service-account JWT from makeGoogleDatastoreTokenProvider (scan-complete.js).
 */

import * as core from '@actions/core'

const FIRESTORE_BASE = 'https://firestore.googleapis.com/v1'

/**
 * Async generator yielding pages of tier-eligible users from Firestore REST runQuery.
 * Filter: permissionTier.tier == 'readonly' AND permissionTier.tierRevokedAt == null.
 * Orders by __name__ for stable cursor pagination.
 *
 * Pages are fetched in chunks of `max(batchSize * 4, 200)` raw documents; the caller
 * applies the in-memory `backfillCircuitBrokenAt` filter and is responsible for
 * `break`ing once it has accumulated enough eligible candidates. The generator paginates
 * until Firestore returns a short page (natural end). This avoids the prior cap on raw
 * page size that could starve the caller of eligible candidates when revoked /
 * circuit-broken users dominate a single page.
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
        compositeFilter: {
          op: 'AND',
          filters: [
            {
              fieldFilter: {
                field: { fieldPath: 'permissionTier.tier' },
                op: 'EQUAL',
                value: { stringValue: 'readonly' },
              },
            },
            {
              unaryFilter: {
                field: { fieldPath: 'permissionTier.tierRevokedAt' },
                op: 'IS_NULL',
              },
            },
          ],
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

    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ structuredQuery }),
      signal: AbortSignal.timeout(60_000),
    })

    if (!resp.ok) {
      const text = await resp.text().catch(() => '<unreadable>')
      throw new Error(`Firestore runQuery ${resp.status}: ${text}`)
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
 * @returns {{ tier: string, tierRevokedAt: string|null, backfillCircuitBrokenAt: string|null } | null}
 */
function extractPermissionTier(doc) {
  const fields = doc?.fields?.permissionTier?.mapValue?.fields
  if (!fields) return null

  const tier = fields.tier?.stringValue
  if (!tier) return null

  const tierRevokedAt = fields.tierRevokedAt?.stringValue ?? null
  const backfillCircuitBrokenAt = fields.backfillCircuitBrokenAt?.stringValue ?? null

  return { tier, tierRevokedAt, backfillCircuitBrokenAt }
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
 * @param {{ tokenProvider: () => Promise<string>, gcpProjectId: string, userId: string }} opts
 * @returns {Promise<{ present: boolean }>}
 */
export async function checkLegacyTokenPresence({ tokenProvider, gcpProjectId, userId }) {
  const accessToken = await tokenProvider()
  const path = `projects/${encodeURIComponent(gcpProjectId)}/databases/(default)/documents/users-sensitive-data/${encodeURIComponent(userId)}/oauth-token/youtube`
  const url = new URL(`${FIRESTORE_BASE}/${path}`)
  url.searchParams.set('mask.fieldPaths', '__name__')

  const resp = await fetch(url.toString(), {
    method: 'GET',
    headers: { Authorization: `Bearer ${accessToken}` },
    signal: AbortSignal.timeout(30_000),
  })

  if (resp.status === 404) {
    await resp.body?.cancel().catch(() => {})
    return { present: false }
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => '<unreadable>')
    throw new Error(`Firestore token check ${resp.status} for userId=${userId}: ${text}`)
  }

  await resp.body?.cancel().catch(() => {})
  return { present: true }
}
