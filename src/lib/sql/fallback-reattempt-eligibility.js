// Read-only eligibility query for the §A1 / NFR-3 fallback re-attempt sweep
// (creatorland/dealsync-v2#522, Phase 3). Identifies failed 60-day LOOKBACK
// sync_states whose `fallback_reason` was persisted by core-email-metadata-ingestion
// but which haven't yet had a 45-day successor row created (either because the
// inline trigger from metadataFetcher → backend failed, or the inline trigger
// is disabled in this environment).
//
// One-shot guarantee: the query excludes rows that already appear as
// `originating_sync_state_id` on a successor row, so a failed 45-day attempt
// never triggers a second re-attempt at a narrower window.
//
// Bounded history: rows older than 7 days are excluded so the sweep doesn't
// keep re-attempting ancient failures whose users may have re-granted by now
// (or whose context has otherwise gone stale).

import { sanitizeSchema } from './sanitize.js'

export const fallbackReattemptEligibility = {
  /**
   * @param {string} emailCoreSchema
   * @param {number} [batchSize=200]
   * @returns {string}
   */
  selectUnreattemptedFallbacks(emailCoreSchema, batchSize = 200) {
    const ec = sanitizeSchema(emailCoreSchema)
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new Error(`batchSize must be a positive integer: ${batchSize}`)
    }
    // 7 days = 10080 minutes. Using MINUTE here because that's the unit
    // proven against SxT in dealsync-action's deal-states.js (which executes
    // INTERVAL 'N' MINUTE in production); the DAY unit is standard SQL but
    // not exercised against this SxT instance, so picking the safer form.
    const sevenDaysInMinutes = 7 * 24 * 60
    return `SELECT
  ss.id AS sync_state_id,
  ss.user_id,
  ss.fallback_reason,
  ss.created_at AS originating_created_at
FROM ${ec}.sync_states ss
WHERE ss.sync_strategy = 'LOOKBACK'
  AND ss.status = 'failed'
  AND ss.fallback_reason IS NOT NULL
  AND ss.originating_sync_state_id IS NULL
  AND ss.created_at >= CURRENT_TIMESTAMP - INTERVAL '${sevenDaysInMinutes}' MINUTE
  AND NOT EXISTS (
    SELECT 1
    FROM ${ec}.sync_states succ
    WHERE succ.originating_sync_state_id = ss.id
  )
ORDER BY ss.created_at ASC
LIMIT ${batchSize}`
  },
}
