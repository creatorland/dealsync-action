// Read-only eligibility query for scan_complete webhook (first completed LOOKBACK only).
// Uses CTEs — unlike row-level builders in deal-states.js — OK for this analytics-style read.
// Parity: backend/src/services/dealsync-v2.sync.service.ts (getSyncStatus, isFirstCompletedSync)
// Plan: docs/plans/2026-04-16-scan-complete-w3-cron-tech-spec.md

import { sanitizeSchema } from './sanitize.js'

export const scanCompleteEligibility = {
  /**
   * @param {string} emailCoreSchema
   * @param {string} dealsyncSchema
   * @returns {string}
   */
  selectEligibleUsers(emailCoreSchema, dealsyncSchema) {
    const ec = sanitizeSchema(emailCoreSchema)
    const ds = sanitizeSchema(dealsyncSchema)
    return `WITH latest_sync AS (
  SELECT sync_state_id, user_id, created_at, sync_strategy
  FROM (
    SELECT
      id AS sync_state_id,
      user_id,
      created_at,
      sync_strategy,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
    FROM ${ec}.sync_states
  ) ranked
  WHERE rn = 1
),
status_inputs AS (
  SELECT
    ls.user_id,
    ls.sync_state_id,
    ls.created_at AS initiated_at,
    (SELECT COUNT(*) FROM ${ec}.email_metadata em WHERE em.user_id = ls.user_id) AS total_messages,
    COALESCE((
      SELECT SUM(
        CASE WHEN ds.status NOT IN ('pending','filtering','pending_classification','classifying')
          THEN 1 ELSE 0 END
      )
      FROM ${ds}.deal_states ds
      WHERE ds.user_id = ls.user_id
    ), 0) AS processed_messages
  FROM latest_sync ls
  WHERE ls.sync_strategy = 'LOOKBACK'
),
completed_candidates AS (
  SELECT *
  FROM status_inputs
  WHERE initiated_at IS NOT NULL
    AND processed_messages >= total_messages
),
prior_lookback_success AS (
  SELECT ss.user_id, ss.created_at
  FROM ${ec}.sync_states ss
  WHERE ss.sync_strategy = 'LOOKBACK'
    AND EXISTS (
      SELECT 1
      FROM ${ec}.sync_events se_s
      WHERE se_s.sync_state_id = ss.id
        AND se_s.event IN ('metadata_ingestion_end','content_ingestion_end','completed')
        AND se_s.created_at = (
          SELECT MAX(se2.created_at)
          FROM ${ec}.sync_events se2
          WHERE se2.sync_state_id = ss.id
            AND se2.event IN ('metadata_ingestion_end','content_ingestion_end','completed')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM ${ec}.sync_events se_f
          WHERE se_f.sync_state_id = ss.id
            AND se_f.event IN ('metadata_ingestion_failed','content_ingestion_failed')
            AND se_f.created_at > se_s.created_at
        )
    )
),
eligible AS (
  SELECT c.*
  FROM completed_candidates c
  WHERE NOT EXISTS (
    SELECT 1
    FROM prior_lookback_success pls
    WHERE pls.user_id = c.user_id
      AND pls.created_at < c.initiated_at
  )
),
deal_agg AS (
  SELECT
    e.user_id,
    SUM(CASE WHEN d.category = 'new' THEN 1 ELSE 0 END) AS db_new,
    SUM(CASE WHEN d.category = 'in_progress' THEN 1 ELSE 0 END) AS db_in_progress,
    SUM(CASE WHEN d.category = 'completed' THEN 1 ELSE 0 END) AS db_completed,
    SUM(CASE WHEN d.category = 'not_interested' THEN 1 ELSE 0 END) AS db_not_interested,
    SUM(CASE WHEN d.category = 'likely_scam' THEN 1 ELSE 0 END) AS db_likely_scam,
    SUM(CASE WHEN d.category = 'low_confidence' THEN 1 ELSE 0 END) AS db_low_confidence
  FROM eligible e
  LEFT JOIN ${ds}.deals d
    ON d.user_id = e.user_id AND d.updated_at >= e.initiated_at
  GROUP BY e.user_id
),
contact_agg AS (
  SELECT
    e.user_id,
    COUNT(DISTINCT c.email) AS contacts_added
  FROM eligible e
  INNER JOIN ${ds}.deal_contacts dc
    ON dc.user_id = e.user_id
  INNER JOIN ${ds}.deals d
    ON d.id = dc.deal_id AND d.user_id = e.user_id AND d.updated_at >= e.initiated_at
  INNER JOIN ${ec}.contacts c
    ON c.user_id = dc.user_id AND c.email = dc.email
  GROUP BY e.user_id
)
SELECT
  e.user_id,
  e.initiated_at,
  COALESCE(da.db_new, 0) AS db_new,
  COALESCE(da.db_in_progress, 0) AS db_in_progress,
  COALESCE(da.db_completed, 0) AS db_completed,
  COALESCE(da.db_not_interested, 0) AS db_not_interested,
  COALESCE(da.db_likely_scam, 0) AS db_likely_scam,
  COALESCE(da.db_low_confidence, 0) AS db_low_confidence,
  COALESCE(ca.contacts_added, 0) AS contacts_added
FROM eligible e
LEFT JOIN deal_agg da ON da.user_id = e.user_id
LEFT JOIN contact_agg ca ON ca.user_id = e.user_id
ORDER BY e.initiated_at DESC`
  },
}
