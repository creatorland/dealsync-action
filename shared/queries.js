/**
 * Shared SQL queries for Dealsync W3 workflow actions.
 *
 * All queries target DEAL_STATES (not EMAIL_METADATA).
 * All column names UPPERCASE (SxT convention).
 * Schema is passed as a parameter — never hardcoded.
 * IDs must be sanitized before interpolation.
 *
 * Status-based state machine:
 *   pending → filtering → pending_classification → classifying → deal | not_deal
 *   filtering → filter_rejected (terminal)
 *   filtering/classifying → retry via attempts increment
 */

// ============================================================
// STATUS CONSTANTS
// ============================================================

export const STATUS = {
  PENDING: 'pending',
  FILTERING: 'filtering',
  PENDING_CLASSIFICATION: 'pending_classification',
  CLASSIFYING: 'classifying',
  DEAL: 'deal',
  NOT_DEAL: 'not_deal',
  FILTER_REJECTED: 'filter_rejected',
}

// ============================================================
// ORCHESTRATOR QUERIES
// ============================================================

export const orchestrator = {
  /** Count deal_states at each status for concurrency and pending checks */
  checkConcurrency: (schema) =>
    `SELECT
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.FILTERING}') AS ACTIVE_FILTER,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.CLASSIFYING}') AS ACTIVE_CLASSIFY,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}') AS PENDING_FILTER,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING_CLASSIFICATION}') AS PENDING_CLASSIFY`,

  /** Reset stale filtering back to pending, stale classifying back to pending_classification (with attempts) */
  expireStale: (schema, minutes = 10, maxAttempts = 3) =>
    `UPDATE ${schema}.DEAL_STATES SET
      STATUS = CASE
        WHEN STATUS = '${STATUS.FILTERING}' THEN '${STATUS.PENDING}'
        WHEN STATUS = '${STATUS.CLASSIFYING}' THEN '${STATUS.PENDING_CLASSIFICATION}'
      END,
      ATTEMPTS = ATTEMPTS + 1,
      BATCH_ID = NULL
    WHERE STATUS IN ('${STATUS.FILTERING}', '${STATUS.CLASSIFYING}')
    AND ATTEMPTS < ${maxAttempts}
    AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${minutes}' MINUTE`,
}

// ============================================================
// DISPATCH QUERIES
// ============================================================

export const dispatch = {
  /** Atomically claim pending deal_states into filtering with a trigger hash */
  claimFilterBatch: (schema, batchId, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', BATCH_ID = '${batchId}'
    WHERE EMAIL_METADATA_ID IN (
      SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize}
    )`,

  /** Atomically claim pending_classification deal_states into classifying (sync-level + thread-level guard) */
  claimClassifyBatch: (schema, batchId, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}'
    WHERE EMAIL_METADATA_ID IN (
      SELECT ds.EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES ds
      WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}'
        AND NOT EXISTS (
          SELECT 1 FROM ${schema}.DEAL_STATES ds2
          WHERE ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
            AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')
        )
      LIMIT ${batchSize}
    )`,

  /** Count deal_states claimed by a trigger hash (verify claim) */
  countClaimed: (schema, batchId) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,

  /** Reset claimed deal_states back to original status on trigger failure */
  resetClaimed: (schema, batchId, resetStatus) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${resetStatus}', BATCH_ID = NULL WHERE BATCH_ID = '${batchId}'`,
}

// ============================================================
// FILTER PROCESSOR QUERIES
// ============================================================

export const filter = {
  /** Fetch deal_states claimed by a trigger hash for filtering */
  fetchBatch: (schema, batchId) =>
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID
    FROM ${schema}.DEAL_STATES
    WHERE BATCH_ID = '${batchId}'`,

  /** Move filtered deal_states to pending_classification */
  updateFiltered: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.PENDING_CLASSIFICATION}', BATCH_ID = NULL, ATTEMPTS = 0 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move rejected deal_states to filter_rejected */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTER_REJECTED}', BATCH_ID = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Fetch deal_states + AI context for detection */
  fetchBatchWithContext: (schema, batchId) =>
    `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID,
      latest_eval.AI_SUMMARY AS PREVIOUS_AI_SUMMARY,
      d.ID AS EXISTING_DEAL_ID
    FROM ${schema}.DEAL_STATES ds
    LEFT JOIN (
      SELECT THREAD_ID, AI_SUMMARY,
        ROW_NUMBER() OVER (PARTITION BY THREAD_ID ORDER BY UPDATED_AT DESC) AS RN
      FROM ${schema}.EMAIL_THREAD_EVALUATIONS
    ) latest_eval ON latest_eval.THREAD_ID = ds.THREAD_ID AND latest_eval.RN = 1
    LEFT JOIN ${schema}.DEALS d ON d.THREAD_ID = ds.THREAD_ID AND d.USER_ID = ds.USER_ID
    WHERE ds.BATCH_ID = '${batchId}'`,

  /** Move deal deal_states to deal status */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.DEAL}', BATCH_ID = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-deal deal_states to not_deal status */
  updateNotDeal: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.NOT_DEAL}', BATCH_ID = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// SAVE RESULTS QUERIES (detection pipeline DML)
// ============================================================

export const saveResults = {
  insertAudit: (
    schema,
    { id, threadCount, emailCount, cost, inputTokens, outputTokens, model, evaluation },
  ) =>
    `INSERT INTO ${schema}.AI_EVALUATION_AUDITS
      (ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT)
    VALUES
      ('${id}', ${threadCount}, ${emailCount}, ${cost}, ${inputTokens}, ${outputTokens}, '${model}', '${evaluation}', CURRENT_TIMESTAMP)`,

  deleteThreadEvaluation: (schema, threadId) =>
    `DELETE FROM ${schema}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID = '${threadId}'`,

  insertThreadEvaluation: (
    schema,
    { id, threadId, auditId, category, summary, isDeal, likelyScam, score },
  ) =>
    `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
      (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${threadId}', '${auditId}', '${category}', '${summary}', ${isDeal}, ${likelyScam}, ${score}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete deal by thread_id (one deal per thread) */
  deleteDeal: (schema, threadId) =>
    `DELETE FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}'`,

  insertDeal: (
    schema,
    { id, userId, threadId, evalId, dealName, dealType, category, value, currency, brand },
  ) =>
    `INSERT INTO ${schema}.DEALS
      (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${userId}', '${threadId}', '${evalId}', '${dealName}', '${dealType}', '${category}', ${value}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete deal contacts by deal_id */
  deleteDealContact: (schema, dealId) =>
    `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID = '${dealId}'`,

  /** Insert deal contact using email directly (no contact_id FK) */
  insertDealContact: (schema, { id, dealId, contactEmail }) =>
    `INSERT INTO ${schema}.DEAL_CONTACTS
      (ID, DEAL_ID, CONTACT_EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${dealId}', '${contactEmail}', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
}

// ============================================================
// FINALIZE QUERIES
// ============================================================

export const finalize = {
  /** Reset any deal_states still claimed by a trigger hash back to their pre-claim status */
  resetLeftovers: (schema, batchId, resetStatus) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${resetStatus}', BATCH_ID = NULL WHERE BATCH_ID = '${batchId}'`,
}

// ============================================================
// WORKFLOW TRIGGERS QUERIES
// ============================================================

export const workflowTriggers = {
  /** Fetch current workflow_triggers for all deal_states claimed by a trigger hash */
  fetchByBatchId: (schema, batchId) =>
    `SELECT EMAIL_METADATA_ID, WORKFLOW_TRIGGERS FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,

  /** Update workflow_triggers for a single deal_state */
  update: (schema, emailMetadataId, serializedJson) =>
    `UPDATE ${schema}.DEAL_STATES SET WORKFLOW_TRIGGERS = '${serializedJson}' WHERE EMAIL_METADATA_ID = '${emailMetadataId}'`,
}

// ============================================================
// UTILITIES
// ============================================================

export function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

export function sanitizeString(s) {
  return (s || '').replace(/'/g, "''")
}

export function toSqlIdList(ids) {
  return ids.map((id) => `'${sanitizeId(id)}'`).join(',')
}

export function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}
