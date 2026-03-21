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
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.CLASSIFYING}') AS ACTIVE_DETECT,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}') AS PENDING_FILTER,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING_CLASSIFICATION}') AS PENDING_DETECT`,

  /** Reset stale filtering back to pending, stale classifying back to pending_classification (with attempts) */
  expireStale: (schema, minutes = 10, maxAttempts = 3) =>
    `UPDATE ${schema}.DEAL_STATES SET
      STATUS = CASE
        WHEN STATUS = '${STATUS.FILTERING}' AND ATTEMPTS < ${maxAttempts} THEN '${STATUS.PENDING}'
        WHEN STATUS = '${STATUS.CLASSIFYING}' AND ATTEMPTS < ${maxAttempts} THEN '${STATUS.PENDING_CLASSIFICATION}'
        ELSE STATUS
      END,
      ATTEMPTS = ATTEMPTS + 1,
      ACTIVE_TRIGGER_HASH = NULL
    WHERE STATUS IN ('${STATUS.FILTERING}', '${STATUS.CLASSIFYING}')
    AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${minutes}' MINUTE`,
}

// ============================================================
// DISPATCH QUERIES
// ============================================================

export const dispatch = {
  /** Atomically claim pending deal_states into filtering with a trigger hash */
  claimFilterBatch: (schema, triggerHash, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', ACTIVE_TRIGGER_HASH = '${triggerHash}'
    WHERE EMAIL_METADATA_ID IN (
      SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize}
    )`,

  /** Atomically claim pending_classification deal_states into classifying (with thread-completeness check) */
  claimDetectBatch: (schema, triggerHash, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', ACTIVE_TRIGGER_HASH = '${triggerHash}'
    WHERE EMAIL_METADATA_ID IN (
      SELECT ds.EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES ds
      WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}'
        AND NOT EXISTS (
          SELECT 1 FROM ${schema}.DEAL_STATES ds2
          WHERE ds2.THREAD_ID = ds.THREAD_ID
            AND ds2.USER_ID = ds.USER_ID
            AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')
        )
      LIMIT ${batchSize}
    )`,

  /** Count deal_states claimed by a trigger hash (verify claim) */
  countClaimed: (schema, triggerHash) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.DEAL_STATES WHERE ACTIVE_TRIGGER_HASH = '${triggerHash}'`,

  /** Reset claimed deal_states back to original status on trigger failure */
  resetClaimed: (schema, triggerHash, resetStatus) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${resetStatus}', ACTIVE_TRIGGER_HASH = NULL WHERE ACTIVE_TRIGGER_HASH = '${triggerHash}'`,
}

// ============================================================
// FILTER PROCESSOR QUERIES
// ============================================================

export const filter = {
  /** Fetch deal_states claimed by a trigger hash for filtering */
  fetchBatch: (schema, triggerHash) =>
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID
    FROM ${schema}.DEAL_STATES
    WHERE ACTIVE_TRIGGER_HASH = '${triggerHash}'`,

  /** Move filtered deal_states to pending_classification */
  updateFiltered: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.PENDING_CLASSIFICATION}', ACTIVE_TRIGGER_HASH = NULL, ATTEMPTS = 0 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move rejected deal_states to filter_rejected */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTER_REJECTED}', ACTIVE_TRIGGER_HASH = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Fetch deal_states + AI context for detection */
  fetchBatchWithContext: (schema, triggerHash) =>
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
    WHERE ds.ACTIVE_TRIGGER_HASH = '${triggerHash}'`,

  /** Move deal deal_states to deal status */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.DEAL}', ACTIVE_TRIGGER_HASH = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-deal deal_states to not_deal status */
  updateNotDeal: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.NOT_DEAL}', ACTIVE_TRIGGER_HASH = NULL WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
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
      (ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', ${threadCount}, ${emailCount}, ${cost}, ${inputTokens}, ${outputTokens}, '${model}', '${evaluation}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

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

  deleteContact: (schema, email) => `DELETE FROM ${schema}.CONTACTS WHERE EMAIL = '${email}'`,

  insertContact: (schema, { id, email, name, company, title }) =>
    `INSERT INTO ${schema}.CONTACTS
      (ID, EMAIL, NAME, COMPANY_NAME, TITLE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${email}', '${name}', '${company}', '${title}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  deleteDeal: (schema, threadId, userId) =>
    `DELETE FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}' AND USER_ID = '${userId}'`,

  insertDeal: (
    schema,
    { id, userId, threadId, evalId, dealName, dealType, category, value, currency, brand },
  ) =>
    `INSERT INTO ${schema}.DEALS
      (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${userId}', '${threadId}', '${evalId}', '${dealName}', '${dealType}', '${category}', ${value}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  deleteDealContact: (schema, dealId, contactId) =>
    `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID = '${dealId}' AND CONTACT_ID = '${contactId}'`,

  insertDealContact: (schema, { id, dealId, contactId }) =>
    `INSERT INTO ${schema}.DEAL_CONTACTS
      (ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${dealId}', '${contactId}', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
}

// ============================================================
// FINALIZE QUERIES
// ============================================================

export const finalize = {
  /** Reset any deal_states still claimed by a trigger hash back to their pre-claim status */
  resetLeftovers: (schema, triggerHash, resetStatus) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${resetStatus}', ACTIVE_TRIGGER_HASH = NULL WHERE ACTIVE_TRIGGER_HASH = '${triggerHash}'`,
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
