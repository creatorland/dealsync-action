/**
 * Shared SQL queries for Dealsync W3 workflow actions.
 *
 * All queries target DEAL_STATES (not EMAIL_METADATA).
 * All column names UPPERCASE (SxT convention).
 * Schema is passed as a parameter — never hardcoded.
 * IDs must be sanitized before interpolation.
 */

// ============================================================
// ORCHESTRATOR QUERIES
// ============================================================

export const orchestrator = {
  /** Count deal_states at each stage for concurrency and pending checks */
  checkConcurrency: (schema) =>
    `SELECT
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STAGE BETWEEN 1001 AND 10000) AS ACTIVE_FILTER,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STAGE BETWEEN 11001 AND 60000) AS ACTIVE_DETECT,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STAGE = 2) AS PENDING_FILTER,
      (SELECT COUNT(*) FROM ${schema}.DEAL_STATES WHERE STAGE = 3) AS PENDING_DETECT`,

  /** Reset stale filter transitions back to stage 2, stale detect back to stage 3 */
  expireStale: (schema, minutes = 10) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = CASE
      WHEN STAGE BETWEEN 1001 AND 10000 THEN 2
      WHEN STAGE BETWEEN 11001 AND 60000 THEN 3
    END
    WHERE (STAGE BETWEEN 1001 AND 10000 OR STAGE BETWEEN 11001 AND 60000)
    AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${minutes}' MINUTE`,
}

// ============================================================
// DISPATCH QUERIES
// ============================================================

export const dispatch = {
  /** Atomically claim stage-2 deal_states into a filter transition stage */
  claimFilterBatch: (schema, transitionStage, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = ${transitionStage}
    WHERE EMAIL_METADATA_ID IN (
      SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STAGE = 2 LIMIT ${batchSize}
    )`,

  /** Atomically claim stage-3 deal_states into a detect transition stage (with thread-completeness check) */
  claimDetectBatch: (schema, transitionStage, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = ${transitionStage}
    WHERE EMAIL_METADATA_ID IN (
      SELECT ds.EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES ds
      WHERE ds.STAGE = 3
        AND NOT EXISTS (
          SELECT 1 FROM ${schema}.DEAL_STATES ds2
          WHERE ds2.THREAD_ID = ds.THREAD_ID
            AND ds2.USER_ID = ds.USER_ID
            AND ds2.STAGE IN (1, 2)
        )
      LIMIT ${batchSize}
    )`,

  /** Count deal_states at a transition stage (verify claim) */
  countAtStage: (schema, stage) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.DEAL_STATES WHERE STAGE = ${stage}`,

  /** Reset claimed deal_states back to original stage on trigger failure */
  resetClaimedEmails: (schema, transitionStage, resetStage) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = ${resetStage} WHERE STAGE = ${transitionStage}`,
}

// ============================================================
// FILTER PROCESSOR QUERIES
// ============================================================

export const filter = {
  /** Fetch deal_states at a transition stage for filtering */
  fetchMetadata: (schema, transitionStage) =>
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, SYNC_STATE_ID, THREAD_ID
    FROM ${schema}.DEAL_STATES
    WHERE STAGE = ${transitionStage}`,

  /** Move filtered deal_states to stage 3 */
  updateFiltered: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 3 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move rejected deal_states to stage 106 */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 106 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move failed content fetch deal_states to stage 666 */
  updateFailed: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 666 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Fetch deal_states + AI context for detection */
  fetchMetadataWithContext: (schema, transitionStage) =>
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
    WHERE ds.STAGE = ${transitionStage}`,

  /** Move deal deal_states to stage 4 */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 4 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-deal deal_states to stage 106 */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 106 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-English deal_states to stage 107 */
  updateNonEnglish: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = 107 WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
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
  /** Reset any deal_states still at a transition stage back to their pre-claim stage */
  resetLeftovers: (schema, transitionStage, resetStage) =>
    `UPDATE ${schema}.DEAL_STATES SET STAGE = ${resetStage} WHERE STAGE = ${transitionStage}`,
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
