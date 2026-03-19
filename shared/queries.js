/**
 * Shared SQL queries for Dealsync W3 workflow actions.
 *
 * All queries use UPPERCASE column names (SxT convention).
 * Schema is passed as a parameter — never hardcoded.
 * IDs must be sanitized before interpolation.
 */

// ============================================================
// ORCHESTRATOR QUERIES
// ============================================================

export const orchestrator = {
  /** Count emails at each stage for concurrency and pending checks */
  checkConcurrency: (schema) =>
    `SELECT
      (SELECT COUNT(*) FROM ${schema}.EMAIL_METADATA WHERE STAGE BETWEEN 1001 AND 10000) AS ACTIVE_FILTER,
      (SELECT COUNT(*) FROM ${schema}.EMAIL_METADATA WHERE STAGE BETWEEN 11001 AND 60000) AS ACTIVE_DETECT,
      (SELECT COUNT(*) FROM ${schema}.EMAIL_METADATA WHERE STAGE = 2) AS PENDING_FILTER,
      (SELECT COUNT(*) FROM ${schema}.EMAIL_METADATA WHERE STAGE = 3) AS PENDING_DETECT`,

  /** Reset stale filter transitions (>N min) back to stage 2, stale detect to stage 3 */
  expireStale: (schema, minutes = 10) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = CASE
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
  /** Atomically claim stage-2 emails into a filter transition stage */
  claimFilterBatch: (schema, transitionStage, batchSize) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${transitionStage}
    WHERE ID IN (
      SELECT ID FROM ${schema}.EMAIL_METADATA WHERE STAGE = 2 LIMIT ${batchSize}
    )`,

  /** Atomically claim stage-3 emails into a detect transition stage (with thread-completeness check) */
  claimDetectBatch: (schema, transitionStage, batchSize) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${transitionStage}
    WHERE ID IN (
      SELECT em.ID FROM ${schema}.EMAIL_METADATA em
      WHERE em.STAGE = 3
        AND NOT EXISTS (
          SELECT 1 FROM ${schema}.EMAIL_METADATA m2
          WHERE m2.THREAD_ID = em.THREAD_ID
            AND m2.USER_ID = em.USER_ID
            AND m2.STAGE IN (1, 2)
        )
      LIMIT ${batchSize}
    )`,

  /** Count emails at a transition stage (verify claim) */
  countAtStage: (schema, stage) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.EMAIL_METADATA WHERE STAGE = ${stage}`,

  /** Reset claimed emails back to original stage on trigger failure */
  resetClaimedEmails: (schema, transitionStage, resetStage) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${resetStage} WHERE STAGE = ${transitionStage}`,
}

// ============================================================
// FILTER PROCESSOR QUERIES
// ============================================================

export const filter = {
  /** Fetch metadata for emails at a transition stage (filter) */
  fetchMetadata: (schema, transitionStage) =>
    `SELECT ID, MESSAGE_ID, USER_ID, USER_REPORT_ID
    FROM ${schema}.EMAIL_METADATA
    WHERE STAGE = ${transitionStage}`,

  /** Move filtered emails to stage 3 */
  updateFiltered: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 3 WHERE ID IN (${sqlQuotedIds})`,

  /** Move rejected emails to stage 106 */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 106 WHERE ID IN (${sqlQuotedIds})`,

  /** Move failed content fetch emails to stage 666 */
  updateFailed: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 666 WHERE ID IN (${sqlQuotedIds})`,
}

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Fetch metadata + AI context for emails at a transition stage (detection) */
  fetchMetadataWithContext: (schema, transitionStage) =>
    `SELECT em.ID, em.MESSAGE_ID, em.USER_ID, em.THREAD_ID, em.USER_REPORT_ID,
      latest_eval.AI_SUMMARY AS PREVIOUS_AI_SUMMARY,
      d.ID AS EXISTING_DEAL_ID
    FROM ${schema}.EMAIL_METADATA em
    LEFT JOIN (
      SELECT THREAD_ID, AI_SUMMARY,
        ROW_NUMBER() OVER (PARTITION BY THREAD_ID ORDER BY UPDATED_AT DESC) AS RN
      FROM ${schema}.EMAIL_THREAD_EVALUATIONS
    ) latest_eval ON latest_eval.THREAD_ID = em.THREAD_ID AND latest_eval.RN = 1
    LEFT JOIN ${schema}.DEALS d ON d.THREAD_ID = em.THREAD_ID AND d.USER_ID = em.USER_ID
    WHERE em.STAGE = ${transitionStage}`,

  /** Move deal emails to stage 4 */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 4 WHERE ID IN (${sqlQuotedIds})`,

  /** Move non-deal emails to stage 106 */
  updateRejected: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 106 WHERE ID IN (${sqlQuotedIds})`,

  /** Move non-English emails to stage 107 */
  updateNonEnglish: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = 107 WHERE ID IN (${sqlQuotedIds})`,
}

// ============================================================
// SAVE RESULTS QUERIES (detection pipeline DML)
// ============================================================

export const saveResults = {
  /** Insert AI evaluation audit record */
  insertAudit: (schema, { id, threadCount, emailCount, cost, inputTokens, outputTokens, model, evaluation }) =>
    `INSERT INTO ${schema}.AI_EVALUATION_AUDITS
      (ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', ${threadCount}, ${emailCount}, ${cost}, ${inputTokens}, ${outputTokens}, '${model}', '${evaluation}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete existing thread evaluation (for upsert) */
  deleteThreadEvaluation: (schema, threadId) =>
    `DELETE FROM ${schema}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID = '${threadId}'`,

  /** Insert thread evaluation */
  insertThreadEvaluation: (schema, { id, threadId, auditId, category, summary, isDeal, likelyScam, score }) =>
    `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
      (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${threadId}', '${auditId}', '${category}', '${summary}', ${isDeal}, ${likelyScam}, ${score}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete existing contact by email (for upsert) */
  deleteContact: (schema, email) =>
    `DELETE FROM ${schema}.CONTACTS WHERE EMAIL = '${email}'`,

  /** Insert contact */
  insertContact: (schema, { id, email, name, company, title }) =>
    `INSERT INTO ${schema}.CONTACTS
      (ID, EMAIL, NAME, COMPANY_NAME, TITLE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${email}', '${name}', '${company}', '${title}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete existing deal (for upsert) */
  deleteDeal: (schema, threadId, userId) =>
    `DELETE FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}' AND USER_ID = '${userId}'`,

  /** Insert deal */
  insertDeal: (schema, { id, userId, threadId, evalId, dealName, dealType, category, value, currency, brand }) =>
    `INSERT INTO ${schema}.DEALS
      (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${userId}', '${threadId}', '${evalId}', '${dealName}', '${dealType}', '${category}', ${value}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,

  /** Delete existing deal-contact relationship (for upsert) */
  deleteDealContact: (schema, dealId, contactId) =>
    `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID = '${dealId}' AND CONTACT_ID = '${contactId}'`,

  /** Insert deal-contact relationship */
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
  /** Reset any emails still at a transition stage back to their pre-claim stage */
  resetLeftovers: (schema, transitionStage, resetStage) =>
    `UPDATE ${schema}.EMAIL_METADATA SET STAGE = ${resetStage} WHERE STAGE = ${transitionStage}`,
}

// ============================================================
// UTILITIES
// ============================================================

/** Sanitize an ID for safe SQL interpolation */
export function sanitizeId(id) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID format: ${id}`)
  }
  return id
}

/** Escape a string for SQL single-quote interpolation */
export function sanitizeString(s) {
  return (s || '').replace(/'/g, "''")
}

/** Format IDs as SQL-quoted comma-separated list for IN clauses */
export function toSqlIdList(ids) {
  return ids.map((id) => `'${sanitizeId(id)}'`).join(',')
}

/** Validate schema name */
export function sanitizeSchema(schema) {
  if (!/^[a-zA-Z0-9_]+$/.test(schema)) {
    throw new Error(`Invalid schema: ${schema}`)
  }
  return schema
}
