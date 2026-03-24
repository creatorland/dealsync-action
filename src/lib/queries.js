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
  /** Find stuck batches that can be retried.
   *  - Batch must be stuck > staleMinutes
   *  - Retrigger count < maxRetriggers (dead letter if exceeded)
   *  - Last event must be > cooldownMinutes ago (prevents spam retriggers) */
  findStuckBatches: (schema, staleMinutes = 10, maxRetriggers = 3, cooldownMinutes = 5) =>
    `SELECT ds.BATCH_ID,
      CASE WHEN ds.STATUS = '${STATUS.FILTERING}' THEN 'filter' ELSE 'classify' END AS BATCH_TYPE
    FROM ${schema}.DEAL_STATES ds
    LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID AND be.EVENT_TYPE = 'retrigger'
    WHERE ds.STATUS IN ('${STATUS.FILTERING}', '${STATUS.CLASSIFYING}')
    AND ds.BATCH_ID IS NOT NULL
    AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${staleMinutes}' MINUTE
    AND NOT EXISTS (
      SELECT 1 FROM ${schema}.BATCH_EVENTS recent
      WHERE recent.BATCH_ID = ds.BATCH_ID
      AND recent.CREATED_AT > CURRENT_TIMESTAMP - INTERVAL '${cooldownMinutes}' MINUTE
    )
    GROUP BY ds.BATCH_ID, ds.STATUS
    HAVING COUNT(be.TRIGGER_HASH) < ${maxRetriggers}`,
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
  /** Claim by thread — batchSize = max threads, claims ALL emails in those threads.
   *  Thread is eligible only if ALL its emails in the same sync are done filtering. */
  claimClassifyBatch: (schema, batchId, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}'
    WHERE THREAD_ID IN (
      SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds
      WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}'
        AND NOT EXISTS (
          SELECT 1 FROM ${schema}.DEAL_STATES ds2
          WHERE ds2.THREAD_ID = ds.THREAD_ID
            AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
            AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')
        )
      LIMIT ${batchSize}
    ) AND STATUS = '${STATUS.PENDING_CLASSIFICATION}'`,

  /** Count deal_states claimed by a trigger hash (verify claim) */
  countClaimed: (schema, batchId) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,

  /** Count total in-flight deal_states at a given status */
  countInFlight: (schema, status) =>
    `SELECT COUNT(*) AS CNT FROM ${schema}.DEAL_STATES WHERE STATUS = '${status}'`,

  /** Reset claimed deal_states back to original status on trigger failure */
  resetClaimed: (schema, batchId, resetStatus) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${resetStatus}', BATCH_ID = NULL WHERE BATCH_ID = '${batchId}'`,
}

// ============================================================
// DETECTION PROCESSOR QUERIES
// ============================================================

export const detection = {
  /** Move deal deal_states to deal status */
  updateDeals: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.DEAL}' WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,

  /** Move non-deal deal_states to not_deal status */
  updateNotDeal: (schema, sqlQuotedIds) =>
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.NOT_DEAL}' WHERE EMAIL_METADATA_ID IN (${sqlQuotedIds})`,
}

// ============================================================
// SAVE RESULTS QUERIES (detection pipeline DML)
// ============================================================

export const saveResults = {
  /** Check if audit already exists for this batch (checkpoint) */
  getAuditByBatchId: (schema, batchId) =>
    `SELECT AI_EVALUATION FROM ${schema}.AI_EVALUATION_AUDITS WHERE BATCH_ID = '${batchId}'`,

  /** Insert audit — only after successful AI JSON parse (checkpoint) */
  insertAudit: (
    schema,
    { id, batchId, threadCount, emailCount, cost, inputTokens, outputTokens, model, evaluation },
  ) =>
    `INSERT INTO ${schema}.AI_EVALUATION_AUDITS
      (ID, BATCH_ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT)
    VALUES
      ('${id}', '${batchId}', ${threadCount}, ${emailCount}, ${cost}, ${inputTokens}, ${outputTokens}, '${model}', '${evaluation}', CURRENT_TIMESTAMP)`,

  /** Upsert thread evaluation — ON CONFLICT updates existing */
  upsertThreadEvaluation: (
    schema,
    { id, threadId, auditId, category, summary, isDeal, likelyScam, score },
  ) =>
    `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
      (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${threadId}', '${auditId}', '${category}', '${summary}', ${isDeal}, ${likelyScam}, ${score}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (THREAD_ID) DO UPDATE SET
      AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID,
      AI_INSIGHT = EXCLUDED.AI_INSIGHT,
      AI_SUMMARY = EXCLUDED.AI_SUMMARY,
      IS_DEAL = EXCLUDED.IS_DEAL,
      LIKELY_SCAM = EXCLUDED.LIKELY_SCAM,
      AI_SCORE = EXCLUDED.AI_SCORE,
      UPDATED_AT = CURRENT_TIMESTAMP`,

  /** Upsert deal — one deal per thread, ON CONFLICT updates existing */
  upsertDeal: (
    schema,
    { id, userId, threadId, evalId, dealName, dealType, category, value, currency, brand },
  ) =>
    `INSERT INTO ${schema}.DEALS
      (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
    VALUES
      ('${id}', '${userId}', '${threadId}', '${evalId}', '${dealName}', '${dealType}', '${category}', ${value}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (THREAD_ID) DO UPDATE SET
      EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID,
      DEAL_NAME = EXCLUDED.DEAL_NAME,
      DEAL_TYPE = EXCLUDED.DEAL_TYPE,
      CATEGORY = EXCLUDED.CATEGORY,
      VALUE = EXCLUDED.VALUE,
      CURRENCY = EXCLUDED.CURRENCY,
      BRAND = EXCLUDED.BRAND,
      UPDATED_AT = CURRENT_TIMESTAMP`,

  /** Delete deal by thread_id (for removing deals when reclassified as not_deal) */
  deleteDeal: (schema, threadId) =>
    `DELETE FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}'`,

  /** Delete deal contacts by thread_id (via deals table) — correct for re-classification */
  deleteDealContactByThread: (schema, threadId) =>
    `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID IN (SELECT ID FROM ${schema}.DEALS WHERE THREAD_ID = '${threadId}')`,

  /** Insert deal contact — stores email in CONTACT_ID field */
  insertDealContact: (schema, { id, dealId, contactEmail }) =>
    `INSERT INTO ${schema}.DEAL_CONTACTS
      (ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, CREATED_AT, UPDATED_AT)
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
// BATCH EVENTS (append-only audit log)
// ============================================================

export const batchEvents = {
  /** Insert one or more batch events. Values is a pre-built VALUES string. */
  insert: (schema, valuesStr) =>
    `INSERT INTO ${schema}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ${valuesStr}`,
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
