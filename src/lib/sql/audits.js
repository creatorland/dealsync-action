import { sanitizeId, sanitizeString, sanitizeSchema } from './sanitize.js'

export const audits = {
  selectByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT AI_EVALUATION FROM ${s}.AI_EVALUATION_AUDITS WHERE BATCH_ID = '${bid}'`
  },

  insert: (
    schema,
    { id, batchId, threadCount, emailCount, cost, inputTokens, outputTokens, model, evaluation },
  ) => {
    const s = sanitizeSchema(schema)
    const safeId = sanitizeId(id)
    const safeBid = sanitizeId(batchId)
    const safeModel = sanitizeString(model)
    const safeEval = sanitizeString(evaluation)
    return `INSERT INTO ${s}.AI_EVALUATION_AUDITS (ID, BATCH_ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT) VALUES ('${safeId}', '${safeBid}', ${Number(threadCount)}, ${Number(emailCount)}, ${Number(cost)}, ${Number(inputTokens)}, ${Number(outputTokens)}, '${safeModel}', '${safeEval}', CURRENT_TIMESTAMP)`
  },
}
