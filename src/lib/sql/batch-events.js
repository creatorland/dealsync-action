import { sanitizeId, sanitizeString, sanitizeSchema } from './sanitize.js'

export const batchEvents = {
  upsert: (schema, triggerHash, batchId, batchType, eventType) => {
    const s = sanitizeSchema(schema)
    const th = sanitizeId(triggerHash)
    const bid = sanitizeId(batchId)
    const bt = sanitizeString(batchType)
    const et = sanitizeString(eventType)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ('${th}', '${bid}', '${bt}', '${et}', CURRENT_TIMESTAMP) ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },

  upsertBulk: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },
}
