import { sanitizeId, sanitizeString } from './sql/sanitize.js'

/**
 * Map an AI-classified deal thread to a SQL VALUES tuple for the deals table.
 *
 * Pure function — no I/O, no side effects. Consumes only schema-coerced fields
 * produced by `parseAndValidate` in `src/lib/ai.js`:
 *   - `deal_value`: number | null
 *   - `deal_currency`: string | null
 *   - `main_contact`: { company?: string } | null
 *
 * Column order (must match run-classify-pipeline.js deal upsert):
 *   deal_id, user_id, thread_id, '', deal_name, deal_type, category,
 *   deal_value, currency, brand, is_active, created_at, updated_at
 */
export function threadToDealTuple(thread, { userId }) {
  const threadId = sanitizeId(thread.thread_id)
  const uid = userId ? sanitizeId(userId) : ''
  const dealId = threadId
  const dealName = sanitizeString(thread.deal_name || '')
  const dealType = sanitizeString(thread.deal_type || '')
  const category = sanitizeString(thread.category || '')

  const rawValue = thread.deal_value
  const dealValue =
    typeof rawValue === 'number' && Number.isFinite(rawValue) && rawValue >= 0 ? rawValue : 0

  const rawCurrency =
    typeof thread.deal_currency === 'string' ? thread.deal_currency.trim() : ''
  const currency = sanitizeString(rawCurrency || 'USD')

  const brand = thread.main_contact
    ? sanitizeString(thread.main_contact.company || '')
    : ''

  return `('${dealId}', '${uid}', '${threadId}', '', '${dealName}', '${dealType}', '${category}', ${dealValue}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
}
