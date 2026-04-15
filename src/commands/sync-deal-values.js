import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import { parseAndValidate } from '../lib/ai.js'
import { sanitizeSchema } from '../lib/sql/sanitize.js'
import { deals as dealsSql } from '../lib/sql/deals.js'
import { audits as auditsSql } from '../lib/sql/audits.js'

export async function runSyncDealValues() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const startDate = core.getInput('backfill-start-date') || '2026-03-31'
  const batchSize = parseInt(core.getInput('backfill-batch-size') || '500', 10)
  const auditPageSize = parseInt(core.getInput('backfill-audit-page-size') || '500', 10)
  const dryRun = core.getInput('backfill-dry-run') === 'true'

  console.log(
    `[sync-deal-values] starting startDate=${startDate} batchSize=${batchSize} auditPageSize=${auditPageSize} dryRun=${dryRun}`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const summary = {
    recovered: 0,
    skipped: { auditMissing: 0, valueNull: 0, parseError: 0 },
    totalScanned: 0,
    auditsScanned: 0,
    auditsParseFailed: 0,
    threadEntries: 0,
    pages: 0,
    sqlCalls: 0,
  }

  // ============================================================
  // Step 1: Build thread_id → entry map by scanning all audits in window.
  // ============================================================
  const threadMap = {}
  let auditCursor = ''
  while (true) {
    const auditPage = await exec(
      auditsSql.selectSinceDatePage(schema, {
        startDate,
        cursorId: auditCursor,
        limit: auditPageSize,
      }),
    )
    summary.sqlCalls++
    if (!auditPage || auditPage.length === 0) break
    summary.auditsScanned += auditPage.length
    for (const a of auditPage) {
      try {
        const parsed = parseAndValidate(a.AI_EVALUATION)
        // Audits paginated by ID ASC (uuidv7 = time-ordered). Always overwrite
        // so the final map holds the LATEST (re-)classification per thread.
        for (const t of parsed) {
          if (!t.thread_id) continue
          if (!(t.thread_id in threadMap)) summary.threadEntries++
          threadMap[t.thread_id] = {
            deal_value: t.deal_value,
            deal_currency: t.deal_currency,
          }
        }
      } catch {
        summary.auditsParseFailed++
      }
    }
    auditCursor = auditPage[auditPage.length - 1].ID
    if (auditPage.length < auditPageSize) break
  }
  console.log(
    `[sync-deal-values] audit scan complete audits=${summary.auditsScanned} parse_failed=${summary.auditsParseFailed} thread_entries=${summary.threadEntries}`,
  )

  // ============================================================
  // Step 2: Iterate affected deals; resolve from in-memory map; bulk update.
  // ============================================================
  let cursorId = ''
  while (true) {
    const page = await exec(
      dealsSql.findAffectedForBackfill(schema, { startDate, cursorId, limit: batchSize }),
    )
    summary.sqlCalls++
    if (!page || page.length === 0) break
    summary.pages++
    summary.totalScanned += page.length

    const updates = []
    for (const deal of page) {
      const entry = threadMap[deal.THREAD_ID]
      if (!entry) {
        console.warn(
          `[sync-deal-values] skip deal_id=${deal.ID} thread_id=${deal.THREAD_ID} reason=audit_missing`,
        )
        summary.skipped.auditMissing++
        continue
      }
      if (entry.deal_value == null) {
        console.warn(
          `[sync-deal-values] skip deal_id=${deal.ID} thread_id=${deal.THREAD_ID} reason=deal_value_null`,
        )
        summary.skipped.valueNull++
        continue
      }
      const value = Number(entry.deal_value)
      const currency = entry.deal_currency || 'USD'
      updates.push({ dealId: deal.ID, value, currency })
      console.log(
        `[sync-deal-values] ${dryRun ? 'would-recover' : 'recovered'} deal_id=${deal.ID} thread_id=${deal.THREAD_ID} value=${value} currency=${currency}`,
      )
      summary.recovered++
    }

    if (!dryRun && updates.length > 0) {
      const sql = dealsSql.bulkBackfillValues(schema, updates)
      if (sql) {
        await exec(sql)
        summary.sqlCalls++
        console.log(`[sync-deal-values] page=${summary.pages} bulk-updated ${updates.length} rows`)
      }
    }

    cursorId = page[page.length - 1].ID
    if (page.length < batchSize) break
  }

  console.log(
    `[sync-deal-values] done recovered=${summary.recovered} skipped_audit_missing=${summary.skipped.auditMissing} skipped_value_null=${summary.skipped.valueNull} skipped_parse_error=${summary.skipped.parseError} scanned=${summary.totalScanned} audits=${summary.auditsScanned} thread_entries=${summary.threadEntries} pages=${summary.pages} sql_calls=${summary.sqlCalls}`,
  )

  return summary
}
