/**
 * Classify-cron true-success heartbeat (Story 8.1, Task 5.3).
 *
 * WHY THIS EXISTS — the run-state-overstates-health trap: the classify pipeline
 * catches its Supabase writes (run-classify-pipeline.js Step 6.5) NON-FATALLY in
 * `both` mode, so a W3 run reports `workflowRunCompleted` even when it wrote
 * nothing to Supabase. W3 freshness alone therefore overstates ingestion health.
 * Each run writes ONE explicit heartbeat row — run id + per-table rows written +
 * an honest status — to public.classify_heartbeat (dealsync-v2 migration 0014)
 * via the action's existing service-role creds. The dealsync-v2
 * supabase-scrape-exporter reads the latest row per node and re-emits two Cloud
 * Monitoring gauges (classify_rows_written_last_run, classify_last_true_success_
 * age_seconds), so the dashboard distinguishes "the cron ran" from "the cron
 * actually wrote to Supabase".
 *
 * This module is the PURE row builder — no I/O, unit-tested in isolation (the
 * PostgREST write lives in supabase-writer.js writeClassifyHeartbeat). Mirrors
 * the dealsync-v2 supabase-scrape-exporter's pure-helper convention.
 */

/**
 * Build the heartbeat row for a completed classify run.
 *
 * Status contract (matches the public.classify_heartbeat CHECK in 0014):
 *   success — no write failures this run. rows_written_total MAY be 0: a run
 *             with nothing to classify is a clean run, and the scraper's
 *             "true success" partial index already excludes zero-row rows, so it
 *             is not double-counted as a real ingestion success.
 *   partial — at least one write failed but some rows still landed.
 *   failure — writes failed and nothing landed.
 *
 * @param {object} p
 * @param {string} p.runId — the run identifier (GITHUB_RUN_ID under W3/GHA).
 * @param {'testnet'|'betanet'} p.node — the W3 node this run executed on.
 * @param {Record<string, number>} [p.rowsByTable] — per-table rows written this
 *   run; zero/negative/non-finite entries are dropped (an absent table = 0).
 * @param {string|null} [p.ingestWriteTarget] — effective INGEST_WRITE_TARGET
 *   (supabase | both); informational.
 * @param {number} [p.failureCount] — count of swallowed/lost Supabase-write
 *   failures this run (both-mode caught writes + dead-lettered batches).
 * @returns {{run_id: string, node: string, status: string, rows_written_total: number, rows_by_table: Record<string, number>, ingest_write_target: string|null}}
 */
export function buildHeartbeatRow({
  runId,
  node,
  rowsByTable = {},
  ingestWriteTarget = null,
  failureCount = 0,
}) {
  const cleanCounts = {}
  let total = 0
  for (const [table, raw] of Object.entries(rowsByTable || {})) {
    const n = Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 0
    if (n > 0) {
      cleanCounts[table] = n
      total += n
    }
  }

  let status
  if (failureCount <= 0) status = 'success'
  else if (total > 0) status = 'partial'
  else status = 'failure'

  return {
    run_id: runId,
    node,
    status,
    rows_written_total: total,
    rows_by_table: cleanCounts,
    ingest_write_target: ingestWriteTarget || null,
  }
}
