# sync-deal-values backfill workflow — design

**Date:** 2026-04-15
**Status:** Approved, pending implementation plan
**Goal:** One-off, manually-triggered W3 workflow to backfill `VALUE` and `CURRENCY` on deals written with bad values during the 2026-03-31 → fix-deploy bug window. Reads from `AI_EVALUATION_AUDITS` (no AI re-run) and updates `DEALS` in place.

## Problem

Fix [`9bc8c91`](../../src/commands/run-classify-pipeline.js) (2026-03-31) broke the classify pipeline for ~2+ weeks: every deal written to `DEALSYNC_PROD_V2.DEALS` landed with `VALUE = 0` and `CURRENCY = 'USD'`. ~4,771 affected rows. The AI output was correct — only the SQL write was broken. Raw AI output is still available in `AI_EVALUATION_AUDITS.AI_EVALUATION`.

See fix design: [2026-04-15-classify-pipeline-deal-value-currency-fix-design.md](./2026-04-15-classify-pipeline-deal-value-currency-fix-design.md)

## Non-goals

- Cron schedule. Backfill is one-off; trigger manually.
- Re-running AI. Audit data is authoritative for the affected rows.
- A failures table. Per-row warn + summary is sufficient.
- Modifying anything other than `DEALS.VALUE` and `DEALS.CURRENCY`.

## Architecture

New command `sync-deal-values` in `src/commands/`, registered in `src/main.js` dispatch. Driven by a new W3 workflow `ds-sync-deal-values-<commit7>` with `on: workflow_dispatch` only.

Flow per claimed batch:
1. Select deals where `VALUE = 0 OR VALUE IS NULL` and `CREATED_AT >= backfill-start-date`, paginated by `ID`.
2. For each deal, look up its `AI_EVALUATION_AUDIT` via `EMAIL_THREAD_EVALUATIONS.AI_EVALUATION_AUDIT_ID` keyed on the deal's thread.
3. Parse `AI_EVALUATION` (raw AI-output JSON) through `parseAndValidate` from `src/lib/ai.js` — same zod-backed coercion the live pipeline uses.
4. Find the entry matching this thread. Extract `deal_value` and `deal_currency`.
5. If value is a usable number, `UPDATE DEALS SET VALUE = ..., CURRENCY = ...`. The `WHERE VALUE = 0 OR VALUE IS NULL` filter also lives on the UPDATE to prevent re-triggered runs from overwriting corrected data.
6. Skip with a per-row warn on: audit missing, thread not found in audit payload, `deal_value` absent or null, JSON unparsable.
7. Emit a final summary: recovered count, skipped counts grouped by reason.

Safe to re-run. Idempotent by construction (the UPDATE's `WHERE` guard).

## Components

### `src/commands/sync-deal-values.js` (new)

Inputs (via `@actions/core.getInput`):
- All standard SxT inputs (`sxt-auth-url`, `sxt-auth-secret`, `sxt-api-url`, `sxt-biscuit`, `sxt-schema`).
- `backfill-start-date` — default `2026-03-31` (bug commit date).
- `backfill-batch-size` — default `500`.
- `backfill-dry-run` — default `false`. When true, logs each would-be UPDATE without executing.

Output: JSON `{ recovered, skipped: { auditMissing, threadNotFound, valueNull, parseError }, totalScanned }`. Set via `@actions/core` outputs on success.

### `src/lib/sql/deals.js` (extend)

- `findAffectedDeals(schema, { startDate, cursorId, limit })` — `SELECT ID, THREAD_ID, USER_ID FROM {schema}.DEALS WHERE (VALUE = 0 OR VALUE IS NULL) AND CREATED_AT >= '{startDate}' AND ID > '{cursorId}' ORDER BY ID LIMIT {limit}`. Cursor-paginated for SxT's row cap.
- `updateDealValueCurrency(schema, { dealId, value, currency })` — `UPDATE {schema}.DEALS SET VALUE = {value}, CURRENCY = '{currency}', UPDATED_AT = CURRENT_TIMESTAMP WHERE ID = '{dealId}' AND (VALUE = 0 OR VALUE IS NULL)`. Idempotent guard in the `WHERE` clause.

### `src/lib/sql/audits.js` (extend)

- `findAuditByThread(schema, threadId)` — joins `EMAIL_THREAD_EVALUATIONS` and `AI_EVALUATION_AUDITS` on `AI_EVALUATION_AUDIT_ID`, returns `{ AI_EVALUATION }` for the thread.

### `src/main.js` (modify)

Register `sync-deal-values` in the `COMMANDS` map.

### `.github/workflows/dealsync-sync-deal-values.testnet.yml` (new)

```yaml
name: ds-sync-deal-values-<commit7>
authority: '...'
on:
  workflow_dispatch:
concurrency:
  group: sync-deal-values-testnet
  cancel-in-progress: false
jobs:
  backfill:
    timeout-minutes: 60
    runs-on: ubuntu-latest
    environment: '...'
    steps:
      - id: backfill
        name: Run deal-value backfill
        uses: creatorland/dealsync-action@<full-sha>
        with:
          command: sync-deal-values
          # ...all sxt-* and backfill-* inputs
```

Betanet equivalent under a separate file; same structure, different environment hash.

## Error handling

Per-row failures produce `console.warn` lines in the form `[sync-deal-values] skip deal_id=<id> thread_id=<tid> reason=<reason>`. Reasons: `audit_missing`, `thread_not_in_audit`, `deal_value_null`, `parse_error`.

Pipeline-level errors (auth failure, connectivity, schema drift) throw and fail the run. Because the UPDATE is idempotent, re-running after a crash is safe.

## Testing

`__tests__/sync-deal-values.test.js`:
- Happy path: mock `executeSql` to return one affected deal, a valid audit JSON. Assert the UPDATE SQL includes the recovered value and currency.
- Skip: audit missing → no UPDATE, warn emitted, summary `auditMissing` incremented.
- Skip: thread not found in audit payload → skip, summary `threadNotFound` incremented.
- Skip: `deal_value: null` in audit → skip, summary `valueNull`.
- Skip: parse error → skip, summary `parseError`.
- Dry-run: no `UPDATE` calls made; summary still reports would-be-recovered count.
- Pagination: two batches, second returns empty, loop terminates.
- Idempotency check via the SQL: the UPDATE's `WHERE` includes `(VALUE = 0 OR VALUE IS NULL)`.

No new tests on the SQL builders — they're trivial templates covered by the command test.

## Rollout

1. Implement directly on main (no PR gate — one-off backfill tool).
2. Deploy workflow to W3 testnet. Trigger manually on the Tiana reset set first. Verify recovered counts and spot-check 5–10 UPDATEs against audit JSON.
3. Deploy workflow to betanet/prod. Trigger with a small `backfill-batch-size` (e.g., 100) canary run. Inspect. Then re-trigger with full batch size until `totalScanned = 0`.

Manually trigger via `mcp__w3__trigger-workflow` or through W3 UI.
