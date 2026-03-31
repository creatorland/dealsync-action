# Already-Evaluated Thread Skip

## Problem

When resyncing an entire inbox (e.g., after migration to the unified system), all threads re-enter the pipeline from `pending`. Threads that already have a deal in DEALS and no newer emails should skip classification to avoid wasting AI calls and to preserve existing deal data in the UI during the resync.

## Decision

Skip logic lives in the classify stage — after content fetch, before the AI call. The content fetch is needed to get email dates from the content fetcher response.

Threads still flow through filtering normally and land in `pending_classification`. The check only applies at classification time.

## Skip Logic

1. After fetching email content, group emails by threadId
2. Collect all unique thread IDs in the batch
3. Query DEALS for existing rows matching those thread IDs (using `deals.selectByThreadIds` which returns `UPDATED_AT`)
4. For each thread with an existing deal:
   - Get the latest email date from fetched content for that thread
   - If latest email date <= deal's UPDATED_AT → skip (already evaluated)
   - If latest email date > deal's UPDATED_AT → classify (new emails since deal)
5. Remove skipped threads' emails from the AI prompt
6. For skipped threads: directly update DEAL_STATES → `deal`
7. Classify remaining threads normally

## What Changes

| File                                    | Change                                                                 |
| --------------------------------------- | ---------------------------------------------------------------------- |
| `src/lib/sql/deals.js`                  | Add `UPDATED_AT` to `selectByThreadIds` return columns                 |
| `src/commands/run-classify-pipeline.js` | Add skip logic in `processClassifyBatch()` after fetch, before AI call |
| `src/commands/fetch-and-classify.js`    | Same skip logic for standalone command path                            |

## What Doesn't Change

- Downstream steps (save-evals, save-deals, save-deal-contacts, update-deal-states) — skipped threads aren't in the audit, so downstream logic never touches them
- Filter pipeline — threads flow through filtering normally
- Audit format — only classified threads appear in the audit
- State machine — no new states needed

## Edge Cases

- **Thread with deal + newer emails**: goes through classifier, deal gets upserted (existing ON CONFLICT handles this)
- **Thread with no deal**: normal classification regardless of email age
- **All threads in batch skipped**: no AI call, no audit, just direct state updates + batch completion event
- **Mix of skipped and classified threads**: skipped threads get direct state update, rest go through normal pipeline
- **Content fetch fails for a thread**: handled by existing unfetchable logic (no date to compare → can't skip → falls through to unfetchable handler)

## Key Constraint

No audit entry for skipped threads. The audit is per-batch and only contains threads that went through the classifier. Skipped threads get their terminal state set directly.
