# Revert + Partial Failure Re-implementation

**Date:** 2026-04-01
**Status:** Approved

## Problem

Commit 2bf6833 introduced `fetchThreadEmails` — a thread-aware retry layer with a 200s deadline and 10 internal retries per message. Combined with `runPool`'s 6 batch-level retries, this creates a worst-case wall time of 20+ minutes per failing batch. GitHub Action timeout kills the process before `runPool` can complete or dead-letter, leaving rows stuck in `filtering`/`classifying` indefinitely.

Evidence: 7,000 rows stuck in `filtering` across 35 batches, zero retrigger or dead_letter events. Stuck batch recovery never runs because pending rows always exist when `claimBatch` checks.

## Solution

1. **Revert to 55859cd** — removes `fetchThreadEmails`, global semaphore, and semaphore revert (3 commits)
2. **Modify `fetchEmails` in-place** to handle 207/502 responses from content fetcher (PR creatorland/dealsync-v2#349)
3. No new files — no `fetch-threads.js`, no thread-awareness layer

## fetchEmails changes (src/lib/emails.js)

The old per-chunk retry loop becomes per-messageId aware:

```
For each chunk:
  pendingIds = chunk
  For each retry attempt (max 3):
    POST pendingIds to content fetcher
    HTTP 200 -> accept all, done
    HTTP 207 -> accept data[], re-queue only failed messageIds
    HTTP 502 -> parse errors if JSON, re-queue all failed messageIds
    Other/timeout -> re-queue entire chunk
    pendingIds = only the still-failed messageIds
    If pendingIds empty -> done
```

Key properties preserved from old code:

- Sequential chunks (not concurrent Promise.allSettled)
- 3 retries per chunk, fast backoff (1s base)
- 30s timeout per request (filter) / 120s (classify)
- Returns flat `allEmails[]` array — same contract
- Throws only if `allEmails.length === 0 && messageIds.length > 0`
- No thread-awareness, no 200s deadline

## Pipeline changes

**Filter pipeline** — reverted to old code, calls `fetchEmails` directly. Partial results processed normally; unfetched messages stay in `filtering`, recovered via stuck batch / dead letter.

**Classify pipeline** — reverted to old code. Existing unfetchable handler resolves missing threads via previous eval or `not_deal`. Batches that can't complete get dead-lettered by `runPool` / `sweepStuckRows`.

## What this fixes

- **No retry explosion**: 3 retries x 30s timeout = ~90s max per chunk, within Action timeout
- **Stuck batch recovery works**: `runPool` completes/dead-letters in time, `sweepStuckRows` runs, `claimBatch` reaches stuck batch check
- **Partial results accepted**: 207 responses yield successful emails immediately, only failed messageIds retry

## Files touched

| File                                    | Change                                                          |
| --------------------------------------- | --------------------------------------------------------------- |
| `src/lib/emails.js`                     | Modify `fetchEmails` to handle 207/502 with per-messageId retry |
| `src/lib/fetch-threads.js`              | Deleted (via revert)                                            |
| `src/commands/run-filter-pipeline.js`   | Reverted to calling `fetchEmails` directly                      |
| `src/commands/run-classify-pipeline.js` | Reverted to calling `fetchEmails` directly                      |
