# Content Fetcher Partial Failure Handling — Design

**Date:** 2026-04-01
**Status:** Approved
**Companion PRs:** creatorland/backend#1184, creatorland/dealsync-v2#349 (PROJ-5820)

## Problem

The content fetcher API (`POST /email-content/fetch`) is gaining partial failure semantics (HTTP 207 + per-message errors, HTTP 502 + total failure body). Our `fetchEmails()` in `src/lib/emails.js` has no awareness of these — it treats any non-2xx as a retry-worthy transport error and silently drops failed messageIds on 207. This means we can filter or classify threads with incomplete email data, leading to incorrect results.

## Goals

- Handle 200/207/502 response semantics from the content fetcher
- Retry only failed messageIds, not entire chunks — minimize round trips
- Group emails by thread — never process a thread with incomplete emails
- Efficient re-batching of failures for maximum throughput
- Proper memory management — no duplicate data, GC-friendly

## Non-Goals

- Dead letter handling (future concern, not in scope)
- Changes to the content fetcher API itself (handled by PROJ-5820 PRs)

## Design

### Layer 1: fetchEmails() — Low-Level, Single-Shot

`fetchEmails()` in `src/lib/emails.js` becomes a single-shot fire-and-parse function. All retry logic is removed from this layer.

**Response handling:**

- **200** — `{ status: 'success', data }` — all emails fetched
- **207** — `{ status: 'partial', data, errors }` — extract both fetched and failed
- **502** — `{ status: 'failure', data: [], errors }` — parse JSON body, extract all failures
- **Transport error** (timeout, connection reset) — all messageIds in chunk marked failed

**Return type changes** from `EmailContent[]` to `{ fetched: EmailContent[], failed: { messageId, error }[] }`.

**Logging per chunk:**

- Request: chunk index, messageId count, format
- Response: HTTP status, fetched count, failed count, response time ms
- 207/502: failed messageIds + error reasons (messageIds are not PII)
- Transport error: error type, chunk index, messageId count

### Layer 2: fetchThreadEmails() — Thread-Aware Retry

New function that sits between the pipelines and `fetchEmails()`. Both pipelines call this instead of `fetchEmails()` directly.

**Inputs:** Same as current `fetchEmails()` plus `deadlineMs` (wall-clock budget, default 200s).

**Algorithm:**

1. Build thread map: `{ threadId: [messageId, ...] }`
2. Pack threads into chunks respecting chunk size, keeping threads intact. If a single thread exceeds chunk size, it spans multiple chunks.
3. Initialize `fetchedMap` (messageId to EmailContent), `attemptCounts` (messageId to number), deadline.
4. **Fetch loop:**
   - Fire all chunks concurrently via `fetchEmails()` (single-shot)
   - Store successes in `fetchedMap`, increment `attemptCounts` for failures
   - Check thread completeness — if all messageIds for a thread are in `fetchedMap`, the thread is complete
   - Complete threads: move data out of `fetchedMap` into `completedThreads` array, delete map entries (GC eligible)
   - Collect failed messageIds where `attemptCounts < 10`
   - If no failures remaining or past deadline: break
   - Apply backoff: `min(1s * 2^round, 60s)`
   - Re-pack incomplete threads into new chunks, only requesting messageIds NOT in `fetchedMap` (stitching)
   - Continue loop
5. Return `{ completedThreads, unfetchableThreadIds }`

**Logging per round:**

- Round start: round number, thread count, messageId count, chunk count, backoff duration
- Round complete: newly completed threads, still incomplete threads
- Deadline hit: round number, incomplete thread count
- Final summary: total complete vs unfetchable

### Pipeline Integration

**Filter pipeline** (`run-filter-pipeline.js`):

1. Claim emails in PENDING, set status to FILTERING
2. Call `fetchThreadEmails()` with `format: 'metadata'`, `chunkSize: 50`
3. Process complete threads: apply `isRejected()` per email
4. Update deal states: passed to `pending_classification`, rejected to `filter_rejected`
5. If unfetchable threads exist: batch worker throws, triggers batch-level retry via `runPool()`

**Classify pipeline** (`run-classify-pipeline.js`):

1. Claim emails in PENDING_CLASSIFICATION, set status to CLASSIFYING
2. Call `fetchThreadEmails()` with `format: full`, `chunkSize: 10`
3. Process complete threads: buildPrompt, AI classify (4-layer fallback), save audit
4. WriteBatcher: upsert evaluations, deals, contacts
5. Direct SQL: update deal states to terminal (deal/not_deal)
6. If unfetchable threads exist: batch worker throws, triggers batch-level retry via `runPool()`

### Retry Hierarchy

Three levels of retry, each with different scope:

1. **Message-level** (new): 10 attempts with exponential backoff (1s base, 60s cap), within a single batch run. Retries only the failed messageIds.
2. **Batch-level** (existing): `max-retries` (default 6) via `runPool()`. Retries the entire batch, but emails that already transitioned to next state are skipped (status no longer matches claim query).
3. **Dead letter** (future): Emails stuck in `filtering`/`classifying` after all batch retries exhausted. No special handling now.

### Memory Management

- `fetchedMap` is the single copy of email content during fetch phase
- When a thread is complete, its data is moved out of `fetchedMap` (entries deleted) — no duplication
- Pipeline processes batches sequentially or via pool — once a batch finishes and deal states are written, the batch array goes out of scope and is GC eligible
- No accumulator holds all results for the entire run

### Batch Writes

All existing batch write patterns are unchanged. They use `ON CONFLICT` upserts (idempotent), so re-processing on batch-level retry is safe:

**Filter:** 2 direct SQL updates (pending_classification + filter_rejected) + 1 batch event insert
**Classify:** WriteBatcher for evaluations/deals/contacts + direct SQL for deal state updates + batch event

## Flow Diagrams

See [2026-04-01-content-fetcher-flows.md](2026-04-01-content-fetcher-flows.md) for mermaid visualizations of:

- Retry level hierarchy
- Filter pipeline end-to-end flow
- Classify pipeline end-to-end flow
- Fetch loop internals (shared)
- Memory management data flow
