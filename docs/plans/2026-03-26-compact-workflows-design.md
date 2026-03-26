# Compact Workflows Design

Replace the distributed multi-workflow dispatch model with three self-contained compact workflows that manage concurrency internally. Eliminates per-batch workflow overhead while preserving the existing pipeline logic, retry semantics, and multi-instance safety.

## Problem

The current architecture dispatches a separate GitHub Actions / W3 workflow run per batch pair (filter + classify). Each run has fixed overhead (runner spin-up, checkout, dependency install). At scale this is expensive and slow. The scheduler-service orchestrator also adds complexity by managing batch claiming, stuck detection, and retrigger externally.

## Solution

Three independent compact workflows triggered via `workflow_dispatch`:

1. **Compact Sync** — create missing deal_states from email_metadata
2. **Compact Filter** — claim + filter batches concurrently until exhausted
3. **Compact Classify** — claim + classify batches concurrently until exhausted

Each workflow is a single GitHub Action step calling one command. Concurrency is managed by a JS promise pool inside the action. Multiple instances of the same workflow can run safely — batch claiming is atomic.

---

## New Commands

### `claim-filter-batch`

Atomically claims pending deal_states for filtering.

**Inputs:** `batch-size` (default 200), `max-retries`, SxT auth, schema
**Output:** `{ batch_id, count, attempts }` or `{ batch_id: null, count: 0 }`

**Claim logic:**

1. Generate UUID for batch_id
2. Claim pending items:
   ```sql
   UPDATE DEAL_STATES SET STATUS='filtering', BATCH_ID='{uuid}', UPDATED_AT=CURRENT_TIMESTAMP
   WHERE EMAIL_METADATA_ID IN (
     SELECT EMAIL_METADATA_ID FROM DEAL_STATES WHERE STATUS='pending' LIMIT {batchSize}
   )
   ```
3. If no rows claimed, look for stuck batches (retriggerable):
   ```sql
   SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS attempts
   FROM DEAL_STATES ds
   LEFT JOIN BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
   WHERE ds.STATUS = 'filtering'
     AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
   GROUP BY ds.BATCH_ID
   HAVING COUNT(DISTINCT be.TRIGGER_HASH) < {maxRetries}
   LIMIT 1
   ```
   If found, return existing batch_id with its current attempt count.
4. Insert BATCH_EVENTS row: `(triggerHash=uuid, batchId, batchType='filter', eventType='new')`
5. Return `{ batch_id, count, attempts }` — attempts is 0 for new, existing count for retrigger

### `claim-classify-batch`

Atomically claims pending_classification deal_states for classification. Thread-aware — only claims threads where all messages have cleared filtering.

**Inputs:** `batch-size` (default 5, in threads), `max-retries`, SxT auth, schema
**Output:** `{ batch_id, count, attempts }` or `{ batch_id: null, count: 0 }`

**Claim logic:**

1. Generate UUID for batch_id
2. Claim threads:
   ```sql
   UPDATE DEAL_STATES SET STATUS='classifying', BATCH_ID='{uuid}', UPDATED_AT=CURRENT_TIMESTAMP
   WHERE THREAD_ID IN (
     SELECT DISTINCT ds.THREAD_ID FROM DEAL_STATES ds
     WHERE ds.STATUS = 'pending_classification'
       AND NOT EXISTS (
         SELECT 1 FROM DEAL_STATES ds2
         WHERE ds2.THREAD_ID = ds.THREAD_ID
           AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
           AND ds2.STATUS IN ('pending', 'filtering')
       )
     LIMIT {batchSize}
   ) AND STATUS = 'pending_classification'
   ```
3. If no rows claimed, look for stuck classify batches (same pattern as filter, with `STATUS='classifying'`)
4. Insert BATCH_EVENTS row: `(triggerHash=uuid, batchId, batchType='classify', eventType='new')`
5. Return `{ batch_id, count, attempts }`

### `run-filter-pipeline`

Orchestrator that claims and processes filter batches concurrently until exhausted.

**Inputs:**

- `max-concurrent` (default 5) — max parallel workers
- `filter-batch-size` (default 200) — items per batch
- `max-retries` (default 3) — max attempts per batch before dead letter
- `chunk-size` (default 50) — content fetcher chunk size
- `fetch-timeout-ms` (default 30000)
- SxT auth, schema, content-fetcher-url

**Output:** `{ batches_processed, batches_failed, total_filtered, total_rejected }`

**Worker flow (per batch):**

1. Receive batch_id, deal_state rows, current attempts count
2. Fetch email headers from content fetcher (chunked, 3 retries with exponential backoff, 429 handling)
3. Apply `isRejected()` filter rules from `filter-rules.js`
4. Execute two UPDATEs via `executeSql()`:
   - Passed IDs: `SET STATUS='pending_classification'`
   - Rejected IDs: `SET STATUS='filter_rejected'`
5. Insert BATCH_EVENTS with `eventType='complete'`

**Worker retry on failure:**

```
currentAttempts = batch.attempts
while currentAttempts < maxRetries:
  try:
    process(batch)
    insertBatchEvent(batchId, 'complete')
    return success
  catch:
    currentAttempts++
    insertBatchEvent(batchId, 'retrigger')
    if currentAttempts >= maxRetries:
      // dead letter — leave in 'filtering', stop
      return failure
    exponentialBackoff(currentAttempts)
```

### `run-classify-pipeline`

Orchestrator that claims and processes classify batches concurrently until exhausted.

**Inputs:**

- `max-concurrent` (default 3) — max parallel workers
- `classify-batch-size` (default 5) — threads per batch
- `max-retries` (default 3) — max attempts per batch before dead letter
- `chunk-size` (default 10) — content fetcher chunk size
- `fetch-timeout-ms` (default 120000)
- SxT auth, schema, content-fetcher-url, hyperbolic-key, primary-model, fallback-model

**Output:** `{ batches_processed, batches_failed, total_deals, total_not_deals }`

**Worker flow (per batch):**

1. Receive batch_id, deal_state rows, current attempts count
2. Check for existing audit in DB (retry case) — if valid, use it as in-memory audit
3. If no audit:
   a. Fetch email content from content fetcher (chunked, 3 retries with exponential backoff)
   b. Build prompt via `buildPrompt()`
   c. Run AI classification via 4-layer resilience pipeline:
   - Layer 0: Primary model (`callModel()` with HTTP retries + exponential backoff)
   - Layer 1: Local JSON repair (`parseAndValidate()`)
   - Layer 2: Corrective retry (same model, send broken output + error)
   - Layer 3: Fallback model
     d. Save audit checkpoint to AI_EVALUATION_AUDITS
4. **In-memory data flow from here** — pass `threads` array through:
   a. Upsert EMAIL_THREAD_EVALUATIONS (batch SQL)
   b. Upsert DEALS, delete non-deal threads (batch SQL)
   c. Insert DEAL_CONTACTS from main_contact enrichment (batch SQL)
   d. Update DEAL_STATES to terminal status (`deal`/`not_deal`)
5. Insert BATCH_EVENTS with `eventType='complete'`

**Same retry pattern as filter pipeline.** On retry, step 2 picks up the existing audit so AI is not re-invoked.

---

## Concurrency Pool

Shared by both pipeline commands. Lives in `src/lib/pipeline.js`.

```
runPool(claimFn, workerFn, { maxConcurrent, maxRetries }):
  active = Set()
  results = { processed: 0, failed: 0 }

  while true:
    if active.size < maxConcurrent:
      batch = claimFn()
      if batch is null:
        if active.size == 0: break           // exhausted
        await any worker in active to finish  // wait for capacity or new items
        continue
      worker = workerFn(batch)
      active.add(worker)
      worker.finally(() => active.delete(worker))
    else:
      await any worker in active to finish

  await all remaining active workers
  return results
```

Key behavior:

- Claim returns null → wait for active workers to finish → try claiming again (workers completing may produce new claimable items)
- Only exits when claim returns null AND no active workers
- Failed workers don't stop the pool — they increment `results.failed` and free their slot

---

## Workflows

### `dealsync-compact-sync.yml`

```yaml
name: dealsync-compact-sync
on:
  workflow_dispatch:
    inputs:
      offset:
        type: string
        default: '0'
      limit:
        type: string
        default: '500'

jobs:
  sync:
    runs-on: ubuntu-latest
    environment: '0x226c2acd33ef649bff3339670b6de489880a094acf7390df6cad4b5e18a17665'
    steps:
      - uses: creatorland/dealsync-action@main
        with:
          command: sync-deal-states
          offset: ${{ inputs.offset }}
          limit: ${{ inputs.limit }}
          # SxT auth secrets...
```

### `dealsync-compact-filter.yml`

```yaml
name: dealsync-compact-filter
on:
  workflow_dispatch:
    inputs:
      max_concurrent:
        type: string
        default: '5'
      filter_batch_size:
        type: string
        default: '200'
      max_retries:
        type: string
        default: '3'
      chunk_size:
        type: string
        default: '50'
      fetch_timeout_ms:
        type: string
        default: '30000'

jobs:
  filter:
    runs-on: ubuntu-latest
    environment: '0x226c2acd33ef649bff3339670b6de489880a094acf7390df6cad4b5e18a17665'
    steps:
      - uses: creatorland/dealsync-action@main
        with:
          command: run-filter-pipeline
          max-concurrent: ${{ inputs.max_concurrent }}
          filter-batch-size: ${{ inputs.filter_batch_size }}
          max-retries: ${{ inputs.max_retries }}
          chunk-size: ${{ inputs.chunk_size }}
          fetch-timeout-ms: ${{ inputs.fetch_timeout_ms }}
          # SxT auth + content-fetcher-url secrets...
```

### `dealsync-compact-classify.yml`

```yaml
name: dealsync-compact-classify
on:
  workflow_dispatch:
    inputs:
      max_concurrent:
        type: string
        default: '3'
      classify_batch_size:
        type: string
        default: '5'
      max_retries:
        type: string
        default: '3'
      chunk_size:
        type: string
        default: '10'
      fetch_timeout_ms:
        type: string
        default: '120000'

jobs:
  classify:
    runs-on: ubuntu-latest
    environment: '0x226c2acd33ef649bff3339670b6de489880a094acf7390df6cad4b5e18a17665'
    steps:
      - uses: creatorland/dealsync-action@main
        with:
          command: run-classify-pipeline
          max-concurrent: ${{ inputs.max_concurrent }}
          classify-batch-size: ${{ inputs.classify_batch_size }}
          max-retries: ${{ inputs.max_retries }}
          chunk-size: ${{ inputs.chunk_size }}
          fetch-timeout-ms: ${{ inputs.fetch_timeout_ms }}
          # SxT auth + AI + content-fetcher-url secrets...
```

---

## New Files

```
src/commands/claim-filter-batch.js      # atomic filter batch claim
src/commands/claim-classify-batch.js    # atomic classify batch claim
src/commands/run-filter-pipeline.js     # filter orchestrator with concurrency pool
src/commands/run-classify-pipeline.js   # classify orchestrator with concurrency pool
src/lib/email-client.js                 # shared email fetching with standardized retry
src/lib/pipeline.js                     # concurrency pool + batch event helpers
.github/workflows/dealsync-compact-sync.yml
.github/workflows/dealsync-compact-filter.yml
.github/workflows/dealsync-compact-classify.yml
```

## Reused As-Is (imported, not reimplemented)

- `authenticate`, `executeSql`, `withTimeout` from `sxt-client.js`
- `callModel`, `parseAndValidate` from `ai-client.js`
- `isRejected` from `filter-rules.js`
- `buildPrompt` from `build-prompt.js`
- `sanitizeId`, `sanitizeSchema`, `sanitizeString`, `toSqlIdList` from `queries.js`
- All status constants from `queries.js`

## Existing Code Changes

None. Existing commands and workflows are untouched. The compact workflows are purely additive.

---

## Retry & Error Handling

All external calls use exponential backoff on retry:

| Client             | Retries                    | Backoff             | Notes                                   |
| ------------------ | -------------------------- | ------------------- | --------------------------------------- |
| SxT (`executeSql`) | Built-in 401 re-auth       | Existing            | Rate limiter fail-open                  |
| AI (`callModel`)   | 2 HTTP retries             | 1s \* 2^attempt     | 429: up to 10 waits, no budget consumed |
| Email client       | 3 retries                  | exponential backoff | 429: respect retryAfterMs               |
| Worker-level       | configurable `max-retries` | exponential backoff | Creates BATCH_EVENTS per attempt        |

### Dead letter handling

When a batch exhausts max retries:

- Stays in `filtering`/`classifying` status
- BATCH_EVENTS shows all attempts
- Claim queries exclude batches with `>= max_retries` attempts
- Handled separately (future dead letter processing, outside this design)

---

## Multi-Instance Safety

- **Atomic claiming** — UPDATE...WHERE subquery prevents two instances from claiming the same rows
- **Batch events** — every claim and retry is recorded, providing observability across instances
- **Stuck batch re-claiming** — claim queries pick up stuck batches (>5 min in non-terminal status) with attempts < max_retries, naturally folding retrigger logic into the claim
- **No shared state** — each instance manages its own promise pool; coordination happens entirely through DB state
