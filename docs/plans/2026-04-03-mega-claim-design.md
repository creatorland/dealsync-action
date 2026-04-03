# Mega-Claim Optimization Design

Date: 2026-04-03

## Problem

The classify pipeline spends 86% of wall time on serial SxT round-trips, claiming 5 threads at a time. With 384 claims per run, each taking ~1.35s (3 SxT calls), the claim loop consumes 518s of a 600s run. Only 24 of 70 worker slots are ever active because the serial claim loop can't feed them fast enough.

## Solution

Two-phase mega-claim: claim many threads in one SxT call, split into sub-batches in a second call, then process sub-batches through the existing worker pipeline unchanged.

## Batch ID Format

| Format | Meaning | Example |
|--------|---------|---------|
| `{uuid}` | Normal sub-batch (current format) | `019d4a2b-...` |
| `mega:{megaId}` | Mega-claim, not yet split into sub-batches | `mega:019d4a2b-...` |

Once split, the `mega:{megaId}` is gone. Sub-batches are plain UUIDs. All downstream systems (audit, stuck detector, retry, WriteBatcher) see normal batches.

## Claim Flow (3 SxT calls per mega-claim)

### Step 1: Mega-claim

```sql
UPDATE DEAL_STATES
SET STATUS = 'classifying', BATCH_ID = 'mega:{megaId}', UPDATED_AT = CURRENT_TIMESTAMP
WHERE THREAD_ID IN (
  SELECT DISTINCT ds.THREAD_ID
  FROM DEAL_STATES ds
  WHERE ds.STATUS = 'pending_classification'
  AND NOT EXISTS (
    SELECT 1 FROM DEAL_STATES ds2
    WHERE ds2.THREAD_ID = ds.THREAD_ID
    AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
    AND ds2.STATUS IN ('pending', 'filtering')
  )
  LIMIT {claimSize}
) AND STATUS = 'pending_classification'
```

Optimistic lock: `STATUS = 'pending_classification'` in outer WHERE prevents concurrent claims on the same rows.

### Step 2: SELECT claimed rows

```sql
SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID,
       ete.AI_SUMMARY, ete.IS_DEAL, uss.EMAIL AS CREATOR_EMAIL
FROM DEAL_STATES ds
LEFT JOIN EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID
LEFT JOIN USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID
WHERE ds.BATCH_ID = 'mega:{megaId}'
```

### Step 3: Split and re-stamp

JS groups rows by thread, chunks into groups of `classify-batch-size` threads, generates a UUID per group.

```sql
UPDATE DEAL_STATES SET BATCH_ID = CASE
  WHEN THREAD_ID IN ('t1','t2','t3','t4','t5') THEN '{subId1}'
  WHEN THREAD_ID IN ('t6','t7','t8','t9','t10') THEN '{subId2}'
  ...
END
WHERE BATCH_ID = 'mega:{megaId}'
```

Optimistic lock on re-stamp: `WHERE BATCH_ID = 'mega:{megaId}'` ensures only one process splits a mega-claim.

After step 3, each sub-batch is a normal batch with a plain UUID. Workers, audit, batch events, stuck detector all work unchanged.

## Crash Recovery

If the process dies between step 1 and step 3, rows are stuck with `STATUS = 'classifying'` and `BATCH_ID LIKE 'mega:%'`.

The stuck detector (findStuckBatches) already finds batches where `STATUS = 'classifying'` and `UPDATED_AT` is stale. When it encounters a BATCH_ID starting with `mega:`, it re-runs steps 2-3 (SELECT rows, split, re-stamp) instead of reprocessing directly.

Optimistic lock on re-split: the CASE WHEN UPDATE targets `WHERE BATCH_ID = 'mega:{megaId}'`. If two processes detect the same stuck mega-batch, only one succeeds.

## Pool Integration

`runPool` stays the same loop structure. Two changes:

1. `claimBatch()` returns an **array** of sub-batch objects instead of a single batch
2. `runPool` dispatches each sub-batch as a separate worker

With claim-size=100 and classify-batch-size=5, each mega-claim yields ~20 sub-batches. The pool feeds 20 workers per ~1.2s claim cycle. In 33s (one AI call duration), it can feed `33 / 1.2 * 20 = ~550` sub-batches. This saturates all 70 worker slots.

## sanitizeId Change

Current regex: `/^[a-zA-Z0-9_-]+$/`
New regex: `/^[a-zA-Z0-9_:-]+$/`

The `:` character is added to support the `mega:{uuid}` format. `:` has no special meaning in SQL string literals and does not enable injection.

## Input / Config Audit

### New input

| Input | Default | Purpose |
|-------|---------|---------|
| `claim-size` | `100` | Threads per mega-claim (LIMIT in step 1 SQL) |

### Existing input clarification

`classify-batch-size` (default `5`) now exclusively controls AI prompt size (threads per LLM call). It no longer controls the claim LIMIT.

### Betanet workflow config cleanup

Current betanet workflow inputs have confusing naming and inconsistent defaults. Proposed cleanup:

| Input | Old name | New name | Betanet default | Purpose |
|-------|----------|----------|-----------------|---------|
| Workers | `max_concurrent` | `max_concurrent` | `70` | Parallel worker pool size |
| Claim size | (new) | `claim_size` | `100` | Threads per mega-claim |
| AI batch | `classify_batch_size` | `classify_batch_size` | `5` | Threads per AI prompt |
| Retries | `max_retries` | `max_retries` | `6` | Retries before dead-letter |
| Fetch chunk | `chunk_size` | `fetch_chunk_size` | `10` | Emails per content-fetcher request |
| Fetch timeout | `fetch_timeout_ms` | `fetch_timeout_ms` | `240000` | Content-fetcher request timeout |
| Email provider | `email_provider` | `email_provider` | `content-fetcher` | Email data source |

### action.yml default alignment

Update action.yml defaults to match production values so betanet workflows don't need to override everything:

| Input | Current default | New default | Reason |
|-------|----------------|-------------|--------|
| `max-concurrent` | `5` | `70` | 5 was never used in production |
| `chunk-size` | `50` | `10` | Betanet uses 10 |
| `fetch-timeout-ms` | `30000` | `240000` | Betanet uses 240000 |

## Performance

| Metric | Current | After mega-claim |
|--------|---------|-----------------|
| Claims per 2,122 rows | 384 | ~4 |
| SxT calls for claiming | 1,152 | 12 |
| Claim time | 518s (86%) | ~5s (5%) |
| Active workers | 24/70 | 70/70 |
| Rows/hour | ~11,000 | ~160,000 |
| 500K backlog | ~45h | ~3h |

## What Changes

| Component | Change |
|-----------|--------|
| `action.yml` | Add `claim-size` input, rename `chunk-size` to `fetch-chunk-size`, update defaults |
| `sanitize.js` | Allow `:` in `sanitizeId` regex |
| `deal-states.js` | Add `megaClaimClassifyBatch` and `restampSubBatches` SQL builders |
| `run-classify-pipeline.js` | `claimBatch()` does two-phase mega-claim, returns array of sub-batches |
| `pipeline.js` | `runPool` accepts array from `claimFn`, dispatches each sub-batch |
| Stuck detector | Check for `mega:` prefix, re-split instead of reprocess |
| Betanet workflow YAMLs | Add `claim_size` input, rename `chunk_size` to `fetch_chunk_size` |

## What Stays the Same

| Component | Status |
|-----------|--------|
| `processClassifyBatch()` worker | No change |
| WriteBatcher | No change |
| Audit system (1 batch = 1 audit) | No change |
| Batch events | No change |
| Dead-letter / retry logic | No change (sub-batches are normal UUIDs) |
| AI prompt (5 threads per call) | No change |
| Optimistic lock via status transition | No change |
| Concurrency group serialization | No change |
