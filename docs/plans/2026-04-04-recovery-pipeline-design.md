# Recovery Pipeline Design

**Goal:** Reprocess `failed` deal_states by checking if emails are still fetchable. Recoverable emails go back to `pending`; permanently gone emails get marked `dead`.

**Architecture:** New command `run-recovery-pipeline` following the same runPool/concurrent pattern as filter/classify. Groups work by user for efficient email service requests. Processes oldest failures first.

**Tech Stack:** Existing fetchEmails client, runPool concurrency, SxT SQL helpers.

---

## New Status: `dead`

Terminal status meaning the email is permanently unfetchable (message deleted or user revoked OAuth access). The recovery pipeline only processes `failed` rows and never touches `dead` rows, preventing reprocessing loops.

**Updated state machine:**
```
pending -> filtering -> pending_classification -> classifying -> deal | not_deal
                     -> filter_rejected
filtering (stuck) -> failed (dead-letter)
classifying (stuck) -> failed (dead-letter)
failed -> pending (recovery: fetchable, BATCH_ID nulled)
failed -> dead (recovery: unfetchable after retries)
```

## Command

New entry in `COMMANDS` map: `run-recovery-pipeline` -> `runRecoveryPipeline()`.

## Flow

1. **Claim function** queries distinct users with `failed` rows, ordered by oldest `UPDATED_AT`. For each user, selects up to `recovery-claim-size` failed rows (oldest first). Returns `{ userId, rows }` as a work item.

2. **Worker function** (concurrent via `runPool`):
   - Build `metaByMessageId` map from rows (same pattern as filter pipeline)
   - Call `fetchEmails(messageIds, metaByMessageId, { format: 'metadata', ... })` with existing retry/chunk config
   - Determine fetchable vs unfetchable message IDs
   - Fetchable -> `UPDATE SET STATUS = 'pending', BATCH_ID = NULL` (re-enters pipeline from scratch)
   - Unfetchable -> `UPDATE SET STATUS = 'dead'`

3. **After pool**: Log totals (recovered, dead, per user).

## Concurrency

- Multiple users processed concurrently via `runPool` with `pipeline-recovery-max-concurrent`
- Within a user, `fetchEmails()` handles chunked concurrent fetches via `chunkSize`
- Direct writes per batch (no WriteBatcher needed -- only 2 UPDATE operations per work item)

## SQL Changes

### New status constant
```js
STATUS.DEAD = 'dead'
```

### New SQL helpers in deal-states.js

**`findUsersWithFailedRows(schema, limit)`**
```sql
SELECT DISTINCT USER_ID, MIN(UPDATED_AT) AS OLDEST
FROM {schema}.DEAL_STATES
WHERE STATUS = 'failed'
GROUP BY USER_ID
ORDER BY OLDEST ASC
LIMIT {limit}
```

**`selectFailedByUser(schema, userId, limit)`**
```sql
SELECT ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, SYNC_STATE_ID
FROM {schema}.DEAL_STATES
WHERE STATUS = 'failed' AND USER_ID = '{userId}'
ORDER BY UPDATED_AT ASC
LIMIT {limit}
```

**`resetToPending(schema, quotedIds)`**
```sql
UPDATE {schema}.DEAL_STATES
SET STATUS = 'pending', BATCH_ID = NULL, UPDATED_AT = CURRENT_TIMESTAMP
WHERE ID IN ({quotedIds})
```

Reuse existing `updateStatusByIds()` for marking `dead`.

## Configuration

| Input | Default | Purpose |
|---|---|---|
| `pipeline-recovery-max-concurrent` | 10 | Concurrent users being processed |
| `recovery-claim-size` | 500 | Max failed rows per user per run |
| `pipeline-fetch-chunk-size` | (existing) | Chunk size for fetchEmails |
| `pipeline-fetch-timeout-ms` | (existing) | Fetch timeout |
| `email-max-retries` | (existing) | Retries before marking dead |
| `db-max-retries` | (existing) | SxT retry count |

## Workflow (W3 betanet)

```yaml
name: ds-recovery-1h
on:
  schedule:
    - cron: '0 * * * *'
  workflow_dispatch:

jobs:
  recovery:
    timeout-minutes: 60
    runs-on: ubuntu-latest
    environment: '0x12c3f02f05146db270f75d2abda0557e3250598c1d7029bd221ec3d028bbba53'
    steps:
      - id: recovery
        name: Run recovery pipeline
        timeout-minutes: 60
        uses: creatorland/dealsync-action@{commit}
        with:
          command: run-recovery-pipeline
          sxt-auth-url: ${{ secrets.SXT_AUTH_URL }}
          sxt-auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
          sxt-api-url: ${{ secrets.SXT_API_URL }}
          sxt-biscuit: ${{ secrets.SXT_BISCUIT }}
          sxt-schema: ${{ secrets.SXT_SCHEMA }}
          sxt-rate-limiter-url: ${{ secrets.SXT_RATE_LIMITER_URL }}
          sxt-rate-limiter-api-key: ${{ secrets.SXT_RATE_LIMITER_API_KEY }}
          email-provider: ${{ secrets.EMAIL_PROVIDER }}
          email-content-fetcher-url: ${{ secrets.EMAIL_CONTENT_FETCHER_URL }}
          email-service-url: ${{ secrets.EMAIL_SERVICE_URL }}
          pipeline-recovery-max-concurrent: ${{ secrets.PIPELINE_RECOVERY_MAX_CONCURRENT }}
          recovery-claim-size: ${{ secrets.RECOVERY_CLAIM_SIZE }}
          pipeline-fetch-chunk-size: ${{ secrets.PIPELINE_FETCH_CHUNK_SIZE }}
          pipeline-fetch-timeout-ms: ${{ secrets.PIPELINE_FETCH_TIMEOUT_MS }}
          pipeline-max-retries: ${{ secrets.PIPELINE_MAX_RETRIES }}
          db-max-retries: ${{ secrets.DB_MAX_RETRIES }}
          email-max-retries: ${{ secrets.EMAIL_MAX_RETRIES }}
```
