# Recovery Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `run-recovery-pipeline` command that reprocesses `failed` deal_states — fetchable emails reset to `pending`, unfetchable ones marked `dead`.

**Architecture:** New command following the existing runPool pattern. Groups work by user (oldest failures first), processes users concurrently. Uses existing `fetchEmails()` with `format: 'metadata'` to check fetchability. Direct SQL writes per user batch (no WriteBatcher needed).

**Tech Stack:** Node 24 ESM, existing SxT SQL helpers, existing fetchEmails client, runPool concurrency pool.

---

### Task 1: Add `dead` status and new SQL helpers

**Files:**
- Modify: `src/lib/sql/deal-states.js:15-24` (STATUS constant)
- Modify: `src/lib/sql/deal-states.js` (add 3 new helpers)

**Step 1: Add DEAD to STATUS constant**

In `src/lib/sql/deal-states.js`, add `DEAD` to the STATUS object:

```js
export const STATUS = {
  PENDING: 'pending',
  FILTERING: 'filtering',
  PENDING_CLASSIFICATION: 'pending_classification',
  CLASSIFYING: 'classifying',
  DEAL: 'deal',
  NOT_DEAL: 'not_deal',
  FILTER_REJECTED: 'filter_rejected',
  FAILED: 'failed',
  DEAD: 'dead',
}
```

**Step 2: Add `findUsersWithFailedRows` SQL helper**

Add to the `dealStates` object in `src/lib/sql/deal-states.js`:

```js
  findUsersWithFailedRows: (schema, limit = 200) => {
    const s = sanitizeSchema(schema)
    return `SELECT USER_ID, MIN(UPDATED_AT) AS OLDEST FROM ${s}.DEAL_STATES WHERE STATUS = 'failed' GROUP BY USER_ID ORDER BY OLDEST ASC LIMIT ${Number(limit)}`
  },
```

**Step 3: Add `selectFailedByUser` SQL helper**

```js
  selectFailedByUser: (schema, userId, limit = 500) => {
    const s = sanitizeSchema(schema)
    const uid = sanitizeString(userId)
    return `SELECT ID, EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${s}.DEAL_STATES WHERE STATUS = 'failed' AND USER_ID = '${uid}' ORDER BY UPDATED_AT ASC LIMIT ${Number(limit)}`
  },
```

**Step 4: Add `resetToPending` SQL helper**

```js
  resetToPending: (schema, quotedIds) => {
    const s = sanitizeSchema(schema)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'pending', BATCH_ID = NULL, UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds.join(',')})`
  },
```

**Step 5: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js`
Expected: All existing tests PASS (no behavior changed, only additions)

**Step 6: Commit**

```bash
git add src/lib/sql/deal-states.js
git commit -m "feat: add dead status and recovery SQL helpers"
```

---

### Task 2: Create the recovery pipeline command

**Files:**
- Create: `src/commands/run-recovery-pipeline.js`

**Step 1: Create the recovery pipeline**

Create `src/commands/run-recovery-pipeline.js`:

```js
import * as core from '@actions/core'
import { runPool } from '../lib/pipeline.js'
import { authenticate, executeSql } from '../lib/db.js'
import { fetchEmails } from '../lib/emails.js'
import {
  sanitizeSchema,
  sanitizeId,
  STATUS,
  dealStates as dealStatesSql,
} from '../lib/sql/index.js'

export async function runRecoveryPipeline() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const contentFetcherUrl = core.getInput('email-content-fetcher-url')
  const emailProvider = core.getInput('email-provider') || ''
  const emailServiceUrl = core.getInput('email-service-url')
  const maxConcurrent = parseInt(core.getInput('pipeline-recovery-max-concurrent') || '10', 10)
  const claimSize = parseInt(core.getInput('recovery-claim-size') || '500', 10)
  const fetchChunkSize = parseInt(core.getInput('pipeline-fetch-chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('pipeline-fetch-timeout-ms') || '30000', 10)
  const maxRetries = parseInt(core.getInput('pipeline-max-retries') || '2', 10)

  console.log(
    `[run-recovery-pipeline] starting (maxConcurrent=${maxConcurrent}, claimSize=${claimSize}, fetchChunkSize=${fetchChunkSize}, fetchTimeoutMs=${fetchTimeoutMs})`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  let totalRecovered = 0
  let totalDead = 0
  let usersProcessed = 0
  const runStart = Date.now()

  // Claim function: get next user with failed rows
  let userQueue = []
  let userQueueExhausted = false

  async function claimBatch() {
    // Refill user queue if empty
    if (userQueue.length === 0 && !userQueueExhausted) {
      const users = await exec(dealStatesSql.findUsersWithFailedRows(schema))
      if (!users || users.length === 0) {
        userQueueExhausted = true
        return null
      }
      userQueue = users
      console.log(`[run-recovery-pipeline] found ${users.length} users with failed rows`)
    }

    if (userQueue.length === 0) return null

    const user = userQueue.shift()
    const userId = user.USER_ID

    // Select failed rows for this user
    const rows = await exec(dealStatesSql.selectFailedByUser(schema, userId, claimSize))
    if (!rows || rows.length === 0) return null

    const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
    console.log(
      `[run-recovery-pipeline] claimed ${rows.length} failed rows for user ${userId} (elapsed: ${elapsed}s)`,
    )

    return { batch_id: `recovery:${userId}`, count: rows.length, attempts: 0, rows, userId }
  }

  // Worker function: check fetchability and update statuses
  async function processRecoveryBatch(batch) {
    const { rows, userId } = batch
    const batchStart = Date.now()

    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const syncStateId = rows[0].SYNC_STATE_ID
    const messageIds = rows.map((r) => r.MESSAGE_ID)

    // Fetch with metadata-only format
    let emails = []
    try {
      emails = await fetchEmails(messageIds, metaByMessageId, {
        contentFetcherUrl,
        emailProvider,
        emailServiceUrl,
        userId,
        syncStateId,
        chunkSize: fetchChunkSize,
        fetchTimeoutMs,
        format: 'metadata',
      })
    } catch (err) {
      console.log(
        `[run-recovery-pipeline] fetch failed for user ${userId}: ${err.message}`,
      )
      // All unfetchable on total failure
      emails = []
    }

    const fetchedMessageIds = new Set(emails.map((e) => e.messageId || e.id))

    // Split into recoverable vs dead
    const recoverableIds = []
    const deadIds = []

    for (const row of rows) {
      if (fetchedMessageIds.has(row.MESSAGE_ID)) {
        recoverableIds.push(row.EMAIL_METADATA_ID)
      } else {
        deadIds.push(row.EMAIL_METADATA_ID)
      }
    }

    // Write: reset recoverable to pending (with BATCH_ID = NULL)
    if (recoverableIds.length > 0) {
      const quotedIds = recoverableIds.map((id) => `'${sanitizeId(id)}'`)
      await exec(dealStatesSql.resetToPending(schema, quotedIds))
    }

    // Write: mark dead
    if (deadIds.length > 0) {
      const quotedIds = deadIds.map((id) => `'${sanitizeId(id)}'`)
      await exec(dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.DEAD))
    }

    totalRecovered += recoverableIds.length
    totalDead += deadIds.length
    usersProcessed++

    const totalMs = Date.now() - batchStart
    const elapsed = ((Date.now() - runStart) / 1000).toFixed(1)
    console.log(
      `[run-recovery-pipeline] user ${userId}: ${recoverableIds.length} recovered, ${deadIds.length} dead (${totalMs}ms) | total: recovered=${totalRecovered}, dead=${totalDead}, users=${usersProcessed}, elapsed=${elapsed}s`,
    )
  }

  const poolResults = await runPool(claimBatch, processRecoveryBatch, {
    maxConcurrent,
    maxRetries,
    onDeadLetter: async () => {},
  })

  const runMs = Date.now() - runStart
  console.log(
    `[run-recovery-pipeline] done — recovered=${totalRecovered}, dead=${totalDead}, users=${usersProcessed}, batches=${poolResults.processed}, failed=${poolResults.failed} (${(runMs / 1000).toFixed(1)}s)`,
  )

  return {
    total_recovered: totalRecovered,
    total_dead: totalDead,
    users_processed: usersProcessed,
    batches_processed: poolResults.processed,
    batches_failed: poolResults.failed,
  }
}
```

**Step 2: Commit**

```bash
git add src/commands/run-recovery-pipeline.js
git commit -m "feat: add run-recovery-pipeline command"
```

---

### Task 3: Register the command and add action inputs

**Files:**
- Modify: `src/main.js:1-14` (add import and COMMANDS entry)
- Modify: `action.yml` (add new inputs)

**Step 1: Register in main.js**

Add import at top of `src/main.js`:

```js
import { runRecoveryPipeline } from './commands/run-recovery-pipeline.js'
```

Add entry to COMMANDS:

```js
'run-recovery-pipeline': runRecoveryPipeline,
```

**Step 2: Add inputs to action.yml**

Add after the existing `pipeline-max-retries` input:

```yaml
  pipeline-recovery-max-concurrent:
    description: 'Max parallel users for recovery pipeline'
    default: '10'
  recovery-claim-size:
    description: 'Max failed rows per user per recovery run'
    default: '500'
```

Update the `description` at top of action.yml to include the new command:

```yaml
description: 'Dealsync pipeline action — run-filter-pipeline, run-classify-pipeline, run-recovery-pipeline, sync-deal-states, eval, eval-compare'
```

**Step 3: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js`
Expected: All tests PASS

**Step 4: Package**

Run: `npm run package`
Expected: `dist/index.js` rebuilt successfully

**Step 5: Commit**

```bash
git add src/main.js action.yml dist/index.js
git commit -m "feat: register run-recovery-pipeline command and add action inputs"
```

---

### Task 4: Create the betanet workflow YAML

**Files:**
- Create: `.github/workflows/dealsync-recovery.betanet.yml`

**Step 1: Create the workflow file**

```yaml
name: ds-recovery-1h
authority: '0x7504607d40696d6Ad89e72f4D820fD1fBCD1143b'
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
        uses: creatorland/dealsync-action@COMMIT_HASH
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

Note: Replace `COMMIT_HASH` with the actual commit hash after pushing.

**Step 2: Commit**

```bash
git add .github/workflows/dealsync-recovery.betanet.yml
git commit -m "feat: add recovery pipeline betanet workflow"
```

---

### Task 5: Push, deploy to W3 betanet

**Step 1: Push to main**

```bash
git push origin main
```

**Step 2: Update workflow YAML with actual commit hash**

After pushing, get the commit hash and update the `uses:` line in `.github/workflows/dealsync-recovery.betanet.yml`.

**Step 3: Deploy to W3 betanet**

Deploy the workflow YAML to `https://1.w3-betanet.io` using `mcp__w3__deploy-workflow`.

**Step 4: Add secrets to W3 environment**

Ensure `PIPELINE_RECOVERY_MAX_CONCURRENT` and `RECOVERY_CLAIM_SIZE` are set in the W3 betanet environment secrets. Suggested values:
- `PIPELINE_RECOVERY_MAX_CONCURRENT=10`
- `RECOVERY_CLAIM_SIZE=500`

**Step 5: Verify first run**

Trigger the workflow manually and check logs for expected output:
- Users found with failed rows
- Per-user recovery/dead counts
- Final totals
