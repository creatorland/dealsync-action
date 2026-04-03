# Mega-Claim Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reduce classify pipeline SxT claim overhead from 86% to ~5% of wall time by claiming threads in bulk and splitting into sub-batches, yielding ~13x throughput improvement.

**Architecture:** Two-phase mega-claim: one UPDATE claims N threads with a `mega:{id}` BATCH_ID, one SELECT reads them back, one UPDATE re-stamps into sub-batches of 5 with plain UUIDs. Workers, audit, retry, and WriteBatcher are unchanged.

**Tech Stack:** Node 24 ESM, Jest (--experimental-vm-modules), SxT SQL, W3 workflow YAML

**Testing on staging first:** Deploy to testnet (`https://1.w3-testnet.io`) with `claim-size=30` before betanet.

---

### Task 1: Update `sanitizeId` to allow `:` in batch IDs

**Files:**
- Modify: `src/lib/sql/sanitize.js:4-8`
- Modify: `__tests__/sql/deal-states.test.js:145-155`

**Step 1: Write failing test**

In `__tests__/sql/deal-states.test.js`, add a test in the "SQL injection prevention" describe block:

```js
it('sanitizeId allows colon for mega batch IDs', () => {
  expect(() => dealStates.claimClassifyBatch(S, 'mega:019d4a2b-1234', 5)).not.toThrow()
  const sql = dealStates.claimClassifyBatch(S, 'mega:019d4a2b-1234', 5)
  expect(sql).toContain("BATCH_ID = 'mega:019d4a2b-1234'")
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js -t "allows colon"`
Expected: FAIL with "Invalid ID format: mega:019d4a2b-1234"

**Step 3: Update sanitizeId regex**

In `src/lib/sql/sanitize.js` line 5, change:

```js
// Before
if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
// After
if (!/^[a-zA-Z0-9_:-]+$/.test(id)) {
```

**Step 4: Run test to verify it passes**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/lib/sql/sanitize.js __tests__/sql/deal-states.test.js
git commit -m "feat: allow colon in sanitizeId for mega batch IDs"
```

---

### Task 2: Add `restampSubBatches` SQL builder

**Files:**
- Modify: `src/lib/sql/deal-states.js`
- Modify: `__tests__/sql/deal-states.test.js`

**Step 1: Write failing test**

Add to `__tests__/sql/deal-states.test.js`:

```js
describe('restampSubBatches', () => {
  it('builds CASE WHEN UPDATE for sub-batch assignment', () => {
    const groups = [
      { subBatchId: 'sub-1', threadIds: ['t1', 't2', 't3'] },
      { subBatchId: 'sub-2', threadIds: ['t4', 't5'] },
    ]
    const sql = dealStates.restampSubBatches(S, 'mega:mega-id', groups)
    expect(sql).toContain(`UPDATE ${S}.DEAL_STATES`)
    expect(sql).toContain('SET BATCH_ID = CASE')
    expect(sql).toContain("WHEN THREAD_ID IN ('t1','t2','t3') THEN 'sub-1'")
    expect(sql).toContain("WHEN THREAD_ID IN ('t4','t5') THEN 'sub-2'")
    expect(sql).toContain('END')
    expect(sql).toContain("WHERE BATCH_ID = 'mega:mega-id'")
  })

  it('rejects invalid mega batch ID', () => {
    expect(() =>
      dealStates.restampSubBatches(S, "'; DROP TABLE --", [])
    ).toThrow('Invalid ID')
  })
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js -t "restampSubBatches"`
Expected: FAIL with "restampSubBatches is not a function"

**Step 3: Implement restampSubBatches**

Add to `src/lib/sql/deal-states.js` in the `dealStates` object:

```js
restampSubBatches: (schema, megaBatchId, groups) => {
  const s = sanitizeSchema(schema)
  const megaBid = sanitizeId(megaBatchId)
  const cases = groups.map(({ subBatchId, threadIds }) => {
    const sid = sanitizeId(subBatchId)
    const ids = threadIds.map((id) => `'${sanitizeId(id)}'`).join(',')
    return `WHEN THREAD_ID IN (${ids}) THEN '${sid}'`
  }).join(' ')
  return `UPDATE ${s}.DEAL_STATES SET BATCH_ID = CASE ${cases} END, UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${megaBid}'`
},
```

**Step 4: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/lib/sql/deal-states.js __tests__/sql/deal-states.test.js
git commit -m "feat: add restampSubBatches SQL builder for mega-claim splitting"
```

---

### Task 3: Update `runPool` to accept arrays from `claimFn`

**Files:**
- Modify: `src/lib/pipeline.js:25-87`
- Modify: `__tests__/pipeline.test.js`

**Step 1: Write failing test**

Add to `__tests__/pipeline.test.js` in the `runPool` describe:

```js
it('dispatches each sub-batch when claimFn returns an array', async () => {
  let callNum = 0
  const claimFn = jest.fn(async () => {
    callNum++
    if (callNum === 1) {
      return [
        { batch_id: 'sub-1', attempts: 0, rows: [] },
        { batch_id: 'sub-2', attempts: 0, rows: [] },
        { batch_id: 'sub-3', attempts: 0, rows: [] },
      ]
    }
    return null
  })
  const workerFn = jest.fn(async () => {})

  const results = await runPool(claimFn, workerFn, { maxConcurrent: 5, maxRetries: 3 })

  expect(results).toEqual({ processed: 3, failed: 0 })
  expect(workerFn).toHaveBeenCalledTimes(3)
  expect(workerFn).toHaveBeenCalledWith(
    expect.objectContaining({ batch_id: 'sub-1' }),
    { attempt: 0 },
  )
  expect(workerFn).toHaveBeenCalledWith(
    expect.objectContaining({ batch_id: 'sub-2' }),
    { attempt: 0 },
  )
  expect(workerFn).toHaveBeenCalledWith(
    expect.objectContaining({ batch_id: 'sub-3' }),
    { attempt: 0 },
  )
})

it('handles mix of array and single batch returns from claimFn', async () => {
  let callNum = 0
  const claimFn = jest.fn(async () => {
    callNum++
    if (callNum === 1) {
      return [
        { batch_id: 'sub-1', attempts: 0 },
        { batch_id: 'sub-2', attempts: 0 },
      ]
    }
    if (callNum === 2) return { batch_id: 'single-1', attempts: 0 }
    return null
  })
  const workerFn = jest.fn(async () => {})

  const results = await runPool(claimFn, workerFn, { maxConcurrent: 5, maxRetries: 3 })

  expect(results).toEqual({ processed: 3, failed: 0 })
  expect(workerFn).toHaveBeenCalledTimes(3)
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js -t "claimFn returns an array"`
Expected: FAIL — array gets passed as a single batch to workerFn

**Step 3: Update runPool to handle arrays**

In `src/lib/pipeline.js`, modify the `while (true)` loop inside `runPool`. Replace lines 70-84:

```js
while (true) {
  if (active.size < maxConcurrent) {
    const result = await claimFn()
    if (result === null) {
      if (active.size === 0) break
      await Promise.race(active)
      continue
    }
    // Normalize to array — claimFn can return a single batch or an array of sub-batches
    const batches = Array.isArray(result) ? result : [result]
    for (const batch of batches) {
      // Wait for a slot if pool is full
      while (active.size >= maxConcurrent) {
        await Promise.race(active)
      }
      const worker = runWorker(batch)
      active.add(worker)
      worker.finally(() => active.delete(worker))
    }
  } else {
    await Promise.race(active)
  }
}
```

**Step 4: Run all pipeline tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js`
Expected: All PASS (existing tests + new tests)

**Step 5: Commit**

```bash
git add src/lib/pipeline.js __tests__/pipeline.test.js
git commit -m "feat: runPool accepts array of sub-batches from claimFn"
```

---

### Task 4: Add `claim-size` input to `action.yml`

**Files:**
- Modify: `action.yml`

**Step 1: Add `claim-size` input**

Add after the `classify-batch-size` input (around line 85):

```yaml
  claim-size:
    description: 'Threads per mega-claim (run-classify-pipeline). Higher = fewer SxT round-trips. Each mega-claim is split into sub-batches of classify-batch-size for AI.'
    default: '5'
```

Default is `5` for backward compatibility — mega-claim is opt-in via workflow YAML.

**Step 2: Rename `chunk-size` to `fetch-chunk-size`**

In `action.yml`, rename the existing `chunk-size` input:

```yaml
  fetch-chunk-size:
    description: 'Emails per content-fetcher HTTP request (run-filter-pipeline, run-classify-pipeline)'
    default: '10'
```

Keep `chunk-size` as a deprecated alias — read both in the pipeline commands:

```js
const chunkSize = parseInt(core.getInput('fetch-chunk-size') || core.getInput('chunk-size') || '10', 10)
```

**Step 3: Update action.yml defaults to match production values**

| Input | Old default | New default |
|-------|-------------|-------------|
| `max-concurrent` | `5` | `70` |
| `fetch-timeout-ms` | `30000` | `120000` |

**Step 4: Commit**

```bash
git add action.yml
git commit -m "feat: add claim-size input, rename chunk-size to fetch-chunk-size, update defaults"
```

---

### Task 5: Implement mega-claim in `claimBatch()`

**Files:**
- Modify: `src/commands/run-classify-pipeline.js:26-132`
- Modify: `__tests__/run-classify-pipeline.test.js`

**Step 1: Read existing classify pipeline test**

Read `__tests__/run-classify-pipeline.test.js` to understand test structure and mocks.

**Step 2: Write failing test for mega-claim**

Add a test that verifies: when `claim-size > classify-batch-size`, `claimBatch` returns an array of sub-batches. The test should mock `executeSql` to return rows for the mega-claim SELECT, and verify:
- The mega-claim UPDATE uses `LIMIT {claimSize}` and `BATCH_ID` starts with `mega:`
- The re-stamp UPDATE uses CASE WHEN with sub-batch UUIDs
- The returned array has the correct number of sub-batches
- Each sub-batch has its own `batch_id`, `rows`, and `count`

**Step 3: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js -t "mega-claim"`
Expected: FAIL

**Step 4: Implement mega-claim claimBatch**

In `src/commands/run-classify-pipeline.js`, modify `claimBatch()`:

```js
const claimSize = parseInt(core.getInput('claim-size') || '5', 10)
const classifyBatchSize = parseInt(core.getInput('classify-batch-size') || '5', 10)
const fetchChunkSize = parseInt(core.getInput('fetch-chunk-size') || core.getInput('chunk-size') || '10', 10)

async function claimBatch() {
  const claimStart = Date.now()

  // If claim-size <= classify-batch-size, use current single-batch behavior
  if (claimSize <= classifyBatchSize) {
    const batchId = uuidv7()
    await exec(dealStatesSql.claimClassifyBatch(schema, batchId, classifyBatchSize))
    const rows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, batchId))
    const count = rows ? rows.length : 0
    console.log(`[run-classify-pipeline] claimed ${count} pending rows in ${Date.now() - claimStart}ms`)

    if (count > 0) {
      await insertBatchEvent(exec, schema, {
        triggerHash: batchId,
        batchId,
        batchType: 'classify',
        eventType: 'new',
      })
      return { batch_id: batchId, count, attempts: 0, rows }
    }

    // Fall through to stuck batch detection below
    return checkStuckBatches()
  }

  // --- MEGA-CLAIM PATH ---
  const megaId = uuidv7()
  const megaBatchId = `mega:${megaId}`

  // Step 1: Mega-claim — atomic UPDATE with LIMIT claimSize
  await exec(dealStatesSql.claimClassifyBatch(schema, megaBatchId, claimSize))

  // Step 2: SELECT claimed rows
  const allRows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, megaBatchId))
  const totalCount = allRows ? allRows.length : 0
  console.log(`[run-classify-pipeline] mega-claimed ${totalCount} rows in ${Date.now() - claimStart}ms`)

  if (totalCount === 0) {
    return checkStuckBatches()
  }

  // Group rows by thread
  const threadMap = new Map()
  for (const row of allRows) {
    if (!threadMap.has(row.THREAD_ID)) threadMap.set(row.THREAD_ID, [])
    threadMap.get(row.THREAD_ID).push(row)
  }
  const threadIds = [...threadMap.keys()]

  // Chunk threads into groups of classifyBatchSize
  const groups = []
  for (let i = 0; i < threadIds.length; i += classifyBatchSize) {
    const chunkThreadIds = threadIds.slice(i, i + classifyBatchSize)
    const subBatchId = uuidv7()
    const subRows = chunkThreadIds.flatMap((tid) => threadMap.get(tid))
    groups.push({ subBatchId, threadIds: chunkThreadIds, rows: subRows })
  }

  // Step 3: Re-stamp sub-batches with plain UUIDs
  await exec(dealStatesSql.restampSubBatches(schema, megaBatchId, groups))

  console.log(`[run-classify-pipeline] mega-claim split into ${groups.length} sub-batches`)

  // Insert batch events for each sub-batch
  const eventValues = groups.map(
    (g) => `('${g.subBatchId}', '${g.subBatchId}', 'classify', 'new', CURRENT_TIMESTAMP)`,
  )
  await batcher.pushBatchEvents(eventValues)

  // Return array of sub-batches for runPool to dispatch
  return groups.map((g) => ({
    batch_id: g.subBatchId,
    count: g.rows.length,
    attempts: 0,
    rows: g.rows,
  }))
}

// Extract stuck batch detection into helper
async function checkStuckBatches() {
  console.log(`[run-classify-pipeline] no pending rows, checking for stuck batches`)

  const stuckBatches = await exec(
    dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries),
  )

  if (!stuckBatches || stuckBatches.length === 0) {
    console.log(`[run-classify-pipeline] no stuck batches found, nothing to do`)
    return null
  }

  const stuckBatchId = stuckBatches[0].BATCH_ID
  const attempts = parseInt(stuckBatches[0].ATTEMPTS, 10)

  // Check if this is an unsplit mega-claim
  if (stuckBatchId.startsWith('mega:')) {
    console.log(`[run-classify-pipeline] found unsplit mega-claim ${stuckBatchId}, re-splitting`)

    const allRows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, stuckBatchId))
    const totalCount = allRows ? allRows.length : 0

    if (totalCount === 0) {
      console.log(`[run-classify-pipeline] mega-claim ${stuckBatchId} has no rows, skipping`)
      return null
    }

    // Re-split (same logic as above)
    const threadMap = new Map()
    for (const row of allRows) {
      if (!threadMap.has(row.THREAD_ID)) threadMap.set(row.THREAD_ID, [])
      threadMap.get(row.THREAD_ID).push(row)
    }
    const threadIds = [...threadMap.keys()]

    const groups = []
    for (let i = 0; i < threadIds.length; i += classifyBatchSize) {
      const chunkThreadIds = threadIds.slice(i, i + classifyBatchSize)
      const subBatchId = uuidv7()
      const subRows = chunkThreadIds.flatMap((tid) => threadMap.get(tid))
      groups.push({ subBatchId, threadIds: chunkThreadIds, rows: subRows })
    }

    await exec(dealStatesSql.restampSubBatches(schema, stuckBatchId, groups))

    const eventValues = groups.map(
      (g) => `('${g.subBatchId}', '${g.subBatchId}', 'classify', 'retrigger', CURRENT_TIMESTAMP)`,
    )
    await batcher.pushBatchEvents(eventValues)

    console.log(`[run-classify-pipeline] re-split mega-claim into ${groups.length} sub-batches`)

    return groups.map((g) => ({
      batch_id: g.subBatchId,
      count: g.rows.length,
      attempts,
      rows: g.rows,
    }))
  }

  // Normal stuck batch — existing behavior
  console.log(`[run-classify-pipeline] re-claiming stuck batch ${stuckBatchId} (attempts=${attempts})`)

  const stuckRows = await exec(dealStatesSql.selectEmailsWithEvalAndCreator(schema, stuckBatchId))
  await exec(dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId))

  const triggerHash = uuidv7()
  await insertBatchEvent(exec, schema, {
    triggerHash,
    batchId: stuckBatchId,
    batchType: 'classify',
    eventType: 'retrigger',
  })

  const stuckCount = stuckRows ? stuckRows.length : 0
  return { batch_id: stuckBatchId, count: stuckCount, attempts, rows: stuckRows }
}
```

**Step 5: Update `chunkSize` references**

In `run-classify-pipeline.js`, update the `chunkSize` variable to read from the new input name:

```js
const fetchChunkSize = parseInt(core.getInput('fetch-chunk-size') || core.getInput('chunk-size') || '10', 10)
```

Update all references from `chunkSize` to `fetchChunkSize` in the file.

Do the same in `src/commands/run-filter-pipeline.js`.

**Step 6: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js`
Expected: All PASS

**Step 7: Commit**

```bash
git add src/commands/run-classify-pipeline.js src/commands/run-filter-pipeline.js
git commit -m "feat: implement two-phase mega-claim with sub-batch splitting"
```

---

### Task 6: Run full test suite and package

**Step 1: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js`
Expected: All PASS

**Step 2: Package**

Run: `npm run package`
Expected: `dist/index.js` regenerated without errors

**Step 3: Commit dist**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist for mega-claim"
```

---

### Task 7: Update testnet workflow for staging test

**Files:**
- Modify: `.github/workflows/dealsync-classify.testnet.yml`

**Step 1: Add `claim_size` input and update `chunk_size` name**

```yaml
on:
  workflow_dispatch:
    inputs:
      max_concurrent:
        type: string
        default: '70'
      claim_size:
        type: string
        default: '30'
      classify_batch_size:
        type: string
        default: '5'
      max_retries:
        type: string
        default: '6'
      fetch_chunk_size:
        type: string
        default: '10'
      fetch_timeout_ms:
        type: string
        default: '240000'
      email_provider:
        type: string
        default: 'content-fetcher'
```

Update the step `with:` block:

```yaml
        with:
          command: run-classify-pipeline
          max-concurrent: ${{ inputs.max_concurrent }}
          claim-size: ${{ inputs.claim_size }}
          classify-batch-size: ${{ inputs.classify_batch_size }}
          max-retries: ${{ inputs.max_retries }}
          fetch-chunk-size: ${{ inputs.fetch_chunk_size }}
          fetch-timeout-ms: ${{ inputs.fetch_timeout_ms }}
          # ... secrets unchanged
```

**Step 2: Do the same for filter testnet workflow**

Update `.github/workflows/dealsync-filter.testnet.yml`:
- Rename `chunk_size` to `fetch_chunk_size` in inputs and `with:` block

**Step 3: Commit**

```bash
git add .github/workflows/dealsync-classify.testnet.yml .github/workflows/dealsync-filter.testnet.yml
git commit -m "feat: add claim-size to testnet workflows, rename chunk-size to fetch-chunk-size"
```

---

### Task 8: Deploy to testnet and validate

**Step 1: Deploy classify testnet workflow**

Deploy to `https://1.w3-testnet.io` with `claim_size=30` (conservative — 6 parallel AI calls per mega-claim).

**Step 2: Trigger a test run**

Trigger `dealsync-classify-testnet` via `mcp__w3__trigger-workflow`.

**Step 3: Check logs**

Look for:
- `mega-claimed N rows` — confirms mega-claim path
- `mega-claim split into M sub-batches` — confirms re-stamp
- `batch {uuid} complete` — confirms sub-batches process normally
- No `Invalid ID format` errors

**Step 4: Compare throughput**

Check rows processed vs previous runs. With 30-thread mega-claims, expect ~3x fewer SxT claim calls.

---

### Task 9: Update betanet workflows

Only after testnet validation succeeds.

**Files:**
- Modify: `.github/workflows/dealsync-classify.betanet.yml`
- Modify: `.github/workflows/dealsync-filter.betanet.yml`

**Step 1: Add `claim_size` input to classify betanet**

Add `claim_size` with default `100` to the workflow inputs. Rename `chunk_size` to `fetch_chunk_size`.

**Step 2: Update filter betanet**

Rename `chunk_size` to `fetch_chunk_size`.

**Step 3: Deploy to betanet**

Deploy all updated workflows to `https://1.w3-betanet.io`.

**Step 4: Commit**

```bash
git add .github/workflows/dealsync-classify.betanet.yml .github/workflows/dealsync-filter.betanet.yml
git commit -m "feat: add claim-size to betanet classify, rename chunk-size to fetch-chunk-size"
```

---

## Config Reference (after all changes)

### action.yml inputs (classify pipeline)

| Input | Default | Purpose |
|-------|---------|---------|
| `max-concurrent` | `70` | Worker pool size |
| `claim-size` | `5` | Threads per mega-claim (LIMIT in claim SQL) |
| `classify-batch-size` | `5` | Threads per AI prompt |
| `max-retries` | `6` | Retries before dead-letter |
| `fetch-chunk-size` | `10` | Emails per content-fetcher request |
| `fetch-timeout-ms` | `120000` | Content-fetcher timeout |
| `flush-interval-ms` | `5000` | WriteBatcher timer interval |
| `flush-threshold` | `5` | WriteBatcher count threshold |
| `primary-model` | `Qwen3-235B` | Primary AI model |
| `fallback-model` | `DeepSeek-V3` | Fallback AI model |

### Betanet classify workflow inputs

| Input | Default | Notes |
|-------|---------|-------|
| `max_concurrent` | `70` | |
| `claim_size` | `100` | 20 sub-batches per mega-claim |
| `classify_batch_size` | `5` | Unchanged |
| `max_retries` | `6` | Unchanged |
| `fetch_chunk_size` | `10` | Renamed from chunk_size |
| `fetch_timeout_ms` | `240000` | Unchanged |
| `email_provider` | `content-fetcher` | Unchanged |

### Secrets (unchanged)

| Secret | Used by | Purpose |
|--------|---------|---------|
| `SXT_AUTH_URL` | all | SxT auth proxy |
| `SXT_AUTH_SECRET` | all | SxT shared secret |
| `SXT_API_URL` | all | SxT REST API |
| `SXT_BISCUIT` | all | Multi-table biscuit |
| `SXT_SCHEMA` | all | Schema name |
| `SXT_RATE_LIMITER_URL` | all | Rate limiter service |
| `SXT_RATE_LIMITER_API_KEY` | all | Rate limiter API key |
| `CONTENT_FETCHER_URL` | filter, classify | Content fetcher |
| `EMAIL_SERVICE_URL` | filter, classify | Email service |
| `HYPERBOLIC_KEY` | classify | AI API key |
| `AI_PRIMARY_MODEL` | classify | Primary model override |
| `AI_FALLBACK_MODEL` | classify | Fallback model override |
