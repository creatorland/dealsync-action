# Already-Evaluated Thread Skip — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Skip AI classification for threads that already have a deal in DEALS with no newer emails, setting their state directly to `deal`.

**Architecture:** After content fetch, query DEALS for existing rows matching the batch's thread IDs. Compare each thread's latest email date against the deal's UPDATED_AT. Threads where all emails are older than the deal are skipped — their DEAL_STATES are set to `deal` directly. Remaining threads go through the normal AI classification pipeline.

**Tech Stack:** Node 24 ESM, Jest (ESM), SxT SQL, existing pipeline infrastructure

---

### Task 1: Update `deals.selectByThreadIds` SQL builder to include UPDATED_AT

**Files:**
- Modify: `src/lib/sql/deals.js:14-17`
- Test: `__tests__/sql/deals.test.js:20-25`

**Step 1: Write the failing test**

In `__tests__/sql/deals.test.js`, update the existing `selectByThreadIds` test to assert `UPDATED_AT` is in the SELECT:

```js
it('selectByThreadIds', () => {
  const sql = deals.selectByThreadIds(S, ["'th-1'", "'th-2'"])
  expect(sql).toContain('SELECT ID, THREAD_ID, USER_ID, UPDATED_AT')
  expect(sql).toContain(`FROM ${S}.DEALS`)
  expect(sql).toContain("'th-1'")
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deals.test.js -v`
Expected: FAIL — `SELECT ID, THREAD_ID, USER_ID` does not contain `UPDATED_AT`

**Step 3: Write minimal implementation**

In `src/lib/sql/deals.js`, update `selectByThreadIds`:

```js
selectByThreadIds: (schema, quotedThreadIds) => {
  const s = sanitizeSchema(schema)
  return `SELECT ID, THREAD_ID, USER_ID, UPDATED_AT FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
},
```

**Step 4: Run test to verify it passes**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deals.test.js -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/lib/sql/deals.js __tests__/sql/deals.test.js
git commit -m "feat: add UPDATED_AT to deals.selectByThreadIds"
```

---

### Task 2: Add already-evaluated skip logic to `run-classify-pipeline.js`

**Files:**
- Modify: `src/commands/run-classify-pipeline.js:10` (add `deals` import)
- Modify: `src/commands/run-classify-pipeline.js:163-241` (add skip logic after fetch, before AI call)
- Test: `__tests__/run-classify-pipeline.test.js`

**Context:** The skip logic goes inside `processClassifyBatch()`, in the `if (!threads)` block, after the content fetch succeeds and unfetchable threads are handled (after line 229), but before the `if (allEmails.length === 0)` early return (line 231). This way we can remove skipped emails from `allEmails` before the prompt is built.

**Step 1: Write the failing test — all threads skipped (existing deals, older emails)**

Add a new test in `__tests__/run-classify-pipeline.test.js`:

```js
it('skips classification for threads with existing deals and older emails', async () => {
  mockInputs()

  const rows = makeBatchRows(2)
  const threads = makeThreads(rows)

  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    // Claim phase
    mockExecuteSql
      .mockResolvedValueOnce([]) // UPDATE claim
      .mockResolvedValueOnce(rows) // SELECT claimed rows

    const batch = await claimFn()

    // Step 2: Check existing audit (none)
    mockExecuteSql.mockResolvedValueOnce([]) // getAuditByBatchId -> empty

    // Step 3: Fetch emails — emails have dates older than existing deals
    const emails = rows.map((r) => ({
      messageId: r.MESSAGE_ID,
      id: r.EMAIL_METADATA_ID,
      threadId: r.THREAD_ID,
      body: 'test email body',
      date: '2025-01-01T00:00:00Z',
    }))
    mockFetchEmails.mockResolvedValueOnce(emails)

    // Query for existing deals — both threads have deals with newer UPDATED_AT
    mockExecuteSql.mockResolvedValueOnce([
      { THREAD_ID: 'thread-1', UPDATED_AT: '2026-01-01T00:00:00Z' },
      { THREAD_ID: 'thread-2', UPDATED_AT: '2026-01-01T00:00:00Z' },
    ])

    // Direct state update for skipped threads
    mockExecuteSql.mockResolvedValueOnce([]) // updateStatusByIds -> deal

    await workerFn(batch, { attempt: 0 })

    return { processed: 1, failed: 0 }
  })

  await runClassifyPipeline()

  // AI should NOT have been called — all threads skipped
  expect(mockCallModel).not.toHaveBeenCalled()
  expect(mockBuildPrompt).not.toHaveBeenCalled()

  // Batch events should still be recorded
  expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)
})
```

**Step 2: Write the failing test — mixed batch (some skipped, some classified)**

```js
it('classifies only threads without existing deals or with newer emails', async () => {
  mockInputs()

  // 3 rows: thread-1, thread-2, thread-3
  const rows = makeBatchRows(3)
  const classifiedThreads = [
    {
      thread_id: 'thread-2',
      is_deal: false,
      ai_score: 3,
      ai_summary: 'Not a deal',
      category: null,
      deal_name: null,
      deal_type: null,
      deal_value: '0',
      currency: 'USD',
      main_contact: null,
    },
    {
      thread_id: 'thread-3',
      is_deal: true,
      ai_score: 9,
      ai_summary: 'New deal',
      category: 'new',
      deal_name: 'New Deal',
      deal_type: 'brand_collaboration',
      deal_value: '500',
      currency: 'USD',
      main_contact: { name: 'Bob', email: 'bob@co.com', company: 'BobCo', title: 'CTO', phone_number: null },
    },
  ]

  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    mockExecuteSql
      .mockResolvedValueOnce([]) // UPDATE claim
      .mockResolvedValueOnce(rows) // SELECT claimed rows

    const batch = await claimFn()

    // No existing audit
    mockExecuteSql.mockResolvedValueOnce([])

    // Fetch emails — all 3 threads have emails
    const emails = rows.map((r) => ({
      messageId: r.MESSAGE_ID,
      id: r.EMAIL_METADATA_ID,
      threadId: r.THREAD_ID,
      body: 'test email body',
      date: '2025-06-01T00:00:00Z',
    }))
    mockFetchEmails.mockResolvedValueOnce(emails)

    // Query for existing deals — only thread-1 has a deal with newer UPDATED_AT
    mockExecuteSql.mockResolvedValueOnce([
      { THREAD_ID: 'thread-1', UPDATED_AT: '2026-01-01T00:00:00Z' },
    ])

    // Direct state update for skipped thread-1
    mockExecuteSql.mockResolvedValueOnce([])

    // AI classification runs for thread-2 and thread-3 only
    mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
    mockCallModel.mockResolvedValueOnce({ content: '[]' })
    mockParseAndValidate.mockReturnValueOnce(classifiedThreads)

    // Save audit
    mockExecuteSql.mockResolvedValueOnce([])

    await workerFn(batch, { attempt: 0 })

    return { processed: 1, failed: 0 }
  })

  await runClassifyPipeline()

  // AI should have been called — but only for non-skipped threads
  expect(mockCallModel).toHaveBeenCalledTimes(1)

  // buildPrompt should have received only thread-2 and thread-3 emails
  const promptEmails = mockBuildPrompt.mock.calls[0][0]
  const promptThreadIds = [...new Set(promptEmails.map((e) => e.threadId))]
  expect(promptThreadIds).not.toContain('thread-1')
  expect(promptThreadIds).toContain('thread-2')
  expect(promptThreadIds).toContain('thread-3')

  // Evals, deals etc should have been saved for classified threads
  expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
})
```

**Step 3: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js -v`
Expected: FAIL — no skip logic exists yet

**Step 4: Implement the skip logic**

In `src/commands/run-classify-pipeline.js`:

4a. Add `deals` to the import on line 10:

```js
import { dealStates as dealStatesSql, evaluations as evalSql, deals as dealsSql } from '../lib/sql/index.js'
```

4b. After the unfetchable thread handling block (after line 229) and before the `if (allEmails.length === 0)` check (line 231), insert the skip logic:

```js
      // ---------------------------------------------------------------
      // Already-evaluated skip: threads with existing deals + no newer emails
      // ---------------------------------------------------------------

      const fetchedThreadIds2 = [...new Set(allEmails.map((e) => e.threadId).filter(Boolean))]

      if (fetchedThreadIds2.length > 0) {
        const quotedFetched = fetchedThreadIds2.map((id) => `'${sanitizeId(id)}'`)
        const existingDeals = await execNoRL(dealsSql.selectByThreadIds(schema, quotedFetched))

        if (existingDeals && existingDeals.length > 0) {
          const dealByThread = {}
          for (const d of existingDeals) {
            dealByThread[d.THREAD_ID] = d.UPDATED_AT
          }

          // Group emails by thread and find latest date per thread
          const emailsByThread = {}
          for (const email of allEmails) {
            if (!email.threadId) continue
            if (!emailsByThread[email.threadId]) emailsByThread[email.threadId] = []
            emailsByThread[email.threadId].push(email)
          }

          const skippedEmailIds = []
          const skippedThreadIds = []

          for (const [threadId, dealUpdatedAt] of Object.entries(dealByThread)) {
            const threadEmails = emailsByThread[threadId]
            if (!threadEmails || threadEmails.length === 0) continue

            const latestEmailDate = threadEmails.reduce((latest, e) => {
              const d = new Date(e.date)
              return d > latest ? d : latest
            }, new Date(0))

            if (latestEmailDate <= new Date(dealUpdatedAt)) {
              // All emails are older than the deal — skip classification
              skippedThreadIds.push(threadId)
              const threadRows = rows.filter((r) => r.THREAD_ID === threadId)
              skippedEmailIds.push(...threadRows.map((r) => r.EMAIL_METADATA_ID))
              // Remove these emails from allEmails so they don't go to AI
              allEmails = allEmails.filter((e) => e.threadId !== threadId)
            }
          }

          if (skippedEmailIds.length > 0) {
            const quotedSkipped = skippedEmailIds.map((id) => `'${sanitizeId(id)}'`)
            await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedSkipped, STATUS.DEAL))
            console.log(
              `[run-classify-pipeline] ${skippedEmailIds.length} rows skipped → deal (already evaluated, ${skippedThreadIds.length} threads)`,
            )
          }
        }
      }
```

Note: `allEmails` must be declared with `let` instead of `const` (currently on line ~170 in the `let allEmails` declaration — it's already `let`, so no change needed).

**Step 5: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js -v`
Expected: PASS (all existing tests + new tests)

**Step 6: Run the full test suite**

Run: `npm test`
Expected: PASS

**Step 7: Commit**

```bash
git add src/commands/run-classify-pipeline.js __tests__/run-classify-pipeline.test.js
git commit -m "feat: skip classification for threads with existing deals and older emails"
```

---

### Task 3: Add same skip logic to standalone `fetch-and-classify.js`

**Files:**
- Modify: `src/commands/fetch-and-classify.js:8` (add `deals` import)
- Modify: `src/commands/fetch-and-classify.js:66-78` (add skip logic after fetch, before prompt build)

**Context:** The standalone `fetch-and-classify.js` is simpler — it doesn't handle unfetchable threads or batched writes. The skip logic goes after the content fetch (line 76) and before `buildPrompt` (line 79).

**Step 1: Implement the skip logic**

1a. Update the import on line 8:

```js
import { dealStates as dealStatesSql, deals as dealsSql } from '../lib/sql/index.js'
```

1b. Add `STATUS` to the constants import on line 5:

```js
import { saveResults, sanitizeString, sanitizeSchema, sanitizeId, STATUS } from '../lib/constants.js'
```

1c. After the content fetch (after line 76) and before `buildPrompt` (line 79), insert the skip logic:

```js
  // Already-evaluated skip: threads with existing deals + no newer emails
  const fetchedThreadIds = [...new Set(allEmails.map((e) => e.threadId).filter(Boolean))]

  if (fetchedThreadIds.length > 0) {
    const quotedFetched = fetchedThreadIds.map((id) => `'${sanitizeId(id)}'`)
    const existingDeals = await exec(dealsSql.selectByThreadIds(schema, quotedFetched))

    if (existingDeals && existingDeals.length > 0) {
      const dealByThread = {}
      for (const d of existingDeals) {
        dealByThread[d.THREAD_ID] = d.UPDATED_AT
      }

      const emailsByThread = {}
      for (const email of allEmails) {
        if (!email.threadId) continue
        if (!emailsByThread[email.threadId]) emailsByThread[email.threadId] = []
        emailsByThread[email.threadId].push(email)
      }

      const skippedEmailIds = []
      const skippedThreadIds = []

      for (const [threadId, dealUpdatedAt] of Object.entries(dealByThread)) {
        const threadEmails = emailsByThread[threadId]
        if (!threadEmails || threadEmails.length === 0) continue

        const latestEmailDate = threadEmails.reduce((latest, e) => {
          const d = new Date(e.date)
          return d > latest ? d : latest
        }, new Date(0))

        if (latestEmailDate <= new Date(dealUpdatedAt)) {
          skippedThreadIds.push(threadId)
          const threadRows = metadataRows.filter((r) => r.THREAD_ID === threadId)
          skippedEmailIds.push(...threadRows.map((r) => r.EMAIL_METADATA_ID))
          allEmails = allEmails.filter((e) => e.threadId !== threadId)
        }
      }

      if (skippedEmailIds.length > 0) {
        const quotedSkipped = skippedEmailIds.map((id) => `'${sanitizeId(id)}'`)
        await exec(dealStatesSql.updateStatusByIds(schema, quotedSkipped, STATUS.DEAL))
        console.log(
          `[classify] ${skippedEmailIds.length} rows skipped → deal (already evaluated, ${skippedThreadIds.length} threads)`,
        )
      }
    }
  }

  if (allEmails.length === 0) {
    console.log('[classify] all threads already evaluated — skipping AI')
    return { skipped: true, thread_count: 0 }
  }
```

Note: The `allEmails` variable on line 70 is `const`. Change it to `let` since we now mutate it by filtering out skipped threads.

**Step 2: Run the full test suite**

Run: `npm test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/commands/fetch-and-classify.js
git commit -m "feat: add already-evaluated skip to standalone fetch-and-classify"
```

---

### Task 4: Package and verify

**Files:**
- Regenerate: `dist/index.js`

**Step 1: Run full test suite**

Run: `npm test`
Expected: PASS

**Step 2: Run package**

Run: `npm run package`
Expected: Success — `dist/index.js` regenerated

**Step 3: Run the full `all` script**

Run: `npm run all`
Expected: PASS (format + test + package)

**Step 4: Commit the dist bundle**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist"
```
