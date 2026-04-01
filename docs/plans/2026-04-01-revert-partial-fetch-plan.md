# Revert + Partial Failure Re-implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Revert the fetchThreadEmails changes that cause retry explosions, then add 207/502 partial failure handling directly into the old fetchEmails architecture.

**Architecture:** Revert 3 commits to restore the old sequential per-chunk retry fetchEmails. Modify fetchEmails in-place to handle 207 (partial) and 502 (total failure) responses with per-messageId retry within each chunk. No new files. Pipelines revert to calling fetchEmails directly.

**Tech Stack:** Node 24, ESM, Jest with --experimental-vm-modules

---

### Task 1: Revert source and test files to 55859cd

**Files:**

- Revert: `src/` (all source files)
- Revert: `__tests__/` (all test files)

**Step 1: Checkout source and test files from the revert target**

```bash
git checkout 55859cd -- src/ __tests__/
```

This restores:

- `src/lib/emails.js` — old fetchEmails with per-chunk retry, no 207/502 handling
- `src/commands/run-filter-pipeline.js` — calls fetchEmails directly
- `src/commands/run-classify-pipeline.js` — calls fetchEmails directly
- `__tests__/emails.test.js` — old tests expecting flat array return
- Removes `src/lib/fetch-threads.js` (no longer exists at 55859cd)

**Step 2: Verify files are restored**

```bash
git diff --stat HEAD
```

Expected: changes in src/ and **tests**/ showing revert to old code.

**Step 3: Run existing tests to confirm clean baseline**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js
```

Expected: all tests pass (this is the known-good state).

**Step 4: Commit the revert**

```bash
git add src/ __tests__/
git commit -m "revert: restore pre-fetchThreadEmails code from 55859cd

Reverts commits 2bf6833, c350fb0, c055597 which introduced
fetchThreadEmails retry layer causing 20+ min batch timeouts."
```

---

### Task 2: Add 207/502 handling to fetchEmails with per-messageId retry

**Files:**

- Modify: `src/lib/emails.js` (the `fetchEmails` function, lines 190-285)

The old inner loop retries entire chunks on any failure. We modify it to:

- On 207: accept successful emails, retry only failed messageIds
- On 502: parse per-messageId errors if JSON, retry failed messageIds
- On other errors: retry entire pendingIds (unchanged behavior)

**Step 1: Write failing tests for 207 partial handling**

Add to `__tests__/emails.test.js`, inside the existing `describe('fetchEmails')` block, after the retry behavior section:

```javascript
// -------------------------------------------------------------------------
// HTTP 207 — partial success with per-messageId retry
// -------------------------------------------------------------------------

it('on 207, accepts successful emails and retries only failed messageIds', async () => {
  const messageIds = ['msg-1', 'msg-2', 'msg-3']
  const meta = makeMeta(messageIds)

  // Attempt 1: 207 partial — msg-1 succeeds, msg-2 and msg-3 fail
  mockFetch.mockResolvedValueOnce({
    ok: true,
    status: 207,
    json: async () => ({
      status: 'partial',
      data: [{ messageId: 'msg-1' }],
      errors: [
        { messageId: 'msg-2', error: 'rate limited' },
        { messageId: 'msg-3', error: 'timeout' },
      ],
    }),
    text: async () => '',
  })
  // Attempt 2: 200 success — msg-2 and msg-3 now succeed
  mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-2' }, { messageId: 'msg-3' }]))

  const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

  expect(result).toHaveLength(3)
  expect(result.map((e) => e.messageId).sort()).toEqual(['msg-1', 'msg-2', 'msg-3'])
  // Attempt 1 sent all 3, attempt 2 sent only the 2 failed
  expect(mockFetch).toHaveBeenCalledTimes(2)
  const body2 = JSON.parse(mockFetch.mock.calls[1][1].body)
  expect(body2.messageIds).toEqual(['msg-2', 'msg-3'])
})

it('on 207, retries exhaust and returns only successful emails', async () => {
  const messageIds = ['msg-1', 'msg-2']
  const meta = makeMeta(messageIds)

  // All 3 attempts return 207 with msg-2 failing
  for (let i = 0; i < 3; i++) {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 207,
      json: async () => ({
        status: 'partial',
        data: [{ messageId: 'msg-1' }],
        errors: [{ messageId: 'msg-2', error: 'persistent failure' }],
      }),
      text: async () => '',
    })
  }

  const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

  // msg-1 accepted on first attempt (not duplicated), msg-2 never fetched
  expect(result).toHaveLength(1)
  expect(result[0].messageId).toBe('msg-1')
  expect(mockFetch).toHaveBeenCalledTimes(3)
})
```

Also add a helper at the top with the other helpers:

```javascript
function partialResponse(data, errors) {
  return {
    ok: true,
    status: 207,
    json: async () => ({ status: 'partial', data, errors }),
    text: async () => JSON.stringify({ status: 'partial', data, errors }),
  }
}
```

**Step 2: Write failing tests for 502 handling**

Add to `__tests__/emails.test.js`:

```javascript
// -------------------------------------------------------------------------
// HTTP 502 — total failure with per-messageId retry
// -------------------------------------------------------------------------

it('on 502 with JSON errors, retries failed messageIds', async () => {
  const messageIds = ['msg-1', 'msg-2']
  const meta = makeMeta(messageIds)

  // Attempt 1: 502 total failure
  mockFetch.mockResolvedValueOnce({
    ok: false,
    status: 502,
    text: async () =>
      JSON.stringify({
        status: 'failure',
        data: [],
        errors: [
          { messageId: 'msg-1', error: 'upstream timeout' },
          { messageId: 'msg-2', error: 'upstream timeout' },
        ],
      }),
  })
  // Attempt 2: 200 success
  mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

  const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

  expect(result).toHaveLength(2)
  expect(mockFetch).toHaveBeenCalledTimes(2)
})

it('on 502 with non-JSON body, retries all messageIds in chunk', async () => {
  const messageIds = ['msg-1', 'msg-2']
  const meta = makeMeta(messageIds)

  // Attempt 1: 502 non-JSON
  mockFetch.mockResolvedValueOnce({
    ok: false,
    status: 502,
    text: async () => 'Bad Gateway',
  })
  // Attempt 2: success
  mockFetch.mockResolvedValueOnce(okResponse([{ messageId: 'msg-1' }, { messageId: 'msg-2' }]))

  const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 10, maxRetries: 3 }))

  expect(result).toHaveLength(2)
  expect(mockFetch).toHaveBeenCalledTimes(2)
})

it('on 502 exhausting retries, continues to next chunk', async () => {
  const messageIds = ['msg-1', 'msg-2', 'msg-3', 'msg-4']
  const meta = makeMeta(messageIds)

  // Chunk 1 (msg-1, msg-2): 502 all 3 attempts
  mockFetch
    .mockResolvedValueOnce({
      ok: false,
      status: 502,
      text: async () =>
        JSON.stringify({
          errors: [
            { messageId: 'msg-1', error: 'fail' },
            { messageId: 'msg-2', error: 'fail' },
          ],
        }),
    })
    .mockResolvedValueOnce({
      ok: false,
      status: 502,
      text: async () =>
        JSON.stringify({
          errors: [
            { messageId: 'msg-1', error: 'fail' },
            { messageId: 'msg-2', error: 'fail' },
          ],
        }),
    })
    .mockResolvedValueOnce({
      ok: false,
      status: 502,
      text: async () =>
        JSON.stringify({
          errors: [
            { messageId: 'msg-1', error: 'fail' },
            { messageId: 'msg-2', error: 'fail' },
          ],
        }),
    })
    // Chunk 2 (msg-3, msg-4): success
    .mockResolvedValueOnce(okResponse([{ messageId: 'msg-3' }, { messageId: 'msg-4' }]))

  const result = await fetchEmails(messageIds, meta, makeOpts({ chunkSize: 2, maxRetries: 3 }))

  expect(result).toHaveLength(2)
  expect(result[0].messageId).toBe('msg-3')
})
```

**Step 3: Run tests to verify they fail**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/emails.test.js
```

Expected: new 207/502 tests fail (old fetchEmails doesn't handle these status codes).

**Step 4: Implement 207/502 handling in fetchEmails**

Replace the inner chunk loop in `src/lib/emails.js` (lines 207-278). The key change: replace `let fetched = false` with `let pendingIds = chunk` and track which messageIds still need fetching. The full replacement for lines 205-278:

```javascript
const allEmails = []

for (let i = 0; i < messageIds.length; i += chunkSize) {
  const chunk = messageIds.slice(i, i + chunkSize)
  const chunkIndex = Math.floor(i / chunkSize) + 1
  let pendingIds = [...chunk]

  for (let attempt = 0; attempt < maxRetries && pendingIds.length > 0; attempt++) {
    try {
      const { signal, clear } = withTimeout(fetchTimeoutMs)
      try {
        const body = {
          userId,
          ...(syncStateId ? { syncStateId } : {}),
          messageIds: pendingIds,
          ...(format ? { format } : {}),
        }

        const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
          signal,
        })
        clear()

        // Handle 429 rate limiting
        if (resp.status === 429) {
          const retryBody = await resp.json().catch(() => ({}))
          const retryAfterMs = retryBody.retryAfterMs || backoffMs(attempt, { base: 1000 })
          console.log(
            `[email-client] 429 rate limited, waiting ${retryAfterMs}ms ` +
              `(chunk ${chunkIndex}, attempt ${attempt + 1}/${maxRetries})`,
          )
          await sleep(retryAfterMs)
          continue
        }

        // HTTP 200: full success — accept all, done with this chunk
        if (resp.status === 200) {
          const result = await resp.json()
          const emails = result.data || result
          enrichAndCollect(emails, allEmails, metaByMessageId)
          pendingIds = []
          continue
        }

        // HTTP 207: partial success — accept data, retry only failed messageIds
        if (resp.status === 207) {
          const result = await resp.json()
          const emails = result.data || []
          const errors = result.errors || []
          enrichAndCollect(emails, allEmails, metaByMessageId)
          const failedIds = new Set(errors.map((e) => e.messageId))
          pendingIds = pendingIds.filter((id) => failedIds.has(id))
          if (pendingIds.length > 0) {
            console.log(
              `[email-client] chunk ${chunkIndex}: 207 partial — ` +
                `${emails.length} fetched, ${pendingIds.length} failed ` +
                `(attempt ${attempt + 1}/${maxRetries})`,
            )
            if (attempt < maxRetries - 1) {
              await sleep(backoffMs(attempt, { base: 1000 }))
            }
          }
          continue
        }

        // HTTP 502: total failure — parse per-messageId errors if JSON
        if (resp.status === 502) {
          const raw = await resp.text()
          try {
            const result = JSON.parse(raw)
            if (result.errors && Array.isArray(result.errors) && result.errors.length > 0) {
              const failedIds = new Set(result.errors.map((e) => e.messageId))
              pendingIds = pendingIds.filter((id) => failedIds.has(id))
            }
          } catch {
            // Non-JSON 502 body — retry all pendingIds
          }
          console.log(
            `[email-client] chunk ${chunkIndex}: HTTP 502 — ` +
              `${pendingIds.length} to retry (attempt ${attempt + 1}/${maxRetries})`,
          )
          if (attempt < maxRetries - 1) {
            await sleep(backoffMs(attempt, { base: 1000 }))
          }
          continue
        }

        // Other HTTP errors — throw to trigger retry of all pendingIds
        throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      } catch (err) {
        clear()
        throw err
      }
    } catch (err) {
      console.log(
        `[email-client] chunk ${chunkIndex} fetch failed ` +
          `(attempt ${attempt + 1}/${maxRetries}): ${err.message}`,
      )

      // If not the last attempt, wait with exponential backoff before retry
      if (attempt < maxRetries - 1) {
        const waitMs = backoffMs(attempt, { base: 1000 })
        await sleep(waitMs)
      }
    }
  }
}
```

Also add this helper function above `fetchEmails` (after the `DEFAULT_MAX_RETRIES` constant):

```javascript
function enrichAndCollect(emails, allEmails, metaByMessageId) {
  for (const email of emails) {
    const meta = metaByMessageId.get(email.messageId)
    if (meta) {
      email.id = meta.EMAIL_METADATA_ID
      email.threadId = meta.THREAD_ID
      if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
    }
    allEmails.push(email)
  }
}
```

**Step 5: Run all tests**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/emails.test.js
```

Expected: all tests pass including new 207/502 tests.

**Step 6: Run full test suite**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js
```

Expected: all tests pass. Pipeline tests should pass because pipelines were reverted to calling `fetchEmails` directly and the return type (flat array) is unchanged.

**Step 7: Commit**

```bash
git add src/lib/emails.js __tests__/emails.test.js
git commit -m "feat: add 207/502 partial failure handling to fetchEmails

Handles content fetcher partial failure semantics (PR dealsync-v2#349):
- HTTP 207: accept successful emails, retry only failed messageIds
- HTTP 502: parse per-messageId errors from JSON body, retry failures
- Retries stay fast (3 attempts, 1s backoff) within the old architecture
- No thread-awareness layer, no long deadline — avoids retry explosion"
```

---

### Task 3: Package and verify

**Files:**

- Rebuild: `dist/index.js`

**Step 1: Run package**

```bash
npm run package
```

Expected: builds successfully.

**Step 2: Run full suite one more time**

```bash
npm run all
```

Expected: format + test + package all pass.

**Step 3: Commit dist**

```bash
git add dist/
git commit -m "chore: rebuild dist after partial fetch revert"
```
