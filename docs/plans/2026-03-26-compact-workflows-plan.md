# Compact Workflows Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the distributed multi-workflow dispatch model with three self-contained compact workflows (sync, filter, classify) that manage concurrency internally via a JS promise pool.

**Architecture:** New shared libraries (`email-client.js`, `pipeline.js`) provide standardized email fetching with retry and a concurrency pool with batch-event tracking. Two claim commands (`claim-filter-batch`, `claim-classify-batch`) atomically claim batches from the DB. Two pipeline commands (`run-filter-pipeline`, `run-classify-pipeline`) orchestrate claiming + processing in a concurrent loop. Three minimal workflow YAML files trigger each command.

**Tech Stack:** Node 24 ESM, `@actions/core`, `uuid`, existing SxT/AI clients, GitHub Actions `workflow_dispatch`

**Design doc:** `docs/plans/2026-03-26-compact-workflows-design.md`

---

### Task 1: Shared library — `src/lib/email-client.js`

Extracts the duplicated content-fetcher HTTP logic from `fetch-and-filter.js:50-80` and `fetch-and-classify.js:80-117` into a single function with standardized retry (3 attempts, exponential backoff, 429 handling).

**Files:**

- Create: `src/lib/email-client.js`
- Test: `__tests__/email-client.test.js`

**Step 1: Write the failing test**

```js
// __tests__/email-client.test.js
import { jest } from '@jest/globals'

// Mock global fetch
let fetchMock
beforeEach(() => {
  fetchMock = jest.fn()
  global.fetch = fetchMock
})
afterEach(() => {
  delete global.fetch
})

describe('fetchEmails', () => {
  const baseOpts = {
    contentFetcherUrl: 'https://fetcher.test',
    userId: 'user-1',
    syncStateId: 'sync-1',
    chunkSize: 2,
    fetchTimeoutMs: 5000,
    maxRetries: 3,
    format: 'metadata',
  }

  test('fetches emails in chunks and maps metadata', async () => {
    const { fetchEmails } = await import('../src/lib/email-client.js')
    const metaByMessageId = new Map([
      ['msg-1', { EMAIL_METADATA_ID: 'em-1', THREAD_ID: 'th-1' }],
      ['msg-2', { EMAIL_METADATA_ID: 'em-2', THREAD_ID: 'th-2' }],
      ['msg-3', { EMAIL_METADATA_ID: 'em-3', THREAD_ID: 'th-3' }],
    ])

    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => [
        { messageId: 'msg-1', topLevelHeaders: [] },
        { messageId: 'msg-2', topLevelHeaders: [] },
      ],
    })
    // Second chunk
    fetchMock
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          { messageId: 'msg-1', topLevelHeaders: [] },
          { messageId: 'msg-2', topLevelHeaders: [] },
        ],
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{ messageId: 'msg-3', topLevelHeaders: [] }],
      })

    const result = await fetchEmails(['msg-1', 'msg-2', 'msg-3'], metaByMessageId, baseOpts)

    expect(result.length).toBe(3)
    expect(result[0].id).toBe('em-1')
    expect(fetchMock).toHaveBeenCalledTimes(2) // 2 chunks of 2
  })

  test('retries on failure with exponential backoff', async () => {
    const { fetchEmails } = await import('../src/lib/email-client.js')
    const metaByMessageId = new Map([['msg-1', { EMAIL_METADATA_ID: 'em-1' }]])

    fetchMock
      .mockRejectedValueOnce(new Error('network error'))
      .mockRejectedValueOnce(new Error('network error'))
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{ messageId: 'msg-1' }],
      })

    const result = await fetchEmails(['msg-1'], metaByMessageId, baseOpts)
    expect(result.length).toBe(1)
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  test('retries on 429 respecting retryAfterMs', async () => {
    const { fetchEmails } = await import('../src/lib/email-client.js')
    const metaByMessageId = new Map([['msg-1', { EMAIL_METADATA_ID: 'em-1' }]])

    fetchMock
      .mockResolvedValueOnce({
        ok: false,
        status: 429,
        json: async () => ({ retryAfterMs: 100 }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{ messageId: 'msg-1' }],
      })

    const result = await fetchEmails(['msg-1'], metaByMessageId, baseOpts)
    expect(result.length).toBe(1)
  })

  test('throws if all retries exhausted for all chunks', async () => {
    const { fetchEmails } = await import('../src/lib/email-client.js')
    const metaByMessageId = new Map([['msg-1', { EMAIL_METADATA_ID: 'em-1' }]])

    fetchMock.mockRejectedValue(new Error('network error'))

    await expect(fetchEmails(['msg-1'], metaByMessageId, baseOpts)).rejects.toThrow()
  })
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/email-client.test.js`
Expected: FAIL — module not found

**Step 3: Write implementation**

```js
// src/lib/email-client.js
import { withTimeout } from './sxt-client.js'

const BACKOFF_BASE_MS = 1000
const BACKOFF_MULTIPLIER = 2

/**
 * Fetch emails from the content fetcher service with standardized retry.
 *
 * @param {string[]} messageIds - IDs to fetch
 * @param {Map} metaByMessageId - Map of messageId → deal_state row (for enrichment)
 * @param {Object} opts
 * @param {string} opts.contentFetcherUrl
 * @param {string} opts.userId
 * @param {string} [opts.syncStateId]
 * @param {number} opts.chunkSize - messages per request
 * @param {number} opts.fetchTimeoutMs
 * @param {number} opts.maxRetries - retries per chunk
 * @param {string} opts.format - 'metadata' (headers only) or 'full' (with body)
 * @returns {Object[]} emails with id/threadId enriched from metadata
 */
export async function fetchEmails(messageIds, metaByMessageId, opts) {
  const {
    contentFetcherUrl,
    userId,
    syncStateId,
    chunkSize,
    fetchTimeoutMs,
    maxRetries = 3,
    format,
  } = opts

  const allEmails = []

  for (let i = 0; i < messageIds.length; i += chunkSize) {
    const chunk = messageIds.slice(i, i + chunkSize)
    let fetched = false

    for (let attempt = 0; attempt < maxRetries && !fetched; attempt++) {
      try {
        const { signal, clear } = withTimeout(fetchTimeoutMs)
        const body = {
          userId,
          ...(syncStateId ? { syncStateId } : {}),
          messageIds: chunk,
          ...(format ? { format } : {}),
        }

        const resp = await fetch(`${contentFetcherUrl}/email-content/fetch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
          signal,
        })
        clear()

        if (resp.status === 429) {
          const respBody = await resp.json().catch(() => ({}))
          const retryAfter =
            respBody.retryAfterMs || BACKOFF_BASE_MS * Math.pow(BACKOFF_MULTIPLIER, attempt)
          console.log(
            `[email-client] 429 on chunk ${Math.floor(i / chunkSize) + 1}, waiting ${retryAfter}ms (attempt ${attempt + 1}/${maxRetries})`,
          )
          await new Promise((r) => setTimeout(r, retryAfter))
          continue
        }

        if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)

        const result = await resp.json()
        const emails = result.data || result

        for (const email of emails) {
          const meta = metaByMessageId.get(email.messageId)
          if (meta) {
            email.id = meta.EMAIL_METADATA_ID
            email.threadId = meta.THREAD_ID
            if (meta.PREVIOUS_AI_SUMMARY) email.previousAiSummary = meta.PREVIOUS_AI_SUMMARY
          }
          allEmails.push(email)
        }
        fetched = true
      } catch (err) {
        console.log(
          `[email-client] chunk ${Math.floor(i / chunkSize) + 1} attempt ${attempt + 1}/${maxRetries} failed: ${err.message}`,
        )
        if (attempt < maxRetries - 1) {
          const delay = BACKOFF_BASE_MS * Math.pow(BACKOFF_MULTIPLIER, attempt)
          await new Promise((r) => setTimeout(r, delay))
        }
      }
    }
  }

  if (allEmails.length === 0 && messageIds.length > 0) {
    throw new Error(`all content fetches failed — 0/${messageIds.length} emails retrieved`)
  }

  return allEmails
}
```

**Step 4: Run test to verify it passes**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/email-client.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/lib/email-client.js __tests__/email-client.test.js
git commit -m "feat: add email-client.js with standardized retry and exponential backoff"
```

---

### Task 2: Shared library — `src/lib/pipeline.js`

Concurrency pool + batch event helper. Used by both `run-filter-pipeline` and `run-classify-pipeline`.

**Files:**

- Create: `src/lib/pipeline.js`
- Test: `__tests__/pipeline.test.js`

**Step 1: Write the failing test**

```js
// __tests__/pipeline.test.js
import { jest } from '@jest/globals'

describe('runPool', () => {
  test('processes all batches up to maxConcurrent', async () => {
    const { runPool } = await import('../src/lib/pipeline.js')

    let claimCount = 0
    const claimFn = async () => {
      claimCount++
      if (claimCount > 3) return null
      return { batch_id: `batch-${claimCount}`, count: 1, attempts: 0 }
    }

    const processed = []
    const workerFn = async (batch) => {
      await new Promise((r) => setTimeout(r, 10))
      processed.push(batch.batch_id)
    }

    const result = await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })

    expect(processed).toEqual(['batch-1', 'batch-2', 'batch-3'])
    expect(result.processed).toBe(3)
    expect(result.failed).toBe(0)
  })

  test('respects maxConcurrent limit', async () => {
    const { runPool } = await import('../src/lib/pipeline.js')

    let concurrent = 0
    let maxSeen = 0
    let claimCount = 0

    const claimFn = async () => {
      claimCount++
      if (claimCount > 5) return null
      return { batch_id: `b-${claimCount}`, count: 1, attempts: 0 }
    }

    const workerFn = async () => {
      concurrent++
      maxSeen = Math.max(maxSeen, concurrent)
      await new Promise((r) => setTimeout(r, 50))
      concurrent--
    }

    await runPool(claimFn, workerFn, { maxConcurrent: 2, maxRetries: 3 })
    expect(maxSeen).toBeLessThanOrEqual(2)
  })

  test('retries failed workers up to maxRetries and records failures', async () => {
    const { runPool } = await import('../src/lib/pipeline.js')

    let claimCount = 0
    const claimFn = async () => {
      claimCount++
      if (claimCount > 1) return null
      return { batch_id: 'fail-batch', count: 1, attempts: 0 }
    }

    const workerFn = async () => {
      throw new Error('worker failed')
    }

    const result = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3 })
    expect(result.failed).toBe(1)
    expect(result.processed).toBe(0)
  })

  test('starts at existing attempt count for retriggered batches', async () => {
    const { runPool } = await import('../src/lib/pipeline.js')

    let attemptsSeen = []
    let claimCount = 0
    const claimFn = async () => {
      claimCount++
      if (claimCount > 1) return null
      return { batch_id: 'retrigger-batch', count: 1, attempts: 2 } // already 2 attempts
    }

    const workerFn = async (batch, { attempt }) => {
      attemptsSeen.push(attempt)
      throw new Error('fail')
    }

    const result = await runPool(claimFn, workerFn, { maxConcurrent: 1, maxRetries: 3 })
    // Should only try once more (attempt 2 → fail → 3 >= maxRetries → dead letter)
    expect(result.failed).toBe(1)
  })
})

describe('insertBatchEvent', () => {
  test('executes INSERT with correct values', async () => {
    const { insertBatchEvent } = await import('../src/lib/pipeline.js')

    const executeSqlMock = jest.fn().mockResolvedValue([])
    await insertBatchEvent(executeSqlMock, 'SCHEMA', {
      triggerHash: 'hash-1',
      batchId: 'batch-1',
      batchType: 'filter',
      eventType: 'new',
    })

    expect(executeSqlMock).toHaveBeenCalledTimes(1)
    const sql = executeSqlMock.mock.calls[0][0]
    expect(sql).toContain('BATCH_EVENTS')
    expect(sql).toContain('hash-1')
    expect(sql).toContain('batch-1')
    expect(sql).toContain('filter')
    expect(sql).toContain('new')
  })
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js`
Expected: FAIL — module not found

**Step 3: Write implementation**

```js
// src/lib/pipeline.js
import { sanitizeId, sanitizeString } from './queries.js'

const BACKOFF_BASE_MS = 2000
const BACKOFF_MULTIPLIER = 2

/**
 * Run a concurrent pool of workers, claiming batches until exhausted.
 *
 * @param {Function} claimFn - async () => { batch_id, count, attempts } | null
 * @param {Function} workerFn - async (batch, { attempt }) => void
 * @param {Object} opts
 * @param {number} opts.maxConcurrent
 * @param {number} opts.maxRetries
 * @returns {{ processed: number, failed: number }}
 */
export async function runPool(claimFn, workerFn, { maxConcurrent, maxRetries }) {
  const active = new Set()
  const results = { processed: 0, failed: 0 }

  function waitForAny() {
    if (active.size === 0) return Promise.resolve()
    return Promise.race(active)
  }

  async function runWorker(batch) {
    let currentAttempt = batch.attempts
    while (currentAttempt < maxRetries) {
      try {
        await workerFn(batch, { attempt: currentAttempt })
        results.processed++
        return
      } catch (err) {
        currentAttempt++
        console.log(
          `[pipeline] batch ${batch.batch_id} attempt ${currentAttempt}/${maxRetries} failed: ${err.message}`,
        )
        if (currentAttempt >= maxRetries) {
          console.log(
            `[pipeline] batch ${batch.batch_id} dead-lettered after ${maxRetries} attempts`,
          )
          results.failed++
          return
        }
        const delay = BACKOFF_BASE_MS * Math.pow(BACKOFF_MULTIPLIER, currentAttempt - 1)
        await new Promise((r) => setTimeout(r, delay))
      }
    }
    results.failed++
  }

  while (true) {
    if (active.size < maxConcurrent) {
      const batch = await claimFn()
      if (!batch) {
        if (active.size === 0) break
        await waitForAny()
        continue
      }
      const task = runWorker(batch).then(() => active.delete(task))
      active.add(task)
    } else {
      await waitForAny()
    }
  }

  return results
}

/**
 * Insert a BATCH_EVENTS row.
 *
 * @param {Function} executeSqlFn - (sql) => Promise — bound executeSql with auth
 * @param {string} schema
 * @param {Object} event
 * @param {string} event.triggerHash
 * @param {string} event.batchId
 * @param {string} event.batchType - 'filter' | 'classify'
 * @param {string} event.eventType - 'new' | 'retrigger' | 'complete'
 */
export async function insertBatchEvent(
  executeSqlFn,
  schema,
  { triggerHash, batchId, batchType, eventType },
) {
  const sql = `INSERT INTO ${sanitizeId(schema)}.BATCH_EVENTS
    (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT)
    VALUES ('${sanitizeId(triggerHash)}', '${sanitizeId(batchId)}', '${sanitizeString(batchType)}', '${sanitizeString(eventType)}', CURRENT_TIMESTAMP)`
  await executeSqlFn(sql)
}
```

**Step 4: Run test to verify it passes**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/lib/pipeline.js __tests__/pipeline.test.js
git commit -m "feat: add pipeline.js with concurrency pool and batch event helper"
```

---

### Task 3: Claim command — `src/commands/claim-filter-batch.js`

Atomic claim of pending deal_states for filtering. Falls back to stuck batch re-claiming.

**Files:**

- Create: `src/commands/claim-filter-batch.js`
- Modify: `src/main.js:13-24` (add to COMMANDS map)
- Modify: `action.yml:5-74` (add new inputs)
- Test: `__tests__/claim-filter-batch.test.js`

**Step 1: Write the failing test**

```js
// __tests__/claim-filter-batch.test.js
import { jest } from '@jest/globals'

// Mock @actions/core
const mockInputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn((name) => mockInputs[name] || ''),
}))

// Mock sxt-client
const executeSqlMock = jest.fn()
const authenticateMock = jest.fn().mockResolvedValue('jwt-token')
jest.unstable_mockModule('../src/lib/sxt-client.js', () => ({
  authenticate: authenticateMock,
  executeSql: executeSqlMock,
}))

// Mock uuid
jest.unstable_mockModule('uuid', () => ({
  v7: jest.fn(() => 'mock-uuid-v7'),
}))

beforeEach(() => {
  jest.clearAllMocks()
  Object.assign(mockInputs, {
    'auth-url': 'https://auth',
    'auth-secret': 'secret',
    'api-url': 'https://api',
    biscuit: 'biscuit',
    schema: 'DEALSYNC',
    'filter-batch-size': '200',
    'max-retries': '3',
  })
})

describe('runClaimFilterBatch', () => {
  test('claims pending items and returns batch info', async () => {
    const { runClaimFilterBatch } = await import('../src/commands/claim-filter-batch.js')

    // UPDATE returns nothing, SELECT returns claimed rows
    executeSqlMock
      .mockResolvedValueOnce([]) // UPDATE claim
      .mockResolvedValueOnce([
        // SELECT claimed rows
        {
          EMAIL_METADATA_ID: 'em-1',
          MESSAGE_ID: 'msg-1',
          USER_ID: 'u1',
          THREAD_ID: 'th-1',
          SYNC_STATE_ID: 's1',
        },
        {
          EMAIL_METADATA_ID: 'em-2',
          MESSAGE_ID: 'msg-2',
          USER_ID: 'u1',
          THREAD_ID: 'th-2',
          SYNC_STATE_ID: 's1',
        },
      ])
      .mockResolvedValueOnce([]) // INSERT batch event

    const result = await runClaimFilterBatch()

    expect(result.batch_id).toBe('mock-uuid-v7')
    expect(result.count).toBe(2)
    expect(result.attempts).toBe(0)
  })

  test('returns null batch_id when nothing to claim and no stuck batches', async () => {
    const { runClaimFilterBatch } = await import('../src/commands/claim-filter-batch.js')

    executeSqlMock
      .mockResolvedValueOnce([]) // UPDATE claim — no rows
      .mockResolvedValueOnce([]) // SELECT claimed — empty
      .mockResolvedValueOnce([]) // SELECT stuck — empty

    const result = await runClaimFilterBatch()

    expect(result.batch_id).toBeNull()
    expect(result.count).toBe(0)
  })

  test('reclaims stuck batch with existing attempt count', async () => {
    const { runClaimFilterBatch } = await import('../src/commands/claim-filter-batch.js')

    executeSqlMock
      .mockResolvedValueOnce([]) // UPDATE claim — no rows
      .mockResolvedValueOnce([]) // SELECT claimed — empty
      .mockResolvedValueOnce([{ BATCH_ID: 'stuck-batch', ATTEMPTS: 2 }]) // SELECT stuck
      .mockResolvedValueOnce([
        // SELECT rows for stuck batch
        {
          EMAIL_METADATA_ID: 'em-3',
          MESSAGE_ID: 'msg-3',
          USER_ID: 'u1',
          THREAD_ID: 'th-3',
          SYNC_STATE_ID: 's1',
        },
      ])
      .mockResolvedValueOnce([]) // UPDATE touched_at for stuck batch
      .mockResolvedValueOnce([]) // INSERT batch event

    const result = await runClaimFilterBatch()

    expect(result.batch_id).toBe('stuck-batch')
    expect(result.attempts).toBe(2)
  })
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/claim-filter-batch.test.js`
Expected: FAIL — module not found

**Step 3: Write implementation**

```js
// src/commands/claim-filter-batch.js
import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeSchema, STATUS } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent } from '../lib/pipeline.js'

/**
 * Atomically claim a batch of pending deal_states for filtering.
 * Falls back to re-claiming stuck batches (filtering >5 min, attempts < maxRetries).
 *
 * Returns { batch_id, count, attempts, rows } or { batch_id: null, count: 0 }
 */
export async function runClaimFilterBatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchSize = parseInt(core.getInput('filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '3', 10)

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const batchId = uuidv7()

  // 1. Try to claim pending items
  await exec(
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP
     WHERE EMAIL_METADATA_ID IN (
       SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize}
     )`,
  )

  const claimed = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
     FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

  if (claimed.length > 0) {
    await insertBatchEvent(exec, schema, {
      triggerHash: batchId,
      batchId,
      batchType: 'filter',
      eventType: 'new',
    })
    console.log(`[claim-filter] claimed ${claimed.length} items → batch ${batchId}`)
    return { batch_id: batchId, count: claimed.length, attempts: 0, rows: claimed }
  }

  // 2. No pending items — look for stuck batches
  const stuck = await exec(
    `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS
     FROM ${schema}.DEAL_STATES ds
     LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
     WHERE ds.STATUS = '${STATUS.FILTERING}'
       AND ds.BATCH_ID IS NOT NULL
       AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
     GROUP BY ds.BATCH_ID
     HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries}
     LIMIT 1`,
  )

  if (stuck.length === 0) {
    console.log('[claim-filter] nothing to claim')
    return { batch_id: null, count: 0 }
  }

  const stuckBatchId = stuck[0].BATCH_ID
  const attempts = stuck[0].ATTEMPTS || 0

  const stuckRows = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
     FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`,
  )

  // Touch updated_at so other instances don't also grab this
  await exec(
    `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP
     WHERE BATCH_ID = '${stuckBatchId}'`,
  )

  await insertBatchEvent(exec, schema, {
    triggerHash: uuidv7(),
    batchId: stuckBatchId,
    batchType: 'filter',
    eventType: 'retrigger',
  })

  console.log(
    `[claim-filter] reclaimed stuck batch ${stuckBatchId} (${stuckRows.length} items, ${attempts} prior attempts)`,
  )
  return { batch_id: stuckBatchId, count: stuckRows.length, attempts, rows: stuckRows }
}
```

**Step 4: Run test to verify it passes**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/claim-filter-batch.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/claim-filter-batch.js __tests__/claim-filter-batch.test.js
git commit -m "feat: add claim-filter-batch command with stuck batch re-claiming"
```

---

### Task 4: Claim command — `src/commands/claim-classify-batch.js`

Same pattern as filter claim but thread-aware. Only claims threads where all messages have cleared filtering.

**Files:**

- Create: `src/commands/claim-classify-batch.js`
- Test: `__tests__/claim-classify-batch.test.js`

**Step 1: Write the failing test**

Mirror the test structure from Task 3 but with classify-specific SQL behavior:

- Claims by THREAD_ID not EMAIL_METADATA_ID
- Excludes threads with pending/filtering messages
- Uses `STATUS.CLASSIFYING` and `STATUS.PENDING_CLASSIFICATION`

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/claim-classify-batch.test.js`
Expected: FAIL

**Step 3: Write implementation**

```js
// src/commands/claim-classify-batch.js
import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeSchema, STATUS } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { insertBatchEvent } from '../lib/pipeline.js'

export async function runClaimClassifyBatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchSize = parseInt(core.getInput('classify-batch-size') || '5', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '3', 10)

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const batchId = uuidv7()

  // 1. Claim threads where all messages have cleared filtering
  await exec(
    `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP
     WHERE THREAD_ID IN (
       SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds
       WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}'
         AND NOT EXISTS (
           SELECT 1 FROM ${schema}.DEAL_STATES ds2
           WHERE ds2.THREAD_ID = ds.THREAD_ID
             AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
             AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')
         )
       LIMIT ${batchSize}
     ) AND STATUS = '${STATUS.PENDING_CLASSIFICATION}'`,
  )

  const claimed = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
     FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
  )

  if (claimed.length > 0) {
    await insertBatchEvent(exec, schema, {
      triggerHash: batchId,
      batchId,
      batchType: 'classify',
      eventType: 'new',
    })
    console.log(`[claim-classify] claimed ${claimed.length} items → batch ${batchId}`)
    return { batch_id: batchId, count: claimed.length, attempts: 0, rows: claimed }
  }

  // 2. Look for stuck classify batches
  const stuck = await exec(
    `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS
     FROM ${schema}.DEAL_STATES ds
     LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
     WHERE ds.STATUS = '${STATUS.CLASSIFYING}'
       AND ds.BATCH_ID IS NOT NULL
       AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
     GROUP BY ds.BATCH_ID
     HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries}
     LIMIT 1`,
  )

  if (stuck.length === 0) {
    console.log('[claim-classify] nothing to claim')
    return { batch_id: null, count: 0 }
  }

  const stuckBatchId = stuck[0].BATCH_ID
  const attempts = stuck[0].ATTEMPTS || 0

  const stuckRows = await exec(
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
     FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`,
  )

  await exec(
    `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP
     WHERE BATCH_ID = '${stuckBatchId}'`,
  )

  await insertBatchEvent(exec, schema, {
    triggerHash: uuidv7(),
    batchId: stuckBatchId,
    batchType: 'classify',
    eventType: 'retrigger',
  })

  console.log(
    `[claim-classify] reclaimed stuck batch ${stuckBatchId} (${stuckRows.length} items, ${attempts} prior attempts)`,
  )
  return { batch_id: stuckBatchId, count: stuckRows.length, attempts, rows: stuckRows }
}
```

**Step 4: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/claim-classify-batch.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/claim-classify-batch.js __tests__/claim-classify-batch.test.js
git commit -m "feat: add claim-classify-batch command with thread-aware claiming"
```

---

### Task 5: Pipeline command — `src/commands/run-filter-pipeline.js`

Orchestrator that loops claiming + filtering batches concurrently.

**Files:**

- Create: `src/commands/run-filter-pipeline.js`
- Test: `__tests__/run-filter-pipeline.test.js`

**Step 1: Write the failing test**

Test the integration: mock SxT client and email client, verify the pipeline claims batches, fetches headers, applies filter rules, saves results, and records batch events. Verify retry behavior and dead lettering.

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js`
Expected: FAIL

**Step 3: Write implementation**

```js
// src/commands/run-filter-pipeline.js
import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId, STATUS } from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { isRejected } from '../lib/filter-rules.js'
import { fetchEmails } from '../lib/email-client.js'
import { runPool, insertBatchEvent } from '../lib/pipeline.js'

export async function runFilterPipeline() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const maxConcurrent = parseInt(core.getInput('max-concurrent') || '5', 10)
  const batchSize = parseInt(core.getInput('filter-batch-size') || '200', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '3', 10)
  const chunkSize = parseInt(core.getInput('chunk-size') || '50', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '30000', 10)

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  let totalFiltered = 0
  let totalRejected = 0

  // Claim function — inline, uses same auth context
  async function claimBatch() {
    const batchId = uuidv7()

    await exec(
      `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTERING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP
       WHERE EMAIL_METADATA_ID IN (
         SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES WHERE STATUS = '${STATUS.PENDING}' LIMIT ${batchSize}
       )`,
    )

    const claimed = await exec(
      `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
       FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
    )

    if (claimed.length > 0) {
      await insertBatchEvent(exec, schema, {
        triggerHash: batchId,
        batchId,
        batchType: 'filter',
        eventType: 'new',
      })
      console.log(`[filter-pipeline] claimed ${claimed.length} items → batch ${batchId}`)
      return { batch_id: batchId, count: claimed.length, attempts: 0, rows: claimed }
    }

    // Look for stuck batches
    const stuck = await exec(
      `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS
       FROM ${schema}.DEAL_STATES ds
       LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
       WHERE ds.STATUS = '${STATUS.FILTERING}'
         AND ds.BATCH_ID IS NOT NULL
         AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
       GROUP BY ds.BATCH_ID
       HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries}
       LIMIT 1`,
    )

    if (stuck.length === 0) return null

    const stuckBatchId = stuck[0].BATCH_ID
    const attempts = stuck[0].ATTEMPTS || 0
    const stuckRows = await exec(
      `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
       FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`,
    )
    await exec(
      `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${stuckBatchId}'`,
    )
    await insertBatchEvent(exec, schema, {
      triggerHash: uuidv7(),
      batchId: stuckBatchId,
      batchType: 'filter',
      eventType: 'retrigger',
    })
    console.log(
      `[filter-pipeline] reclaimed stuck batch ${stuckBatchId} (${attempts} prior attempts)`,
    )
    return { batch_id: stuckBatchId, count: stuckRows.length, attempts, rows: stuckRows }
  }

  // Worker function — processes a single filter batch
  async function processFilterBatch(batch, { attempt }) {
    const { batch_id, rows } = batch
    console.log(`[filter-pipeline] processing batch ${batch_id} (attempt ${attempt})`)

    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const messageIds = rows.map((r) => r.MESSAGE_ID)
    const userId = rows[0].USER_ID
    const syncStateId = rows[0].SYNC_STATE_ID

    // Fetch email headers
    const emails = await fetchEmails(messageIds, metaByMessageId, {
      contentFetcherUrl,
      userId,
      syncStateId,
      chunkSize,
      fetchTimeoutMs,
      format: 'metadata',
    })

    // Apply filter rules
    const filteredIds = []
    const rejectedIds = []
    for (const email of emails) {
      if (isRejected(email)) {
        rejectedIds.push(email.id)
      } else {
        filteredIds.push(email.id)
      }
    }

    // Save results
    if (filteredIds.length > 0) {
      const quotedIds = filteredIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await exec(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.PENDING_CLASSIFICATION}' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }
    if (rejectedIds.length > 0) {
      const quotedIds = rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(',')
      await exec(
        `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.FILTER_REJECTED}' WHERE EMAIL_METADATA_ID IN (${quotedIds})`,
      )
    }

    await insertBatchEvent(exec, schema, {
      triggerHash: uuidv7(),
      batchId: batch_id,
      batchType: 'filter',
      eventType: 'complete',
    })

    totalFiltered += filteredIds.length
    totalRejected += rejectedIds.length
    console.log(
      `[filter-pipeline] batch ${batch_id} done: ${filteredIds.length} passed, ${rejectedIds.length} rejected`,
    )
  }

  console.log(
    `[filter-pipeline] starting (concurrent=${maxConcurrent}, batchSize=${batchSize}, maxRetries=${maxRetries})`,
  )
  const result = await runPool(claimBatch, processFilterBatch, { maxConcurrent, maxRetries })

  console.log(
    `[filter-pipeline] complete: ${result.processed} batches processed, ${result.failed} failed, ${totalFiltered} filtered, ${totalRejected} rejected`,
  )
  return {
    batches_processed: result.processed,
    batches_failed: result.failed,
    total_filtered: totalFiltered,
    total_rejected: totalRejected,
  }
}
```

**Step 4: Run test**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/run-filter-pipeline.js __tests__/run-filter-pipeline.test.js
git commit -m "feat: add run-filter-pipeline command with concurrent batch processing"
```

---

### Task 6: Pipeline command — `src/commands/run-classify-pipeline.js`

Orchestrator for classify. Same pool pattern but with in-memory audit passing through the save chain.

**Files:**

- Create: `src/commands/run-classify-pipeline.js`
- Test: `__tests__/run-classify-pipeline.test.js`

**Step 1: Write the failing test**

Test: mock AI client returns valid classification, verify in-memory audit flows through save-evals → save-deals → save-deal-contacts → update-deal-states without re-querying audit. Test retry case where audit already exists in DB.

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js`
Expected: FAIL

**Step 3: Write implementation**

```js
// src/commands/run-classify-pipeline.js
import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import {
  sanitizeSchema,
  sanitizeId,
  sanitizeString,
  toSqlIdList,
  STATUS,
  saveResults,
  detection,
} from '../lib/queries.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { callModel, parseAndValidate } from '../lib/ai-client.js'
import { buildPrompt } from '../lib/build-prompt.js'
import { fetchEmails } from '../lib/email-client.js'
import { runPool, insertBatchEvent } from '../lib/pipeline.js'

export async function runClassifyPipeline() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const fallbackModel = core.getInput('fallback-model') || 'moonshotai/Kimi-K2-Instruct'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'
  const maxConcurrent = parseInt(core.getInput('max-concurrent') || '3', 10)
  const batchSize = parseInt(core.getInput('classify-batch-size') || '5', 10)
  const maxRetries = parseInt(core.getInput('max-retries') || '3', 10)
  const chunkSize = parseInt(core.getInput('chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '120000', 10)

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  let totalDeals = 0
  let totalNotDeals = 0

  // --- Claim function ---
  async function claimBatch() {
    const batchId = uuidv7()

    await exec(
      `UPDATE ${schema}.DEAL_STATES SET STATUS = '${STATUS.CLASSIFYING}', BATCH_ID = '${batchId}', UPDATED_AT = CURRENT_TIMESTAMP
       WHERE THREAD_ID IN (
         SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds
         WHERE ds.STATUS = '${STATUS.PENDING_CLASSIFICATION}'
           AND NOT EXISTS (
             SELECT 1 FROM ${schema}.DEAL_STATES ds2
             WHERE ds2.THREAD_ID = ds.THREAD_ID
               AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
               AND ds2.STATUS IN ('${STATUS.PENDING}', '${STATUS.FILTERING}')
           )
         LIMIT ${batchSize}
       ) AND STATUS = '${STATUS.PENDING_CLASSIFICATION}'`,
    )

    const claimed = await exec(
      `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
       FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${batchId}'`,
    )

    if (claimed.length > 0) {
      await insertBatchEvent(exec, schema, {
        triggerHash: batchId,
        batchId,
        batchType: 'classify',
        eventType: 'new',
      })
      console.log(`[classify-pipeline] claimed ${claimed.length} items → batch ${batchId}`)
      return { batch_id: batchId, count: claimed.length, attempts: 0, rows: claimed }
    }

    // Look for stuck classify batches
    const stuck = await exec(
      `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS
       FROM ${schema}.DEAL_STATES ds
       LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
       WHERE ds.STATUS = '${STATUS.CLASSIFYING}'
         AND ds.BATCH_ID IS NOT NULL
         AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
       GROUP BY ds.BATCH_ID
       HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${maxRetries}
       LIMIT 1`,
    )

    if (stuck.length === 0) return null

    const stuckBatchId = stuck[0].BATCH_ID
    const attempts = stuck[0].ATTEMPTS || 0
    const stuckRows = await exec(
      `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
       FROM ${schema}.DEAL_STATES WHERE BATCH_ID = '${stuckBatchId}'`,
    )
    await exec(
      `UPDATE ${schema}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${stuckBatchId}'`,
    )
    await insertBatchEvent(exec, schema, {
      triggerHash: uuidv7(),
      batchId: stuckBatchId,
      batchType: 'classify',
      eventType: 'retrigger',
    })
    console.log(
      `[classify-pipeline] reclaimed stuck batch ${stuckBatchId} (${attempts} prior attempts)`,
    )
    return { batch_id: stuckBatchId, count: stuckRows.length, attempts, rows: stuckRows }
  }

  // --- Worker function ---
  async function processClassifyBatch(batch, { attempt }) {
    const { batch_id: batchId, rows } = batch
    console.log(`[classify-pipeline] processing batch ${batchId} (attempt ${attempt})`)

    const metaByMessageId = new Map(rows.map((r) => [r.MESSAGE_ID, r]))
    const messageIds = rows.map((r) => r.MESSAGE_ID)
    const userId = rows[0].USER_ID
    const syncStateId = rows[0].SYNC_STATE_ID

    // --- Step 1: Get or create audit (in-memory) ---
    let threads

    const existingAudit = await exec(saveResults.getAuditByBatchId(schema, batchId))
    if (existingAudit.length > 0 && existingAudit[0].AI_EVALUATION) {
      try {
        const parsed = JSON.parse(existingAudit[0].AI_EVALUATION)
        threads = parsed.threads || parsed || []
        console.log(
          `[classify-pipeline] batch ${batchId} using existing audit (${threads.length} threads)`,
        )
      } catch {
        console.log(
          `[classify-pipeline] batch ${batchId} existing audit invalid JSON, re-running AI`,
        )
      }
    }

    let modelUsed = primaryModel

    if (!threads) {
      // Fetch email content
      const emails = await fetchEmails(messageIds, metaByMessageId, {
        contentFetcherUrl,
        userId,
        syncStateId,
        chunkSize,
        fetchTimeoutMs,
      })

      // Build prompt + AI resilience pipeline
      const { systemPrompt, userPrompt } = buildPrompt(emails)
      const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }
      const classifyMessages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ]

      // Layer 0: Primary model
      let primaryRaw
      try {
        const result = await callModel(primaryModel, classifyMessages, {
          temperature: 0,
          ...aiOpts,
        })
        primaryRaw = result.content
      } catch (err) {
        console.log(`[classify-pipeline] primary model failed: ${err.message}`)
        primaryRaw = null
      }

      if (primaryRaw) {
        // Layer 1: Local JSON repair
        try {
          threads = parseAndValidate(primaryRaw)
        } catch (parseError) {
          // Layer 2: Corrective retry
          try {
            const correctiveMessages = [
              ...classifyMessages,
              { role: 'assistant', content: primaryRaw },
              {
                role: 'user',
                content: `Your previous classification response could not be parsed as valid JSON.\n\nParse error:\n${parseError.message}\n\nPlease return the corrected classification as a valid JSON array. Fix only the JSON formatting issue. Do not change any classification decisions. Return ONLY the JSON array with no other text.`,
              },
            ]
            const corrected = await callModel(primaryModel, correctiveMessages, {
              temperature: 0,
              ...aiOpts,
            })
            threads = parseAndValidate(corrected.content)
            modelUsed = `${primaryModel}(corrective-retry)`
          } catch (correctiveError) {
            console.log(`[classify-pipeline] corrective retry failed: ${correctiveError.message}`)
          }
        }
      }

      // Layer 3: Fallback model
      if (!threads) {
        modelUsed = fallbackModel
        const fallbackResult = await callModel(fallbackModel, classifyMessages, {
          temperature: 0.6,
          ...aiOpts,
        })
        threads = parseAndValidate(fallbackResult.content)
      }

      // Save audit checkpoint
      const aiOutput = { threads }
      const evaluation = sanitizeString(JSON.stringify(aiOutput).substring(0, 6400))
      try {
        await exec(
          saveResults.insertAudit(schema, {
            id: uuidv7(),
            batchId,
            threadCount: threads.length,
            emailCount: rows.length,
            cost: 0,
            inputTokens: 0,
            outputTokens: 0,
            model: modelUsed,
            evaluation,
          }),
        )
      } catch (err) {
        if (
          !err.message.includes('integrity constraint') &&
          !err.message.includes('unique') &&
          !err.message.includes('duplicate')
        ) {
          throw err
        }
      }
    }

    // --- Step 2: Save evals (in-memory) ---
    if (threads.length > 0) {
      const evalValues = threads
        .map((thread) => {
          const threadId = sanitizeId(thread.thread_id)
          return `('${uuidv7()}', '${threadId}', '', '${sanitizeString(thread.category || '')}', '${sanitizeString(thread.ai_summary || '')}', ${thread.is_deal ? 'true' : 'false'}, ${(thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'}, ${typeof thread.ai_score === 'number' ? thread.ai_score : 0}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
        })
        .join(', ')

      await exec(
        `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
          (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
        VALUES ${evalValues}
        ON CONFLICT (THREAD_ID) DO UPDATE SET
          AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID,
          AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY,
          IS_DEAL = EXCLUDED.IS_DEAL, LIKELY_SCAM = EXCLUDED.LIKELY_SCAM,
          AI_SCORE = EXCLUDED.AI_SCORE, UPDATED_AT = CURRENT_TIMESTAMP`,
      )
    }

    // --- Step 3: Save deals (in-memory) ---
    const userByThread = {}
    for (const row of rows) {
      userByThread[row.THREAD_ID] = row.USER_ID
    }

    const dealThreads = threads.filter((t) => t.is_deal)
    const notDealThreadIds = threads.filter((t) => !t.is_deal).map((t) => sanitizeId(t.thread_id))

    if (notDealThreadIds.length > 0) {
      const quotedIds = notDealThreadIds.map((id) => `'${id}'`).join(',')
      await exec(`DELETE FROM ${schema}.DEALS WHERE THREAD_ID IN (${quotedIds})`)
    }

    if (dealThreads.length > 0) {
      const dealValues = dealThreads
        .map((thread) => {
          const threadId = sanitizeId(thread.thread_id)
          const uid = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
          const brand = thread.main_contact ? sanitizeString(thread.main_contact.company || '') : ''
          const dealValue =
            typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
          return `('${uuidv7()}', '${uid}', '${threadId}', '', '${sanitizeString(thread.deal_name || '')}', '${sanitizeString(thread.deal_type || '')}', '${sanitizeString(thread.category || '')}', ${dealValue}, '${sanitizeString(thread.currency || 'USD')}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
        })
        .join(', ')

      await exec(
        `INSERT INTO ${schema}.DEALS
          (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
        VALUES ${dealValues}
        ON CONFLICT (THREAD_ID) DO UPDATE SET
          EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID,
          DEAL_NAME = EXCLUDED.DEAL_NAME, DEAL_TYPE = EXCLUDED.DEAL_TYPE,
          CATEGORY = EXCLUDED.CATEGORY, VALUE = EXCLUDED.VALUE,
          CURRENCY = EXCLUDED.CURRENCY, BRAND = EXCLUDED.BRAND,
          UPDATED_AT = CURRENT_TIMESTAMP`,
      )
    }

    // --- Step 4: Save deal contacts (in-memory) ---
    if (dealThreads.length > 0) {
      const dealThreadIds = dealThreads.map((t) => sanitizeId(t.thread_id))
      const quotedIds = dealThreadIds.map((id) => `'${id}'`).join(',')
      const deals = await exec(
        `SELECT ID, THREAD_ID FROM ${schema}.DEALS WHERE THREAD_ID IN (${quotedIds})`,
      )
      const dealByThread = {}
      for (const row of deals) dealByThread[row.THREAD_ID] = row.ID

      const existingDealIds = Object.values(dealByThread)
      if (existingDealIds.length > 0) {
        const quotedDealIds = existingDealIds.map((id) => `'${sanitizeId(id)}'`).join(',')
        await exec(`DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID IN (${quotedDealIds})`)
      }

      const contactValues = []
      for (const thread of dealThreads) {
        const mc = thread.main_contact
        if (!mc || !mc.email) continue
        const dealId = dealByThread[sanitizeId(thread.thread_id)]
        if (!dealId) continue
        contactValues.push(
          `('${uuidv7()}', '${sanitizeId(dealId)}', '${sanitizeString(mc.email)}', 'primary', '${sanitizeString(mc.name || '')}', '${sanitizeString(mc.email)}', '${sanitizeString(mc.company || '')}', '${sanitizeString(mc.title || '')}', '${sanitizeString(mc.phone_number || '')}', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        )
      }

      if (contactValues.length > 0) {
        await exec(
          `INSERT INTO ${schema}.DEAL_CONTACTS
            (ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, NAME, EMAIL, COMPANY, TITLE, PHONE_NUMBER, IS_FAVORITE, CREATED_AT, UPDATED_AT)
          VALUES ${contactValues.join(', ')}`,
        )
      }
    }

    // --- Step 5: Update deal states to terminal ---
    const metadataByThread = {}
    for (const row of rows) {
      if (!metadataByThread[row.THREAD_ID]) metadataByThread[row.THREAD_ID] = []
      metadataByThread[row.THREAD_ID].push(row)
    }

    const dealEmailIds = []
    const notDealEmailIds = []
    for (const thread of threads) {
      const threadId = sanitizeId(thread.thread_id)
      const threadEmails = metadataByThread[threadId] || []
      const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
      if (thread.is_deal) {
        dealEmailIds.push(...emailIds)
      } else {
        notDealEmailIds.push(...emailIds)
      }
    }

    if (dealEmailIds.length > 0) {
      await exec(detection.updateDeals(schema, toSqlIdList(dealEmailIds)))
    }
    if (notDealEmailIds.length > 0) {
      await exec(detection.updateNotDeal(schema, toSqlIdList(notDealEmailIds)))
    }

    await insertBatchEvent(exec, schema, {
      triggerHash: uuidv7(),
      batchId,
      batchType: 'classify',
      eventType: 'complete',
    })

    totalDeals += dealEmailIds.length
    totalNotDeals += notDealEmailIds.length
    console.log(
      `[classify-pipeline] batch ${batchId} done: ${dealEmailIds.length} deals, ${notDealEmailIds.length} not_deals`,
    )
  }

  console.log(
    `[classify-pipeline] starting (concurrent=${maxConcurrent}, batchSize=${batchSize}, maxRetries=${maxRetries})`,
  )
  const result = await runPool(claimBatch, processClassifyBatch, { maxConcurrent, maxRetries })

  console.log(
    `[classify-pipeline] complete: ${result.processed} batches, ${result.failed} failed, ${totalDeals} deals, ${totalNotDeals} not_deals`,
  )
  return {
    batches_processed: result.processed,
    batches_failed: result.failed,
    total_deals: totalDeals,
    total_not_deals: totalNotDeals,
  }
}
```

**Step 4: Run test**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/run-classify-pipeline.js __tests__/run-classify-pipeline.test.js
git commit -m "feat: add run-classify-pipeline command with in-memory audit flow"
```

---

### Task 7: Register commands in main.js and action.yml

Wire up all new commands.

**Files:**

- Modify: `src/main.js:1-24`
- Modify: `action.yml:5-74`

**Step 1: Add imports and COMMANDS entries in `src/main.js`**

Add to imports (after line 11):

```js
import { runClaimFilterBatch } from './commands/claim-filter-batch.js'
import { runClaimClassifyBatch } from './commands/claim-classify-batch.js'
import { runFilterPipeline } from './commands/run-filter-pipeline.js'
import { runClassifyPipeline } from './commands/run-classify-pipeline.js'
```

Add to COMMANDS object (after line 23):

```js
  'claim-filter-batch': runClaimFilterBatch,
  'claim-classify-batch': runClaimClassifyBatch,
  'run-filter-pipeline': runFilterPipeline,
  'run-classify-pipeline': runClassifyPipeline,
```

**Step 2: Add new inputs to `action.yml`**

Add after existing inputs (before `outputs:`):

```yaml
max-concurrent:
  description: 'Max parallel batch workers (run-filter-pipeline, run-classify-pipeline)'
  default: '5'
filter-batch-size:
  description: 'Items per filter batch (run-filter-pipeline)'
  default: '200'
classify-batch-size:
  description: 'Threads per classify batch (run-classify-pipeline)'
  default: '5'
max-retries:
  description: 'Max attempts per batch before dead letter (run-filter-pipeline, run-classify-pipeline)'
  default: '3'
```

**Step 3: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js`
Expected: All PASS

**Step 4: Commit**

```bash
git add src/main.js action.yml
git commit -m "feat: register compact pipeline commands in main.js and action.yml"
```

---

### Task 8: Workflow YAML files

Create the three compact workflow files.

**Files:**

- Create: `.github/workflows/dealsync-compact-sync.yml`
- Create: `.github/workflows/dealsync-compact-filter.yml`
- Create: `.github/workflows/dealsync-compact-classify.yml`

**Step 1: Create sync workflow**

```yaml
# .github/workflows/dealsync-compact-sync.yml
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
      - id: sync
        name: Sync deal states from email metadata
        uses: creatorland/dealsync-action@main
        with:
          command: sync-deal-states
          offset: ${{ inputs.offset }}
          limit: ${{ inputs.limit }}
          auth-url: ${{ secrets.SXT_AUTH_URL }}
          auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
          api-url: ${{ secrets.SXT_API_URL }}
          biscuit: ${{ secrets.SXT_BISCUIT }}
          schema: ${{ secrets.SXT_SCHEMA }}
          rate-limiter-url: ${{ secrets.SXT_RATE_LIMITER_URL }}
          rate-limiter-api-key: ${{ secrets.SXT_RATE_LIMITER_API_KEY }}
```

**Step 2: Create filter workflow**

```yaml
# .github/workflows/dealsync-compact-filter.yml
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
      - id: filter
        name: Run filter pipeline
        uses: creatorland/dealsync-action@main
        with:
          command: run-filter-pipeline
          max-concurrent: ${{ inputs.max_concurrent }}
          filter-batch-size: ${{ inputs.filter_batch_size }}
          max-retries: ${{ inputs.max_retries }}
          chunk-size: ${{ inputs.chunk_size }}
          fetch-timeout-ms: ${{ inputs.fetch_timeout_ms }}
          auth-url: ${{ secrets.SXT_AUTH_URL }}
          auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
          api-url: ${{ secrets.SXT_API_URL }}
          biscuit: ${{ secrets.SXT_BISCUIT }}
          schema: ${{ secrets.SXT_SCHEMA }}
          rate-limiter-url: ${{ secrets.SXT_RATE_LIMITER_URL }}
          rate-limiter-api-key: ${{ secrets.SXT_RATE_LIMITER_API_KEY }}
          content-fetcher-url: ${{ secrets.CONTENT_FETCHER_URL }}
```

**Step 3: Create classify workflow**

```yaml
# .github/workflows/dealsync-compact-classify.yml
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
      - id: classify
        name: Run classify pipeline
        uses: creatorland/dealsync-action@main
        with:
          command: run-classify-pipeline
          max-concurrent: ${{ inputs.max_concurrent }}
          classify-batch-size: ${{ inputs.classify_batch_size }}
          max-retries: ${{ inputs.max_retries }}
          chunk-size: ${{ inputs.chunk_size }}
          fetch-timeout-ms: ${{ inputs.fetch_timeout_ms }}
          auth-url: ${{ secrets.SXT_AUTH_URL }}
          auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
          api-url: ${{ secrets.SXT_API_URL }}
          biscuit: ${{ secrets.SXT_BISCUIT }}
          schema: ${{ secrets.SXT_SCHEMA }}
          rate-limiter-url: ${{ secrets.SXT_RATE_LIMITER_URL }}
          rate-limiter-api-key: ${{ secrets.SXT_RATE_LIMITER_API_KEY }}
          content-fetcher-url: ${{ secrets.CONTENT_FETCHER_URL }}
          hyperbolic-key: ${{ secrets.HYPERBOLIC_KEY }}
          primary-model: ${{ secrets.AI_PRIMARY_MODEL }}
          fallback-model: ${{ secrets.AI_FALLBACK_MODEL }}
```

**Step 4: Commit**

```bash
git add .github/workflows/dealsync-compact-sync.yml .github/workflows/dealsync-compact-filter.yml .github/workflows/dealsync-compact-classify.yml
git commit -m "feat: add compact workflow YAML files for sync, filter, classify"
```

---

### Task 9: Bundle and verify

**Step 1: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js`
Expected: All PASS

**Step 2: Bundle**

Run: `npm run package`
Expected: `dist/index.js` regenerated with new commands

**Step 3: Run format**

Run: `npm run format:write`

**Step 4: Run full suite**

Run: `npm run all`
Expected: format + test + package all pass

**Step 5: Final commit**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist with compact pipeline commands"
```
