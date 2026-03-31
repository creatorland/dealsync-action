# Simplify Codebase — Delete Dead Commands, Consolidate lib/ Files

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove 9 unused command files + tests, merge email/AI lib files, rename write-batcher, delete unused crypto.js — reduce file count from 25 source files to 13.

**Architecture:** The compact pipeline refactor (PR #6/#7) consolidated all pipeline logic into `run-filter-pipeline.js` and `run-classify-pipeline.js`, making 9 legacy command files dead code. The lib/ directory has 3 email files that form a tight unit and 2 AI files always used together — merging each group reduces cognitive overhead without losing clarity.

**Tech Stack:** Node 24, ESM, Jest (with `--experimental-vm-modules`), Rollup bundler

---

### Task 1: Delete dead command files

These 9 commands are not referenced by any workflow file and no active command imports them. Only `src/main.js` imports them for the COMMANDS dispatch map.

**Files:**

- Delete: `src/commands/fetch-and-filter.js`
- Delete: `src/commands/fetch-and-classify.js`
- Delete: `src/commands/save-evals.js`
- Delete: `src/commands/save-deals.js`
- Delete: `src/commands/save-deal-contacts.js`
- Delete: `src/commands/update-deal-states.js`
- Delete: `src/commands/sxt-execute.js`
- Delete: `src/commands/claim-filter-batch.js`
- Delete: `src/commands/claim-classify-batch.js`

**Step 1: Delete the 9 command files**

```bash
rm src/commands/fetch-and-filter.js \
   src/commands/fetch-and-classify.js \
   src/commands/save-evals.js \
   src/commands/save-deals.js \
   src/commands/save-deal-contacts.js \
   src/commands/update-deal-states.js \
   src/commands/sxt-execute.js \
   src/commands/claim-filter-batch.js \
   src/commands/claim-classify-batch.js
```

**Step 2: Update `src/main.js`**

Remove the 9 dead imports (lines 3-9, 12-13) and their COMMANDS entries. Result should be:

```javascript
import * as core from '@actions/core'
import { runSyncDealStates } from './commands/sync-deal-states.js'
import { runEval } from './commands/eval.js'
import { runEvalCompare } from './commands/eval-compare.js'
import { runFilterPipeline } from './commands/run-filter-pipeline.js'
import { runClassifyPipeline } from './commands/run-classify-pipeline.js'

const COMMANDS = {
  'sync-deal-states': runSyncDealStates,
  eval: runEval,
  'eval-compare': runEvalCompare,
  'run-filter-pipeline': runFilterPipeline,
  'run-classify-pipeline': runClassifyPipeline,
}

export async function run() {
  // ... keep existing dispatch logic unchanged
}
```

**Step 3: Update `action.yml`**

Update lines 2 and 7 to only list active commands:

- `description:` → `'Dealsync pipeline action — run-filter-pipeline, run-classify-pipeline, sync-deal-states, eval, eval-compare'`
- `command.description:` → `'Operation: run-filter-pipeline, run-classify-pipeline, sync-deal-states, eval, eval-compare'`

Remove input descriptions that reference only dead commands (but keep inputs that are shared with active commands). Specifically, remove these inputs that are ONLY used by dead commands:

- `sxt-command` (line 31 — only used by sxt-execute)

Keep all other inputs — they're used by active pipeline commands or eval.

**Step 4: Delete dead command test files**

```bash
rm __tests__/claim-classify-batch.test.js \
   __tests__/claim-filter-batch.test.js \
   __tests__/save-deal-contacts.test.js
```

Note: No test files exist for fetch-and-filter, fetch-and-classify, save-evals, save-deals, update-deal-states, or sxt-execute as standalone tests.

**Step 5: Update `__tests__/main.test.js`**

Remove the 9 dead command mocks (lines 18-62) and update the test cases:

- Change the "routes to fetch-and-filter" test → route to `run-filter-pipeline` instead
- Change the "fails when command throws" test → use `run-classify-pipeline` instead of `fetch-and-classify`

Updated test file:

```javascript
import { jest } from '@jest/globals'

const outputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

jest.unstable_mockModule('../src/commands/sync-deal-states.js', () => ({
  runSyncDealStates: jest.fn().mockResolvedValue({ synced_count: 10, conflict_count: 0 }),
}))

jest.unstable_mockModule('../src/commands/eval.js', () => ({
  runEval: jest.fn().mockResolvedValue({ detection: {}, runs: 1 }),
}))

jest.unstable_mockModule('../src/commands/eval-compare.js', () => ({
  runEvalCompare: jest.fn().mockResolvedValue({ verdict: 'PASS' }),
}))

jest.unstable_mockModule('../src/commands/run-filter-pipeline.js', () => ({
  runFilterPipeline: jest.fn().mockResolvedValue({
    batches_processed: 0,
    batches_failed: 0,
    total_filtered: 0,
    total_rejected: 0,
  }),
}))

jest.unstable_mockModule('../src/commands/run-classify-pipeline.js', () => ({
  runClassifyPipeline: jest.fn().mockRejectedValue(new Error('classify-pipeline not mocked')),
}))

const core = await import('@actions/core')
const { run } = await import('../src/main.js')

describe('dealsync main (command router)', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
  })

  it('routes to run-filter-pipeline command and sets result', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'run-filter-pipeline' : ''))

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({
      batches_processed: 0,
      batches_failed: 0,
      total_filtered: 0,
      total_rejected: 0,
    })
  })

  it('fails on unknown command', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'bogus' : ''))

    await run()

    expect(core.setFailed).toHaveBeenCalledWith(expect.stringContaining('Unknown command: "bogus"'))
  })

  it('sets success=false when command throws', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'run-classify-pipeline' : ''))

    await expect(run()).rejects.toThrow('classify-pipeline not mocked')

    expect(outputs['success']).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith('classify-pipeline not mocked')
  })
})
```

**Step 6: Run tests**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/main.test.js
```

Expected: 3 tests PASS

**Step 7: Commit**

```bash
git add -A
git commit -m "refactor: remove 9 dead command files and tests"
```

---

### Task 2: Delete unused `crypto.js`

No file in `src/` imports from `crypto.js`. It's dead code.

**Files:**

- Delete: `src/lib/crypto.js`

**Step 1: Delete the file**

```bash
rm src/lib/crypto.js
```

**Step 2: Run full test suite to confirm nothing breaks**

```bash
npm test
```

Expected: All tests PASS

**Step 3: Commit**

```bash
git add -A
git commit -m "refactor: remove unused crypto.js"
```

---

### Task 3: Merge email files → `src/lib/emails.js`

Merge `email-client.js` (fetch), `email-sanitizer.js` (parse/clean), and `filter-rules.js` (reject) into a single `emails.js`. These form a tight unit: filter-rules imports email-sanitizer, and all operate on email data.

**Files:**

- Delete: `src/lib/email-client.js` (126 lines)
- Delete: `src/lib/email-sanitizer.js` (71 lines)
- Delete: `src/lib/filter-rules.js` (92 lines)
- Create: `src/lib/emails.js`
- Modify: `src/commands/run-filter-pipeline.js` (update imports)
- Modify: `src/commands/run-classify-pipeline.js` (update imports)
- Modify: `src/commands/eval.js` (update imports)
- Modify: `src/lib/prompt.js` (update imports)
- Modify: `__tests__/email-client.test.js` → rename to `__tests__/emails.test.js`

**Step 1: Create `src/lib/emails.js`**

Concatenate the three files into one, combining imports. The file should contain:

- All imports from email-sanitizer.js (`convert` from `html-to-text`, `EmailReplyParser` from `email-reply-parser`)
- All imports from filter-rules.js (the 6 config JSON imports)
- All imports from email-client.js (`withTimeout` from `sxt-client.js`, `sleep`/`backoffMs` from `retry.js`)
- All exports from each file: `getHeader`, `sanitizeEmailBody`, `isRejected`, `fetchEmails`

Order the code logically: sanitizer functions first (dependencies of filter-rules), then filter rules, then email client (fetching).

**Step 2: Update all consumers**

Find-and-replace imports across the codebase:

| File                                    | Old Import                                                          | New Import                                                         |
| --------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `src/commands/run-filter-pipeline.js`   | `from '../lib/filter-rules.js'` and `from '../lib/email-client.js'` | `from '../lib/emails.js'` (combine: `{ isRejected, fetchEmails }`) |
| `src/commands/run-classify-pipeline.js` | `from '../lib/email-client.js'`                                     | `from '../lib/emails.js'` (`{ fetchEmails }`)                      |
| `src/commands/eval.js`                  | `from '../lib/filter-rules.js'`                                     | `from '../lib/emails.js'` (`{ isRejected }`)                       |
| `src/lib/prompt.js`                     | `from './email-sanitizer.js'`                                       | `from './emails.js'` (`{ getHeader, sanitizeEmailBody }`)          |

**Step 3: Rename test file**

```bash
mv __tests__/email-client.test.js __tests__/emails.test.js
```

Update the import inside the test file: change `../src/lib/email-client.js` → `../src/lib/emails.js`. The mock for `../src/lib/sxt-client.js` stays the same.

**Step 4: Delete old files**

```bash
rm src/lib/email-client.js src/lib/email-sanitizer.js src/lib/filter-rules.js
```

**Step 5: Run tests**

```bash
npm test
```

Expected: All tests PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor: merge email-client, email-sanitizer, filter-rules into emails.js"
```

---

### Task 4: Merge AI files → `src/lib/ai.js`

Merge `ai-client.js` (model calling + validation) and `prompt.js` (prompt building) into `ai.js`. These are always used together in classify and eval commands.

**Files:**

- Delete: `src/lib/ai-client.js` (184 lines)
- Delete: `src/lib/prompt.js` (66 lines)
- Create: `src/lib/ai.js`
- Modify: `src/commands/run-classify-pipeline.js` (update imports)
- Modify: `src/commands/eval.js` (update imports)
- Modify: `__tests__/build-prompt.test.js` → rename to `__tests__/ai.test.js`

**Step 1: Create `src/lib/ai.js`**

Concatenate `ai-client.js` and `prompt.js`. Combine imports:

- From ai-client.js: `sleep`, `backoffMs` from `./retry.js`
- From prompt.js: `getHeader`, `sanitizeEmailBody` from `./emails.js` (note: updated path from Task 3)
- From prompt.js: `systemTemplate` from `../../prompts/system.md`, `classificationInstructions` from `../../prompts/user.md`

Exports: `callModel`, `parseAndValidate`, `buildPrompt`, `AI_REQUEST_TIMEOUT_MS`, `AI_RETRY_DELAY_MS`, `MAX_HTTP_RETRIES`, `MAX_TOKENS`, `VALID_CATEGORIES`, `VALID_DEAL_TYPES`

Order: prompt-building first, then AI client (call + parse).

**Step 2: Update all consumers**

| File                                    | Old Imports                                                | New Import                                                                                                                                                |
| --------------------------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/commands/run-classify-pipeline.js` | `from '../lib/ai-client.js'` and `from '../lib/prompt.js'` | `from '../lib/ai.js'` (`{ callModel, parseAndValidate, buildPrompt }`)                                                                                    |
| `src/commands/eval.js`                  | `from '../lib/ai-client.js'` and `from '../lib/prompt.js'` | `from '../lib/ai.js'` (`{ callModel, parseAndValidate, buildPrompt, isRejected }`) — wait, `isRejected` comes from `emails.js`, keep that import separate |

Corrected eval.js imports:

```javascript
import { callModel, parseAndValidate, buildPrompt } from '../lib/ai.js'
import { isRejected } from '../lib/emails.js'
```

**Step 3: Rename test file**

```bash
mv __tests__/build-prompt.test.js __tests__/ai.test.js
```

Update the import inside: `../src/lib/prompt.js` → `../src/lib/ai.js`

**Step 4: Delete old files**

```bash
rm src/lib/ai-client.js src/lib/prompt.js
```

**Step 5: Run tests**

```bash
npm test
```

Expected: All tests PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor: merge ai-client and prompt into ai.js"
```

---

### Task 5: Rename `write-batcher.js` → `sql-batcher.js`

The batcher exclusively batches SQL operations for SxT. The name `sql-batcher` is more descriptive and pairs with `sxt-client.js`.

**Files:**

- Rename: `src/lib/write-batcher.js` → `src/lib/sql-batcher.js`
- Modify: `src/commands/run-classify-pipeline.js` (update import)
- Rename: `__tests__/write-batcher.test.js` → `__tests__/sql-batcher.test.js`

**Step 1: Rename source file**

```bash
mv src/lib/write-batcher.js src/lib/sql-batcher.js
```

**Step 2: Update import in `src/commands/run-classify-pipeline.js`**

Change `from '../lib/write-batcher.js'` → `from '../lib/sql-batcher.js'`

**Step 3: Rename and update test file**

```bash
mv __tests__/write-batcher.test.js __tests__/sql-batcher.test.js
```

Update the import inside: `../src/lib/write-batcher.js` → `../src/lib/sql-batcher.js`

**Step 4: Run tests**

```bash
npm test
```

Expected: All tests PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename write-batcher to sql-batcher"
```

---

### Task 6: Update CLAUDE.md and rebuild

**Files:**

- Modify: `CLAUDE.md`

**Step 1: Update CLAUDE.md architecture section**

Update the "Architecture" section to reflect the simplified file structure:

- Remove references to the 9 deleted commands and their pipeline stages
- Update lib file names (emails.js, ai.js, sql-batcher.js)
- Remove the "Checkpoint/Audit Pattern" section's references to individual save-\* commands (now internal to run-classify-pipeline)
- Remove crypto.js from any mention

**Step 2: Rebuild the bundle**

```bash
npm run all
```

Expected: format + test + package all pass

**Step 3: Commit**

```bash
git add -A
git commit -m "docs: update CLAUDE.md for simplified codebase"
```

---

## Final State

### src/commands/ (5 files, was 14)

```
run-filter-pipeline.js
run-classify-pipeline.js
sync-deal-states.js
eval.js
eval-compare.js
```

### src/lib/ (7 files + sql/, was 11 + sql/)

```
emails.js          (was: email-client + email-sanitizer + filter-rules)
ai.js              (was: ai-client + prompt)
sql-batcher.js     (was: write-batcher)
constants.js       (unchanged)
pipeline.js        (unchanged)
retry.js           (unchanged)
metrics.js         (unchanged)
sxt-client.js      (unchanged)
sql/               (unchanged)
```

### **tests**/ (updated)

```
emails.test.js       (was: email-client.test.js)
ai.test.js           (was: build-prompt.test.js)
sql-batcher.test.js  (was: write-batcher.test.js)
main.test.js         (updated)
eval-compare.test.js (unchanged)
metrics.test.js      (unchanged)
pipeline.test.js     (unchanged)
run-filter-pipeline.test.js  (unchanged)
run-classify-pipeline.test.js (unchanged)
sync-deal-states.test.js (unchanged)
sql/                 (unchanged)
```
