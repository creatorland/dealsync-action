---
name: compact-workflows
description: Use when triggering, monitoring, or debugging the dealsync compact workflows (sync, filter, classify). Covers GHA triggers, SxT database queries, cancelling runs, and checking pipeline progress.
---

# Compact Workflows Operations

## Trigger Workflows

```bash
# Sync (no inputs)
gh workflow run dealsync-compact-sync.yml --ref main

# Filter (defaults: 5 workers, 200 batch, 6 retries)
gh workflow run dealsync-compact-filter.yml --ref main

# Classify (defaults: 70 workers, 5 threads/batch, 6 retries)
gh workflow run dealsync-compact-classify.yml --ref main

# Classify with custom params
gh workflow run dealsync-compact-classify.yml --ref main \
  -f max_concurrent=30 \
  -f max_retries=50 \
  -f classify_batch_size=5
```

## Check Run Status

```bash
# Latest run per workflow
gh run list --workflow=dealsync-compact-sync.yml --limit 1 --json url,status,conclusion --jq '.[0]'
gh run list --workflow=dealsync-compact-filter.yml --limit 1 --json url,status,conclusion --jq '.[0]'
gh run list --workflow=dealsync-compact-classify.yml --limit 1 --json url,status,conclusion --jq '.[0]'

# All active runs
gh run list --status=queued --status=in_progress --json databaseId,workflowName --limit 200 --jq '.[] | "\(.workflowName)\t\(.databaseId)"'
```

## Cancel Runs

```bash
# Cancel specific workflow
gh run list --workflow=dealsync-compact-classify.yml --status=queued --status=in_progress --json databaseId --limit 10 --jq '.[].databaseId' | xargs -P 10 -I {} gh run cancel {} 2>&1

# Cancel ALL active runs
gh run list --status=queued --status=in_progress --json databaseId --limit 200 --jq '.[].databaseId' | xargs -P 30 -I {} gh run cancel {} 2>&1

# Cancel and retrigger
gh run cancel {RUN_ID} 2>&1; sleep 2; gh workflow run dealsync-compact-classify.yml --ref main -f max_concurrent=70 -f max_retries=50
```

## Check Database Progress

All SxT queries use the scripts at `/Users/rjlacanlaled/Work/ardata/dealsync-v2/.claude/skills/sxt/scripts/`. Must cd there and source env first:

```bash
cd /Users/rjlacanlaled/Work/ardata/dealsync-v2/.claude/skills/sxt/scripts && set -a && source ../../../../.env && set +a
```

### Deal States by Status

```bash
node --experimental-wasm-modules -e "
import { authenticate, executeSql, generateBiscuit } from './sxt-client.js'
const { jwt } = await authenticate()
const biscuit = generateBiscuit('dql_select', 'dealsync_stg_v1.deal_states', process.env.SXT_PRIVATE_KEY)
const result = await executeSql(jwt, 'SELECT STATUS, COUNT(*) AS CNT FROM DEALSYNC_STG_V1.DEAL_STATES GROUP BY STATUS ORDER BY CNT DESC', biscuit)
console.log(result)
const total = result.reduce((s, r) => s + r.CNT, 0)
const terminal = result.filter(r => ['deal','not_deal','filter_rejected'].includes(r.STATUS)).reduce((s, r) => s + r.CNT, 0)
console.log('Total:', total, 'Terminal:', terminal, 'Remaining:', total - terminal)
" 2>&1 | grep -v "biscuit-wasm\|ExperimentalWarning\|trace-warnings"
```

### Check Batch Attempts (stuck batches)

```bash
node --experimental-wasm-modules -e "
import { authenticate, executeSql, generateBiscuit } from './sxt-client.js'
const { jwt } = await authenticate()
const biscuit = generateBiscuit('dql_select', 'dealsync_stg_v1.batch_events', process.env.SXT_PRIVATE_KEY)
const result = await executeSql(jwt, 'SELECT BATCH_ID, COUNT(DISTINCT TRIGGER_HASH) AS ATTEMPTS FROM DEALSYNC_STG_V1.BATCH_EVENTS GROUP BY BATCH_ID HAVING COUNT(DISTINCT TRIGGER_HASH) >= 10 ORDER BY ATTEMPTS DESC LIMIT 20', biscuit)
console.log(result)
" 2>&1 | grep -v "biscuit-wasm\|ExperimentalWarning\|trace-warnings"
```

### Check Specific Batch

```bash
node --experimental-wasm-modules -e "
import { authenticate, executeSql, generateBiscuit } from './sxt-client.js'
const { jwt } = await authenticate()
const biscuit = generateBiscuit('dql_select', 'dealsync_stg_v1.deal_states', process.env.SXT_PRIVATE_KEY)
const result = await executeSql(jwt, \"SELECT EMAIL_METADATA_ID, THREAD_ID, STATUS FROM DEALSYNC_STG_V1.DEAL_STATES WHERE BATCH_ID = '{BATCH_ID}'\", biscuit)
console.log(result)
" 2>&1 | grep -v "biscuit-wasm\|ExperimentalWarning\|trace-warnings"
```

### Count Email Metadata

```bash
node --experimental-wasm-modules -e "
import { authenticate, executeSql, generateBiscuit } from './sxt-client.js'
const { jwt } = await authenticate()
const biscuit = generateBiscuit('dql_select', 'email_core_staging.email_metadata', process.env.SXT_PRIVATE_KEY)
const result = await executeSql(jwt, 'SELECT COUNT(*) AS CNT FROM EMAIL_CORE_STAGING.EMAIL_METADATA', biscuit)
console.log('email_metadata count:', result)
" 2>&1 | grep -v "biscuit-wasm\|ExperimentalWarning\|trace-warnings"
```

## Build & Push

```bash
npm run package           # bundle dist/index.js
npm run all               # format + test + package
git add -A && git commit -m "message" && git push origin main
```

## Key Config Defaults

| Setting           | Filter | Classify                           |
| ----------------- | ------ | ---------------------------------- |
| max-concurrent    | 5      | 70                                 |
| batch-size        | 200    | 5 (threads)                        |
| max-retries       | 6      | 6                                  |
| chunk-size        | 50     | 10                                 |
| fetch-timeout-ms  | 240000 | 240000                             |
| flush-interval-ms | n/a    | 5000                               |
| flush-threshold   | n/a    | 5                                  |
| primary-model     | n/a    | Qwen/Qwen3-235B-A22B-Instruct-2507 |
| fallback-model    | n/a    | deepseek-ai/DeepSeek-V3            |
| timeout-minutes   | 10     | 10                                 |

## Common Issues

- **"no pending rows, no stuck batches"**: All items are already in terminal states or stuck batches have updated_at < 5 min ago
- **SxT 400 on combined INSERT**: Write batcher falls back to individual items automatically
- **State updates not moving**: Check UPDATE response — should return `{ UPDATED: N }`, not `[]`
- **Rate limiter waiting**: Token acquisition can be slow with many workers — check `[sxt-client] Waiting for rate limit token...` logs
- **Stale audits**: If AI_EVALUATION_AUDITS was recreated, old cached audits may have truncated data. Clear audits and re-run.
