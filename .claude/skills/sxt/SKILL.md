---
name: sxt
description: Use when querying, debugging, or managing Space and Time (SxT) database tables for DealSync/VIPC. Covers auth model (proxy JWT + direct login), biscuit generation, table management scripts, E2E tracking, and schema definitions across email_core_staging and dealsync_stg_v1.
allowed-tools: Read, Glob, Grep, Bash
paths: "src/lib/db.js,src/lib/batcher.js,src/sql/**"
---

# Space and Time (SxT) Database Skill

## Overview

SxT is the SQL data warehouse for DealSync. Two schemas: `email_core_staging` (shared email ingestion) and `dealsync_stg_v1` (deal processing). Email content is NOT stored in SxT — fetched on-demand from Gmail via content fetcher.

For full table definitions, see [schemas.md](schemas.md).

## Authentication

Two auth strategies (try proxy first, fall back to direct login):

### Proxy Auth (preferred — cached, no rate limit burn)

```bash
# Env: SXT_AUTH_URL + SXT_AUTH_SECRET
curl -H "x-shared-secret: $SXT_AUTH_SECRET" "$SXT_AUTH_URL"
# Returns: { data: "<jwt>" }
```

### Direct Login (fallback — when proxy returns stale/expired token)

```bash
# Uses SxT proxy login endpoint
curl -X POST https://proxy.api.makeinfinite.dev/auth/login \
  -H "Content-Type: application/json" \
  -d '{"userId": "ardata2", "password": "<SXT_AUTH_SECRET>"}'
# Returns: { accessToken: "<jwt>", sessionId: "<sid>" }
```

**If proxy token returns 401 on queries:** Use direct login. The proxy may cache stale JWTs.

### Biscuit Tokens (table-level authorization)

Every SxT query requires a biscuit token signed with the table's ED25519 private key:

```javascript
import { SpaceAndTime } from 'sxt-nodejs-sdk'
const sxt = new SpaceAndTime()
const auth = sxt.Authorization()
const biscuit = auth.CreateBiscuitToken(
  [{ operation: 'dql_select', resource: 'dealsync_stg_v1.deal_states' }],
  process.env.SXT_PRIVATE_KEY,
).data[0]
```

Operations: `dql_select`, `dml_insert`, `dml_update`, `dml_delete`, `ddl_create`, `ddl_drop`, `ddl_alter`

## Environment Variables

Set via `.env` (root directory):

| Var               | Purpose                                                  |
| ----------------- | -------------------------------------------------------- |
| `SXT_AUTH_URL`    | Proxy auth endpoint                                      |
| `SXT_AUTH_SECRET` | Proxy shared secret (also used as direct login password) |
| `SXT_API_URL`     | SxT REST API (https://api.makeinfinite.dev)              |
| `SXT_PRIVATE_KEY` | ED25519 private key for biscuit signing                  |
| `SXT_SCHEMA`      | Schema name (e.g., DEALSYNC_STG_V1)                      |

## Scripts (`${CLAUDE_SKILL_DIR}/scripts/`)

All scripts require: `cd ${CLAUDE_SKILL_DIR}/scripts && set -a && source ../../../../.env && set +a`

| Script                | Purpose                                                          | Usage                                                                                      |
| --------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `track-e2e.js`        | Track full pipeline progress (ingestion + deal_states by status) | `node --experimental-wasm-modules track-e2e.js <userId> [--poll 30]`                       |
| `reset-user.js`       | Delete all data for a user across both schemas                   | `node --experimental-wasm-modules reset-user.js <userId>`                                  |
| `seed-deal-states.js` | Create deal_states for testing                                   | `node --experimental-wasm-modules seed-deal-states.js <userId>`                            |
| `check-user-state.js` | Quick check of user's pipeline state                             | `node --experimental-wasm-modules check-user-state.js <userId>`                            |
| `manage-table.js`     | DDL operations (create/drop/recreate tables)                     | `node --experimental-wasm-modules manage-table.js <action> <table>`                        |
| `generate-biscuit.js` | Generate biscuit for given tables (all CRUD ops)                 | `node --experimental-wasm-modules generate-biscuit.js <table1> [table2] ... > biscuit.txt` |
| `trigger-sync.js`     | Trigger email sync for a user                                    | `node --experimental-wasm-modules trigger-sync.js <userId>`                                |
| `sxt-client.js`       | Shared auth/query client (imported by other scripts)             | Not run directly                                                                           |

### Generated Files (gitignored)

| File          | Purpose                                                                     |
| ------------- | --------------------------------------------------------------------------- |
| `biscuit.txt` | Latest generated biscuit token — copy this value to W3 `SXT_BISCUIT` secret |

### Generating a New Biscuit

When adding new tables or cross-schema queries, regenerate the biscuit:

```bash
cd ${CLAUDE_SKILL_DIR}/scripts
set -a && source ../../../../.env && set +a
node --experimental-wasm-modules generate-biscuit.js \
  email_core_staging.email_metadata \
  dealsync_stg_v1.deal_states \
  dealsync_stg_v1.deals \
  dealsync_stg_v1.deal_contacts \
  dealsync_stg_v1.contacts \
  dealsync_stg_v1.email_thread_evaluations \
  dealsync_stg_v1.ai_evaluation_audits \
  dealsync_stg_v1.batch_events \
  > biscuit.txt 2>/dev/null
# Strip WASM loading line if present:
grep -v "biscuit-wasm" biscuit.txt > biscuit-clean.txt && mv biscuit-clean.txt biscuit.txt
# Then update the W3 SXT_BISCUIT secret with the contents of biscuit.txt
```

## SxT API Behavior Notes

1. **Column names are UPPERCASE** — all column names normalized to UPPER_CASE in responses
2. **SELECT returns JSON array** — `[{ROW1}, {ROW2}]`
3. **DML returns empty array** — `[]` on success (no rowsAffected)
4. **Single statement per call** — `/v1/sql` accepts one SQL statement
5. **Schema-qualified table names required** — `DEALSYNC_STG_V1.DEAL_STATES`
6. **No parameterized queries** — values are string-interpolated into `sqlText`

## Schema Reference

See [schemas.md](schemas.md) for full table definitions.

## Quick Query Example

```bash
cd ${CLAUDE_SKILL_DIR}/scripts
set -a && source ../../../../.env && set +a
node --experimental-wasm-modules -e "
import { authenticate, generateBiscuit, executeSql } from './sxt-client.js'
const { jwt } = await authenticate()
const pk = process.env.SXT_PRIVATE_KEY
const biscuit = generateBiscuit('dql_select', 'dealsync_stg_v1.deal_states', pk)
const result = await executeSql(jwt, \"SELECT STATUS, COUNT(*) AS CNT FROM DEALSYNC_STG_V1.DEAL_STATES WHERE USER_ID = '<userId>' GROUP BY STATUS\", biscuit)
console.log(result)
"
```
