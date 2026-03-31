# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
npm install                  # install dependencies
npm test                     # run tests (uses --experimental-vm-modules for ESM)
npm run package              # bundle with rollup → dist/index.js
npm run all                  # format + test + package
npm run format:write         # prettier format
npm run format:check         # prettier check
```

Tests use Jest with ESM (no transform). Run a single test:

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/main.test.js
```

After changing any source file, run `npm run package` to regenerate `dist/index.js` — this is the bundled file that GitHub Actions executes (via `runs.main` in action.yml).

## Architecture

This is a **GitHub Action** (Node 24, ESM) that implements a multi-stage email deal detection pipeline for Creatorland. It runs on GitHub Actions and is orchestrated by W3 protocol workflows.

### Command Dispatch Pattern

Entry: `src/index.js` → `src/main.js` → `COMMANDS[command]()`. The `command` GitHub Action input selects which handler runs. Available commands: `run-filter-pipeline`, `run-classify-pipeline`, `sync-deal-states`, `eval`, `eval-compare`. All commands are async, return a JSON result, and set `success`/`result`/`error` outputs via `@actions/core`.

### Pipeline Commands

**`run-filter-pipeline`** — Claims batches of pending emails, fetches headers from content fetcher, applies 6 static rejection rules from `src/lib/emails.js` (configs in `config/*.json`), updates DEAL_STATES to `pending_classification` or `filter_rejected`. Runs batches concurrently via `runPool()`.

**`run-classify-pipeline`** — Claims batches of pending_classification emails, fetches bodies, calls AI classification, saves audit checkpoint to AI_EVALUATION_AUDITS, upserts evaluations/deals/contacts, and sets terminal deal states. Uses `WriteBatcher` (`src/lib/batcher.js`) for batched SQL writes. Runs batches concurrently via `runPool()`.

**`sync-deal-states`** — Paginated sync of deal states.

**`eval` / `eval-compare`** — Evaluation system (see below).

### AI Resilience Pipeline (ai.js)

4-layer fallback for classification:

- **Layer 0**: Primary model (Qwen3-235B) with HTTP retries + exponential backoff
- **Layer 1**: Local JSON repair (strip markdown fences, extract array, unwrap wrapper objects, coerce schema)
- **Layer 2**: Corrective retry — send broken output back to same model with parse error
- **Layer 3**: Fallback model (Kimi-K2) with temperature=0.6

`parseAndValidate()` handles schema coercion: clamps ai_score to 1-10, validates category/deal_type against enum sets, enforces string limits.

### Database Client (db.js)

Auth via proxy with shared secret → JWT. All SQL goes through `executeSql()` which acquires a rate-limit token (fail-open), executes against SxT REST API, and handles 401 re-auth. All column names are UPPERCASE (SxT convention).

### State Machine (deal-states.js)

```
pending → filtering → filter_rejected (terminal)
       → pending_classification → classifying → deal (terminal)
                                              → not_deal (terminal)
```

### Eval System

`eval` command runs multi-run classification against `eval/ground-truth.json` and computes metrics (recall, precision, F2, category accuracy, JSON health). `eval-compare` compares two eval results with pass/fail criteria (recall >= 95%, precision >= 40%, F2 non-regression, recall stddev <= 3%).

### Prompt System

Prompts live in `prompts/system.md` and `prompts/user.md`, imported as strings via rollup-plugin-string. `buildPrompt()` in `src/lib/ai.js` groups emails by thread, sanitizes bodies, and injects thread data into `{{THREAD_DATA}}` placeholder. The `eval` command can fetch prompts from a specific git commit via `prompt-hash`.

## Key Conventions

- All writes use `INSERT ... ON CONFLICT` for idempotency
- SQL IDs sanitized via `sanitizeId()` (alphanumeric, underscore, dash only)
- Schema names sanitized via `sanitizeSchema()` (alphanumeric, underscore only)
- String values escaped via `sanitizeString()` (single quotes doubled)
- Batch SQL operations — entire batches in single statements, no row-by-row loops
- YAML gotcha: hex environment names (0x...) must be quoted in workflow files to prevent YAML integer parsing
