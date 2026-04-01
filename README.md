# Dealsync Action

Email deal detection pipeline for [Creatorland](https://creatorland.com) — filtering, AI classification, and batch orchestration. Runs on GitHub Actions and the [W3 protocol](https://w3.io) runtime.

## About

Dealsync processes Gmail emails through a multi-stage pipeline to detect business deals, sponsorships, and partnerships. Emails are filtered with static rules (DKIM/SPF, blocked domains, marketing headers), then classified by AI (Hyperbolic API with multi-layer fallback). Detected deals are stored with contacts, evaluations, and audit trails in [Space and Time](https://spaceandtime.io).

The pipeline is orchestrated by GitHub Actions workflows triggered by W3.

## Commands

| Command                 | Description                                                                                     |
| ----------------------- | ----------------------------------------------------------------------------------------------- |
| `run-filter-pipeline`   | Claim pending emails, fetch headers, apply 6 static filter rules, update deal states            |
| `run-classify-pipeline` | Claim pending_classification emails, fetch bodies, AI classify (4-layer fallback), save results |
| `sync-deal-states`      | Paginated sync of missing deal_states from email_metadata                                       |
| `eval`                  | Multi-run AI classification against ground truth, compute recall/precision/F2 metrics           |
| `eval-compare`          | Compare two eval results with pass/fail criteria (recall >= 95%, precision >= 40%, etc.)        |

## Workflows

Four GitHub Actions workflows orchestrate the pipeline:

**`dealsync-filter`** — Claims batches of pending emails, fetches headers from content fetcher, applies static rejection rules, updates deal states.

**`dealsync-classify`** — Claims batches of pending_classification emails, fetches full content, runs AI classification with 4-layer fallback, saves evaluations/deals/contacts.

**`dealsync-sync`** — Syncs missing deal_states from email_metadata.

**`dealsync-ab-eval`** — A/B evaluation comparing prompt or model variants against ground truth.

## Pipeline flow

```
Email arrives → metadata ingestion (GCP) → email_metadata in SxT
                                                    ↓
                              dealsync-sync (periodic)
                              └── sync missing deal_states
                                          ↓
                              dealsync-filter (periodic/W3 triggered)
                              └── filter: static rules (DKIM, domains, headers)
                                          ↓
                              dealsync-classify (periodic/W3 triggered)
                              └── classify: AI → deals, contacts, evaluations
```

## AI Classification

4-layer resilience pipeline:

- **Layer 0**: Primary model (default: Qwen3-235B, configurable) with HTTP retries + exponential backoff
- **Layer 1**: Local JSON repair (strip markdown fences, extract array, coerce schema)
- **Layer 2**: Corrective retry — send broken output back to same model with parse error
- **Layer 3**: Fallback model (default: DeepSeek-V3, configurable) with temperature=0.6

## Authentication

| Secret                | Purpose                                             |
| --------------------- | --------------------------------------------------- |
| `SXT_AUTH_URL`        | Auth proxy endpoint                                 |
| `SXT_AUTH_SECRET`     | Shared secret for proxy                             |
| `SXT_API_URL`         | Space and Time REST API                             |
| `SXT_BISCUIT`         | Pre-generated biscuit token for table authorization |
| `SXT_SCHEMA`          | Schema name (e.g., `DEALSYNC_STG_V1`)               |
| `CONTENT_FETCHER_URL` | Email content fetcher service URL                   |
| `HYPERBOLIC_KEY`      | Hyperbolic AI API key                               |

## Development

```bash
npm install
npm test                    # run tests
npm run package             # bundle with rollup
npm run all                 # format, test, package
```

## Project structure

```
action.yml                  # GHA action metadata
src/
  index.js                  # entry point
  main.js                   # command dispatcher
  commands/                 # 5 command implementations
  lib/                      # shared utilities (SxT client, queries, prompts)
  prompts/                  # AI classification instructions
config/                     # filter rule JSON configs
__tests__/                  # jest tests
dist/index.js               # rollup bundle
```
