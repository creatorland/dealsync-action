# Dealsync Action

Email deal detection pipeline for [Creatorland](https://creatorland.com) — filtering, AI classification, and batch orchestration. Runs on both GitHub Actions and the [W3 protocol](https://w3.io) runtime.

## About

Dealsync processes Gmail emails through a multi-stage pipeline to detect business deals, sponsorships, and partnerships. Emails are filtered with static rules (DKIM/SPF, blocked domains, marketing headers), then classified by AI (Hyperbolic API with model chain fallback). Detected deals are stored with contacts, evaluations, and audit trails in [Space and Time](https://spaceandtime.io).

The pipeline is orchestrated by W3 workflows that dispatch batches for parallel processing.

## Quick start

```yaml
- name: Dispatch filter and classify batches
  uses: creatorland/dealsync-action@main
  with:
    command: dispatch
    auth-url: ${{ secrets.SXT_AUTH_URL }}
    auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
    api-url: ${{ secrets.SXT_API_URL }}
    biscuit: ${{ secrets.SXT_BISCUIT }}
    schema: ${{ secrets.SXT_SCHEMA }}
    w3-rpc-url: https://1.w3-testnet.io
    processor-name: dealsync-processor
```

## Commands

| Command                    | Description                                                                         |
| -------------------------- | ----------------------------------------------------------------------------------- |
| `dispatch`                 | Claim pending deal_states into filter/classify batches, trigger processor workflows |
| `dispatch-deal-state-sync` | Count email_metadata without deal_states, dispatch sync workers                     |
| `sync-deal-states`         | Insert missing deal_states from email_metadata diff                                 |
| `retrigger-stuck`          | Find stuck batches and retrigger their processor workflows                          |
| `fetch-and-filter`         | Fetch email headers, apply 6 static filter rules, return pass/reject IDs            |
| `fetch-and-classify`       | Fetch email content, classify with AI (4-model fallback), save audit checkpoint     |
| `save-evals`               | Save AI thread evaluations from audit checkpoint                                    |
| `save-deals`               | Upsert deals and contacts from AI classification                                    |
| `update-deal-states`       | Update deal_states to terminal status (deal/not_deal)                               |
| `sxt-execute`              | Execute raw SQL against Space and Time                                              |

## Workflows

Three W3 workflows orchestrate the pipeline:

**`dealsync-orchestrator`** — Retrigger stuck batches → sync missing deal_states → dispatch filter/classify batches

**`dealsync-processor`** — Two parallel jobs:

- **Filter job**: fetch headers → apply rules → save passed/rejected
- **Classify job**: fetch content → AI classify → save evals → save deals → update states

**`dealsync-deal-state-sync`** — Worker that inserts missing deal_states from email_metadata

## Pipeline flow

```
Email arrives → metadata ingestion (GCP) → email_metadata in SxT
                                                    ↓
                              dealsync-orchestrator (W3, periodic)
                              ├── sync missing deal_states
                              └── dispatch filter + classify batches
                                          ↓
                              dealsync-processor (W3, per batch)
                              ├── filter: static rules (DKIM, domains, headers)
                              └── classify: AI → deals, contacts, evaluations
```

## Authentication

The action authenticates to Space and Time via an auth proxy:

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
  commands/                 # 10 command implementations
  lib/                      # shared utilities (SxT client, queries, prompts)
  prompts/                  # AI classification instructions
config/                     # filter rule JSON configs
__tests__/                  # jest tests
dist/index.js               # rollup bundle
```
