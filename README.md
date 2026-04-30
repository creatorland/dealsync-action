# Dealsync Action

Email deal detection pipeline for [Creatorland](https://creatorland.com) — filtering, AI classification, and batch orchestration. Runs on GitHub Actions and the [W3 protocol](https://w3.io) runtime.

## About

Dealsync processes Gmail emails through a multi-stage pipeline to detect business deals, sponsorships, and partnerships. Emails are filtered with static rules (DKIM/SPF, blocked domains, marketing headers), then classified by AI (Hyperbolic API with multi-layer fallback). Detected deals are stored with contacts, evaluations, and audit trails in [Space and Time](https://spaceandtime.io).

The pipeline is orchestrated by GitHub Actions workflows triggered by W3.

## Commands

| Command                       | Description                                                                                                                                                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `run-filter-pipeline`         | Claim pending emails, fetch headers, apply 6 static filter rules, update deal states                                                                                                                                                   |
| `run-classify-pipeline`       | Claim pending_classification emails, fetch bodies, AI classify (4-layer fallback), save results                                                                                                                                        |
| `run-recovery-pipeline`       | Retry failed rows per user after filter/classify failures (bounded claim + status updates)                                                                                                                                             |
| `sync-deal-states`            | Paginated sync of missing deal_states from email_metadata                                                                                                                                                                              |
| `eval`                        | Multi-run AI classification against ground truth, compute recall/precision/F2 metrics                                                                                                                                                  |
| `eval-compare`                | Compare two eval results with pass/fail criteria (recall >= 95%, precision >= 40%, etc.)                                                                                                                                               |
| `emit-scan-complete-webhooks` | Cron-oriented: SxT eligibility query (first completed LOOKBACK) → Firestore dedupe (`users/{id}.scanCompleteSentAt`) → `POST /dealsync-v2/webhooks` (`scan_complete`). See `docs/plans/2026-04-16-scan-complete-w3-cron-tech-spec.md`. |

## Workflows

Four GitHub Actions workflows orchestrate the pipeline:

**`dealsync-filter`** — Claims batches of pending emails, fetches headers from content fetcher, applies static rejection rules, updates deal states.

**`dealsync-classify`** — Claims batches of pending_classification emails, fetches full content, runs AI classification with 4-layer fallback, saves evaluations/deals/contacts.

**`dealsync-sync`** — Syncs missing deal_states from email_metadata.

**`dealsync-ab-eval`** — A/B evaluation comparing prompt or model variants against ground truth.

## UEI LOOKBACK window (§A1 / Story [#471](https://github.com/creatorland/dealsync-v2/issues/471))

Unified Email Ingestion uses a **60-day** default Gmail history window with a coordinated **45-day** fallback when quota, rate limits, or batch/operational-window constraints apply. Shared constants, date-range helpers (UTC epoch ms: **N × 86_400_000** from range end — not calendar-local midnights), fallback decision logic (`resolveUeiLookbackFallbackReason`), and structured logging (`emitUeiLookbackFallbackLog` / `{ userId, fellBackTo, reason }`, reasons restricted to `UEI_LOOKBACK_FALLBACK_REASONS`) live in [`src/lib/uei-lookback.js`](src/lib/uei-lookback.js) for `core-email-metadata-ingestion` and tooling to import. The SxT helper [`trigger-sync.js`](.claude/skills/sxt/scripts/trigger-sync.js) defaults to 60 days and validates `userId` / sync state id with `sanitizeId` before building SQL.

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

### `emit-scan-complete-webhooks` (lifecycle / cron)

| Input / secret                             | Purpose                                                                                                                                                                                   |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                                  | `emit-scan-complete-webhooks`                                                                                                                                                             |
| `sxt-*`, `sxt-schema`, `email-core-schema` | Same as other commands — Space and Time access                                                                                                                                            |
| `dealsync-backend-base-url`                | Backend base URL; trailing slashes are accepted and normalized, e.g. `https://api.example.com`                                                                                            |
| `dealsync-v2-shared-secret`                | `DEALSYNC_V2_SHARED_SECRET` → header `x-shared-secret`                                                                                                                                    |
| `firestore-service-account-json`           | Full GCP service account JSON (Firestore read-only role on `users` is enough); must include a non-empty `project_id`. Same JSON may be provided via env `FIRESTORE_SERVICE_ACCOUNT_JSON`. |
| `scan-complete-webhook-concurrency`        | Max parallel Firestore reads + webhook POSTs per batch (default `5`)                                                                                                                      |

Schedule this command from W3 or GitHub Actions on a 5–15 minute cadence; wire secrets in the host’s secret store. SQL builder: `src/lib/sql/scan-complete-eligibility.js` (parity with `backend/src/services/dealsync-v2.sync.service.ts`).

**Deploy order (required):** the backend must be running [creatorland/backend#1245](https://github.com/creatorland/backend/pull/1245) or later before this cron is enabled. That PR is what writes `users/{id}.scanCompleteSentAt` inside the `scan_complete` handler; without it, Firestore dedupe is never marked and every cron tick re-POSTs the same eligible users until the backend catches up. Backend idempotency (Redis lock + email-level dedupe) prevents user-visible spam, but logs will show a duplicate POST storm that is easy to mistake for a regression.

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
