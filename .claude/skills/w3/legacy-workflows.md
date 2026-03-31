# W3 DealSync Legacy Workflows Reference (Rust)

## Overview

The current DealSync workflows live in `w3-io/protocol` (Rust monorepo) at `config/workflow/instance/deal-sync/`. Three YAML-defined workflows orchestrate email processing through Rust action implementations. This skill documents what these workflows DO so we can faithfully translate them to JavaScript GHA-style workflows.

## Source Repository

**Repo:** `w3-io/protocol`
**Workflow definitions:** `config/workflow/instance/deal-sync/`
**Action implementations:** `src/core/workflow/action/impls/src/deal_sync/`
**Client implementations:** `src/core/workflow/action/impls/src/deal_sync/clients/`

To read any workflow definition:

```bash
gh api repos/w3-io/protocol/contents/config/workflow/instance/deal-sync --jq '.[].name'
```

## The 3 Workflows

### 1. `deal-sync-user-pipeline.yaml` (Orchestrator)

**Trigger:** RPC call with `{ user_id, user_report_id, sync_lock }`
**Steps:** Guard -> Dispatcher -> Monitor

| Step       | Action Tag                        | Purpose                                                                                                                                                                                 |
| ---------- | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Guard      | `!DealSyncUserPipelineGuard`      | Acquires distributed processing lock (UUIDv7, 30-min TTL, compare-and-swap for stale locks)                                                                                             |
| Dispatcher | `!DealSyncUserPipelineDispatcher` | Checks global batch capacity (limit: 150), claims emails via stage transitions, dispatches filter batches (size: 200) and detection batches (size: 5), triggers child workflows via RPC |
| Monitor    | `!DealSyncUserPipelineMonitor`    | Polls child workflow run status, retriggers failed batches, recovers orphans, self-retriggers if more work remains, completes report + webhook on first sync                            |

### 2. `deal-sync-email-filter-pipeline.yaml` (Static Filtering)

**Trigger:** RPC (from Dispatcher)
**Steps:** Fetcher -> Classifier -> Finalizer

| Step       | Action Tag                       | Purpose                                                                                                                                                   |
| ---------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Fetcher    | `!DealSyncEmailFilterFetcher`    | Queries SxT for emails at assigned transition stage, groups by thread                                                                                     |
| Classifier | `!DealSyncEmailFilterClassifier` | Static rules (no AI): DKIM/SPF/DMARC headers, blocked domains/prefixes, automated subject terms, list headers, non-personalized senders, free email regex |
| Finalizer  | `!DealSyncEmailFilterFinalizer`  | Updates stages in SxT (Filtered=3 or Rejected=106), sweeps orphaned emails back to stage 2                                                                |

### 3. `deal-sync-deal-detection-pipeline.yaml` (AI Classification)

**Trigger:** RPC (from Dispatcher)
**Steps:** Fetcher -> Classifier -> Finalizer

| Step       | Action Tag                         | Purpose                                                                                                                                                                                                                                                                                              |
| ---------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Fetcher    | `!DealSyncDealDetectionFetcher`    | Fetches filtered emails with AI context (previous `ai_summary` from `email_thread_evaluations`, existing `deal_id`). For threads without prior eval, fetches ALL historical thread emails                                                                                                            |
| Classifier | `!DealSyncDealDetectionClassifier` | Sends to Hyperbolic AI (model chain: DeepSeek-V3, Qwen, GPT-oss, Llama fallbacks). Two modes: `FULL_THREAD` (complete history) or `INCREMENTAL` (prev summary + new emails). Returns: `is_deal`, `ai_score` (1-10), `category`, `deal_name`, `deal_type`, `deal_value`, `main_contact`, `ai_summary` |
| Finalizer  | `!DealSyncDealDetectionFinalizer`  | Creates audit records, writes thread evaluations, upserts deals & contacts, creates deal-contact relationships, updates stages (Deal=4, Rejected=106, NonEnglish=107). Full rollback on failure, orphan sweep                                                                                        |

## Email Stage State Machine

| Stage       | Meaning                                             |
| ----------- | --------------------------------------------------- |
| 2           | Unprocessed                                         |
| 3           | Filtered (passed static rules)                      |
| 4           | Deal (AI confirmed)                                 |
| 106         | Rejected (by filter or AI)                          |
| 107         | Non-English Deal                                    |
| 666         | Failed                                              |
| 668         | Self-Sent                                           |
| 10001-19999 | Filter transition stages (in-flight batch locks)    |
| 20001-59999 | Detection transition stages (in-flight batch locks) |

**Transition stages** are used as distributed batch locks: emails are moved to a unique transition stage when claimed by a batch, preventing double-processing.

## External Service Clients

### Space and Time (SxT) Client

- JWT auth via shared-secret endpoint
- Biscuit token generation for table authorization
- Rate limiter integration (fail-open design)
- Retry with exponential backoff
- SQL execution against SxT REST API

### Hyperbolic AI Client

- OpenAI-compatible chat completion API
- Structured JSON schema output
- Model chain fallback (tries multiple models in order)
- Rate limit handling with retry

## Key Database Tables (dealsync_stg_v1 schema)

| Table                      | Key Columns                                                   | Purpose                    |
| -------------------------- | ------------------------------------------------------------- | -------------------------- |
| `email_metadata`           | id, thread_id, user_id, stage, user_report_id                 | Email state tracking       |
| `email_contents`           | email_metadata_id, top_level_headers, body_plain_text         | Email body data            |
| `user_reports`             | id, user_id, sync_lock, synced_at, completed_at, errors       | Sync session tracking      |
| `user_sync_batches`        | id, user_report_id, process_type/trigger_hash, is_complete    | Batch completion tracking  |
| `email_thread_evaluations` | id, thread_id, ai_insight, ai_summary, is_deal, ai_score      | AI evaluation results      |
| `ai_evaluation_audits`     | id, thread_count, email_count, inference_cost, model_used     | AI usage tracking          |
| `deals`                    | id, user_id, thread_id, deal_name, deal_type, category, value | Detected deals             |
| `contacts`                 | id, email, name, company_name, title                          | Contact info               |
| `deal_contacts`            | id, deal_id, contact_id, contact_type                         | Deal-contact relationships |

## Key Architectural Patterns to Preserve

1. **Stage-based state machine** — Emails progress through numbered stages; transition stages (10001+, 20001+) serve as distributed batch locks
2. **Self-retriggering pipeline** — Monitor retriggers User Pipeline if more work remains, creating recursive processing
3. **Orphan recovery** — Both finalizers and Monitor sweep for emails stuck at transition stages
4. **Fail-open rate limiting** — SxT rate limiter never blocks pipeline progress
5. **Model chain fallback** — AI classification tries multiple models in order
6. **Full thread context** — Detection fetcher loads all historical emails for threads without prior AI evaluation
7. **Distributed locking** — UUIDv7-based locks with TTL and compare-and-swap override for stale locks
8. **Batch capacity limits** — Global limit of 150 concurrent batches prevents overload

## Reading the Rust Source

To explore the actual Rust action implementations:

```bash
# List all DealSync action files
gh api repos/w3-io/protocol/git/trees/main?recursive=1 --jq '.tree[].path | select(contains("deal_sync"))'

# Read a specific file (e.g., the dispatcher)
gh api "repos/w3-io/protocol/contents/src/core/workflow/action/impls/src/deal_sync/user_pipeline/dispatcher.rs" --jq '.content' | base64 -d
```
