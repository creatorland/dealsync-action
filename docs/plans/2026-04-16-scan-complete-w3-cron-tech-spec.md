---
title: 'scan_complete via dealsync-action W3 cron + Firestore (first LOOKBACK only)'
slug: scan-complete-w3-cron-firestore
created: '2026-04-16'
status: ready-for-development
source_issue: 'https://github.com/creatorland/dealsync-v2/issues/411'
source_comment: 'https://github.com/creatorland/dealsync-v2/issues/411#issuecomment-4258829092'
---

# Tech spec: `scan_complete` via dealsync-action W3 cron + Firestore (first LOOKBACK only)

**Source:** [dealsync-v2#411](https://github.com/creatorland/dealsync-v2/issues/411) (revised issue comment).

**Revision notes (from issue discussion):**

- Spell out “first LOOKBACK only” explicitly in terms of `sync_events` (mirror backend semantics so edge cases do not drift).
- Define a single source of truth for `dealCounts` / `contactsAdded` to populate `ScanCompleteWebhookDto` (reuse the `getSyncStatus` SQL definitions; do not invent new aggregates).
- W3 cron workflow in `dealsync-action` is the primary implementation (**scheduler-service is out of scope**).

---

## Overview

### Problem statement

Users who complete their **first LOOKBACK** ingestion should receive the `scan_complete` lifecycle email, but detection must not depend on browser login or GET sync-status polling. Delivery must be **at-most-once per user** across cron ticks and overlapping runs.

### Solution

Run a **periodic W3 workflow** that invokes `dealsync-action` to:

1. Execute **one** SpaceAndTime read query that returns eligible `user_id` rows plus aggregate columns aligned with backend `SyncService.getSyncStatus()`.
2. For each row, **read** Firestore `users/{userId}` and skip if `scanCompleteSentAt` is set (backend owns the write after a successful send).
3. **POST** `POST /dealsync-v2/webhooks` with `eventType: scan_complete` and payload matching `ScanCompleteWebhookDto` (same shared-secret auth as daily digest).

### Scope

**In scope**

- New command (name TBD, e.g. `emit-scan-complete-webhooks`) in `dealsync-action`.
- SQL module documenting verbatim parity with `backend/src/services/dealsync-v2.sync.service.ts` (`getSyncStatus`, `isFirstCompletedSync` semantics scoped to LOOKBACK).
- Firestore **read-only** check for `scanCompleteSentAt` on `users/{userId}`.
- W3 cron wiring (schedule, secrets: SxT, Firestore/GCP, `DEALSYNC_V2_SHARED_SECRET`, backend base URL).
- Configurable batch size / concurrency for webhook POSTs.

**Out of scope**

- New backend HTTP endpoints.
- Scheduler-service orchestration for this feature.
- Emitting `scan_complete` for FORWARD-only first sync or subsequent LOOKBACK completions.
- Cron workflow writing `scanCompleteSentAt` (backend remains owner after successful email send).

---

## Constraints (locked)

| Constraint | Detail |
| ---------- | ------ |
| Webhook | Existing lifecycle route: `POST …/dealsync-v2/webhooks` with `JwtOrSecretKeyGuard` + header `x-shared-secret: DEALSYNC_V2_SHARED_SECRET` (same pattern as daily digest). |
| Runtime | Cron lives in **dealsync-action** / W3; not user-login-driven. |
| Dedupe store | Firestore `users/{userId}.scanCompleteSentAt` (Unix seconds, analogous to `dailyDigestSentAt`). |
| SQL parity | **Documented copy** of backend SQL in this repo (not a cross-repo TS library). |
| Product | **First successful LOOKBACK lifecycle only** — see eligibility below. |

**Backend controller reference:** `backend/src/controllers/dealsync-v2.webhooks.controller.ts` — `@Controller('dealsync-v2/webhooks')`, `@Post()`, `@UseGuards(JwtOrSecretKeyGuard)`.

**DTO reference:** `backend/src/dtos/dealsync-v2.webhooks.dto.ts` — `ScanCompleteWebhookDto`, `ScanCompleteEventData` (`dealCounts`, `contactsAdded`).

---

## Eligibility

Two conditions must hold **at once** for a user to be returned by the cron query.

### 1) Pipeline complete (same meaning as GET sync status `completedAt`)

Align with `SyncService.getSyncStatus` in `backend/src/services/dealsync-v2.sync.service.ts`:

- Latest `sync_states` row for the user defines `initiatedAt` / `sync_id` (`ORDER BY created_at DESC LIMIT 1`).
- `total_messages` = count of `email_metadata` for the user.
- `processed_messages` = `deal_states` rows **not** in `pending`, `filtering`, `pending_classification`, `classifying`.
- **Complete** when `initiatedAt != null` and `processed_messages >= total_messages`.
- **`completedAt`** in the API is only non-null when complete; use that same predicate in SQL for the cron query.

Reference (backend):

```247:253:backend/src/services/dealsync-v2.sync.service.ts
    const rawCompletedAt = r.completedAt
      ? new Date(r.completedAt as string)
      : null;

    const isCompleted =
      initiatedAt != null && processedMessages >= totalMessages;
    const completedAt = isCompleted ? rawCompletedAt : null;
```

The cron query must **not** invent a different completion rule — port or mirror the `getSyncStatus` CTEs into a **read-only** module with comments pointing at the backend file.

### 2) First LOOKBACK only

- The **latest** `sync_states` row must have **`sync_strategy = 'LOOKBACK'`**.
- The user must **not** have any **earlier** `sync_states` row with `sync_strategy = 'LOOKBACK'` that already reached **terminal success** using the same semantics as backend `isFirstCompletedSync` / `sync_events`, scoped to LOOKBACK rows only:
  - For a given LOOKBACK `sync_state` row `ss`, **terminal success** means:
    - There exists a success event (one of `metadata_ingestion_end`, `content_ingestion_end`, `completed`) for that `sync_state_id`.
    - That success is the **latest** success event of those types for that `sync_state_id` (max `created_at` among those event types).
    - There is **no later** failure event (`metadata_ingestion_failed`, `content_ingestion_failed`) after that success.
  - **First LOOKBACK only** means: for the *latest* LOOKBACK `sync_states` row, there is **no earlier** LOOKBACK `sync_states` row with terminal success by the rule above.

If the user’s first sync is FORWARD-only, they are **out of scope** for this webhook.

---

## Canonical backend SQL (reference copies — keep in sync)

**Source file:** `backend/src/services/dealsync-v2.sync.service.ts`

### Fragment A — `getSyncStatus()` inputs and aggregates

```sql
WITH latest_sync AS (
  SELECT id, created_at
  FROM ${emailCoreSchema}.sync_states
  WHERE user_id = :userId
  ORDER BY created_at DESC LIMIT 1
),
deal_state_agg AS (
  SELECT
    COUNT(*) AS prepped_messages,
    SUM(
      CASE WHEN status NOT IN (
        'pending',
        'filtering',
        'pending_classification',
        'classifying'
      ) THEN 1 ELSE 0 END
    ) AS processed_messages,
    MAX(
      CASE WHEN status NOT IN (
        'pending',
        'filtering',
        'pending_classification',
        'classifying'
      ) THEN updated_at END
    ) AS completed_at
  FROM ${dealsyncSchema}.deal_states
  WHERE user_id = :userId
),
deal_agg AS (
  SELECT
    SUM(CASE WHEN category = 'new' THEN 1 ELSE 0 END) AS db_new,
    SUM(CASE WHEN category = 'in_progress' THEN 1 ELSE 0 END) AS db_in_progress,
    SUM(CASE WHEN category = 'completed' THEN 1 ELSE 0 END) AS db_completed,
    SUM(CASE WHEN category = 'not_interested' THEN 1 ELSE 0 END) AS db_not_interested,
    SUM(CASE WHEN category = 'likely_scam' THEN 1 ELSE 0 END) AS db_likely_scam,
    SUM(CASE WHEN category = 'low_confidence' THEN 1 ELSE 0 END) AS db_low_confidence
  FROM ${dealsyncSchema}.deals
  WHERE user_id = :userId
    AND updated_at >= (SELECT created_at FROM latest_sync)
),
contact_agg AS (
  SELECT COUNT(DISTINCT c.email) AS identified_contacts_count
  FROM ${dealsyncSchema}.deal_contacts dc
  INNER JOIN ${dealsyncSchema}.deals d ON d.id = dc.deal_id
  INNER JOIN ${emailCoreSchema}.contacts c
    ON c.user_id = dc.user_id AND c.email = dc.email
  WHERE d.user_id = :userId
    AND dc.user_id = :userId
    AND d.updated_at >= (SELECT created_at FROM latest_sync)
)
SELECT
  (SELECT COUNT(*) FROM ${emailCoreSchema}.email_metadata WHERE user_id = :userId) AS total_messages,
  (SELECT id FROM latest_sync) AS sync_id,
  (SELECT created_at FROM latest_sync) AS initiated_at,
  dsa.prepped_messages,
  dsa.processed_messages,
  dsa.completed_at,
  COALESCE(da.db_new, 0) AS db_new,
  COALESCE(da.db_in_progress, 0) AS db_in_progress,
  COALESCE(da.db_completed, 0) AS db_completed,
  COALESCE(da.db_not_interested, 0) AS db_not_interested,
  COALESCE(da.db_likely_scam, 0) AS db_likely_scam,
  COALESCE(da.db_low_confidence, 0) AS db_low_confidence,
  COALESCE(ca.identified_contacts_count, 0) AS identified_contacts_count
FROM deal_state_agg dsa
CROSS JOIN deal_agg da
CROSS JOIN contact_agg ca;
```

### Fragment B — `isFirstCompletedSync()` terminal success (per prior sync row)

```sql
SELECT 1 FROM ${emailCoreSchema}.sync_states ss
WHERE ss.user_id = :userId
  AND ss.id != :currentSyncId
  AND EXISTS (
    SELECT 1
    FROM ${emailCoreSchema}.sync_events se_s
    WHERE se_s.sync_state_id = ss.id
      AND se_s.event IN (
        'metadata_ingestion_end',
        'content_ingestion_end',
        'completed'
      )
      AND se_s.created_at = (
        SELECT MAX(se2.created_at)
        FROM ${emailCoreSchema}.sync_events se2
        WHERE se2.sync_state_id = ss.id
          AND se2.event IN (
            'metadata_ingestion_end',
            'content_ingestion_end',
            'completed'
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM ${emailCoreSchema}.sync_events se_f
        WHERE se_f.sync_state_id = ss.id AND se_f.event IN (
            'metadata_ingestion_failed',
            'content_ingestion_failed'
          )
          AND se_f.created_at > se_s.created_at
      )
  )
LIMIT 1;
```

---

## Canonical eligibility + payload query (single SxT query for cron)

**Required result columns (locked):**

| Column | Purpose |
| ------ | ------- |
| `user_id` | Webhook `userId` |
| `initiated_at` | Latest sync `created_at` (for deal/contact window alignment) |
| `db_*` | Map to `eventData.dealCounts` |
| `contacts_added` | Same meaning as `identified_contacts_count` → `eventData.contactsAdded` |

**Payload mapping (locked):**

| SQL column | DTO field |
| ---------- | --------- |
| `db_new` | `dealCounts.new` |
| `db_in_progress` | `dealCounts.inProgress` |
| `db_completed` | `dealCounts.completed` |
| `db_likely_scam` | `dealCounts.likelyScam` |
| `db_low_confidence` | `dealCounts.lowConfidence` (optional) |
| `db_not_interested` | `dealCounts.notInterested` (optional) |
| `contacts_added` | `contactsAdded` |

**Canonical query shape** (must mirror backend fragments above; substitute schema names via `sanitizeSchema` in `src/lib/sql/scan-complete-eligibility.js` per `action.yml` `sxt-schema` + `email-core-schema`):

```sql
WITH latest_sync AS (
  SELECT sync_state_id, user_id, created_at, sync_strategy
  FROM (
    SELECT
      id AS sync_state_id,
      user_id,
      created_at,
      sync_strategy,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
    FROM ${emailCoreSchema}.sync_states
  ) ranked
  WHERE rn = 1
),
status_inputs AS (
  SELECT
    ls.user_id,
    ls.sync_state_id,
    ls.created_at AS initiated_at,
    (SELECT COUNT(*) FROM ${emailCoreSchema}.email_metadata em WHERE em.user_id = ls.user_id) AS total_messages,
    (
      SELECT SUM(
        CASE WHEN ds.status NOT IN ('pending','filtering','pending_classification','classifying')
          THEN 1 ELSE 0 END
      )
      FROM ${dealsyncSchema}.deal_states ds
      WHERE ds.user_id = ls.user_id
    ) AS processed_messages
  FROM latest_sync ls
  WHERE ls.sync_strategy = 'LOOKBACK'
),
completed_candidates AS (
  SELECT *
  FROM status_inputs
  WHERE initiated_at IS NOT NULL
    AND processed_messages >= total_messages
),
prior_lookback_success AS (
  SELECT ss.user_id, ss.created_at
  FROM ${emailCoreSchema}.sync_states ss
  WHERE ss.sync_strategy = 'LOOKBACK'
    AND EXISTS (
      SELECT 1
      FROM ${emailCoreSchema}.sync_events se_s
      WHERE se_s.sync_state_id = ss.id
        AND se_s.event IN ('metadata_ingestion_end','content_ingestion_end','completed')
        AND se_s.created_at = (
          SELECT MAX(se2.created_at)
          FROM ${emailCoreSchema}.sync_events se2
          WHERE se2.sync_state_id = ss.id
            AND se2.event IN ('metadata_ingestion_end','content_ingestion_end','completed')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM ${emailCoreSchema}.sync_events se_f
          WHERE se_f.sync_state_id = ss.id
            AND se_f.event IN ('metadata_ingestion_failed','content_ingestion_failed')
            AND se_f.created_at > se_s.created_at
        )
    )
),
eligible AS (
  SELECT c.*
  FROM completed_candidates c
  WHERE NOT EXISTS (
    SELECT 1
    FROM prior_lookback_success pls
    WHERE pls.user_id = c.user_id
      AND pls.created_at < c.initiated_at
  )
),
deal_agg AS (
  SELECT
    e.user_id,
    SUM(CASE WHEN d.category = 'new' THEN 1 ELSE 0 END) AS db_new,
    SUM(CASE WHEN d.category = 'in_progress' THEN 1 ELSE 0 END) AS db_in_progress,
    SUM(CASE WHEN d.category = 'completed' THEN 1 ELSE 0 END) AS db_completed,
    SUM(CASE WHEN d.category = 'not_interested' THEN 1 ELSE 0 END) AS db_not_interested,
    SUM(CASE WHEN d.category = 'likely_scam' THEN 1 ELSE 0 END) AS db_likely_scam,
    SUM(CASE WHEN d.category = 'low_confidence' THEN 1 ELSE 0 END) AS db_low_confidence
  FROM eligible e
  LEFT JOIN ${dealsyncSchema}.deals d
    ON d.user_id = e.user_id AND d.updated_at >= e.initiated_at
  GROUP BY e.user_id
),
contact_agg AS (
  SELECT
    e.user_id,
    COUNT(DISTINCT c.email) AS contacts_added
  FROM eligible e
  INNER JOIN ${dealsyncSchema}.deal_contacts dc
    ON dc.user_id = e.user_id
  INNER JOIN ${dealsyncSchema}.deals d
    ON d.id = dc.deal_id AND d.user_id = e.user_id AND d.updated_at >= e.initiated_at
  INNER JOIN ${emailCoreSchema}.contacts c
    ON c.user_id = dc.user_id AND c.email = dc.email
  GROUP BY e.user_id
)
SELECT
  e.user_id,
  e.initiated_at,
  COALESCE(da.db_new, 0) AS db_new,
  COALESCE(da.db_in_progress, 0) AS db_in_progress,
  COALESCE(da.db_completed, 0) AS db_completed,
  COALESCE(da.db_not_interested, 0) AS db_not_interested,
  COALESCE(da.db_likely_scam, 0) AS db_likely_scam,
  COALESCE(da.db_low_confidence, 0) AS db_low_confidence,
  COALESCE(ca.contacts_added, 0) AS contacts_added
FROM eligible e
LEFT JOIN deal_agg da ON da.user_id = e.user_id
LEFT JOIN contact_agg ca ON ca.user_id = e.user_id
ORDER BY e.initiated_at DESC;
```

**Implementation note:** SxT returns UPPERCASE column names; map to webhook DTO in `src/lib/scan-complete.js` (`rowToScanCompleteWebhookBody`).

---

## Firestore dedupe

**Purpose:** At-most-once delivery per user across cron ticks / overlapping runs.

| Item | Decision |
| ---- | -------- |
| Collection | `users` |
| Document ID | Firebase user id (same as `userId` in webhook) |
| Field | `scanCompleteSentAt` — Unix seconds |
| Writer | **Backend** sets the field in `handleScanComplete` **after** the scan-complete email send succeeds (`backend/src/services/dealsync-v2.webhook-processor.service.ts`). |
| Reader | **dealsync-action** cron: skip POST if field present. |

**Per-candidate flow (cron):**

1. Read `users/{userId}`; if `scanCompleteSentAt` exists → skip.
2. POST webhook with `ScanCompleteWebhookDto` shape:
   - `userId`
   - `eventType: 'scan_complete'`
   - `eventData`: `{ dealCounts, contactsAdded }`
3. On POST failure: log and continue; next cron tick retries. Firestore unchanged until backend succeeds.

**Optional backend hardening (not blocking cron MVP):** `handleScanComplete` short-circuits if `scanCompleteSentAt` already set; concurrency-safe write (e.g. lock + transaction re-check).

---

## Wiring in dealsync-action

| Topic | Guidance |
| ----- | -------- |
| Schedule | W3 cron every **5–15 minutes** (tune for cost vs latency). |
| Auth | `x-shared-secret: DEALSYNC_V2_SHARED_SECRET` on webhook POST; JWT alternative exists on the same guard but shared secret matches daily digest automation. |
| Data | SxT: reuse `authenticate` / `executeSql` (`src/lib/sxt-client.js`). Firestore: new thin client using GCP credentials available in W3 (service account JSON secret or equivalent). |
| Concurrency | Inputs for max parallel Firestore reads + HTTP POSTs (default conservative). |

---

## Context for development (this repo)

### Codebase patterns

- Commands are registered in `src/main.js` (`COMMANDS` map) and exposed via `action.yml` `command` input.
- SxT access uses `src/lib/db.js` (`authenticate`, `executeSql`).
- Schema inputs in `action.yml`: `sxt-schema` (dealsync) and `email-core-schema` (default `EMAIL_CORE_STAGING`).

### Files to reference

| File | Purpose |
| ---- | ------- |
| `src/main.js` | Register new command |
| `action.yml` | New inputs: backend base URL, shared secret, Firestore/GCP, webhook concurrency; document command |
| `src/lib/db.js` | Execute eligibility SQL |
| `src/lib/sql/sanitize.js` | Schema sanitization (shared with other SQL builders) |
| `src/commands/emit-scan-complete-webhooks.js` | Pattern for command wiring, SQL execution, and scan-complete orchestration |
| `src/lib/sql/scan-complete-eligibility.js` | `scanCompleteEligibility.selectEligibleUsers` — parity SQL with backend references in file header |
| `src/commands/emit-scan-complete-webhooks.js` | Orchestrate query → Firestore → POST |
| New tests under `__tests__/` | Mock `fetch`, Firestore, SxT |

### Backend files (parity — do not drift)

| File | Purpose |
| ---- | ------- |
| `backend/src/services/dealsync-v2.sync.service.ts` | `getSyncStatus`, `isFirstCompletedSync` |
| `backend/src/controllers/dealsync-v2.webhooks.controller.ts` | Route and guard |
| `backend/src/dtos/dealsync-v2.webhooks.dto.ts` | Payload validation |
| `backend/src/services/dealsync-v2.webhook-processor.service.ts` | `handleScanComplete` |

---

## Implementation plan (ordered by dependency)

1. **Add SQL builder** — `src/lib/sql/scan-complete-eligibility.js` (`scanCompleteEligibility.selectEligibleUsers`) + comments linking to `dealsync-v2.sync.service.ts`. Verify schema placeholders match production staging/prod naming.
2. **Firestore read helper** — REST + OAuth token from service account JSON (`src/lib/scan-complete.js`); `userHasScanCompleteSentAt` → boolean.
3. **Webhook client** — `POST {BACKEND_URL}/dealsync-v2/webhooks` with headers/body matching existing daily-digest automation; handle non-2xx with structured logs.
4. **Command implementation** — `executeSql` → map rows → for each user Firestore check → POST; respect concurrency limit; return summary JSON `{ scanned, skippedDeduped, posted, errors }`.
5. **action.yml + README** — Document inputs, secrets, and W3 example.
6. **W3 workflow** — Cron definition calling `creatorland/dealsync-action` with the new command and secrets (outside this repo if workflows live elsewhere; link from README).
7. **Backend follow-up (if not already done)** — Ensure `handleScanComplete` sets `scanCompleteSentAt` after successful send and optional duplicate guard.

---

## Acceptance criteria

### SQL eligibility

- **Given** a user whose latest `sync_states` row is FORWARD (not LOOKBACK), **when** the cron query runs, **then** that user is not returned.
- **Given** a user whose latest sync is LOOKBACK but `processed_messages < total_messages`, **when** the cron runs, **then** they are not returned.
- **Given** a user with an **earlier** LOOKBACK that achieved terminal success per `sync_events` rules, **when** their latest LOOKBACK completes, **then** they are not returned (not “first” LOOKBACK).
- **Given** a user whose latest LOOKBACK completes and no prior LOOKBACK has terminal success, **when** the cron runs, **then** they appear in the result set with aggregates matching the same time window as `getSyncStatus` for that user (spot-check vs API or SQL).

### Firestore + webhook

- **Given** `users/{userId}.scanCompleteSentAt` is set, **when** the cron processes that `user_id`, **then** no webhook POST is made for that user.
- **Given** `scanCompleteSentAt` is absent and the user is eligible, **when** the cron runs, **then** a single POST is attempted with `eventType: 'scan_complete'` and numeric fields satisfying `ScanCompleteWebhookDto`.
- **Given** the POST fails (5xx/network), **when** the cron completes, **then** Firestore is still unchanged and a later run may retry.

### Security

- **Given** an incorrect or missing shared secret, **when** POSTing the webhook, **then** the backend rejects the request (no email side effects).

---

## Testing strategy

- **Unit tests:** Map uppercase SxT row shapes to DTO; Firestore skip logic; webhook URL/header construction (mock `fetch`).
- **Integration (manual or staging):** For a test user, compare cron query output to `GET` sync status JSON for `dealCounts` / `contactsAdded` after completion.
- **Idempotency:** Run cron twice; verify at most one email per user and second run skips via Firestore.

---

## Dependencies

- Backend deployed with `scan_complete` handling and Firestore write on success.
- W3 (or equivalent) secret store: SxT credentials, `DEALSYNC_V2_SHARED_SECRET`, backend public URL, GCP credentials for Firestore read.
- SpaceAndTime access from W3 network to same schemas as backend.

---

## Notes

- **SxT column casing:** Align SQL aliases or post-map in JS so implementation does not confuse `USER_ID` with `userId`.
- **Cost:** Full-table eligibility query may be heavy; if needed, a future iteration could add incremental filters (document any change with backend parity review).
- **Comment ID:** Spec text originated from [issue #411 comment](https://github.com/creatorland/dealsync-v2/issues/411#issuecomment-4258829092).
