# SxT Database Schemas — Unified Ingestion Reference

## Overview

Two schemas work together. `email_core` is the shared ingestion layer. `dealsync_stg_v1` is the app-specific deal processing layer. Email content is NOT stored in SxT — it's fetched on-demand from Gmail via `core-email-content-fetcher`.

## Pipeline Flow

```
Gmail → core-email-metadata-ingestion → email_core.email_metadata
                                      → email_core.sync_states/sync_events
      → core-email-content-fetcher    → Gmail API (on-demand, cached in Firestore)
      → dealsync-v2-service           → email_core.email_senders
                                      → dealsync_stg_v1.contacts
                                      → dealsync_stg_v1.deal_states
      → W3 Workflow                   → dealsync_stg_v1.email_thread_evaluations
                                      → dealsync_stg_v1.ai_evaluation_audits
                                      → dealsync_stg_v1.deals
                                      → dealsync_stg_v1.deal_contacts
```

## Schema: `email_core_staging` (shared layer)

Written by: `core-email-metadata-ingestion`, `dealsync-v2-service`
Private key: different from `dealsync_stg_v1` (see Rev 1 in design doc)

### email_metadata

Written by: `core-email-metadata-ingestion`

```sql
CREATE TABLE email_core.email_metadata (
    id VARCHAR(255) PRIMARY KEY,          -- UUIDv7
    thread_id VARCHAR(255),               -- Gmail thread ID
    message_id VARCHAR(255),              -- Gmail message ID
    user_id VARCHAR(255),                 -- App user ID
    is_deleted BOOLEAN DEFAULT FALSE,
    history_id VARCHAR(255),              -- Gmail history ID for FORWARD sync
    label_ids VARCHAR(65000),             -- Gmail labels (comma-separated)
    num_of_attachments INT,
    received_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### sync_states

Written by: ingestion trigger API (backend), updated by `core-email-metadata-ingestion`

```sql
CREATE TABLE email_core.sync_states (
    id VARCHAR(255) PRIMARY KEY,          -- UUIDv7 (= dealsync user_report_id)
    user_id VARCHAR(255) NOT NULL,
    sync_strategy VARCHAR(255) NOT NULL,  -- 'LOOKBACK' or 'FORWARD'
    status VARCHAR(255),                  -- 'pending', 'completed', 'failed'
    total_messages INTEGER DEFAULT 0,
    date_range_start TIMESTAMP,
    date_range_end TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_retry_at TIMESTAMP,
    last_processed_email_id VARCHAR(255),
    last_processed_history_id VARCHAR(255),
    next_page_token VARCHAR(540),
    query VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### sync_events

Written by: `core-email-metadata-ingestion`, `core-email-content-fetcher`

```sql
CREATE TABLE email_core.sync_events (
    id VARCHAR(255) PRIMARY KEY,
    sync_state_id VARCHAR(255),           -- FK to sync_states.id
    event VARCHAR(255),                   -- see events below
    errors VARCHAR(65000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Events: `pending`, `metadata_ingestion_start`, `metadata_ingestion_end`, `metadata_ingestion_failed`, `content_fetch_start`, `content_fetch_end`, `content_fetch_failed`

### email_senders

Written by: `dealsync-v2-service` (PR #240 addition)

```sql
CREATE TABLE email_core.email_senders (
    id VARCHAR(255),
    email_metadata_id VARCHAR(255) PRIMARY KEY,  -- FK to email_metadata.id
    sender_email VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### contacts (shared read layer)

```sql
CREATE TABLE email_core_staging.contacts (
    user_id VARCHAR(255),
    email VARCHAR(255),
    name VARCHAR(255),
    company_name VARCHAR(255),
    title VARCHAR(255),
    phone_number VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, email)
);
```

### email_attachments

```sql
CREATE TABLE email_core.email_attachments (
    id VARCHAR(255) PRIMARY KEY,
    email_metadata_id VARCHAR(255) NOT NULL,
    filename VARCHAR(1000),
    mime_type VARCHAR(255),
    size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Tables that do NOT exist in SxT (despite DDL):

- `email_contents` — content fetched on-demand from Gmail, never stored in SxT

## Schema: `dealsync_stg_v1` (app-specific)

Written by: `dealsync-v2-service`, W3 workflows (filter + detection)
Private key: `4b21f...` (same as Rust protocol workflows)

### deal_states (status-based state machine)

Written by: `dealsync-v2-service` Phase 4 (initial creation at `pending`), W3 workflows (status transitions)

```sql
CREATE TABLE dealsync_stg_v1.deal_states (
    id VARCHAR(255) NOT NULL,             -- UUIDv7
    email_metadata_id VARCHAR(255) PRIMARY KEY,  -- FK to email_core.email_metadata.id
    user_id VARCHAR(255) NOT NULL,
    contact_id VARCHAR(255),              -- FK to contacts.id
    sync_state_id VARCHAR(255),           -- FK to email_core.sync_states.id (used as syncStateId for content fetcher)
    message_id VARCHAR(255),              -- denormalized from email_core.email_metadata (Gmail message ID, used by content fetcher)
    thread_id VARCHAR(255),              -- denormalized from email_core.email_metadata (Gmail thread ID, used for thread-completeness checks and AI context)
    -- State machine
    status VARCHAR(50) NOT NULL DEFAULT 'pending',  -- pending → filtering → pending_classification → classifying → deal/not_deal/filter_rejected
    attempts INTEGER NOT NULL DEFAULT 0,             -- retry count, incremented on stale expiry
    batch_id VARCHAR(128),                -- W3 trigger hash claiming this row (NULL when not in-flight)
    workflow_triggers VARCHAR(65000),                -- JSON audit trail of workflow trigger entries
    -- Lifecycle
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Status state machine:**

```
pending → filtering (claimed by orchestrator)
filtering → pending_classification (filter passed)
filtering → filter_rejected (terminal)
filtering → pending + attempts++ (stale/failed, attempts < max)
pending_classification → classifying (claimed by orchestrator)
classifying → deal (terminal)
classifying → not_deal (terminal)
classifying → pending_classification + attempts++ (stale/failed, attempts < max)
Dead letter: WHERE attempts >= 3 AND status IN ('filtering', 'classifying')
```

**Indexes:** status, user_id, message_id, thread_id, sync_state_id, batch_id

### email_metadata (LEGACY — from old Rust pipeline only)

Written by: old Rust W3 workflows (NOT by unified ingestion, NOT by new W3 workflows)

```sql
CREATE TABLE dealsync_stg_v1.email_metadata (
    id VARCHAR(255) PRIMARY KEY,
    thread_id VARCHAR(255),
    message_id VARCHAR(255),
    user_id VARCHAR(255),
    user_report_id VARCHAR(255),
    stage INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**IMPORTANT:** This table is LEGACY. The unified ingestion and new W3 workflows use `deal_states` instead. This table has data only from users processed by the old Rust pipeline. Do NOT query or write to this table in new code.

### contacts

Written by: `dealsync-v2-service` (dual schema — app writes here, shared reads from email_core)

```sql
CREATE TABLE dealsync_stg_v1.contacts (
    id VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255),
    company_name VARCHAR(255),
    title VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### email_thread_evaluations

Written by: W3 workflow (detection pipeline)

```sql
CREATE TABLE dealsync_stg_v1.email_thread_evaluations (
    id VARCHAR(255) PRIMARY KEY,
    thread_id VARCHAR(255),
    ai_evaluation_audit_id VARCHAR(255),
    ai_insight VARCHAR(1500),             -- category from AI
    ai_summary VARCHAR(3000),
    is_deal BOOLEAN,
    likely_scam BOOLEAN,
    ai_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ai_evaluation_audits

Written by: W3 workflow (detection pipeline)

```sql
CREATE TABLE dealsync_stg_v1.ai_evaluation_audits (
    id VARCHAR(255) PRIMARY KEY,
    batch_id VARCHAR(128),               -- links to deal_states batch
    thread_count INTEGER,
    email_count INTEGER,
    inference_cost DECIMAL(5,4),
    input_tokens INTEGER,
    output_tokens INTEGER,
    model_used VARCHAR(255),
    ai_evaluation VARCHAR,                -- raw AI JSON output (no size limit)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### deals

Written by: W3 workflow (detection pipeline)

```sql
CREATE TABLE dealsync_stg_v1.deals (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    thread_id VARCHAR(255),
    email_thread_evaluation_id VARCHAR(255),
    deal_name VARCHAR(6400),
    deal_type VARCHAR(255),
    category VARCHAR(255),
    value DECIMAL(15,2),
    currency VARCHAR(10),
    brand VARCHAR(255),
    is_ai_sorted BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### deal_contacts

Written by: W3 workflow (detection pipeline)

```sql
CREATE TABLE dealsync_stg_v1.deal_contacts (
    deal_id VARCHAR(255),       -- FK: dealsync_stg_v1.deals(id)
    user_id VARCHAR(255),       -- FK: email_core_staging.contacts(user_id)
    email VARCHAR(255),         -- FK: email_core_staging.contacts(email)
    contact_type VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (deal_id, user_id, email)
);
```

### user_sync_settings

Written by: scheduler-service, dealsync-v2-service

```sql
CREATE TABLE dealsync_stg_v1.user_sync_settings (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    email VARCHAR(255) DEFAULT '',            -- user's email address
    timezone VARCHAR(10),
    time_of_day VARCHAR(10) DEFAULT '09:00',
    frequency VARCHAR(20) DEFAULT 'daily',
    next_sync_at TIMESTAMP,
    emails_processed_since_last_sync INTEGER DEFAULT 0,
    last_synced_at TIMESTAMP,
    last_sync_requested_at TIMESTAMP,
    sync_status VARCHAR(20) DEFAULT 'pending',
    skip_inbox BOOLEAN DEFAULT FALSE,
    daily_digest BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Other dealsync tables (NOT touched by W3 workflows):

- `user_reports` — sync session tracking (legacy, being replaced by sync_states)
- `user_sync_batches` — batch tracking (legacy, being replaced by stage-based concurrency)
- `user_report_reprocessing_errors` — error logs
- `deal_tags` — deal tagging
- `category_histories` — category change audit
- `issue_reports` — user issue reports
- `email_contents` — legacy, has old data, NOT populated by unified ingestion
- `email_attachments` — legacy

## Key Relationships

```
email_core.sync_states.id = dealsync_stg_v1.deal_states.sync_state_id
email_core.email_metadata.id = dealsync_stg_v1.deal_states.email_metadata_id
email_core.email_metadata.message_id = dealsync_stg_v1.deal_states.message_id (denormalized)
email_core.email_metadata.thread_id = dealsync_stg_v1.deal_states.thread_id (denormalized)
email_core.email_metadata.id = email_core.email_senders.email_metadata_id
email_core.email_metadata.message_id = Gmail message ID (used by content fetcher)
```

## What Writes Where

| Service                       | email_core tables                        | dealsync_stg_v1 tables                                                                                                                          |
| ----------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| core-email-metadata-ingestion | email_metadata, sync_states, sync_events | —                                                                                                                                               |
| core-email-content-fetcher    | sync_events                              | —                                                                                                                                               |
| dealsync-v2-service           | email_senders                            | contacts, deal_states (creates with status='pending')                                                                                           |
| W3 workflow (filter)          | —                                        | deal_states (status: pending→filtering→pending_classification or filter_rejected)                                                               |
| W3 workflow (detection)       | —                                        | deal_states (status: pending_classification→classifying→deal or not_deal), email_thread_evaluations, ai_evaluation_audits, deals, deal_contacts |
