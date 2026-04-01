# Email Service Design

Standalone email fetching service replacing the content-fetcher. On-demand Gmail API access with Redis caching and single-flight coalescing via Pub/Sub.

## Stack

- Node.js / TypeScript
- Docker on Railway (portable to GCP)
- Redis (cache + coalescing)
- Firestore (OAuth token storage, direct access)

## Architecture

```
Consumer (dealsync, etc.)
    |
    v
+---------------------+
|   email-service API  |  (N stateless instances)
|   +---------------+  |
|   | Request Router |  |
|   +-------+-------+  |
|           v          |
|   +---------------+  |
|   | Single-Flight  |  |
|   |  Coalescer     |  |
|   +-------+-------+  |
|           v          |
+--------+--+----------+
    +----+----+
    v         v
+--------+ +----------+
| Redis  | | Gmail API|
| Cache  | +----------+
+--------+
    ^
    |
+----------+
| Firestore| (OAuth tokens)
+----------+
```

Request flow:

1. Resolve query to message IDs (threads/date-range call `messages.list` first)
2. Multi-get from Redis (MGET) -- split into hits and misses
3. For each miss: single-flight coalescing (see below)
4. Return all results merged

## API Design

Three POST endpoints, all multi-user:

```
POST /v1/emails/messages
POST /v1/emails/threads
POST /v1/emails/search
```

Request shape (same for all):

```json
{
  "requests": [
    { "user_id": "user_a", "ids": ["msg1", "msg2"] },
    { "user_id": "user_b", "ids": ["msg3"] }
  ],
  "format": "raw"
}
```

Search uses date ranges instead of IDs:

```json
{
  "requests": [
    { "user_id": "user_a", "after": "2026-01-01", "before": "2026-04-01" }
  ],
  "format": "normalized"
}
```

Response shape:

```json
{
  "data": [
    {
      "message_id": "abc123",
      "thread_id": "thread456",
      "source": "cache | gmail",
      "format": "raw | normalized",
      "payload": {}
    }
  ],
  "errors": [
    {
      "message_id": "def789",
      "error": "not_found | auth_failed | rate_limited | auth_not_configured | service_error"
    }
  ],
  "meta": {
    "total": 10,
    "fetched": 8,
    "cached": 7,
    "failed": 2
  }
}
```

Partial success -- returns whatever it can, errors for the rest.

## Redis Cache Design

Key structure:

```
email:{user_id}:{message_id}        -> gzip + msgpack serialized email (raw Gmail format)
email:{user_id}:{message_id}:lock   -> "fetching" (SET NX EX 30)
email:{user_id}:{message_id}:done   -> Pub/Sub channel
```

- Content TTL: 7 days (messages are immutable)
- Lock TTL: 30 seconds
- Eviction: LRU policy
- Serialization: msgpack (~30% smaller than JSON)
- Compression: gzip before storing (70-80% compression on text-heavy emails)
- Normalized format is NOT cached -- computed on-the-fly from raw (cheap CPU, avoids double storage)

## Single-Flight Coalescing

Two layers prevent duplicate Gmail API calls:

### Layer 1: In-Process (Map<key, Promise>)

Deduplicates concurrent requests within the same Node.js instance. Zero overhead.

### Layer 2: Cross-Process (Redis Lock + Pub/Sub)

```
fetchMessage(userId, messageId):
  1. GET email:{userId}:{messageId}
     -> hit? Return cached data

  2. SET email:{userId}:{messageId}:lock "fetching" NX EX 30
     -> got lock (winner):
        - Fetch from Gmail API
        - SET email:{userId}:{messageId} <data> EX 604800
        - PUBLISH email:{userId}:{messageId}:done "ok"
        - DEL email:{userId}:{messageId}:lock
        - Return data

     -> didn't get lock (waiter):
        - SUBSCRIBE email:{userId}:{messageId}:done
        - Await message (timeout: 30s)
        - GET email:{userId}:{messageId}
        - UNSUBSCRIBE
        - Return data

  3. Timeout (lock holder crashed):
     - UNSUBSCRIBE
     - Retry from step 1 (lock has expired)
```

### Batch Optimization

Multi-message requests: MGET all keys at once, then only misses go through the lock/fetch path. All misses run concurrently via Promise.all.

## Gmail API Client

### Token Management

- Fetch OAuth token from Firestore directly at the start of each batch (per user)
- No in-memory token caching -- always use fresh token from Firestore
- On 401: return `auth_failed` (token refresh is the responsibility of the OAuth management system)

### Batching and Compression

- Gmail batch API: up to 100 sub-requests per HTTP request
- Group cache misses by user, chunk into batches of 100
- All requests use `Accept-Encoding: gzip`
- Use `fields` parameter for partial responses (only request needed fields)

### Rate Limiting

- Per-user token bucket: 250 quota units/sec (Gmail's limit)
- Track quota by method cost: `messages.get` = 5 units, `messages.list` = 5 units
- When bucket is empty, queue and drain as quota replenishes
- Log aggregate per-project usage, alert near 1.2M units/min ceiling

### Retry Strategy

| Error | Behavior | Max Retries |
|-------|----------|-------------|
| 429 rate limited | Exponential backoff (1s-32s) | 5 |
| 500/502/503 | Exponential backoff | 3 |
| 401 unauthorized | Return auth_failed | 0 |
| 404 not found | Return not_found | 0 |

## Multi-User Execution

Requests spanning multiple users are grouped by user_id. Each user group:

1. Gets its own OAuth token from Firestore
2. Has its own rate limit bucket
3. Batches its own Gmail API calls
4. Runs concurrently with other user groups via Promise.all

```
Incoming: 2 users, 5 messages
  -> MGET all 5 Redis keys
  -> Hits: [msg1, msg4]
  -> Misses grouped by user:
       user_a: [msg2]       -> 1 Gmail batch
       user_b: [msg3, msg5] -> 1 Gmail batch
  -> Both run concurrently
  -> Merge results, return
```

## Normalization Layer

When `format=normalized`, raw Gmail data is transformed on-the-fly:

1. Headers -> structured: from, to, cc, bcc, subject, date, message_id, in_reply_to, references
2. Body -> extract MIME parts, prefer text/plain, fallback text/html -> plaintext
3. Attachments -> metadata only: filename, mimeType, size (no binary)
4. Threads -> chronological order, reply chains preserved

Normalized shape:

```json
{
  "message_id": "abc123",
  "thread_id": "thread456",
  "from": { "name": "Jane Doe", "email": "jane@company.com" },
  "to": [{ "name": "Bob", "email": "bob@other.com" }],
  "cc": [],
  "subject": "Partnership opportunity",
  "date": "2026-03-15T10:30:00Z",
  "body": "Hi Bob, I wanted to discuss...",
  "attachments": [{ "filename": "proposal.pdf", "mimeType": "application/pdf", "size": 45231 }],
  "labels": ["INBOX", "UNREAD"]
}
```

## Error Handling

| Error | Behavior | Caller Sees |
|-------|----------|-------------|
| Token not in Firestore | Fail fast | auth_not_configured |
| Token expired/revoked | Return error | auth_failed |
| Gmail rate limited (429) | Backoff + retry | rate_limited (if exhausted) |
| Gmail server error (5xx) | Backoff + retry | service_error |
| Message not found (404) | No retry | not_found |
| Redis down | Bypass cache, fetch from Gmail | Transparent (degraded perf) |
| Lock holder crashed | Lock expires 30s, next retries | Transparent (slight delay) |

Redis is a cache, not a dependency. Service degrades to direct Gmail proxy if Redis is unavailable.

## Observability

- Structured JSON logs via pino
- Per request: user_ids, message_count, cache_hits, cache_misses, gmail_batches, duration_ms
- Per Gmail call: user_id, method, batch_size, quota_cost, status, duration_ms
- Health endpoint: `GET /health -> { status, redis, uptime_ms }`

## Scale Path

- Day one: single instance, ~20 emails/day
- Growth: add instances behind load balancer (stateless, Redis handles coordination)
- At scale: Redis cluster for cache capacity, per-user rate limiting prevents Gmail quota exhaustion
- No architectural changes needed from 20/day to billions/day
