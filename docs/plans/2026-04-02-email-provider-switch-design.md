# Email Provider Switch Design

Add a feature flag to switch between content-fetcher (existing) and email-service (new) as the email data provider. No breaking changes — content-fetcher remains the default.

## Changes

### action.yml

New inputs:
- `email-provider`: `'content-fetcher'` (default) or `'email-service'`
- `email-service-url`: URL of the email-service API (only used when provider=email-service)

### src/lib/emails.js

Add `fetchEmailsFromService()` — new function that:
1. Groups messageIds by userId
2. Calls `POST {email-service-url}/v1/emails/messages` with `{ requests: [{ user_id, ids }], format: 'compat' }`
3. Maps the response to match existing `fetchEmails()` return shape: `{ fetched: Email[], failed: string[] }`
4. The compat format returns `{ messageId, topLevelHeaders, body, replyBody, date }` — identical to content-fetcher shape

Update `fetchEmails()` to check the provider flag and delegate:
- `content-fetcher` → existing code path (untouched)
- `email-service` → `fetchEmailsFromService()`

### Untouched

- `fetchThreadEmails()` — calls `fetchEmails()`, transparent
- Filter pipeline — receives same email shape
- Classify pipeline — receives same email shape
- Database writes, state machine, AI classification

## Response Mapping

email-service returns:
```json
{
  "data": [{ "message_id": "x", "payload": { "messageId", "topLevelHeaders", "body", "replyBody", "date" } }],
  "errors": [{ "message_id": "y", "error": "not_found" }],
  "meta": { "total", "fetched", "cached", "failed" }
}
```

Maps to existing fetchEmails return shape:
```json
{
  "fetched": [{ "messageId", "topLevelHeaders", "body", "replyBody", "date" }],
  "failed": ["y"]
}
```

## Error Handling

email-service always returns 200 with partial results. The `errors` array contains per-message failures. Map `errors[].message_id` to the `failed` array. No need to handle 207/502 status codes — the email-service handles retries internally.
