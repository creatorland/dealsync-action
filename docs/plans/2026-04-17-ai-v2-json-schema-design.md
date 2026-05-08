# AI V2: JSON Schema Structured Output Design

**Date:** 2026-04-17
**Branch:** feat/ab-eval-framework

## Problem

`ai.js` uses `response_format: { type: 'json_object' }` which tells the model to return valid JSON but doesn't enforce structure. Models return wrapped objects, bare arrays, or malformed JSON requiring a multi-layer repair pipeline (fence stripping, array extraction, wrapper unwrapping, corrective retry). This is fragile and wastes API calls on corrective retries.

## Solution

Create `ai-v2.js` with `response_format: { type: 'json_schema' }` which uses constrained decoding at the token level to force the model to output schema-compliant JSON. Combined with OpenRouter's free Response Healing, this eliminates the need for custom repair code.

## What changes from ai.js

1. **`callModel()`** — `response_format` switches to `json_schema` with strict schema. Same retry/backoff/429 handling.
2. **`parseAndValidate()`** — simplified to: parse JSON, unwrap `.results`, coerce values (clamp scores, validate enums, enforce limits). No fence stripping, no array extraction, no wrapper guessing.
3. **`buildPrompt()`** — unchanged, copied as-is.
4. **`eval.js`** — imports from `ai-v2.js`. No corrective retry layer needed.
5. **Production `ai.js`** — untouched.

## JSON Schema

```json
{
  "type": "object",
  "properties": {
    "results": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "thread_index": { "type": "integer" },
          "is_deal": { "type": "boolean" },
          "is_english": { "type": "boolean" },
          "language": { "type": ["string", "null"] },
          "ai_score": { "type": "integer" },
          "category": { "type": ["string", "null"] },
          "likely_scam": { "type": "boolean" },
          "ai_insight": { "type": "string" },
          "ai_summary": { "type": "string" },
          "main_contact": { "type": ["string", "null"] },
          "deal_brand": { "type": ["string", "null"] },
          "deal_type": { "type": ["string", "null"] },
          "deal_name": { "type": ["string", "null"] },
          "deal_value": { "type": ["number", "null"] },
          "deal_currency": { "type": ["string", "null"] }
        },
        "required": ["thread_index", "is_deal", "is_english", "ai_score", "category", "likely_scam", "ai_insight", "ai_summary"],
        "additionalProperties": false
      }
    }
  },
  "required": ["results"],
  "additionalProperties": false
}
```

Root must be an object (json_schema spec requirement). Results unwrapped in `parseAndValidate()`.
