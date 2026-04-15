# Classify pipeline: deal_value / deal_currency fix — design

**Date:** 2026-04-15
**Status:** Approved, pending implementation plan
**Scope:** Fix + prevention. Backfill deferred to a separate plan.

## Problem

Since commit `9bc8c91` (2026-03-31), every deal written by `run-classify-pipeline` lands in `DEALSYNC_PROD_V2.DEALS` with `VALUE = 0` and `CURRENCY = 'USD'`, regardless of what the AI extracted. ~4,771 prod rows are affected (2026-04-04 → present). See `/tmp/dealsync-action-bug-report.md` for the full report.

Two bugs in [src/commands/run-classify-pipeline.js:596-598](../../src/commands/run-classify-pipeline.js#L596-L598):

1. `typeof thread.deal_value === 'string'` — always false, because `parseAndValidate` already coerces `deal_value` to `number | null`. Every deal falls through to the `0` branch.
2. `thread.currency || 'USD'` — the parsed field is named `deal_currency`. `thread.currency` is always `undefined`, so fallback fires unconditionally.

Root cause is structural: the AI-output contract is implicit. `parseAndValidate` produces one shape; `run-classify-pipeline` reads a drifted shape. Nothing binds them.

## Goal

Fix the bugs and prevent the whole class of "pipeline reads the wrong field name or wrong type from the AI output." Silent drops of AI-extracted data should be impossible to ship.

Non-goals (deferred):
- Backfilling the ~4,771 affected prod rows from `AI_EVALUATION_AUDITS`.
- Revisiting the `VALUE` column's `NOT NULL` semantics.

## Approach

Introduce `zod` as the single source of truth for the AI classification output shape. The schema is used in two places:

1. **`parseAndValidate()`** ([src/lib/ai.js](../../src/lib/ai.js)) delegates coercion/validation to the schema. Rejected outputs continue to feed the existing Layer 2 corrective-retry path.
2. **A new pure `threadToDealTuple()`** function in `src/lib/deal-mapper.js` consumes the schema-typed thread and produces the SQL tuple. Called from `run-classify-pipeline.js` in place of the inline map.

A typo like `thread.currency` becomes a test-time failure, because the mapper is pure, has dedicated tests, and the tests reference the schema's real field names.

Why zod over a hand-rolled validator: one library call replaces ~40 lines of manual coercion in `parseAndValidate`, gives us parse-time rejection of unknown enums, and composes cleanly if more fields are added. Runtime-only, rollup-friendly, ~12 KB.

## Components

### `src/lib/ai-schema.js` (new)

Exports:
- `AiThreadSchema` — Zod object for a single classified thread.
- `AiThreadArraySchema = z.array(AiThreadSchema)`.

Fields mirror the current `parseAndValidate` output contract:
- `thread_id: z.string()`
- `category: z.enum([...known categories])`
- `deal_type: z.enum([...known deal types])`
- `deal_name: z.string()`
- `deal_value: z.union([z.number(), z.string().transform(Number)]).nullable()` (coerces stringy numbers, preserves prior lenient intent)
- `deal_currency: z.string().nullable()`
- `ai_score: z.number().transform((n) => clamp(n, 1, 10))`
- `main_contact: z.object({...}).optional()`
- All other fields currently read by the pipeline, enumerated from existing `parseAndValidate` logic.

Unknown fields are stripped (default zod behavior). Bad enum values or unparsable types cause `.safeParse()` to return `{ success: false }`, which Layer 2 retry already handles.

### `src/lib/ai.js` (modified)

`parseAndValidate()` replaces its manual coercion block with `AiThreadArraySchema.safeParse(input)`. On failure, returns the existing error shape so callers and Layer 2 corrective retry are unchanged.

### `src/lib/deal-mapper.js` (new)

```js
export function threadToDealTuple(thread, { userId }) { ... }
```

Pure function. Uses correct field names. `Number.isFinite(thread.deal_value) && thread.deal_value >= 0` → value, else `0`. `thread.deal_currency?.trim() || 'USD'` → currency. All SQL escaping via existing `sanitizeId` / `sanitizeString`.

### `src/commands/run-classify-pipeline.js` (modified)

Replace lines 590–602 with a `.map()` that calls `threadToDealTuple(thread, { userId: userByThread[...] })`. No other changes to the pipeline.

## Testing

### `__tests__/deal-mapper.test.js` (new)
- `deal_value: 2500` → tuple contains `2500`.
- `deal_value: null` → tuple contains `0`.
- `deal_value: NaN` / `undefined` / negative → `0`.
- `deal_currency: 'EUR'` → tuple contains `'EUR'`.
- `deal_currency: null` → tuple contains `'USD'`.
- `deal_name` with apostrophe → SQL-escaped.

### `__tests__/ai-schema.test.js` (new)
- Valid AI output parses cleanly.
- Stringy `deal_value: "2500"` coerces to `2500`.
- Unknown `category` → `safeParse` fails.
- Extra unknown fields are stripped, parse succeeds.

### `__tests__/ai.test.js` (extend)
- `parseAndValidate` returns the same shape consumers expect (regression guard).

## Rollout

1. `npm install zod@^3 --save` and `npm run all`.
2. Deploy to W3 testnet. Classify a small batch of known deals.
3. Verify `SELECT VALUE, COUNT(*) FROM DEALSYNC_STG_V1.DEALS WHERE CREATED_AT >= <deploy_time> GROUP BY VALUE` shows a non-zero spread.
4. Deploy to betanet → prod.

## Out of scope

- Backfilling `~4,771` affected prod rows from `AI_EVALUATION_AUDITS`. Separate design.
