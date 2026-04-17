# Writing Prompts for Eval — Agent Instructions

This is for AI agents writing or modifying classification prompt variants for the A/B eval framework.

## What the system enforces

The eval uses `json_schema` structured output, which constrains the model's response at the token level. You do **not** need to specify JSON format, field types, or output structure in the prompt. The schema handles:

- Field names and types (`thread_index`, `is_deal`, `ai_score`, etc.)
- Required vs optional fields
- Root object shape (`{ "results": [...] }`)

## What your prompt must be compatible with

Your prompt controls **how** the model reasons about classification. The schema controls **what** it outputs. They must agree on allowed values:

**category** (when `is_deal=true`) — must be one of:
- `new` — brand reached out, creator hasn't responded
- `in_progress` — active conversation or negotiation
- `completed` — deal closed, deliverables confirmed
- `not_interested` — creator declined, ghosted, or brand rejected
- `low_confidence` — might be a deal but unclear
- `likely_scam` — scam indicators present

**deal_type** — must be one of:
- `brand_collaboration`, `sponsorship`, `affiliate`, `product_seeding`, `ambassador`, `content_partnership`, `paid_placement`, `other_business`

**ai_score** — integer 1-10 (clamped by the system if out of range)

**ai_summary** — max 1000 chars. The current production prompt uses a semicolon-delimited format (`brand:; contact_name:; contact_email:; contact_title:; contact_company:; contact_phone:; outreach_type:; offer:; deliverables:; comp_cash:; comp_currency:; comp_product:; comp_commission:; comp_notes:; timeline:; status:; next_steps:; links:; context:`). If your prompt changes this format, downstream parsing may break. Keep the format unless you're also updating the parser.

**main_contact** — string (email address of primary external contact), not an object.

**likely_scam** — boolean. The system also sets this to `true` if `category` is `likely_scam`.

## What you can freely change

- Deal vs not-deal classification rules
- Scoring guidelines and thresholds
- Scam detection heuristics
- Category assignment logic
- How the model interprets email content
- Any reasoning instructions

## What to omit

- JSON output format examples (the schema handles this)
- Field type definitions
- "Return a JSON array" instructions
- Output structure descriptions

These are redundant with the `json_schema` and can confuse the model if they conflict with the enforced schema.

## Tips

- Focus on classification logic, not output format.
- Use `low_confidence` liberally — the eval measures whether deals are caught, not whether uncertain ones are perfectly categorized.
- Test with `runs=10` for stable metrics. Single runs have too much variance.
- The user prompt template (`prompts/user.md`) injects `{{THREAD_DATA}}` — don't change this unless you're also updating the code.
- Check `eval/thresholds.json` for current pass/fail criteria before evaluating.
- See `prompts/system.md` for the current production prompt as reference.

## Promoting a passing variant

When a prompt variant passes evaluation:

1. Update `prompts/system.md` (and `prompts/user.md` if changed) with the new prompt.
2. Run the eval with bundled prompts (10 runs, batch_size=5) to capture the new baseline.
3. Copy the result JSON to `eval/baseline.json`.
4. Archive the comparison in `eval/history/` (see below).
5. If pass/fail criteria need adjusting, update `eval/thresholds.json`. Each boolean criterion can be set to `false` to disable it, and numeric thresholds (`min_recall`, `min_precision`, `max_recall_stddev`) can be tuned.
6. PR everything to main: updated prompts, baseline, history, and thresholds if changed.

## Archiving results

After running an eval comparison, archive results to `eval/history/` following this structure:

```
eval/history/YYYY-MM-DD-<what-changed>/
  README.md          ← findings, verdict, decision, next steps
  system-a.md        ← variant A system prompt
  system-b.md        ← variant B system prompt
  user-a.md          ← variant A user prompt
  user-b.md          ← variant B user prompt
  result-a.json      ← variant A eval result (full JSON)
  result-b.json      ← variant B eval result (full JSON)
```

### Generating the report

The eval result JSONs contain all the data needed to populate the report. After running the eval:

1. The `eval` command outputs a result JSON with `detection.recall.mean`, `detection.precision.mean`, `detection.f2.mean`, `categorization`, `urgency_scoring`, `scam_detection`, `cost`, and `per_thread` breakdowns.
2. The `eval-compare` command outputs a comparison JSON with `comparison`, `regressions`, `pass_fail`, and a `report_markdown` field.
3. Use `report_markdown` as a starting point, then add the Summary, Decision, and Recommended Next Steps sections which require human judgment.

### README template

Follow this exact structure for the README. See [eval/history/2026-04-17-strict-exclusion-rules/](history/2026-04-17-strict-exclusion-rules/) for a complete example.

```markdown
# Eval: <Baseline Name> vs <Candidate Name>

**Date:** YYYY-MM-DD
**Model:** <model-id> (<provider>)
**Runs:** N | **Temp:** N | **Batch Size:** N | **Ground Truth:** N threads

## Variants

| | A (Baseline) | B (Candidate) |
|---|---|---|
| Prompt | <description> ([system-a.md](system-a.md)) | <description> ([system-b.md](system-b.md)) |
| Source | <where it came from> | <where it came from> |

## Detection

| Metric | A | B | Delta |
|---|---|---|---|
| Recall | X% | X% | +/-X% |
| Precision | X% | X% | +/-X% |
| F2 Score | X% | X% | +/-X% |

## Sub-Metrics

| Metric | A | B | Delta |
|---|---|---|---|
| Category Accuracy | X% | X% | +/-X% |
| Urgency Scoring | X% | X% | +/-X% |
| Scam Detection | X% | X% | +/-X% |
| Cost/Thread | $X | $X | +/-$X |

## Per-Category Breakdown

| Category | A | B | Delta | Threads |
|---|---|---|---|---|
| <category> | X% | X% | +/-X% | N |

## Consistency

| | A | B |
|---|---|---|
| Recall stddev | X% | X% |

## Regressions

**Deals caught by A but missed by B (N threads):**
<list thread IDs>

<Brief explanation of why these regressed.>

**Category regressions (N threads):**
<list thread IDs>

## Verdict: PASS/FAIL

| Criterion | Result |
|---|---|
| Recall >= X% | PASS/FAIL (actual%) |
| Precision >= X% | PASS/FAIL |
| F2 non-regression | PASS/FAIL |
| Recall stddev <= X% | PASS/FAIL (actual%) |
| No new missed deals | PASS/FAIL (N misses) |
| No scam regression | PASS/FAIL |
| No category regression | PASS/FAIL |

## Summary

<2-3 sentences: what improved, what regressed, root cause analysis>

## Decision

**<Adopt B / Keep A / Needs iteration>**. <One sentence rationale.>

## Recommended Next Steps

- <Actionable next step>
- <Actionable next step>

## Files

| File | Description |
|---|---|
| [system-a.md](system-a.md) | A system prompt |
| [system-b.md](system-b.md) | B system prompt |
| [user-a.md](user-a.md) | A user prompt |
| [user-b.md](user-b.md) | B user prompt |
| [result-a.json](result-a.json) | A eval result |
| [result-b.json](result-b.json) | B eval result |
```
