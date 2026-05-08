# Eval: V1 Baseline vs V3 Prompt

**Date:** 2026-04-17
**Model:** deepseek/deepseek-chat-v3-0324 (OpenRouter)
**Runs:** 10 | **Temp:** 0 | **Batch Size:** 5 | **Ground Truth:** 38 threads

## Variants

| | A (Baseline) | B (Candidate) |
|---|---|---|
| Prompt | V1 — production prompt ([system-a.md](system-a.md)) | V3 — stricter exclusion rules ([system-b.md](system-b.md)) |
| Source | Bundled in `prompts/system.md` | Brian's v3 draft, adjusted for schema compatibility |

## Detection

| Metric | V1 (A) | V3 (B) | Delta |
|---|---|---|---|
| Recall | 90.0% | 59.2% | -30.8% |
| Precision | 96.5% | 100.0% | +3.5% |
| F2 Score | 91.2% | 64.2% | -27.0% |

## Sub-Metrics

| Metric | V1 (A) | V3 (B) | Delta |
|---|---|---|---|
| Category Accuracy | 63.9% | 77.4% | +13.5% |
| Urgency Scoring | 48.2% | 63.2% | +15.0% |
| Scam Detection | 50.0% | 50.0% | — |
| Cost/Thread | $0.014 | $0.015 | +$0.001 |

## Per-Category Breakdown

| Category | V1 (A) | V3 (B) | Delta | Threads |
|---|---|---|---|---|
| completed | 100% | 100% | — | 2 |
| not_interested | 60.4% | 28.6% | -31.9% | 10 |
| new | 100% | 100% | — | 1 |
| in_progress | 71.4% | 85.7% | +14.3% | 7 |
| likely_scam | 33.3% | 95.0% | +61.7% | 4 |

## Consistency

| | V1 (A) | V3 (B) |
|---|---|---|
| Recall stddev | 2.0% | 10.8% |

## Regressions

**Deals caught by V1 but missed by V3 (10 threads):**
gt-003, gt-005, gt-006, gt-007, gt-008, gt-009, gt-010, gt-031, gt-032, gt-035

Most are threads where the creator declined or the deal was rejected. V3's stricter "not a deal" rules filter them out.

**Category regressions (8 threads):**
gt-003, gt-005, gt-008, gt-009, gt-010, gt-021, gt-027, gt-031

## Verdict: FAIL

| Criterion | Result |
|---|---|
| Recall >= 95% | FAIL (59.2%) |
| Precision >= 40% | PASS (100%) |
| F2 non-regression | FAIL |
| Recall stddev <= 3% | FAIL (10.8%) |
| No new missed deals | FAIL (10 misses) |
| No scam regression | PASS |
| No category regression | PASS |

## Summary

V3 shows clear improvements in category accuracy (+13.5%), urgency scoring (+15.0%), and scam detection (+61.7%). However, recall dropped significantly (90% to 59%), meaning many legitimate deals are being classified as non-deals.

The recall drop appears driven by the stricter exclusion rules — threads where a creator declined or a deal was rejected are being excluded entirely rather than categorized as `not_interested`. These threads still have value for users tracking their deal pipeline.

## Decision

**Keep V1 as baseline.** V3 not promoted.

## Recommended Next Steps

- Relax V3's exclusion rules so declined/rejected threads remain classified as deals with category `not_interested`
- Re-evaluate after adjustment
- V3's scam detection and categorization improvements are worth preserving in a future iteration

## Files

| File | Description |
|---|---|
| [system-a.md](system-a.md) | V1 system prompt (baseline) |
| [system-b.md](system-b.md) | V3 system prompt (candidate) |
| [user-a.md](user-a.md) | V1 user prompt |
| [user-b.md](user-b.md) | V3 user prompt |
| [result-a.json](result-a.json) | V1 eval result (10 runs) |
| [result-b.json](result-b.json) | V3 eval result (10 runs) |
