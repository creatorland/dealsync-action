# A/B Eval Framework Design

**Date:** 2026-04-17
**Issue:** creatorland/dealsync-v2#409 (parent), #404, #405, #406, #407, #408
**Branch:** feat/ab-eval-framework

## Problem

Deal classification prompts may be too strict, causing legitimate deal emails to never reach "Needs Review." We need a reproducible A/B testing framework so anyone on the team can compare prompt/model variants against a ground-truth dataset and get a clear pass/fail verdict with supporting metrics.

## What exists

- **Workflow:** `.github/workflows/dealsync-ab-eval.yml` — `workflow_dispatch` with inputs for `hash_a/b`, `model_a/b`, `runs`, `temperature`, `batch_size`. Three jobs: `eval-a`, `eval-b` (parallel), `compare` (needs both). Compiles clean on W3. Never deployed.
- **`eval` command** (`src/commands/eval.js`): fetches prompts by git commit hash, runs N times against ground-truth, computes detection (recall/precision/F2), category accuracy, scam detection, urgency scoring, JSON health, and per-thread breakdown. Supports configurable model, temperature, batch size, concurrency.
- **`eval-compare` command** (`src/commands/eval-compare.js`): side-by-side deltas for all metrics, per-category comparison, regression detection (new missed deals/scams/categories), cost comparison, and a pass/fail verdict with 7 criteria.
- **Ground truth:** `eval/ground-truth.json` — 38 usable threads after static filtering.
- **Existing report:** `eval/report-qwen3-v2-prompt.md` — baseline results for Qwen3 + v2 prompt.
- **Prompt versioning:** prompts fetched at runtime from GitHub by commit hash (`prompts/system.md`, `prompts/user.md`).

## Gaps identified

| # | Gap | Severity | In scope? |
|---|-----|----------|-----------|
| 1 | ~~Broken job output wiring~~ — W3 handles step-to-job output propagation automatically. Confirmed via `compile-workflow`. | N/A | N/A |
| 2 | No artifact persistence — eval results live only in step output / job logs | Medium | Yes |
| 3 | No human-readable comparison output — `eval-compare` logs a verdict but writes no markdown report | High | Yes |
| 4 | Hardcoded thresholds — recall >= 0.95, precision >= 0.40, etc. baked into `eval-compare.js` | Medium | Yes |
| 5 | No documentation — no README explaining how to run a test, what "pass" means, or how to add variants | High | Yes |
| 6 | Small ground-truth set (38 threads) | Low | No (out of scope) |

## Scope

Gaps 2-5. No ground-truth expansion.

## Design

### 1. Configurable thresholds (`eval/thresholds.json`)

Versioned config file loaded by `eval-compare` at build time (rollup JSON plugin, same pattern as ground-truth.json). Replaces hardcoded `THRESHOLDS` constant.

```json
{
  "min_recall": 0.95,
  "min_precision": 0.40,
  "max_recall_stddev": 0.03,
  "require_f2_non_regression": true,
  "require_no_new_missed_deals": true,
  "require_no_scam_regression": true,
  "require_no_category_regression": true
}
```

Thresholds change rarely and are part of the methodology — they should be versioned and reviewable in PRs, not workflow inputs.

### 2. Markdown report generation

`eval-compare` generates a markdown report as `report_markdown` field in its result JSON. Contents:

- Test configuration (models, hashes, runs, temperature)
- Detection metrics table (recall, precision, F2 — A vs B with deltas)
- Sub-metrics table (category accuracy, urgency scoring, scam detection)
- Per-category breakdown
- JSON health comparison
- Cost comparison
- Regressions section (new missed deals, new missed scams, category regressions)
- Pass/fail verdict with per-criterion breakdown
- Auto-generated recommendation ("ADOPT B" if PASS, "KEEP A" if FAIL)

### 3. Workflow

No structural changes needed — W3 handles output wiring. The report is available in the compare job's step output.

### 4. Documentation (`eval/README.md`)

Operator guide + methodology reference:

- Quick start — how to commit a prompt variant, deploy the workflow, trigger a run
- Inputs reference — what each workflow input does
- Metrics reference — definitions of recall, precision, F2, category accuracy, urgency scoring, scam detection, JSON health
- Pass/fail criteria — what each threshold means, how to adjust via `thresholds.json`
- Reading the report — how to interpret the comparison output
- Prompt versioning — how hashes work, how to add a new variant
- Available models — model list with context lengths and costs
- Required secrets — `AI_API_KEY` (names only, no values)

## Flow

```
                     Repo (versioned)
  ┌──────────────────────────────────────────────┐
  │  eval/ground-truth.json   (38 threads)       │
  │  eval/thresholds.json     (pass/fail config)  │
  │  eval/README.md           (methodology)       │
  │  prompts/ @ hash          (system.md, user.md) │
  └──────────────────────────────────────────────┘
                         │
             ┌───────────┴───────────┐
             v                       v
     ┌───────────────┐       ┌───────────────┐
     │  job: eval-a  │       │  job: eval-b  │
     │  prompt@hashA │       │  prompt@hashB │
     │  model_a      │       │  model_b      │
     │  N runs       │       │  N runs       │
     └───────┬───────┘       └───────┬───────┘
             │  result JSON          │  result JSON
             └───────────┬───────────┘
                         v
               ┌─────────────────┐
               │  job: compare   │
               │  load thresholds │
               │  compute deltas  │
               │  generate report │
               │  verdict: PASS/  │
               │          FAIL    │
               └─────────────────┘
                         │
                         v
               result JSON with report_markdown
```

## Acceptance criteria mapping

| Issue | Criteria | Satisfied by |
|-------|----------|--------------|
| #406 | Workflow definition exists | Existing workflow, already compiles |
| #406 | Success criteria documented | `eval/README.md` pass/fail section + `eval/thresholds.json` |
| #406 | Outputs suitable for metrics capture | `report_markdown` in result JSON |
| #406 | Required secrets documented | `eval/README.md` secrets section |
| #407 | Results include precision, recall, F1, FPR, FNR | Already in `eval` output (F2 instead of F1, by design) |
| #407 | Side-by-side comparison recorded | `report_markdown` comparison tables |
| #407 | Notable misclassifications documented | Per-thread regressions in report |
| #408 | Methodology documented | `eval/README.md` metrics reference |
| #408 | Recommendation with data | Auto-generated in report |
| #408 | Trade-offs explicit | Recall/precision deltas in report |
| #409 | A/B approach documented and reproducible | `eval/README.md` + versioned thresholds |
| #409 | Evaluation metrics defined with baseline | Existing report + thresholds.json |
