# A/B Eval Framework

Operator guide for running prompt/model A/B evaluations against ground truth.

The eval framework runs two classification variants in parallel, then compares their metrics against thresholds to produce a pass/fail verdict. It runs on W3 testnet, not GitHub Actions directly.

## Quick Start

Trigger an eval via W3:

```bash
# Default: compare production prompt (4274af0) against v2 prompt (57e99ad)
w3 trigger dealsync-ab-eval-3f784f8

# Compare two models on the same prompt
w3 trigger dealsync-ab-eval-3f784f8 \
  --input hash_a=4274af0 --input hash_b=4274af0 \
  --input model_a=Qwen/Qwen3-235B-A22B-Instruct-2507 \
  --input model_b=deepseek-ai/DeepSeek-V3-0324

# Multi-run eval for statistical confidence
w3 trigger dealsync-ab-eval-3f784f8 --input runs=5
```

The workflow runs three jobs: `eval-a` and `eval-b` in parallel, then `compare` which produces the report.

## Workflow Inputs

| Input | Type | Default | Description |
|-------|------|---------|-------------|
| `hash_a` | string | `4274af0` | Prompt commit hash for variant A (baseline) |
| `hash_b` | string | `57e99ad` | Prompt commit hash for variant B (candidate) |
| `model_a` | string | `deepseek-ai/DeepSeek-V3-0324` | Model for variant A |
| `model_b` | string | `deepseek-ai/DeepSeek-V3-0324` | Model for variant B |
| `runs` | string | `1` | Number of eval runs per variant |
| `temperature` | string | `0` | AI temperature (0 = deterministic) |
| `batch_size` | string | `1` | Threads per AI call |

**Typical configurations:**

- **Prompt comparison:** Same model for both, different hashes.
- **Model comparison:** Same hash for both, different models.
- **Consistency check:** Same everything, `runs=5`, check stddev.
- **Batch testing:** Increase `batch_size` to test how grouping affects accuracy.

## Available Models

All models served via the Hyperbolic API.

| Model | Notes |
|-------|-------|
| `deepseek-ai/DeepSeek-V3-0324` | Default, current production |
| `deepseek-ai/DeepSeek-R1-0528` | Reasoning model, newest |
| `deepseek-ai/DeepSeek-R1` | Reasoning model |
| `Qwen/Qwen3-Coder-480B-A35B-Instruct` | Coder model, newest |
| `meta-llama/Llama-3.3-70B-Instruct` | Smallest |

## Prompt Versioning

Prompts live in `prompts/system.md` and `prompts/user.md`. At eval time, the `eval` command fetches these files from a specific git commit:

```
https://raw.githubusercontent.com/creatorland/dealsync-action/<hash>/prompts/system.md
https://raw.githubusercontent.com/creatorland/dealsync-action/<hash>/prompts/user.md
```

**To create a new variant:**

1. Edit `prompts/system.md` and/or `prompts/user.md`.
2. Commit the changes.
3. Note the commit hash (first 7 chars is fine).
4. Use that hash as `hash_a` or `hash_b` in the eval workflow.

**Known prompt hashes:**

| Hash | Description |
|------|-------------|
| `4274af0` | Rust baseline prompt (production v1) |
| `57e99ad` | v2 prompt with prefix caching + examples |

## Metrics Reference

The `eval` command classifies all ground truth threads and computes the following metrics. When `runs > 1`, mean/min/max/stddev are reported across runs.

### Detection Metrics

- **Recall** -- Proportion of actual deals correctly identified as deals. A recall of 0.95 means the model caught 95% of real deals. This is the most important metric: missing a deal is worse than a false positive.
- **Precision** -- Proportion of threads predicted as deals that are actually deals. A precision of 0.60 means 60% of "deal" predictions were correct.
- **F2 Score** -- Weighted harmonic mean of precision and recall with beta=2, which weights recall 4x more than precision. We use F2 instead of F1 because missing a deal has higher business cost than a false positive.

### Sub-Metrics

- **Category Accuracy** -- How often the predicted category matches ground truth, evaluated only on deal threads. The report includes a per-category breakdown.
- **Urgency Scoring** -- Proportion of threads where `ai_score` falls within the expected range defined in ground truth (e.g., `[9, 10]` for a completed deal).
- **Scam Detection** -- Accuracy of `likely_scam` prediction on threads marked as scams in ground truth.

### JSON Health

Measures the model's ability to produce valid, parseable JSON output:

- **Clean parse rate** -- Percentage of responses that parsed without intervention.
- **Corrective retry rate** -- Percentage that required a corrective retry (Layer 2 of the AI pipeline).
- **Total failures** -- Responses that could not be parsed even after all fallback layers.

## Pass/Fail Criteria

Thresholds are defined in `eval/thresholds.json`. The `eval-compare` command checks variant B against these criteria:

| Criterion | Threshold | Description |
|-----------|-----------|-------------|
| `min_recall` | `0.95` | B must achieve recall >= 0.95 |
| `min_precision` | `0.40` | B must achieve precision >= 0.40 |
| `max_recall_stddev` | `0.03` | B's recall stddev must be <= 0.03 (requires `runs > 1`) |
| `require_f2_non_regression` | `true` | B's F2 must be >= A's F2 |
| `require_no_new_missed_deals` | `true` | B must not miss any deals that A caught consistently |
| `require_no_scam_regression` | `true` | B must not miss scams that A caught |
| `require_no_category_regression` | `true` | B's category accuracy must be >= A's |

**Adjusting thresholds:** Edit `eval/thresholds.json`. Boolean flags can be set to `false` to disable that criterion. For example, set `require_no_category_regression` to `false` if you are testing a prompt change that intentionally changes category boundaries.

## Reading the Report

The `eval-compare` command produces a `report_markdown` field in its JSON output. The report contains:

1. **Configuration** -- Table showing which hash/model/temperature was used for each variant.
2. **Detection Metrics** -- Side-by-side recall, precision, and F2 for A and B.
3. **Sub-Metrics** -- Category accuracy, urgency scoring, scam detection for both variants.
4. **Per-Category Breakdown** -- Accuracy per deal category (e.g., completed, negotiating, outreach).
5. **JSON Health** -- Clean parse rate, corrective retries, and failures for both variants.
6. **Regressions** -- Lists specific threads where B performed worse than A (missed deals, missed scams, wrong categories). Only appears if regressions exist.
7. **Verdict** -- Pass or fail, with per-criterion results showing which checks passed and which failed.
8. **Recommendation** -- Auto-generated summary: ship, investigate, or reject.

**What to look for:**

- If verdict is PASS, the candidate is safe to promote to production.
- If verdict is FAIL, check the Regressions section first -- it tells you exactly which threads regressed and why.
- If recall stddev is high, run more iterations (`runs=5` or `runs=10`) to get stable numbers.
- If JSON health is degraded, the model may need a different prompt structure or lower temperature.

## Ground Truth

Ground truth lives in `eval/ground-truth.json` -- an array of labeled email threads.

### Format

```json
{
  "id": "gt-001",
  "description": "Contract signed, all deliverables confirmed...",
  "emails": [
    {
      "messageId": "gt-001-msg-1",
      "threadId": "gt-001",
      "topLevelHeaders": [
        { "name": "from", "value": "brand@company.com" },
        { "name": "to", "value": "creator@example.com" },
        { "name": "subject", "value": "Re: Campaign - Contract Signed" },
        { "name": "date", "value": "Fri, 01 Mar 2026 09:15:00 +0000" }
      ],
      "body": "Full email body text..."
    }
  ],
  "expected": {
    "is_deal": true,
    "category": "completed",
    "likely_scam": false,
    "score_range": [9, 10]
  }
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier (convention: `gt-NNN`) |
| `description` | string | Brief description of the scenario |
| `emails` | array | One or more emails in the thread |
| `emails[].messageId` | string | Unique message ID |
| `emails[].threadId` | string | Thread ID (same as `id`) |
| `emails[].topLevelHeaders` | array | Email headers: from, to, subject, date |
| `emails[].body` | string | Full email body |
| `expected.is_deal` | boolean | Whether this thread is a deal |
| `expected.category` | string | Expected category (only checked for deals) |
| `expected.likely_scam` | boolean | Whether this is a scam |
| `expected.score_range` | [min, max] | Expected `ai_score` range (inclusive) |

### Adding entries

1. Add a new object to the array in `eval/ground-truth.json`.
2. Use the next sequential ID (`gt-NNN`).
3. Include realistic email content -- the AI classifies the full body text.
4. Set `expected` fields based on human judgment.
5. Run an eval to verify the new entry does not break existing metrics.

Multi-email threads are supported -- add multiple objects to the `emails` array with the same `threadId`.

## Required Secrets

| Secret | Description |
|--------|-------------|
| `AI_API_KEY` | Hyperbolic API key. Set in the W3 environment. |

The workflow uses W3 environment `0x226c...7665` which must have this secret configured.
