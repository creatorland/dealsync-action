# A/B Eval Framework

Evaluate and compare deal classification prompts against ground truth. Runs on W3 testnet.

## Directory Structure

```
eval/
  README.md              ← you are here
  baseline.json          ← current baseline eval result (10 runs)
  ground-truth.json      ← labeled email threads for evaluation
  thresholds.json        ← configurable pass/fail criteria
  history/               ← archived eval comparisons
    YYYY-MM-DD-<what-changed>/
      README.md          ← findings, decision
      system-a.md        ← baseline prompt
      system-b.md        ← candidate prompt
      user-a.md, user-b.md
      result-a.json      ← baseline eval result
      result-b.json      ← candidate eval result
```

## Workflows

Three W3 workflows on testnet. Click to open in the W3 Explorer:

| Workflow | Purpose | Explorer |
|----------|---------|---------|
| `dealsync-eval-v4` | Run eval on one prompt variant | [Open](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-eval-v4&network=testnet) |
| `dealsync-eval-compare-v3` | Eval candidate + compare against baseline | [Open](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-eval-compare-v3&network=testnet) |
| `dealsync-compare` | Compare two existing results (no API calls) | [Open](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-compare&network=testnet) |

> **Note:** Workflow names include version suffixes because W3 doesn't support overwriting deployed workflows. When workflows are redeployed, update the links above.

## How to Test a New Prompt

### Option 1: Paste prompt directly (recommended)

1. Open [dealsync-eval-v4](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-eval-v4&network=testnet) in the W3 Explorer.
2. Trigger with `system_prompt` set to your prompt text.
3. Wait for completion. Grab the result JSON from the job output.
4. Open [dealsync-compare](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-compare&network=testnet).
5. Trigger with `result_b` set to your result JSON. Leave `result_a` empty — it uses the bundled baseline.
6. Read the verdict and report from the compare job output.

### Option 2: Commit prompt and use hash

1. Edit `prompts/system.md` on a branch, commit, note the hash.
2. Trigger `dealsync-eval-v4` with `prompt_hash` set to your commit hash.
3. Grab result, run compare (same as Option 1, steps 4-6).

### Option 3: One-shot eval + compare

1. Open [dealsync-eval-compare-v3](https://testnet-w3-explorer.netlify.app/workflows/?workflow_name=dealsync-eval-compare-v3&network=testnet).
2. Trigger with your prompt (via `system_prompt` or `prompt_hash`).
3. It runs the eval and comparison automatically. Report is in the compare job output.

### Promoting a Passing Variant

When a prompt variant passes:

1. Update `prompts/system.md` (and `prompts/user.md` if changed) with the new prompt.
2. Run `dealsync-eval-v4` with bundled prompts (10 runs, batch_size=5) to capture the new baseline.
3. Copy the result JSON to `eval/baseline.json` — this becomes the new baseline for all future comparisons.
4. Archive the comparison in `eval/history/` (see [Archiving Eval Results](#archiving-eval-results)).
5. If pass/fail criteria need adjusting, update `eval/thresholds.json` (see [Pass/Fail Criteria](#passfail-criteria)).
6. PR everything to main: updated prompts, baseline, history, and thresholds if changed.

## Workflow Inputs

### dealsync-eval-v4 / dealsync-eval-compare-v3

| Input | Default | Description |
|-------|---------|-------------|
| `model` | `deepseek/deepseek-chat-v3-0324` | Model to evaluate |
| `prompt_hash` | (empty) | Git commit hash for prompts. Empty = bundled. |
| `system_prompt` | (empty) | Inline system prompt. Takes priority over `prompt_hash`. |
| `user_prompt` | (empty) | Inline user prompt. Takes priority over `prompt_hash`. |
| `runs` | `10` | Number of eval runs (higher = more stable metrics) |
| `temperature` | `0` | AI temperature |
| `batch_size` | `5` | Threads per API call |

### dealsync-compare

| Input | Default | Description |
|-------|---------|-------------|
| `result_a` | (empty) | Eval result JSON for variant A. Empty = bundled baseline. |
| `result_b` | (required) | Eval result JSON for variant B. |

## Models

The eval uses OpenRouter. Any OpenRouter model works — just use its model ID as the `model` input.

**Tested models:**

| Model | Notes |
|-------|-------|
| `deepseek/deepseek-chat-v3-0324` | Default. Current production model. |
| `deepseek/deepseek-r1-0528` | Reasoning model, newest |
| `deepseek/deepseek-r1` | Reasoning model |
| `qwen/qwen3-coder-480b-a35b-instruct` | Coder model, newest |
| `meta-llama/llama-3.3-70b-instruct` | Smallest |

Browse all available models: [OpenRouter Models](https://openrouter.ai/models?fmt=cards&output_modalities=text)

## Metrics

All metrics are computed per run, then aggregated with mean/min/max/stddev across runs.

### Detection (most important)

- **Recall** — % of real deals correctly identified. This is the primary metric. Missing a deal costs more than a false positive.
- **Precision** — % of predicted deals that are actually deals.
- **F2 Score** — Harmonic mean weighted toward recall (beta=2).

### Sub-Metrics

- **Category Accuracy** — How often the predicted category matches ground truth (deal threads only).
- **Urgency Scoring** — % of threads where `ai_score` falls in the expected range.
- **Scam Detection** — Accuracy on known scam threads.
- **JSON Health** — Clean parse rate. Should be 100% with `json_schema` structured output.

## Pass/Fail Criteria

Defined in `thresholds.json`. All criteria must pass for a PASS verdict.

| Criterion | Default | Description |
|-----------|---------|-------------|
| `min_recall` | 0.95 | Candidate recall must meet this minimum |
| `min_precision` | 0.40 | Candidate precision must meet this minimum |
| `max_recall_stddev` | 0.03 | Candidate must be consistent across runs |
| `require_f2_non_regression` | true | F2 must not drop vs baseline |
| `require_no_new_missed_deals` | true | Must not miss deals the baseline catches |
| `require_no_scam_regression` | true | Must not miss scams the baseline catches |
| `require_no_category_regression` | true | Category accuracy must not drop |

To disable a criterion, set its boolean to `false` in `thresholds.json`.

## Ground Truth

`ground-truth.json` contains 60 labeled email threads (38 usable after filtering).

### Adding Entries

1. Add to the array in `ground-truth.json`.
2. Use next sequential ID (`gt-NNN`).
3. Include full email content — the model classifies the complete body.
4. Set `expected`: `is_deal`, `category`, `likely_scam`, `score_range`.
5. Run eval to verify the entry doesn't break existing metrics.

### Format

```json
{
  "id": "gt-001",
  "description": "Short description of the scenario",
  "emails": [{
    "messageId": "gt-001-msg-1",
    "threadId": "gt-001",
    "topLevelHeaders": [
      { "name": "from", "value": "brand@company.com" },
      { "name": "to", "value": "creator@example.com" },
      { "name": "subject", "value": "Campaign Proposal" },
      { "name": "date", "value": "Fri, 01 Mar 2026 09:15:00 +0000" }
    ],
    "body": "Full email body..."
  }],
  "expected": {
    "is_deal": true,
    "category": "new",
    "likely_scam": false,
    "score_range": [5, 7]
  }
}
```

## Writing Prompts

For detailed instructions on writing prompt variants compatible with the eval system (allowed values, what to include, what to omit), see [AGENTS.md](AGENTS.md). This applies to both humans and AI agents writing prompts.

## Archiving Eval Results

After each comparison, archive to `eval/history/` for a full audit trail.

### Structure

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

### Steps

1. Create `eval/history/YYYY-MM-DD-<what-changed>/` (e.g. `2026-04-17-strict-exclusion-rules`).
2. Copy both system and user prompts that were tested.
3. Copy both result JSONs from the eval outputs.
4. Write a `README.md` covering:
   - What was tested and why
   - Configuration (model, runs, temperature, batch size)
   - Detection metrics table (recall, precision, F2)
   - Sub-metrics table (category accuracy, urgency, scam detection)
   - Per-category breakdown
   - Regressions (which threads regressed and why)
   - Verdict (pass/fail)
   - Decision (adopt, reject, or iterate)
   - Recommended next steps
5. PR to main.

See [eval/history/2026-04-17-strict-exclusion-rules/](history/2026-04-17-strict-exclusion-rules/) for a complete example.

## Required Secrets

Set in W3 environment `0x248d3b31...f58cfac9f61554804f505dafd140`:

| Secret | Description |
|--------|-------------|
| `AI_API_KEY` | OpenRouter API key |
| `AI_API_URL` | `https://openrouter.ai/api/v1/chat/completions` |
