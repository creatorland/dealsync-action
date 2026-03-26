# A/B Eval Report: Qwen3-235B + v2 Prompt

## Test Configuration

- Model: Qwen/Qwen3-235B-A22B-Instruct-2507
- Prompt hash: 57e99add6f2e4c1983607aa37d2c5e7319023792
- Runs: 10 (averaged)
- Batch size: 1
- Temperature: 0
- Ground truth threads: 38 (after filtering)

## Detection Metrics (10-run average)

| Metric    | Mean   | Min    | Max    | StdDev |
| --------- | ------ | ------ | ------ | ------ |
| Recall    | 0.9708 | 0.9583 | 1      | 0.0191 |
| Precision | 0.8934 | 0.8519 | 0.9231 | 0.0255 |
| F2        | 0.9541 | 0.935  | 0.9836 | 0.0169 |

## Categorization Accuracy

- Overall: 0.7078 (stddev: 0.0399)

## Urgency Scoring

- In-range rate: 0.6789 (stddev: 0.0283)

## Scam Detection

- Accuracy: 0.75 (stddev: 0)
- Total scam threads: 4

## JSON Health

- Clean parse rate: 1
- Total failures: 0

## Cost

- Avg cost per thread: $0.053299

## Model Comparison (all using v2 prompt, 10-run average)

| Model            | Recall    | Precision | F2        | Scam     | JSON Fail | Cost   | Speed |
| ---------------- | --------- | --------- | --------- | -------- | --------- | ------ | ----- |
| **Qwen3-235B**   | **0.971** | 0.893     | **0.954** | **0.75** | 0         | $0.053 | ~6s   |
| DeepSeek-V3-0324 | 0.854     | 0.954     | 0.872     | 0.25     | 0         | $0.053 | ~5s   |
| gpt-oss-120b     | 0.796     | 0.901     | 0.815     | 0.375    | 0         | $0.058 | ~20s  |
| Llama-3.3-70B    | 0.829     | 0.952     | 0.851     | 0.25     | 3         | $0.052 | ~3s   |

## Key Weaknesses to Address via Prompt Iteration

### 1. Category accuracy is low (70.8%)

The model correctly detects deals but often assigns the wrong category.
Specific issues:

- **not_interested vs in_progress**: Threads where creator declined are often classified as in_progress or new (gt-003, gt-004, gt-007, gt-025)
- **likely_scam detection**: gt-032 (tallium.info) never classified as likely_scam (0/10), gt-035 only 5/10

### 2. Urgency scoring misaligned (67.9% in-range)

The model scores many threads outside the expected range:

- Completed deals (gt-001, gt-002) expected 9-10 but scored lower
- Declined deals expected 7-9 but scored lower (gt-003, gt-005, gt-008, gt-009, gt-010, gt-027)

### 3. False positives on non-deals (precision 89.3%)

These non-deal threads were classified as deals:

- gt-047: Financial newsletter (Short Squeez) — classified as deal in 10/10 runs
- gt-059: SaaS outreach (Velocitie) — classified as deal in 10/10 runs
- gt-049: Calendar acceptance — classified as deal in 3/10 runs
- gt-058: CPG community invite — classified as deal in 5/10 runs

### 4. Scam detection needs work (75%)

- gt-032 (tallium.info fake partnership): Never flagged as scam (0/10)
- gt-017 (thrive benefits .info): Incorrectly flagged as scam (10/10) — it is a real B2B service
