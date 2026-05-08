# A/B Eval Framework Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the A/B eval framework complete and self-service — configurable thresholds, human-readable markdown reports, full documentation.

**Architecture:** `eval-compare.js` loads thresholds from `eval/thresholds.json` (bundled by rollup via `@rollup/plugin-json`), generates a markdown report alongside the JSON result. `eval/README.md` documents everything a user needs to run and interpret an A/B test.

**Tech Stack:** Node 24 ESM, Jest (ESM mode), Rollup, W3 workflow runtime

---

### Task 1: Create eval/thresholds.json

**Files:**
- Create: `eval/thresholds.json`

**Step 1: Create the thresholds config**

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

**Step 2: Commit**

```bash
git add eval/thresholds.json
git commit -m "feat: add configurable eval thresholds config"
```

---

### Task 2: Update eval-compare.js to load thresholds

**Files:**
- Modify: `src/commands/eval-compare.js:1-7` (import + replace hardcoded THRESHOLDS)
- Modify: `src/commands/eval-compare.js:95-108` (use new threshold field names)

**Step 1: Write the failing test — thresholds are loaded and used**

Add a new test to `__tests__/eval-compare.test.js` that verifies custom thresholds affect the verdict. The test module needs to mock the thresholds import.

In `__tests__/eval-compare.test.js`, add after the existing `jest.unstable_mockModule('@actions/core', ...)` block (line 9):

```javascript
// After the core mock, before the imports:
// No mock needed — thresholds.json is imported directly and we test via behavior
```

Actually, the existing tests already exercise threshold behavior (e.g., `B has lower recall below threshold = FAIL` checks `b_recall_above_95`). We need to:
1. Update the import in eval-compare.js
2. Update the pass/fail field names to match new config keys
3. Update existing tests to match new field names

**Step 2: Update eval-compare.js import**

Replace lines 1-7 of `src/commands/eval-compare.js`:

Old:
```javascript
import * as core from '@actions/core'

const THRESHOLDS = {
  recall: 0.95,
  precision: 0.4,
  consistency: 0.03,
}
```

New:
```javascript
import * as core from '@actions/core'
import thresholds from '../../eval/thresholds.json'
```

**Step 3: Update pass/fail logic to use thresholds.json fields**

Replace lines 95-108 of `src/commands/eval-compare.js`:

Old:
```javascript
  const passFail = {
    b_recall_above_95: b.detection.recall.mean >= THRESHOLDS.recall,
    b_precision_above_40: b.detection.precision.mean >= THRESHOLDS.precision,
    b_f2_above_baseline: b.detection.f2.mean >= a.detection.f2.mean,
    b_consistency_within_3pct: b.detection.recall.stddev <= THRESHOLDS.consistency,
    no_new_missed_deals: newMissedDeals.length === 0,
    b_scam_no_regression: newMissedScams.length === 0,
    b_category_no_regression: b.categorization.accuracy.mean >= a.categorization.accuracy.mean,
  }
  passFail.verdict = Object.entries(passFail)
    .filter(([k]) => k !== 'verdict')
    .every(([, v]) => v === true)
    ? 'PASS'
    : 'FAIL'
```

New:
```javascript
  const passFail = {
    b_recall_above_min: b.detection.recall.mean >= thresholds.min_recall,
    b_precision_above_min: b.detection.precision.mean >= thresholds.min_precision,
    b_f2_non_regression: !thresholds.require_f2_non_regression || b.detection.f2.mean >= a.detection.f2.mean,
    b_recall_stddev_ok: b.detection.recall.stddev <= thresholds.max_recall_stddev,
    no_new_missed_deals: !thresholds.require_no_new_missed_deals || newMissedDeals.length === 0,
    b_scam_no_regression: !thresholds.require_no_scam_regression || newMissedScams.length === 0,
    b_category_no_regression: !thresholds.require_no_category_regression || b.categorization.accuracy.mean >= a.categorization.accuracy.mean,
  }
  passFail.verdict = Object.entries(passFail)
    .filter(([k]) => k !== 'verdict')
    .every(([, v]) => v === true)
    ? 'PASS'
    : 'FAIL'
```

**Step 4: Update tests to match new field names**

In `__tests__/eval-compare.test.js`:

- Line 73: `result.pass_fail.verdict` stays same
- Line 95: `b_recall_above_95` -> `b_recall_above_min`
- Line 130: `no_new_missed_deals` stays same

**Step 5: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/eval-compare.test.js`
Expected: All 5 tests PASS

**Step 6: Commit**

```bash
git add src/commands/eval-compare.js __tests__/eval-compare.test.js
git commit -m "feat: load eval thresholds from eval/thresholds.json"
```

---

### Task 3: Add markdown report generation to eval-compare.js

**Files:**
- Modify: `src/commands/eval-compare.js` (add `generateReport()` function, include in result)
- Modify: `__tests__/eval-compare.test.js` (verify report_markdown is present and contains key sections)

**Step 1: Write failing test for report generation**

Add to `__tests__/eval-compare.test.js`:

```javascript
it('result includes report_markdown with key sections', async () => {
  const a = makeResult()
  const b = makeResult({
    detection: {
      recall: { mean: 0.98, min: 0.97, max: 0.99, stddev: 0.005 },
      precision: { mean: 0.5, min: 0.48, max: 0.52, stddev: 0.01 },
      f2: { mean: 0.88, min: 0.86, max: 0.9, stddev: 0.01 },
    },
    categorization: { accuracy: { mean: 0.92, min: 0.9, max: 0.94, stddev: 0.01 } },
  })
  core.getInput.mockImplementation((name) => {
    if (name === 'result-a') return JSON.stringify(a)
    if (name === 'result-b') return JSON.stringify(b)
    return ''
  })

  const result = await runEvalCompare()
  expect(result.report_markdown).toBeDefined()
  expect(result.report_markdown).toContain('# A/B Eval Report')
  expect(result.report_markdown).toContain('## Detection Metrics')
  expect(result.report_markdown).toContain('## Pass/Fail Verdict')
  expect(result.report_markdown).toContain('PASS')
  expect(result.report_markdown).toContain('Recall')
  expect(result.report_markdown).toContain('Precision')
})
```

**Step 2: Run test to verify it fails**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/eval-compare.test.js`
Expected: FAIL — `report_markdown` is undefined

**Step 3: Implement generateReport()**

Add this function to `src/commands/eval-compare.js` before `runEvalCompare()`:

```javascript
function generateReport(a, b, comparison, perCategoryComparison, jsonComparison, costComparison, regressions, passFail, thresholds) {
  const fmt = (d) => `${d > 0 ? '+' : ''}${(+d).toFixed(4)}`
  const icon = (v) => (v ? 'PASS' : 'FAIL')
  const winner = (w) => (w === 'b' ? 'B' : w === 'a' ? 'A' : 'Tie')

  let md = `# A/B Eval Report\n\n`
  md += `## Configuration\n\n`
  md += `| | Variant A (Baseline) | Variant B (Candidate) |\n`
  md += `|---|---|---|\n`
  md += `| Model | ${a.model} | ${b.model} |\n`
  md += `| Prompt | ${a.prompt_hash || 'bundled'} | ${b.prompt_hash || 'bundled'} |\n`
  md += `| Runs | ${a.runs} (${a.successful_runs} successful) | ${b.runs} (${b.successful_runs} successful) |\n`
  md += `| Temperature | ${a.temperature} | ${b.temperature} |\n`
  md += `| Batch Size | ${a.batch_size} | ${b.batch_size} |\n\n`

  md += `## Detection Metrics\n\n`
  md += `| Metric | A | B | Delta | Winner |\n`
  md += `|--------|---|---|-------|--------|\n`
  md += `| Recall | ${comparison.recall.a} | ${comparison.recall.b} | ${fmt(comparison.recall.delta)} | ${winner(comparison.recall.winner)} |\n`
  md += `| Precision | ${comparison.precision.a} | ${comparison.precision.b} | ${fmt(comparison.precision.delta)} | ${winner(comparison.precision.winner)} |\n`
  md += `| F2 | ${comparison.f2.a} | ${comparison.f2.b} | ${fmt(comparison.f2.delta)} | ${winner(comparison.f2.winner)} |\n\n`

  md += `## Sub-Metrics\n\n`
  md += `| Metric | A | B | Delta | Winner |\n`
  md += `|--------|---|---|-------|--------|\n`
  md += `| Category Accuracy | ${comparison.category_accuracy.a} | ${comparison.category_accuracy.b} | ${fmt(comparison.category_accuracy.delta)} | ${winner(comparison.category_accuracy.winner)} |\n`
  md += `| Urgency Scoring | ${comparison.score_in_range.a} | ${comparison.score_in_range.b} | ${fmt(comparison.score_in_range.delta)} | ${winner(comparison.score_in_range.winner)} |\n`
  md += `| Scam Detection | ${comparison.scam_detection.a} | ${comparison.scam_detection.b} | ${fmt(comparison.scam_detection.delta)} | ${winner(comparison.scam_detection.winner)} |\n`
  md += `| Cost/Thread | $${costComparison.a} | $${costComparison.b} | ${fmt(costComparison.delta)} | ${winner(costComparison.winner)} |\n\n`

  md += `## Per-Category Breakdown\n\n`
  md += `| Category | A | B | Delta | Threads |\n`
  md += `|----------|---|---|-------|---------|\n`
  for (const [cat, data] of Object.entries(perCategoryComparison)) {
    md += `| ${cat} | ${data.a} | ${data.b} | ${fmt(data.delta)} | ${data.a_count} |\n`
  }
  md += `\n`

  md += `## JSON Health\n\n`
  md += `| Metric | A | B | Delta |\n`
  md += `|--------|---|---|-------|\n`
  md += `| Clean Parse Rate | ${jsonComparison.clean_parse_rate.a} | ${jsonComparison.clean_parse_rate.b} | ${fmt(jsonComparison.clean_parse_rate.delta)} |\n`
  md += `| Corrective Retry Rate | ${jsonComparison.corrective_retry_rate.a} | ${jsonComparison.corrective_retry_rate.b} | ${fmt(jsonComparison.corrective_retry_rate.delta)} |\n`
  md += `| Total Failures | ${jsonComparison.total_failures.a} | ${jsonComparison.total_failures.b} | — |\n\n`

  if (regressions.new_missed_deals.length > 0 || regressions.new_missed_scams.length > 0 || regressions.category_regressions.length > 0) {
    md += `## Regressions\n\n`
    if (regressions.new_missed_deals.length > 0)
      md += `**New missed deals:** ${regressions.new_missed_deals.join(', ')}\n\n`
    if (regressions.new_missed_scams.length > 0)
      md += `**New missed scams:** ${regressions.new_missed_scams.join(', ')}\n\n`
    if (regressions.category_regressions.length > 0)
      md += `**Category regressions:** ${regressions.category_regressions.join(', ')}\n\n`
  }

  md += `## Pass/Fail Verdict\n\n`
  md += `**${passFail.verdict}**\n\n`
  md += `| Criterion | Result |\n`
  md += `|-----------|--------|\n`
  md += `| Recall >= ${thresholds.min_recall} | ${icon(passFail.b_recall_above_min)} |\n`
  md += `| Precision >= ${thresholds.min_precision} | ${icon(passFail.b_precision_above_min)} |\n`
  md += `| F2 non-regression | ${icon(passFail.b_f2_non_regression)} |\n`
  md += `| Recall stddev <= ${thresholds.max_recall_stddev} | ${icon(passFail.b_recall_stddev_ok)} |\n`
  md += `| No new missed deals | ${icon(passFail.no_new_missed_deals)} |\n`
  md += `| No scam regression | ${icon(passFail.b_scam_no_regression)} |\n`
  md += `| No category regression | ${icon(passFail.b_category_no_regression)} |\n\n`

  md += `## Recommendation\n\n`
  if (passFail.verdict === 'PASS') {
    md += `Variant B meets all acceptance criteria. Recommend **adopting Variant B**.`
  } else {
    const failures = Object.entries(passFail).filter(([k, v]) => k !== 'verdict' && v === false).map(([k]) => k)
    md += `Variant B does not meet acceptance criteria. Failed: ${failures.join(', ')}. Recommend **keeping Variant A** (baseline).`
  }
  md += `\n`

  return md
}
```

Then in `runEvalCompare()`, after building the `result` object (before the console.log block), add:

```javascript
  result.report_markdown = generateReport(a, b, comparison, perCategoryComparison, jsonComparison, costComparison, result.regressions, passFail, thresholds)
```

**Step 4: Run tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/eval-compare.test.js`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add src/commands/eval-compare.js __tests__/eval-compare.test.js
git commit -m "feat: generate markdown report in eval-compare"
```

---

### Task 4: Write eval/README.md

**Files:**
- Create: `eval/README.md`

**Step 1: Write the README**

Content covers:
1. **Quick Start** — how to trigger an A/B eval (deploy workflow to W3, trigger with inputs)
2. **Workflow Inputs** — table of all `workflow_dispatch` inputs with descriptions and defaults
3. **Available Models** — model list with context lengths and costs (from workflow comment)
4. **Prompt Versioning** — how `prompt-hash` works, how to commit a new variant
5. **Metrics Reference** — definitions of recall, precision, F2, category accuracy, urgency scoring, scam detection, JSON health
6. **Pass/Fail Criteria** — what each threshold means, reference to `thresholds.json`
7. **Reading the Report** — how to interpret the markdown output
8. **Ground Truth** — format of `ground-truth.json`, how to add entries
9. **Required Secrets** — `AI_API_KEY` (name only)

See Task 4 implementation for full content.

**Step 2: Commit**

```bash
git add eval/README.md
git commit -m "docs: add eval README with methodology and operator guide"
```

---

### Task 5: Rebuild dist

**Step 1: Run package**

Run: `npm run package`
Expected: Builds successfully, `dist/index.js` updated

**Step 2: Run all tests**

Run: `npm test`
Expected: All tests pass

**Step 3: Commit**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist"
```

---

### Task 6: Deploy workflow to W3 and verify

**Step 1: Compile the workflow**

Use `mcp__w3__compile-workflow` with the current YAML to verify no regressions.
Expected: `valid: true`

**Step 2: Deploy to W3 testnet**

Use `mcp__w3__deploy-workflow` with the workflow YAML, targeting testnet.
Expected: Deploy succeeds, trigger hash returned.

**Step 3: Verify deployment**

Use `mcp__w3__list-workflows` to confirm the workflow appears.
Expected: `dealsync-ab-eval-3f784f8` in the list.

Note: Actual A/B run for Brian's prompt is a separate step after this framework is merged.
