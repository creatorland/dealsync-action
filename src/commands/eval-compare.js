import * as core from '@actions/core'
import thresholds from '../../eval/thresholds.json'
import baseline from '../../eval/baseline.json'

function compareMetric(valA, valB) {
  const delta = +(valB - valA).toFixed(4)
  const winner = delta > 0.001 ? 'b' : delta < -0.001 ? 'a' : 'tie'
  return { a: +valA.toFixed(4), b: +valB.toFixed(4), delta, winner }
}

function generateReport(a, b, comparison, perCategoryComparison, jsonComparison, costComparison, regressions, passFail) {
  const lines = []
  const pct = (v) => `${(v * 100).toFixed(1)}%`
  const delta = (d) => `${d > 0 ? '+' : ''}${d}`
  const winner = (w) => (w === 'tie' ? 'Tie' : w === 'a' ? 'A' : 'B')

  lines.push('# A/B Eval Report')
  lines.push('')

  // Configuration
  lines.push('## Configuration')
  lines.push('')
  lines.push('| | A | B |')
  lines.push('|---|---|---|')
  lines.push(`| Model | ${a.model || '-'} | ${b.model || '-'} |`)
  lines.push(`| Prompt Hash | ${a.prompt_hash || '-'} | ${b.prompt_hash || '-'} |`)
  lines.push(`| Runs | ${a.runs} | ${b.runs} |`)
  lines.push(`| Temperature | ${a.temperature ?? '-'} | ${b.temperature ?? '-'} |`)
  lines.push(`| Batch Size | ${a.batch_size ?? '-'} | ${b.batch_size ?? '-'} |`)
  lines.push('')

  // Detection Metrics
  lines.push('## Detection Metrics')
  lines.push('')
  lines.push('| Metric | A | B | Delta | Winner |')
  lines.push('|---|---|---|---|---|')
  lines.push(`| Recall | ${pct(comparison.recall.a)} | ${pct(comparison.recall.b)} | ${delta(comparison.recall.delta)} | ${winner(comparison.recall.winner)} |`)
  lines.push(`| Precision | ${pct(comparison.precision.a)} | ${pct(comparison.precision.b)} | ${delta(comparison.precision.delta)} | ${winner(comparison.precision.winner)} |`)
  lines.push(`| F2 | ${pct(comparison.f2.a)} | ${pct(comparison.f2.b)} | ${delta(comparison.f2.delta)} | ${winner(comparison.f2.winner)} |`)
  lines.push('')

  // Sub-Metrics
  lines.push('## Sub-Metrics')
  lines.push('')
  lines.push('| Metric | A | B | Delta | Winner |')
  lines.push('|---|---|---|---|---|')
  lines.push(`| Category Accuracy | ${pct(comparison.category_accuracy.a)} | ${pct(comparison.category_accuracy.b)} | ${delta(comparison.category_accuracy.delta)} | ${winner(comparison.category_accuracy.winner)} |`)
  lines.push(`| Urgency Scoring | ${pct(comparison.score_in_range.a)} | ${pct(comparison.score_in_range.b)} | ${delta(comparison.score_in_range.delta)} | ${winner(comparison.score_in_range.winner)} |`)
  lines.push(`| Scam Detection | ${pct(comparison.scam_detection.a)} | ${pct(comparison.scam_detection.b)} | ${delta(comparison.scam_detection.delta)} | ${winner(comparison.scam_detection.winner)} |`)
  lines.push(`| Cost/Thread | $${costComparison.a} | $${costComparison.b} | ${delta(costComparison.delta)} | ${winner(costComparison.winner)} |`)
  lines.push('')

  // Per-Category Breakdown
  lines.push('## Per-Category Breakdown')
  lines.push('')
  lines.push('| Category | A | B | Delta | Threads | Winner |')
  lines.push('|---|---|---|---|---|---|')
  for (const [cat, data] of Object.entries(perCategoryComparison)) {
    lines.push(`| ${cat} | ${pct(data.a)} | ${pct(data.b)} | ${delta(data.delta)} | ${data.a_count} | ${winner(data.winner)} |`)
  }
  lines.push('')

  // JSON Health
  lines.push('## JSON Health')
  lines.push('')
  lines.push('| Metric | A | B | Delta |')
  lines.push('|---|---|---|---|')
  lines.push(`| Clean Parse Rate | ${pct(jsonComparison.clean_parse_rate.a)} | ${pct(jsonComparison.clean_parse_rate.b)} | ${delta(jsonComparison.clean_parse_rate.delta)} |`)
  lines.push(`| Corrective Retry Rate | ${pct(jsonComparison.corrective_retry_rate.a)} | ${pct(jsonComparison.corrective_retry_rate.b)} | ${delta(jsonComparison.corrective_retry_rate.delta)} |`)
  lines.push(`| Total Failures | ${jsonComparison.total_failures.a} | ${jsonComparison.total_failures.b} | - |`)
  lines.push('')

  // Regressions (only if any)
  const hasRegressions = regressions.new_missed_deals.length > 0 || regressions.new_missed_scams.length > 0 || regressions.category_regressions.length > 0
  if (hasRegressions) {
    lines.push('## Regressions')
    lines.push('')
    if (regressions.new_missed_deals.length > 0) {
      lines.push(`- **New Missed Deals:** ${regressions.new_missed_deals.join(', ')}`)
    }
    if (regressions.new_missed_scams.length > 0) {
      lines.push(`- **New Missed Scams:** ${regressions.new_missed_scams.join(', ')}`)
    }
    if (regressions.category_regressions.length > 0) {
      lines.push(`- **Category Regressions:** ${regressions.category_regressions.join(', ')}`)
    }
    lines.push('')
  }

  // Pass/Fail Verdict
  lines.push('## Pass/Fail Verdict')
  lines.push('')
  lines.push(`**${passFail.verdict}**`)
  lines.push('')
  lines.push('| Criterion | Result |')
  lines.push('|---|---|')
  for (const [key, val] of Object.entries(passFail)) {
    if (key === 'verdict') continue
    lines.push(`| ${key} | ${val ? 'PASS' : 'FAIL'} |`)
  }
  lines.push('')

  // Recommendation
  lines.push('## Recommendation')
  lines.push('')
  if (passFail.verdict === 'PASS') {
    lines.push('Adopt **B** — all criteria passed.')
  } else {
    const failures = Object.entries(passFail)
      .filter(([k, v]) => k !== 'verdict' && v === false)
      .map(([k]) => k)
    lines.push(`Keep **A** — B failed: ${failures.join(', ')}.`)
  }
  lines.push('')

  return lines.join('\n')
}

export async function runEvalCompare() {
  const resultAStr = core.getInput('result-a')
  const resultBStr = core.getInput('result-b')

  if (!resultBStr) throw new Error('result-b is required')

  // Use bundled eval/baseline.json when result-a is not provided
  const a = resultAStr ? JSON.parse(resultAStr) : baseline
  const b = JSON.parse(resultBStr)

  if (!resultAStr) {
    console.log(`[eval-compare] using bundled baseline (model=${a.model}, prompt=${a.prompt_hash || 'bundled'}, runs=${a.runs})`)
  }

  // Side-by-side metric comparison
  const comparison = {
    recall: compareMetric(a.detection.recall.mean, b.detection.recall.mean),
    precision: compareMetric(a.detection.precision.mean, b.detection.precision.mean),
    f2: compareMetric(a.detection.f2.mean, b.detection.f2.mean),
    category_accuracy: compareMetric(
      a.categorization.accuracy.mean,
      b.categorization.accuracy.mean,
    ),
    score_in_range: compareMetric(
      a.urgency_scoring.in_range_rate.mean,
      b.urgency_scoring.in_range_rate.mean,
    ),
    scam_detection: compareMetric(a.scam_detection.accuracy.mean, b.scam_detection.accuracy.mean),
  }

  // Per-category comparison
  const perCategoryComparison = {}
  const allCategories = new Set([
    ...Object.keys(a.categorization.per_category || {}),
    ...Object.keys(b.categorization.per_category || {}),
  ])
  for (const cat of allCategories) {
    const aCat = a.categorization.per_category?.[cat]
    const bCat = b.categorization.per_category?.[cat]
    perCategoryComparison[cat] = {
      ...compareMetric(aCat?.mean ?? 0, bCat?.mean ?? 0),
      a_count: aCat?.ground_truth_count ?? 0,
      b_count: bCat?.ground_truth_count ?? 0,
    }
  }

  // JSON health comparison
  const jsonComparison = {
    clean_parse_rate: compareMetric(
      a.json_health.clean_parse_rate.mean,
      b.json_health.clean_parse_rate.mean,
    ),
    corrective_retry_rate: compareMetric(
      a.json_health.corrective_retry_rate.mean,
      b.json_health.corrective_retry_rate.mean,
    ),
    total_failures: { a: a.json_health.total_failures, b: b.json_health.total_failures },
  }

  // Cost comparison
  const costComparison = compareMetric(a.cost.avg_cost_per_thread, b.cost.avg_cost_per_thread)

  // Find regressions: threads A caught but B missed
  const aPerThread = new Map(a.per_thread.map((t) => [t.id, t]))
  const bPerThread = new Map(b.per_thread.map((t) => [t.id, t]))
  const newMissedDeals = []
  const newMissedScams = []
  const categoryRegressions = []

  for (const [id, at] of aPerThread) {
    const bt = bPerThread.get(id)
    if (!bt) continue
    // A always caught it, B missed it at least once
    if (at.detection_correct === at.total_runs && bt.detection_correct < bt.total_runs) {
      if (at.expected.is_deal) newMissedDeals.push(id)
    }
    if (at.scam_correct === at.total_runs && bt.scam_correct < bt.total_runs) {
      newMissedScams.push(id)
    }
    if (at.category_correct > bt.category_correct) {
      categoryRegressions.push(id)
    }
  }

  // Pass/fail verdict
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

  const result = {
    variant_a: { model: a.model, runs: a.runs, successful_runs: a.successful_runs },
    variant_b: { model: b.model, runs: b.runs, successful_runs: b.successful_runs },
    comparison,
    per_category: perCategoryComparison,
    json_health: jsonComparison,
    cost: costComparison,
    consistency: {
      a_recall_stddev: a.detection.recall.stddev,
      b_recall_stddev: b.detection.recall.stddev,
    },
    regressions: {
      new_missed_deals: newMissedDeals,
      new_missed_scams: newMissedScams,
      category_regressions: categoryRegressions,
    },
    pass_fail: passFail,
  }

  result.report_markdown = generateReport(a, b, comparison, perCategoryComparison, jsonComparison, costComparison, result.regressions, passFail)

  const fmt = (d) => `${d > 0 ? '+' : ''}${d}`
  console.log(`[eval-compare] verdict: ${passFail.verdict}`)
  console.log(`[eval-compare] --- Detection ---`)
  console.log(
    `[eval-compare] recall:    ${comparison.recall.a} → ${comparison.recall.b} (${fmt(comparison.recall.delta)})`,
  )
  console.log(
    `[eval-compare] precision: ${comparison.precision.a} → ${comparison.precision.b} (${fmt(comparison.precision.delta)})`,
  )
  console.log(
    `[eval-compare] f2:        ${comparison.f2.a} → ${comparison.f2.b} (${fmt(comparison.f2.delta)})`,
  )
  console.log(`[eval-compare] --- Sub-metrics ---`)
  console.log(
    `[eval-compare] category:  ${comparison.category_accuracy.a} → ${comparison.category_accuracy.b} (${fmt(comparison.category_accuracy.delta)})`,
  )
  console.log(
    `[eval-compare] urgency:   ${comparison.score_in_range.a} → ${comparison.score_in_range.b} (${fmt(comparison.score_in_range.delta)})`,
  )
  console.log(
    `[eval-compare] scam:      ${comparison.scam_detection.a} → ${comparison.scam_detection.b} (${fmt(comparison.scam_detection.delta)})`,
  )
  console.log(
    `[eval-compare] cost:      $${costComparison.a} → $${costComparison.b} (${fmt(costComparison.delta)})`,
  )
  console.log(`[eval-compare] --- Per Category ---`)
  for (const [cat, data] of Object.entries(perCategoryComparison)) {
    console.log(
      `[eval-compare] ${cat}: ${data.a} → ${data.b} (${fmt(data.delta)}) [${data.a_count} threads]`,
    )
  }
  if (newMissedDeals.length > 0)
    console.log(`[eval-compare] NEW MISSED DEALS: ${newMissedDeals.join(', ')}`)

  return result
}
