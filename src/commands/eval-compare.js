import * as core from '@actions/core'

const THRESHOLDS = {
  recall: 0.95,
  precision: 0.4,
  consistency: 0.03,
}

function compareMetric(valA, valB) {
  const delta = +(valB - valA).toFixed(4)
  const winner = delta > 0.001 ? 'b' : delta < -0.001 ? 'a' : 'tie'
  return { a: +valA.toFixed(4), b: +valB.toFixed(4), delta, winner }
}

export async function runEvalCompare() {
  const resultAStr = core.getInput('result-a')
  const resultBStr = core.getInput('result-b')

  if (!resultAStr || !resultBStr) throw new Error('result-a and result-b are required')

  const a = JSON.parse(resultAStr)
  const b = JSON.parse(resultBStr)

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
