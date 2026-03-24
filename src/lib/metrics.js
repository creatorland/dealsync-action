/**
 * Compute { mean, min, max, stddev } from an array of numbers.
 */
export function aggregateStats(values) {
  if (values.length === 0) return { mean: 0, min: 0, max: 0, stddev: 0 }
  const mean = values.reduce((a, b) => a + b, 0) / values.length
  const min = Math.min(...values)
  const max = Math.max(...values)
  const stddev = Math.sqrt(values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / values.length)
  return { mean: +mean.toFixed(4), min: +min.toFixed(4), max: +max.toFixed(4), stddev: +stddev.toFixed(4) }
}

/**
 * Compute detection metrics (recall, precision, F2) across N runs.
 * Each run is an array of classification results from the AI.
 * Ground truth is the array from ground-truth.json.
 */
export function computeDetectionMetrics(allRuns, groundTruth) {
  const recallPerRun = []
  const precisionPerRun = []
  const f2PerRun = []

  for (const run of allRuns) {
    const resultMap = new Map(run.map((r) => [r.thread_id, r]))
    let tp = 0, fp = 0, fn = 0

    for (const gt of groundTruth) {
      const result = resultMap.get(gt.id)
      if (!result) {
        if (gt.expected.is_deal) fn++
        continue
      }
      if (gt.expected.is_deal && result.is_deal) tp++
      else if (!gt.expected.is_deal && result.is_deal) fp++
      else if (gt.expected.is_deal && !result.is_deal) fn++
    }

    const recall = tp + fn > 0 ? tp / (tp + fn) : 1
    const precision = tp + fp > 0 ? tp / (tp + fp) : 1
    const f2 = precision + recall > 0
      ? (5 * precision * recall) / (4 * precision + recall)
      : 0

    recallPerRun.push(recall)
    precisionPerRun.push(precision)
    f2PerRun.push(f2)
  }

  return {
    recall: aggregateStats(recallPerRun),
    precision: aggregateStats(precisionPerRun),
    f2: aggregateStats(f2PerRun),
  }
}

/**
 * For deal threads correctly detected, was category correct?
 */
export function computeCategoryAccuracy(allRuns, groundTruth) {
  const accuracyPerRun = []
  const dealThreads = groundTruth.filter((gt) => gt.expected.is_deal && gt.expected.category)

  for (const run of allRuns) {
    const resultMap = new Map(run.map((r) => [r.thread_id, r]))
    let correct = 0
    let total = 0

    for (const gt of dealThreads) {
      const result = resultMap.get(gt.id)
      if (!result || !result.is_deal) continue
      total++
      if (result.category === gt.expected.category) correct++
    }

    accuracyPerRun.push(total > 0 ? correct / total : 1)
  }

  return { accuracy: aggregateStats(accuracyPerRun) }
}

/**
 * For each thread, was ai_score within expected score_range?
 */
export function computeScoreInRange(allRuns, groundTruth) {
  const ratePerRun = []

  for (const run of allRuns) {
    const resultMap = new Map(run.map((r) => [r.thread_id, r]))
    let inRange = 0
    let total = 0

    for (const gt of groundTruth) {
      if (!gt.expected.score_range) continue
      const result = resultMap.get(gt.id)
      if (!result) continue
      total++
      const [lo, hi] = gt.expected.score_range
      if (result.ai_score >= lo && result.ai_score <= hi) inRange++
    }

    ratePerRun.push(total > 0 ? inRange / total : 1)
  }

  return { in_range_rate: aggregateStats(ratePerRun) }
}

/**
 * For threads where expected.likely_scam = true, did AI flag likely_scam?
 */
export function computeScamDetection(allRuns, groundTruth) {
  const scamThreads = groundTruth.filter((gt) => gt.expected.likely_scam)
  const accuracyPerRun = []

  for (const run of allRuns) {
    const resultMap = new Map(run.map((r) => [r.thread_id, r]))
    let caught = 0

    for (const gt of scamThreads) {
      const result = resultMap.get(gt.id)
      if (result && result.likely_scam) caught++
    }

    accuracyPerRun.push(scamThreads.length > 0 ? caught / scamThreads.length : 1)
  }

  return {
    accuracy: aggregateStats(accuracyPerRun),
    total_scam_threads: scamThreads.length,
  }
}

/**
 * Per-thread breakdown: how many runs got each metric right.
 */
export function computePerThread(allRuns, groundTruth) {
  return groundTruth.map((gt) => {
    let detectionCorrect = 0
    let categoryCorrect = 0
    let scoreInRange = 0
    let scamCorrect = 0

    for (const run of allRuns) {
      const result = run.find((r) => r.thread_id === gt.id)
      if (!result) continue

      // Detection
      if (result.is_deal === gt.expected.is_deal) detectionCorrect++

      // Category (only for deals)
      if (gt.expected.is_deal && result.is_deal && gt.expected.category) {
        if (result.category === gt.expected.category) categoryCorrect++
      }

      // Score range
      if (gt.expected.score_range) {
        const [lo, hi] = gt.expected.score_range
        if (result.ai_score >= lo && result.ai_score <= hi) scoreInRange++
      }

      // Scam
      if (result.likely_scam === gt.expected.likely_scam) scamCorrect++
    }

    return {
      id: gt.id,
      description: gt.description,
      expected: gt.expected,
      detection_correct: detectionCorrect,
      category_correct: categoryCorrect,
      score_in_range: scoreInRange,
      scam_correct: scamCorrect,
      total_runs: allRuns.length,
    }
  })
}

/**
 * Compute JSON health metrics across runs.
 * Each run tracks: clean_parse, repaired, corrective_retry, failed
 */
export function computeJsonHealth(jsonHealthPerRun) {
  const cleanRates = []
  const repairedRates = []
  const correctiveRates = []
  let totalFailures = 0

  for (const run of jsonHealthPerRun) {
    cleanRates.push(run.clean ? 1 : 0)
    repairedRates.push(run.repaired ? 1 : 0)
    correctiveRates.push(run.corrective_retry ? 1 : 0)
    if (run.failed) totalFailures++
  }

  return {
    clean_parse_rate: aggregateStats(cleanRates),
    repaired_rate: aggregateStats(repairedRates),
    corrective_retry_rate: aggregateStats(correctiveRates),
    total_failures: totalFailures,
  }
}
