import { jest } from '@jest/globals'

const {
  aggregateStats,
  computeDetectionMetrics,
  computeCategoryAccuracy,
  computeScoreInRange,
  computeScamDetection,
  computePerThread,
  computeJsonHealth,
} = await import('../src/lib/metrics.js')

describe('aggregateStats', () => {
  it('computes mean, min, max, stddev', () => {
    const result = aggregateStats([0.9, 0.95, 1.0])
    expect(result.mean).toBeCloseTo(0.95, 2)
    expect(result.min).toBe(0.9)
    expect(result.max).toBe(1.0)
    expect(result.stddev).toBeGreaterThan(0)
  })

  it('handles empty array', () => {
    expect(aggregateStats([])).toEqual({ mean: 0, min: 0, max: 0, stddev: 0 })
  })

  it('handles single value', () => {
    const result = aggregateStats([0.95])
    expect(result.mean).toBe(0.95)
    expect(result.stddev).toBe(0)
  })
})

const GT_DEALS = [
  {
    id: 'gt-001',
    expected: { is_deal: true, category: 'new', likely_scam: false, score_range: [5, 7] },
  },
  {
    id: 'gt-002',
    expected: { is_deal: true, category: 'in_progress', likely_scam: false, score_range: [7, 9] },
  },
  {
    id: 'gt-003',
    expected: { is_deal: true, category: 'likely_scam', likely_scam: true, score_range: [1, 2] },
  },
]
const GT_NON_DEALS = [
  {
    id: 'gt-004',
    expected: { is_deal: false, category: null, likely_scam: false, score_range: [1, 2] },
  },
  {
    id: 'gt-005',
    expected: { is_deal: false, category: null, likely_scam: false, score_range: [1, 2] },
  },
]
const GT = [...GT_DEALS, ...GT_NON_DEALS]

function makeRun(overrides = {}) {
  return GT.map((gt) => ({
    thread_id: gt.id,
    is_deal: gt.expected.is_deal,
    category: gt.expected.category,
    likely_scam: gt.expected.likely_scam,
    ai_score: gt.expected.score_range ? gt.expected.score_range[0] : 5,
    ...overrides[gt.id],
  }))
}

describe('computeDetectionMetrics', () => {
  it('perfect detection = 100% recall and precision', () => {
    const runs = [makeRun(), makeRun()]
    const result = computeDetectionMetrics(runs, GT)
    expect(result.recall.mean).toBe(1)
    expect(result.precision.mean).toBe(1)
    expect(result.f2.mean).toBe(1)
  })

  it('missed deal lowers recall', () => {
    const runs = [makeRun({ 'gt-001': { is_deal: false } })]
    const result = computeDetectionMetrics(runs, GT)
    expect(result.recall.mean).toBeCloseTo(0.6667, 3) // 2/3 deals caught
    expect(result.precision.mean).toBe(1) // no false positives
  })

  it('false positive lowers precision', () => {
    const runs = [makeRun({ 'gt-004': { is_deal: true } })]
    const result = computeDetectionMetrics(runs, GT)
    expect(result.recall.mean).toBe(1)
    expect(result.precision.mean).toBeCloseTo(0.75, 2) // 3/(3+1)
  })
})

describe('computeCategoryAccuracy', () => {
  it('perfect categories = 100%', () => {
    const runs = [makeRun()]
    const result = computeCategoryAccuracy(runs, GT)
    expect(result.accuracy.mean).toBe(1)
  })

  it('wrong category lowers accuracy', () => {
    const runs = [makeRun({ 'gt-001': { category: 'completed' } })]
    const result = computeCategoryAccuracy(runs, GT)
    expect(result.accuracy.mean).toBeCloseTo(0.6667, 3) // 2/3
  })
})

describe('computeScoreInRange', () => {
  it('all scores in range = 100%', () => {
    const runs = [makeRun()]
    const result = computeScoreInRange(runs, GT)
    expect(result.in_range_rate.mean).toBe(1)
  })

  it('out of range score lowers rate', () => {
    const runs = [makeRun({ 'gt-001': { ai_score: 10 } })] // expected 5-7
    const result = computeScoreInRange(runs, GT)
    expect(result.in_range_rate.mean).toBeLessThan(1)
  })
})

describe('computeScamDetection', () => {
  it('catches all scams = 100%', () => {
    const runs = [makeRun()]
    const result = computeScamDetection(runs, GT)
    expect(result.accuracy.mean).toBe(1)
    expect(result.total_scam_threads).toBe(1)
  })

  it('missed scam lowers accuracy', () => {
    const runs = [makeRun({ 'gt-003': { likely_scam: false } })]
    const result = computeScamDetection(runs, GT)
    expect(result.accuracy.mean).toBe(0)
  })
})

describe('computePerThread', () => {
  it('returns per-thread breakdown', () => {
    const runs = [makeRun(), makeRun({ 'gt-001': { is_deal: false } })]
    const result = computePerThread(runs, GT)
    expect(result).toHaveLength(5)

    const gt001 = result.find((t) => t.id === 'gt-001')
    expect(gt001.detection_correct).toBe(1) // correct in 1 of 2 runs
    expect(gt001.total_runs).toBe(2)
  })
})

describe('computeJsonHealth', () => {
  it('all clean parses', () => {
    const runs = [
      { clean: true, repaired: false, corrective_retry: false, failed: false },
      { clean: true, repaired: false, corrective_retry: false, failed: false },
    ]
    const result = computeJsonHealth(runs)
    expect(result.clean_parse_rate.mean).toBe(1)
    expect(result.total_failures).toBe(0)
  })

  it('tracks failures and repairs', () => {
    const runs = [
      { clean: true, repaired: false, corrective_retry: false, failed: false },
      { clean: false, repaired: false, corrective_retry: true, failed: false },
      { clean: false, repaired: false, corrective_retry: false, failed: true },
    ]
    const result = computeJsonHealth(runs)
    expect(result.clean_parse_rate.mean).toBeCloseTo(0.3333, 3)
    expect(result.corrective_retry_rate.mean).toBeCloseTo(0.3333, 3)
    expect(result.total_failures).toBe(1)
  })
})
