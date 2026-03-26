import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const core = await import('@actions/core')
const { runEvalCompare } = await import('../src/commands/eval-compare.js')

function makeResult(overrides = {}) {
  return {
    model: 'test-model',
    runs: 10,
    successful_runs: 10,
    detection: {
      recall: { mean: 0.96, min: 0.93, max: 0.97, stddev: 0.01 },
      precision: { mean: 0.45, min: 0.42, max: 0.48, stddev: 0.02 },
      f2: { mean: 0.82, min: 0.79, max: 0.84, stddev: 0.01 },
    },
    categorization: { accuracy: { mean: 0.88, min: 0.85, max: 0.9, stddev: 0.02 } },
    urgency_scoring: { in_range_rate: { mean: 0.8, min: 0.75, max: 0.85, stddev: 0.03 } },
    scam_detection: {
      accuracy: { mean: 1.0, min: 1.0, max: 1.0, stddev: 0 },
      total_scam_threads: 5,
    },
    json_health: {
      clean_parse_rate: { mean: 0.8, min: 0.7, max: 0.9, stddev: 0.05 },
      corrective_retry_rate: { mean: 0.1, min: 0.0, max: 0.2, stddev: 0.05 },
      total_failures: 0,
    },
    cost: { total_input_tokens: 100000, total_output_tokens: 5000, avg_cost_per_thread: 0.03 },
    per_thread: [
      {
        id: 'gt-001',
        expected: { is_deal: true },
        detection_correct: 10,
        category_correct: 9,
        scam_correct: 10,
        total_runs: 10,
      },
      {
        id: 'gt-002',
        expected: { is_deal: false },
        detection_correct: 10,
        category_correct: 10,
        scam_correct: 10,
        total_runs: 10,
      },
    ],
    ...overrides,
  }
}

describe('runEvalCompare', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('identical results = PASS with all ties', async () => {
    const a = makeResult()
    const b = makeResult()
    core.getInput.mockImplementation((name) => {
      if (name === 'result-a') return JSON.stringify(a)
      if (name === 'result-b') return JSON.stringify(b)
      return ''
    })

    const result = await runEvalCompare()
    expect(result.pass_fail.verdict).toBe('PASS')
    expect(result.comparison.recall.winner).toBe('tie')
    expect(result.regressions.new_missed_deals).toEqual([])
  })

  it('B has lower recall below threshold = FAIL', async () => {
    const a = makeResult()
    const b = makeResult({
      detection: {
        recall: { mean: 0.9, min: 0.85, max: 0.93, stddev: 0.03 },
        precision: { mean: 0.5, min: 0.45, max: 0.55, stddev: 0.02 },
        f2: { mean: 0.78, min: 0.74, max: 0.82, stddev: 0.02 },
      },
    })
    core.getInput.mockImplementation((name) => {
      if (name === 'result-a') return JSON.stringify(a)
      if (name === 'result-b') return JSON.stringify(b)
      return ''
    })

    const result = await runEvalCompare()
    expect(result.pass_fail.verdict).toBe('FAIL')
    expect(result.pass_fail.b_recall_above_95).toBe(false)
    expect(result.comparison.recall.winner).toBe('a')
    expect(result.comparison.recall.delta).toBeCloseTo(-0.06, 2)
  })

  it('B misses a deal A caught = new_missed_deals populated', async () => {
    const a = makeResult()
    const b = makeResult({
      per_thread: [
        {
          id: 'gt-001',
          expected: { is_deal: true },
          detection_correct: 8,
          category_correct: 7,
          scam_correct: 10,
          total_runs: 10,
        },
        {
          id: 'gt-002',
          expected: { is_deal: false },
          detection_correct: 10,
          category_correct: 10,
          scam_correct: 10,
          total_runs: 10,
        },
      ],
    })
    core.getInput.mockImplementation((name) => {
      if (name === 'result-a') return JSON.stringify(a)
      if (name === 'result-b') return JSON.stringify(b)
      return ''
    })

    const result = await runEvalCompare()
    expect(result.regressions.new_missed_deals).toEqual(['gt-001'])
    expect(result.pass_fail.no_new_missed_deals).toBe(false)
    expect(result.pass_fail.verdict).toBe('FAIL')
  })

  it('B beats A on all metrics = PASS', async () => {
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
    expect(result.pass_fail.verdict).toBe('PASS')
    expect(result.comparison.recall.winner).toBe('b')
    expect(result.comparison.f2.winner).toBe('b')
  })

  it('throws when inputs missing', async () => {
    core.getInput.mockReturnValue('')
    await expect(runEvalCompare()).rejects.toThrow('result-a and result-b are required')
  })
})
