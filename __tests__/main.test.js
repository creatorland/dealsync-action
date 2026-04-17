import { jest } from '@jest/globals'

const outputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

jest.unstable_mockModule('../src/commands/sync-deal-states.js', () => ({
  runSyncDealStates: jest.fn().mockResolvedValue({ synced_count: 10, conflict_count: 0 }),
}))

jest.unstable_mockModule('../src/commands/eval.js', () => ({
  runEval: jest.fn().mockResolvedValue({ detection: {}, runs: 1 }),
}))

jest.unstable_mockModule('../src/commands/eval-compare.js', () => ({
  runEvalCompare: jest.fn().mockResolvedValue({ verdict: 'PASS' }),
}))

jest.unstable_mockModule('../src/commands/run-filter-pipeline.js', () => ({
  runFilterPipeline: jest.fn().mockResolvedValue({
    batches_processed: 0,
    batches_failed: 0,
    total_filtered: 0,
    total_rejected: 0,
  }),
}))

jest.unstable_mockModule('../src/commands/run-classify-pipeline.js', () => ({
  runClassifyPipeline: jest.fn().mockRejectedValue(new Error('classify-pipeline not mocked')),
}))

jest.unstable_mockModule('../src/commands/emit-scan-complete-webhooks.js', () => ({
  runEmitScanCompleteWebhooks: jest
    .fn()
    .mockResolvedValue({ scanned: 0, skippedDeduped: 0, posted: 0, errors: 0 }),
}))

const core = await import('@actions/core')
const { run } = await import('../src/main.js')

describe('dealsync main (command router)', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
  })

  it('routes to run-filter-pipeline command and sets result', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'run-filter-pipeline' : ''))

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({
      batches_processed: 0,
      batches_failed: 0,
      total_filtered: 0,
      total_rejected: 0,
    })
  })

  it('routes emit-scan-complete-webhooks', async () => {
    core.getInput.mockImplementation((name) =>
      name === 'command' ? 'emit-scan-complete-webhooks' : '',
    )

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({
      scanned: 0,
      skippedDeduped: 0,
      posted: 0,
      errors: 0,
    })
  })

  it('fails on unknown command', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'bogus' : ''))

    await run()

    expect(core.setFailed).toHaveBeenCalledWith(expect.stringContaining('Unknown command: "bogus"'))
  })

  it('sets success=false when command throws', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'run-classify-pipeline' : ''))

    await expect(run()).rejects.toThrow('classify-pipeline not mocked')

    expect(outputs['success']).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith('classify-pipeline not mocked')
  })
})
