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

jest.unstable_mockModule('../src/commands/dispatch.js', () => ({
  runDispatch: jest.fn().mockResolvedValue({ dispatched_filter_count: 1, dispatched_classify_count: 0 }),
}))

jest.unstable_mockModule('../src/commands/dispatch-deal-state-sync.js', () => ({
  runDispatchDealStateSync: jest.fn().mockResolvedValue({ workers_triggered: 2, total_emails: 1000 }),
}))

jest.unstable_mockModule('../src/commands/sync-deal-states.js', () => ({
  runSyncDealStates: jest.fn().mockResolvedValue({ synced_count: 10, conflict_count: 0 }),
}))

jest.unstable_mockModule('../src/commands/fetch-and-filter.js', () => ({
  runFetchAndFilter: jest.fn().mockResolvedValue({ filtered_ids: "'id-1'", rejected_ids: '', total: 1 }),
}))

jest.unstable_mockModule('../src/commands/fetch-and-classify.js', () => ({
  runFetchAndClassify: jest.fn().mockRejectedValue(new Error('fetch-and-classify not mocked')),
}))

jest.unstable_mockModule('../src/commands/retrigger-stuck.js', () => ({
  runRetriggerStuck: jest.fn().mockResolvedValue({ retriggered: 0 }),
}))

jest.unstable_mockModule('../src/commands/save-deals.js', () => ({
  runSaveDeals: jest.fn().mockResolvedValue({ deals_created: 0 }),
}))

jest.unstable_mockModule('../src/commands/save-evals.js', () => ({
  runSaveEvals: jest.fn().mockResolvedValue({ upserted: 0, total: 0 }),
}))

jest.unstable_mockModule('../src/commands/sxt-execute.js', () => ({
  runSxtQuery: jest.fn().mockResolvedValue({ result: [] }),
}))

jest.unstable_mockModule('../src/commands/update-deal-states.js', () => ({
  runUpdateDealStates: jest.fn().mockResolvedValue({ deal: 0, not_deal: 0 }),
}))

const core = await import('@actions/core')
const { run } = await import('../src/main.js')

describe('dealsync main (command router)', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
  })

  it('routes to dispatch command and sets result', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'dispatch' : ''))

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({ dispatched_filter_count: 1, dispatched_classify_count: 0 })
  })

  it('routes to fetch-and-filter command and sets result', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'fetch-and-filter' : ''))

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({ filtered_ids: "'id-1'", rejected_ids: '', total: 1 })
  })

  it('fails on unknown command', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'bogus' : ''))

    await run()

    expect(core.setFailed).toHaveBeenCalledWith(expect.stringContaining('Unknown command: "bogus"'))
  })

  it('sets success=false when command throws', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'fetch-and-classify' : ''))

    await run()

    expect(outputs['success']).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith('fetch-and-classify not mocked')
  })
})
