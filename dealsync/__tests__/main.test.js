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

const core = await import('@actions/core')

// Mock filter to return a known result
jest.unstable_mockModule('../src/filter.js', () => ({
  runFilter: jest.fn().mockResolvedValue({ filtered_ids: "'id-1'", rejected_ids: '' }),
}))

jest.unstable_mockModule('../src/build-prompt.js', () => ({
  runBuildPrompt: jest.fn().mockRejectedValue(new Error('build-prompt not implemented yet')),
}))

jest.unstable_mockModule('../src/classify.js', () => ({
  runClassify: jest.fn().mockRejectedValue(new Error('classify not implemented yet')),
}))

jest.unstable_mockModule('../src/dispatch.js', () => ({
  runDispatch: jest.fn().mockRejectedValue(new Error('dispatch not implemented yet')),
}))

const { run } = await import('../src/main.js')

describe('dealsync main (command router)', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
  })

  it('routes to filter command and sets result', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'filter' : ''))

    await run()

    expect(outputs['success']).toBe('true')
    expect(JSON.parse(outputs['result'])).toEqual({ filtered_ids: "'id-1'", rejected_ids: '' })
  })

  it('fails on unknown command', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'bogus' : ''))

    await run()

    expect(core.setFailed).toHaveBeenCalledWith(expect.stringContaining('Unknown command: "bogus"'))
  })

  it('sets success=false when command throws', async () => {
    core.getInput.mockImplementation((name) => (name === 'command' ? 'build-prompt' : ''))

    await run()

    expect(outputs['success']).toBe('false')
    expect(core.setFailed).toHaveBeenCalledWith('build-prompt not implemented yet')
  })
})
