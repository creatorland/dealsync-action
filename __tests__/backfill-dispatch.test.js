import { jest } from '@jest/globals'
import { postBackfillIngestionTrigger } from '../src/lib/backfill-dispatch.js'

describe('postBackfillIngestionTrigger', () => {
  let origFetch

  beforeEach(() => {
    origFetch = global.fetch
  })
  afterEach(() => {
    global.fetch = origFetch
  })

  const baseOpts = {
    backendBaseUrl: 'https://backend.example',
    sharedSecret: 's3cret',
    userId: 'user-1',
    attributionTag: 'brand-contacts-backfill',
    dryRun: false,
  }

  it('POSTs the backfill body with origin + attributionTag + dryRun and NO lookbackDaysOverride', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      status: 201,
      ok: true,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })
    global.fetch = fetchMock

    const res = await postBackfillIngestionTrigger(baseOpts)

    expect(res).toEqual({ ok: true, status: 201, alreadyInProgress: false })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('https://backend.example/api/v1/ingestion/trigger')
    expect(init.method).toBe('POST')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(init.headers['x-shared-secret']).toBe('s3cret')
    const body = JSON.parse(init.body)
    expect(body).toEqual({
      userId: 'user-1',
      syncStrategy: 'LOOKBACK',
      origin: 'backfill',
      attributionTag: 'brand-contacts-backfill',
      dryRun: false,
    })
    expect(body).not.toHaveProperty('lookbackDaysOverride')
    expect(body).not.toHaveProperty('originatingSyncStateId')
  })

  it('returns ok:true + status:200 on 200 response', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })

    const res = await postBackfillIngestionTrigger(baseOpts)
    expect(res).toEqual({ ok: true, status: 200, alreadyInProgress: false })
  })

  it('treats 409 as alreadyInProgress (not a failure)', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 409,
      ok: false,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })

    const res = await postBackfillIngestionTrigger(baseOpts)
    expect(res).toEqual({ ok: true, status: 409, alreadyInProgress: true })
  })

  it('returns ok:false on 4xx with body text', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 400,
      ok: false,
      text: jest.fn().mockResolvedValue('bad request body'),
    })

    const res = await postBackfillIngestionTrigger(baseOpts)
    expect(res).toEqual({
      ok: false,
      status: 400,
      alreadyInProgress: false,
      text: 'bad request body',
    })
  })

  it('returns ok:false on 5xx with body text', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 503,
      ok: false,
      text: jest.fn().mockResolvedValue('service unavailable'),
    })

    const res = await postBackfillIngestionTrigger(baseOpts)
    expect(res).toEqual({
      ok: false,
      status: 503,
      alreadyInProgress: false,
      text: 'service unavailable',
    })
  })

  it('trims trailing slashes from backend base URL', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })
    global.fetch = fetchMock

    await postBackfillIngestionTrigger({
      ...baseOpts,
      backendBaseUrl: 'https://backend.example///',
    })

    const [url] = fetchMock.mock.calls[0]
    expect(url).toBe('https://backend.example/api/v1/ingestion/trigger')
  })

  it('forwards extra headers (e.g. x-correlation-id)', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })
    global.fetch = fetchMock

    await postBackfillIngestionTrigger({
      ...baseOpts,
      extraHeaders: { 'x-correlation-id': 'cid-abc' },
    })

    const [, init] = fetchMock.mock.calls[0]
    expect(init.headers['x-correlation-id']).toBe('cid-abc')
  })

  it('sends dryRun:true when requested', async () => {
    const fetchMock = jest.fn().mockResolvedValue({
      status: 201,
      ok: true,
      body: { cancel: jest.fn().mockResolvedValue(undefined) },
    })
    global.fetch = fetchMock

    await postBackfillIngestionTrigger({ ...baseOpts, dryRun: true })

    const body = JSON.parse(fetchMock.mock.calls[0][1].body)
    expect(body.dryRun).toBe(true)
  })
})
