/**
 * Shared SxT helpers for classify, dispatch, and sxt-query commands.
 * Auth via proxy, static biscuit from input.
 *
 * All fetch calls have a 2-minute timeout by default.
 */

const DEFAULT_TIMEOUT_MS = 120000 // 2 minutes

function withTimeout(ms = DEFAULT_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), ms)
  return { signal: controller.signal, clear: () => clearTimeout(timeout) }
}

export { withTimeout }

export async function authenticate(authUrl, authSecret) {
  const { signal, clear } = withTimeout()
  try {
    const resp = await fetch(authUrl, {
      method: 'GET',
      headers: { 'x-shared-secret': authSecret },
      signal,
    })
    if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
    const data = await resp.json()
    return data.data || data.accessToken || data
  } finally {
    clear()
  }
}

export async function executeSql(apiUrl, jwt, biscuit, sql) {
  const { signal, clear } = withTimeout()
  try {
    const resp = await fetch(`${apiUrl}/v1/sql`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${jwt}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
      signal,
    })
    if (!resp.ok) throw new Error(`SxT ${resp.status}: ${await resp.text()}`)
    return resp.json()
  } finally {
    clear()
  }
}
