/**
 * Shared SxT helpers for classify, dispatch, and sxt-query commands.
 * Auth via proxy. Biscuits generated per-run via SxT API (no WASM, no static biscuit).
 */

export async function authenticate(authUrl, authSecret) {
  const resp = await fetch(authUrl, {
    method: 'GET',
    headers: { 'x-shared-secret': authSecret },
  })
  if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
  const data = await resp.json()
  return data.data || data.accessToken || data
}

export async function generateBiscuit(apiUrl, jwt, schema) {
  const resp = await fetch(`${apiUrl}/v1/biscuits/generated/${schema.toLowerCase()}`, {
    headers: { Authorization: `Bearer ${jwt}` },
  })
  if (!resp.ok) throw new Error(`Biscuit generation failed: ${resp.status}: ${await resp.text()}`)
  const data = await resp.json()
  // Response is [{BISCUIT: "..."}] or {BISCUIT: "..."}
  if (Array.isArray(data)) return data[0]?.BISCUIT
  return data.BISCUIT || data
}

export async function executeSql(apiUrl, jwt, biscuit, sql) {
  const resp = await fetch(`${apiUrl}/v1/sql`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${jwt}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
  })
  if (!resp.ok) throw new Error(`SxT ${resp.status}: ${await resp.text()}`)
  return resp.json()
}
