/**
 * Phase 0 spike runtime helpers — shared by the three outbox-settlement-*
 * actions and the local-runner orchestrator.
 *
 * Replaces @actions/core for spike scope. Story 0.6 swaps these for the
 * @actions/core equivalents when wrapping the actions for W3 dispatch.
 */

/**
 * Emit a structured event to stdout in the convention Cloud Logging filters
 * pick up via `jsonPayload.message.event` (matches backend's auth-events
 * shape per creatorland/backend's CLAUDE.md).
 */
export function emitEvent(event, fields = {}) {
  const payload = { message: { event, ...fields, ts: new Date().toISOString() } }
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(payload))
}

export function requireEnv(name) {
  const v = process.env[name]
  if (!v) {
    throw new Error(`spike-runtime: required env var ${name} is not set`)
  }
  return v
}

/**
 * Call a Supabase PostgREST RPC. Service-role key bypasses RLS (mandatory
 * for the worker RPCs which are GRANTed only to service_role).
 *
 * Returns the parsed JSON response — array for SETOF functions, scalar
 * for boolean/bigint RETURNS.
 */
export async function supabaseRpc(supabaseUrl, serviceRoleKey, rpcName, params) {
  const url = `${supabaseUrl.replace(/\/$/, '')}/rest/v1/rpc/${rpcName}`
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: serviceRoleKey,
      Authorization: `Bearer ${serviceRoleKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(params),
  })
  const text = await resp.text()
  if (!resp.ok) {
    throw new Error(`supabaseRpc ${rpcName} ${resp.status}: ${text}`)
  }
  try {
    return text ? JSON.parse(text) : null
  } catch {
    return text
  }
}
