/**
 * Shared SxT client for skill scripts.
 *
 * Auth strategy: proxy JWT first, direct login fallback.
 *
 * Required env vars:
 *   SXT_API_URL          — e.g., https://api.makeinfinite.dev
 *   SXT_PRIVATE_KEY      — Table-level ED25519 private key (hex)
 *
 * Auth env vars (at least one set required):
 *   Proxy:  SXT_AUTH_URL + SXT_AUTH_SECRET
 *   Login:  SXT_USER_ID + SXT_PASSWORD
 *
 * Optional:
 *   SXT_SCHEMA_PRIVATE_KEY — Schema-level private key (for DDL on schema-owned tables)
 *   SXT_PUBLIC_KEY         — Override derived public key for CREATE TABLE WITH clause
 */

import { SpaceAndTime } from 'sxt-nodejs-sdk'

const API_URL = process.env.SXT_API_URL || 'https://api.makeinfinite.dev'
const PROXY_URL = process.env.SXT_PROXY_URL || 'https://proxy.api.makeinfinite.dev'

let _sxt = null
function getSxt() {
  if (!_sxt) _sxt = new SpaceAndTime()
  return _sxt
}

/**
 * Authenticate — try proxy first, fall back to direct login.
 * @returns {{ jwt: string, sessionId?: string }}
 */
export async function authenticate() {
  const authUrl = process.env.SXT_AUTH_URL
  const authSecret = process.env.SXT_AUTH_SECRET

  // Strategy 1: Proxy JWT (preferred — cached, no rate limit burn)
  if (authUrl && authSecret) {
    try {
      const resp = await fetch(authUrl, {
        method: 'GET',
        headers: { 'x-shared-secret': authSecret },
      })
      if (resp.ok) {
        const data = await resp.json()
        const jwt = data.data || data.accessToken || data
        if (jwt) return { jwt }
      }
    } catch {
      // Fall through to login
    }
  }

  // Strategy 2: Direct login (fallback)
  const userId = process.env.SXT_USER_ID
  const password = process.env.SXT_PASSWORD
  if (!userId || !password) {
    throw new Error(
      'Auth failed: set SXT_AUTH_URL+SXT_AUTH_SECRET (proxy) or SXT_USER_ID+SXT_PASSWORD (login)',
    )
  }

  const resp = await fetch(`${PROXY_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId, password }),
  })
  if (!resp.ok) throw new Error(`Login failed: ${resp.status} ${await resp.text()}`)
  const data = await resp.json()
  return { jwt: data.accessToken, sessionId: data.sessionId }
}

/**
 * Generate a biscuit for a given operation + resource.
 * @param {string} operation — e.g., 'dql_select', 'ddl_create'
 * @param {string} resource — e.g., 'dealsync_stg_v1.deal_states'
 * @param {string} [privateKey] — override, defaults to SXT_PRIVATE_KEY
 */
export function generateBiscuit(operation, resource, privateKey) {
  const key = privateKey || process.env.SXT_PRIVATE_KEY
  if (!key) throw new Error('SXT_PRIVATE_KEY is required for biscuit generation')

  const auth = getSxt().Authorization()
  return auth.CreateBiscuitToken([{ operation, resource }], key).data[0]
}

/**
 * Generate a master biscuit with all operations for a resource.
 */
export function generateMasterBiscuit(resource, privateKey) {
  const key = privateKey || process.env.SXT_PRIVATE_KEY
  const auth = getSxt().Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource },
      { operation: 'dml_insert', resource },
      { operation: 'dml_update', resource },
      { operation: 'dml_delete', resource },
      { operation: 'dml_merge', resource },
      { operation: 'ddl_create', resource },
      { operation: 'ddl_drop', resource },
    ],
    key,
  ).data[0]
}

/**
 * Execute SQL against SxT.
 * @param {string} jwt
 * @param {string} sql
 * @param {string} biscuit
 * @returns {Promise<any>}
 */
export async function executeSql(jwt, sql, biscuit) {
  const resp = await fetch(`${API_URL}/v1/sql`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${jwt}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ sqlText: sql, biscuits: [biscuit] }),
  })

  const body = await resp.text()
  if (!resp.ok) throw new Error(`SxT ${resp.status}: ${body}`)
  try {
    return body ? JSON.parse(body) : null
  } catch {
    return null
  }
}

/**
 * Derive ED25519 public key from private key (hex).
 */
export async function derivePublicKey(privateKeyHex) {
  const { getPublicKeyAsync } = await import('@noble/ed25519')
  const pubBytes = await getPublicKeyAsync(Buffer.from(privateKeyHex, 'hex'))
  return Buffer.from(pubBytes).toString('hex')
}

/**
 * Upload a biscuit to the SxT dashboard.
 * Requires login auth (needs sessionId).
 */
export async function uploadBiscuit(sessionId, name, biscuit, publicKey) {
  const resp = await fetch(`${PROXY_URL}/biscuits/generated`, {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'Content-Type': 'application/json',
      sid: sessionId,
    },
    body: JSON.stringify({ name, biscuit, publicKey }),
  })
  if (!resp.ok && resp.status !== 204) {
    throw new Error(`Upload biscuit failed: ${resp.status} ${await resp.text()}`)
  }
}
