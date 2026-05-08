/**
 * Shared parsing helpers for GitHub Action inputs.
 */

/**
 * @param {string} raw
 * @param {string} inputName
 * @returns {number}
 */
export function parsePositiveIntegerInput(raw, inputName) {
  const normalized = String(raw ?? '').trim()
  if (!/^[1-9][0-9]*$/.test(normalized)) {
    throw new Error(`${inputName} must be a positive integer`)
  }
  return Number(normalized)
}

/**
 * Parse a boolean-shaped action input with explicit whitelist semantics.
 * Empty / unset → caller's default. Recognized truthy: 'true' / '1' / 'yes'.
 * Recognized falsy: 'false' / '0' / 'no'. Anything else throws — protects
 * against typos like 'treu' silently falling back to the default and (for
 * destructive operations like `backfill-dry-run`) running live when the
 * operator intended dry-run.
 *
 * @param {string} raw
 * @param {string} inputName
 * @param {boolean} defaultValue
 * @returns {boolean}
 */
export function parseStrictBoolean(raw, inputName, defaultValue) {
  const normalized = String(raw ?? '')
    .trim()
    .toLowerCase()
  if (normalized === '') return defaultValue
  if (['true', '1', 'yes'].includes(normalized)) return true
  if (['false', '0', 'no'].includes(normalized)) return false
  throw new Error(
    `${inputName} must be one of: true, false, 1, 0, yes, no (case-insensitive); got "${raw}"`,
  )
}
