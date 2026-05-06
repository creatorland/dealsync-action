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
