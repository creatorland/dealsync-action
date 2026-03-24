/**
 * Shared email utility functions.
 */

export function getHeader(email, name) {
  const header = email.topLevelHeaders?.find((h) => h.name.toLowerCase() === name.toLowerCase())
  return header?.value || ''
}
