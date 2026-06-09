// ADR-008 identity derivation — headless Supabase UUID from Firestore uid.
// Source: dealsync-v2/_bmad-output/planning-artifacts/Architecture-Dealsync-2-Data-Architecture.md
import { v5 as uuidv5 } from 'uuid'

// Canonical namespace for UUIDv5 tenant-key derivation per ADR-008.
// Hard-coded constant — never env-sourced; pinned by Story 2.11 Task 1.
export const DEALSYNC_IDENTITY_NAMESPACE = '5ced37e1-0ede-40a0-98aa-ae066dac4ce1'

/**
 * Derive the Supabase tenant key (deals.user_id / contacts.user_id) from a
 * Firestore uid. Runs headless with no session and no mapping-table lookup.
 *
 * Arg order: name (uid) first — Node uuid is v5(name, namespace), the reverse
 * of Postgres uuid_generate_v5(namespace, name). Trim is applied before hashing
 * so a padded uid derives the same UUID as its trimmed form.
 *
 * @param {string} firestoreUid — the Firestore user ID (USER_ID on the SxT batch)
 * @returns {string} lowercase UUID string matching the derived sub in the Supabase JWT
 */
export function deriveSupabaseUserId(firestoreUid) {
  if (typeof firestoreUid !== 'string') {
    throw new TypeError(
      `deriveSupabaseUserId: firestore_uid must be a string, got ${typeof firestoreUid}`,
    )
  }
  const uid = firestoreUid.trim()
  if (!uid) throw new Error('deriveSupabaseUserId: firestore_uid must be non-blank')
  return uuidv5(uid, DEALSYNC_IDENTITY_NAMESPACE)
}
