/**
 * The naming rules of the sync engine: what an entity may be called, which
 * fields may form a row key, how their values render into one, and what the
 * resulting key may contain.
 *
 * The rules are a wire contract, not an implementation detail. Entity names and
 * row keys travel in CrudMutation, address rows in server storage, and are
 * pasted into this client's KV key layout (`c/<entity>/<ws>/<key>` and friends,
 * see keys.ts) and its cross-tab change tokens. Both implementations therefore
 * have to agree character for character: the Go mirror is
 * engine/internal/naming, and docs/reference/naming.md states the rules once for
 * both. A change here is a change to all three.
 *
 * Validation runs where a mistake is still cheap to explain — registration for
 * names and key fields, mutation-build time for row keys — because the failure
 * modes it prevents are otherwise silent. An unset key field derives the empty
 * key, whose KV key is the entity's own prefix, so the row round-trips locally
 * and is only refused once it reaches the server; a slash in an entity name
 * forges an extra key segment, so entity `a` row `b/x` and entity `a/b` row `x`
 * land on one KV key; a control character collides with the separator in a
 * change token.
 */

import { type DescField, ScalarType } from '@bufbuild/protobuf';

/**
 * Caps an entity name. Names are hand-written schema identifiers, so the cap
 * only bounds what ends up repeated in every key.
 */
export const MAX_ENTITY_NAME_LENGTH = 64;

/**
 * Caps a row key in UTF-8 bytes, escaping included. Keys are stored, scanned
 * and prefix-matched on both sides; an unbounded key is an unbounded index
 * entry. It is deliberately a constant rather than a server limit: the client
 * derives keys locally and must be able to reject one without asking, which
 * only works while both sides hold the same number.
 */
export const MAX_ROW_KEY_BYTES = 512;

/**
 * An entity's wire name — (syncplane.v1.entity).name, and CrudMutation.entity —
 * must be a non-empty identifier: an ASCII letter or underscore followed by
 * letters, digits or underscores.
 *
 * The shape is narrow on purpose. An entity name is a segment in every KV key
 * (so no `/`), a token in the cross-tab change channel (so no separator
 * characters), a property name on the db facade (`db.todo`), and — for schemas
 * declared at runtime — is lowered to a proto message name by the schema
 * builder. An identifier is the intersection of all four.
 */
const ENTITY_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Throws unless `name` satisfies the entity-name rule. */
export function validateEntityName(name: string, where?: string): void {
	const prefix = where === undefined ? '' : `${where}: `;
	if (name === '') throw new Error(`${prefix}entity name is empty`);
	if (name.length > MAX_ENTITY_NAME_LENGTH) {
		throw new Error(
			`${prefix}entity name "${name}" is ${name.length} characters, the limit is ${MAX_ENTITY_NAME_LENGTH}`,
		);
	}
	if (!ENTITY_NAME.test(name)) {
		throw new Error(
			`${prefix}entity name "${name}" is not an identifier: use letters, digits and underscores, `
				+ 'starting with a letter or underscore',
		);
	}
}

/**
 * The scalar types a key field may have. They are the ones whose values
 * `scalarKeyString` renders identically here and in Go.
 *
 * Everything else is refused rather than rendered on a best effort. A message,
 * list or map has no canonical text form at all. Bytes and floats have one per
 * language: the server stringifies bytes through protoreflect while this side
 * holds a Uint8Array, and Go's `%v` and JS's `String()` disagree on float
 * exponents (1e7 renders "1e+07" against "10000000"). A key that means one row
 * on the server and another on the client is worse than a schema that will not
 * register.
 */
const KEY_SCALARS: ReadonlySet<ScalarType> = new Set([
	ScalarType.STRING,
	ScalarType.BOOL,
	ScalarType.INT32,
	ScalarType.INT64,
	ScalarType.UINT32,
	ScalarType.UINT64,
	ScalarType.SINT32,
	ScalarType.SINT64,
	ScalarType.FIXED32,
	ScalarType.FIXED64,
	ScalarType.SFIXED32,
	ScalarType.SFIXED64,
]);

/** Throws unless `field` may form (part of) a row key — see KEY_SCALARS. */
export function validateKeyField(entity: string, field: DescField): void {
	if (field.fieldKind === 'enum') return;
	if (field.fieldKind === 'scalar' && KEY_SCALARS.has(field.scalar)) return;
	const kind = field.fieldKind === 'scalar' ? ScalarType[field.scalar] : field.fieldKind;
	throw new Error(
		`entity "${entity}": key field "${field.name}" must be a string, integer, bool or enum, is ${kind}`,
	);
}

/**
 * Throws unless `key` satisfies the row-key rule: non-empty, encodable as
 * UTF-8, free of ASCII control characters, and at most MAX_ROW_KEY_BYTES long.
 *
 * Empty is the one that bites in practice — it is what a write whose key field
 * was never set derives. Control characters are refused because they separate
 * fields in the cross-tab change tokens (\u001f) and turn logs and debug output
 * into something that cannot be read back; a key is an identifier, so nothing
 * is lost by keeping them out. An unpaired surrogate is the only way a JS
 * string fails "encodable as UTF-8": TextEncoder would substitute \ufffd for it
 * silently, so the key the server stored would not be the key this client
 * derived.
 */
export function validateRowKey(entity: string, key: string): void {
	if (key === '') {
		throw new Error(`entity "${entity}": row key is empty: every key field must be set`);
	}
	const bytes = utf8Length(entity, key);
	if (bytes > MAX_ROW_KEY_BYTES) {
		throw new Error(
			`entity "${entity}": row key is ${bytes} bytes, the limit is ${MAX_ROW_KEY_BYTES}`,
		);
	}
	for (let i = 0; i < key.length; i++) {
		const c = key.charCodeAt(i);
		if (c < 0x20 || c === 0x7f) {
			throw new Error(
				`entity "${entity}": row key contains the control character \\u${
					c.toString(16).padStart(4, '0')
				}`,
			);
		}
	}
}

/**
 * UTF-8 byte length of a key, throwing on an unpaired surrogate (which has no
 * UTF-8 form — see validateRowKey). Counted rather than encoded: keys are
 * derived on every write, and the common case is short and ASCII.
 */
function utf8Length(entity: string, key: string): number {
	let bytes = 0;
	for (let i = 0; i < key.length; i++) {
		const c = key.charCodeAt(i);
		if (c < 0x80) bytes += 1;
		else if (c < 0x800) bytes += 2;
		else if (c < 0xd800 || c > 0xdfff) bytes += 3;
		else {
			// A high surrogate followed by a low one is one 4-byte code point;
			// anything else — a lone high, a bare low — cannot encode.
			const next = c < 0xdc00 && i + 1 < key.length ? key.charCodeAt(i + 1) : 0;
			if (next >= 0xdc00 && next <= 0xdfff) {
				bytes += 4;
				i++;
			} else {
				throw new Error(`entity "${entity}": row key has an unpaired surrogate at index ${i}`);
			}
		}
	}
	return bytes;
}

/**
 * Canonicalizes one key-field value as text: strings verbatim, integers
 * base-10, bools "true"/"false", enums by number. It is the exact rendering the
 * server performs, and the reason validateKeyField admits only those kinds.
 *
 * It also renders the identity of a keyed list's element (see mutations.ts),
 * which is the same question asked of a nested field — hence the tolerance for
 * kinds a row key never sees.
 */
export function scalarKeyString(
	value: unknown,
	scalar: ScalarType | undefined,
	isEnum: boolean,
): string {
	if (isEnum) return String(value);
	switch (scalar) {
		case ScalarType.STRING:
			return value as string;
		case ScalarType.BOOL:
			return value ? 'true' : 'false';
		default:
			return String(value); // numbers and bigints render base-10
	}
}

/**
 * Joins the rendered key-field values of one row into its storage key. A single
 * field is used verbatim — the common case, where the key reads as the id it
 * is. Composite keys escape "\" and "/" in each part before joining with "/",
 * so distinct tuples like ("a/b", "c") and ("a", "b/c") cannot collide into one
 * key.
 */
export function rowKey(parts: readonly string[]): string {
	if (parts.length === 1) return parts[0]!;
	return parts.map(escapeKeyPart).join('/');
}

/** Escapes the composite-key separator and the escape character itself. */
function escapeKeyPart(s: string): string {
	return s.replace(/[\\/]/g, (c) => `\\${c}`);
}
