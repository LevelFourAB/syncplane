// Index entries: turning a row ordering into sortable keys.
//
// An index entry is the key a row occupies in a secondary index. The one
// property it must have is that KEY order over entries equals the row order
// compareRows produces over the same rows and the same order keys — that
// equivalence is what lets a range scan replace a scan-and-sort, and nothing
// else about the encoding matters. It lives next to the comparator because a
// change to one that is not mirrored in the other silently corrupts every
// index rather than failing; testdata/indexkey pins the bytes so that drift
// is caught, and engine/query/indexkey.go is the other half of the mirror.
//
// 	entry     := component* rowKey
// 	component := tag payload?          (no payload for the non-present tags)
//
// 	tag         0x01 present   0xfd NaN   0xff missing   — never inverted
// 	payload (ascending; for a desc component every payload byte is inverted,
// 	         terminator included — the tag is not)
// 	  string  : CESU-8, 0x00 escaped to 0x00 0xff, terminated 0x00 0x00
// 	  bool    : 0x00 | 0x01
// 	  number  : IEEE-754 float64 big-endian, sign-flipped
// 	  bigint  : 8 bytes big-endian; signed flips the sign bit, unsigned as-is
// 	rowKey    : CESU-8 escaped string, ALWAYS ascending
//
// Entries are BYTES, rendered here as a string of one character per byte (all
// <= 0xff, below the surrogate range), because that is what a KV key is on
// this side. Rendering one byte per code unit is what keeps the KV's string
// order identical to the server's byte order over the same entry.
//
// Four choices carry the weight:
//
// - CESU-8, not UTF-8. Byte order over CESU-8 equals UTF-16 code-unit order,
//   which is what JS `<` and the Go CompareStringsUTF16 give. Plain UTF-8
//   sorts astral characters above every BMP one and would disagree. Since a
//   JS string IS UTF-16, encoding it is a walk over its code units — including
//   a lone surrogate, which encodes here and reaches the server as U+FFFD,
//   the same divergence local evaluation already has.
// - The tag sits OUTSIDE the inversion, so its three values encode the order
//   ranks (real < NaN < missing) directly. compareRows applies the rank before
//   the direction, so a desc component must too.
// - The row-key suffix is never inverted, because compareRows breaks final
//   ties on the row key ascending whatever the directions were.
// - A component's kind is static (fixed by the leaf's proto type), so each one
//   has exactly one payload shape and two entries in the same index are always
//   comparable character for character.
//
// One row, several entries
//
// A component whose path runs through a repeated field has no single value per
// row, so such a row takes one POSITION PER ELEMENT — the posting list that
// makes "some assignee is Maya" a range read instead of a scan. Three rules
// keep that honest:
//
// - Every component that fans out must fan out over the SAME repeated field,
//   and they fan out together, element by element. A cross product would pair
//   one element's id with another's role, and a query meant to describe one
//   element would match rows where no element does.
// - A row whose list is empty or unset still writes exactly one entry, with the
//   missing tag at each fan-out component. Every live row occupies at least one
//   position in every index, which is what lets a range that does NOT constrain
//   the fan-out component still see every row.
// - Duplicate elements collapse: they would produce identical keys, and an
//   entry is a position, not a count.

import type { DescMessage } from '@bufbuild/protobuf';

import { isTextField } from '../mutations.js';
import type { Path, Scalar } from './ast.js';
import { resolvePath, resolveWithField } from './evaluate.js';
import {
	elementKind,
	type LeafKind,
	leafKind,
	localPathOf,
	requireComparable,
	requireComparableElement,
	resolveLeafRef,
} from './wire.js';

// The tags double as the encoded order ranks: 0x01 < 0xfd < 0xff is
// real < NaN < missing, so comparing two entries reproduces compareRows's
// rank check without decoding anything.
const TAG_PRESENT = 0x01;
const TAG_NAN = 0xfd;
const TAG_MISSING = 0xff;

/**
 * Folded into every specHash. Bumping it invalidates every stored index,
 * which is the escape hatch for a change to the encoding that leaves the spec
 * itself untouched.
 */
const ENCODING_VERSION = 1;

/** The payload shapes a component can take. Text and other incomparable
 * leaves never become components, so they have no shape here. */
export type IndexKind = 'string' | 'number' | 'bigint' | 'bool';

/** One resolved component of an index key. */
export interface IndexComponent {
	/** localName path segments — what the value is resolved through. */
	path: Path;
	/** The same segments as PROTO field names, which is what specHash reads so
	 * the fingerprint matches the Go side's. */
	protoPath: Path;
	desc: boolean;
	/** Payload shape, fixed by the leaf's proto type. */
	kind: IndexKind;
	/** A 64-bit unsigned leaf is already in order; a signed one is biased. */
	unsigned: boolean;
	/**
	 * Position in `path` of the repeated field this component fans out over, or
	 * -1 when it yields one value per row. When it is the last position the
	 * elements themselves are the values (a repeated scalar); otherwise the rest
	 * of the path resolves against each element.
	 */
	multiAt: number;
}

/**
 * Resolves an index component's path against the schema. It rejects the same
 * leaves an order key rejects — a component that could never take part in a
 * comparison would sort every row into one bucket — plus collaborative text,
 * which is comparable but would rewrite its entry on every keystroke.
 *
 * A path ending at a single-key reference extends to that key field, exactly
 * as prepareQuery does, so an index and a query on the same reference agree —
 * including a to-many one, whose row then holds one entry per referenced key.
 *
 * Unlike a condition path, this one may descend through a repeated field (and
 * may end at one): that is what an index over "some element's value" is made
 * of. At most one repeated field per component, since a second would need a
 * cross product within the component itself.
 *
 * `path` is in PROTO field names — the form an index spec is stated in on both
 * sides of the wire contract — and is translated to localNames here, because
 * that is what the resolver and the evaluator address fields by. Every segment
 * whose two names differ (`sort_key`) resolves to nothing otherwise, which
 * `compileIndexes` would swallow as an unusable index.
 */
export function compileIndexComponent(
	schema: DescMessage,
	path: Path,
	desc: boolean,
): IndexComponent {
	const leaf = resolveLeafRef(schema, localPathOf(schema, path), true, true);
	const leafIsList = leaf.multiAt === leaf.path.length - 1;
	if (leafIsList) {
		// The leaf IS the repeated field, so the stored values are its elements
		// and it is the element kind that has to be comparable.
		requireComparableElement(leaf.path, leaf.field);
	} else {
		requireComparable(leaf.path, leaf.field);
	}
	if (leaf.field.fieldKind === 'scalar' && isTextField(leaf.field)) {
		throw new Error(
			`index path ${
				leaf.path.join('.')
			}: a collaborative text field cannot be indexed; every keystroke rewrites its value, so the entry would be rewritten with it`,
		);
	}
	return {
		path: leaf.path,
		protoPath: leaf.protoPath,
		desc,
		kind: indexKindOf(leafIsList ? elementKind(leaf.field) : leafKind(leaf.field), leaf.path),
		unsigned: isUnsigned64(leaf.field),
		multiAt: leaf.multiAt,
	};
}

function indexKindOf(kind: LeafKind, path: Path): IndexKind {
	if (kind === 'list' || kind === 'other') {
		// Unreachable: requireComparable has already rejected both.
		throw new Error(`index path ${path.join('.')}: ${kind} leaves cannot be indexed`);
	}
	return kind;
}

function isUnsigned64(field: { fieldKind: string; scalar?: number }): boolean {
	// ScalarType.UINT64 === 4, ScalarType.FIXED64 === 6 — the proto field-type
	// numbers, which protobuf-es reuses verbatim.
	return field.fieldKind === 'scalar' && (field.scalar === 4 || field.scalar === 6);
}

/** Turns rows into the entries of one index. */
export class IndexEncoder {
	/**
	 * The path to the repeated field every fan-out component descends through,
	 * or undefined when none does. Holding it once is what makes the components
	 * fan out TOGETHER: the list is resolved a single time and every component
	 * reads the same element.
	 */
	private readonly fanPath: Path | undefined;

	/**
	 * Throws when two components fan out over different repeated fields. Their
	 * cross product would be the only way to store both, and a cross product
	 * pairs values that never belonged to the same element — so such an index is
	 * refused at declaration rather than answering a subtly wrong question at
	 * read time.
	 */
	constructor(readonly components: IndexComponent[]) {
		for (const c of components) {
			if (c.multiAt < 0) continue;
			const path = c.path.slice(0, c.multiAt + 1);
			if (this.fanPath === undefined) {
				this.fanPath = path;
			} else if (this.fanPath.join('.') !== path.join('.')) {
				throw new Error(
					`an index cannot fan out over both ${this.fanPath.join('.')} and ${
						path.join('.')
					}; one repeated field per index`,
				);
			}
		}
	}

	/**
	 * The entries `row` occupies in this index — one per element of the fanned-
	 * out list, or a single entry when no component fans out. `rowKey` is the
	 * row's KV key, which terminates every entry so entries are unique and tie
	 * exactly the way compareRows does.
	 *
	 * Never empty: a row whose list is empty or unset still takes one position,
	 * with the missing tag at each fan-out component, so every live row is
	 * reachable through every index.
	 */
	encode(row: unknown, rowKey: string, schema?: DescMessage): string[] {
		if (this.fanPath === undefined) {
			return [ this.encodeAt(row, rowKey, schema, undefined, undefined) ];
		}
		const r = resolveWithField(row, this.fanPath, schema);
		const list = Array.isArray(r.value) ? r.value : undefined;
		if (!r.found || list === undefined || list.length === 0) {
			return [ this.encodeAt(row, rowKey, schema, undefined, undefined) ];
		}
		const element = r.field?.fieldKind === 'list' && r.field.listKind === 'message'
			? r.field.message
			: undefined;
		const out: string[] = [];
		const seen = new Set<string>();
		for (const e of list) {
			const entry = this.encodeAt(row, rowKey, schema, e, element);
			// Duplicate elements encode identically. An entry is a position, not
			// a count, so the second one would just overwrite the first.
			if (seen.has(entry)) continue;
			seen.add(entry);
			out.push(entry);
		}
		return out;
	}

	/** One entry, reading fan-out components from `element`. An undefined
	 * element means "no element", which is how a row with an empty list still
	 * gets its single entry. */
	private encodeAt(
		row: unknown,
		rowKey: string,
		schema: DescMessage | undefined,
		element: unknown,
		elementSchema: DescMessage | undefined,
	): string {
		const out: number[] = [];
		for (const c of this.components) {
			if (c.multiAt < 0) {
				const r = resolvePath(row, c.path, schema);
				appendComponent(out, c, r.found, r.value);
			} else if (element === undefined) {
				appendComponent(out, c, false, undefined);
			} else if (c.multiAt === c.path.length - 1) {
				// The leaf is the list itself, so the element IS the value.
				appendComponent(out, c, true, element);
			} else {
				const r = resolvePath(element, c.path.slice(c.multiAt + 1), elementSchema);
				appendComponent(out, c, r.found, r.value);
			}
		}
		appendIndexString(out, rowKey);
		return bytesToKey(out);
	}

	/**
	 * The first `values.length` components encoded from explicit values — the
	 * leading equality terms of a probe — giving the inclusive lower bound of
	 * the range holding every row that agrees with them. Pair it with
	 * {@link upperBound} for the exclusive end.
	 */
	encodePrefix(values: Scalar[]): string {
		if (values.length > this.components.length) {
			throw new Error(
				`index probe has ${values.length} values but the index has ${this.components.length} components`,
			);
		}
		const out: number[] = [];
		values.forEach((v, i) => appendComponent(out, this.components[i]!, true, v));
		return bytesToKey(out);
	}
}

/**
 * The exclusive end of the range of every entry starting with `prefix`.
 * `undefined` means "no upper bound" — the prefix is all 0xff, so nothing
 * sorts above it.
 */
export function upperBound(prefix: string): string | undefined {
	for (let i = prefix.length - 1; i >= 0; i--) {
		if (prefix.charCodeAt(i) !== 0xff) {
			return prefix.slice(0, i) + String.fromCharCode(prefix.charCodeAt(i) + 1);
		}
	}
	return undefined;
}

/**
 * Fingerprints everything an index's stored entries depend on and nothing
 * else: the component paths, their directions, the payload shape each leaf
 * resolves to, where the component fans out, and the encoding version. The
 * fan-out position is derived from the path but not stated by it, so a field
 * that becomes repeated keeps its path and changes how many entries a row has —
 * exactly the drift a fingerprint exists to catch. A stored index whose hash no
 * longer matches its declaration is stale, not wrong — its entries are simply
 * not readable under the current spec, and the index rebuilds.
 *
 * Mirrors engine/query/indexkey.go's SpecHash character for character. It has
 * to: nothing forces the two to agree — a fingerprint never crosses the wire,
 * and each side only compares its own stored hash to its own declaration — but
 * one that differed for no reason would be a trap for anyone reading them side
 * by side.
 *
 * It reads protoPath, not path: proto field names are what the two languages
 * state identically.
 */
export function specHash(comps: IndexComponent[]): string {
	let s = `v${ENCODING_VERSION}`;
	for (const c of comps) {
		s += `|${c.protoPath.join('.')}:${c.kind}:${c.unsigned}:${c.desc}:${c.multiAt}`;
	}
	return fingerprint(s);
}

/**
 * FNV-1a/64 as 16 hex digits. An index fingerprint only has to CHANGE when
 * the spec does — it never crosses the wire and nothing authenticates against
 * it — so a hash with no dependencies and a synchronous implementation beats a
 * cryptographic one (SubtleCrypto is async, which would infect every caller).
 */
function fingerprint(s: string): string {
	const PRIME = 1099511628211n;
	const MASK = (1n << 64n) - 1n;
	let h = 14695981039346656037n;
	for (let i = 0; i < s.length; i++) {
		h = ((h ^ BigInt(s.charCodeAt(i) & 0xff)) * PRIME) & MASK;
	}
	return h.toString(16).padStart(16, '0');
}

function appendComponent(
	out: number[],
	c: IndexComponent,
	found: boolean,
	value: unknown,
): void {
	if (!found) {
		out.push(TAG_MISSING);
		return;
	}
	if (typeof value === 'number' && Number.isNaN(value)) {
		out.push(TAG_NAN);
		return;
	}
	out.push(TAG_PRESENT);
	const start = out.length;
	switch (c.kind) {
		case 'string':
			appendIndexString(out, value as string);
			break;
		case 'bool':
			out.push(value ? 0x01 : 0x00);
			break;
		case 'number':
			appendIndexNumber(out, value as number);
			break;
		case 'bigint':
			appendIndexBigint(out, value as bigint, c.unsigned);
			break;
	}
	if (c.desc) {
		for (let i = start; i < out.length; i++) out[i] = ~out[i]! & 0xff;
	}
}

/**
 * Writes `s` as CESU-8: each UTF-16 code unit encoded as if it were a code
 * point, which is what makes byte order equal code-unit order.
 */
function appendIndexString(out: number[], s: string): void {
	for (let i = 0; i < s.length; i++) {
		const c = s.charCodeAt(i);
		if (c === 0) {
			// Escaped so a NUL cannot be read as half a terminator. 0xff keeps
			// the escape ABOVE the terminator, so "a" still sorts before
			// "a\u0000".
			out.push(0x00, 0xff);
		} else if (c < 0x80) {
			out.push(c);
		} else if (c < 0x800) {
			out.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f));
		} else {
			out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
		}
	}
	// A terminator no payload byte can contain, so no encoded string is ever a
	// prefix of another: "a" ends before "ab" diverges, and the desc inversion
	// — which only reverses order between strings where neither is a prefix of
	// the other — stays correct.
	out.push(0x00, 0x00);
}

const scratch = new DataView(new ArrayBuffer(8));

/**
 * The standard order-preserving flip: negatives invert whole, non-negatives
 * set the sign bit, so the unsigned byte order of the result is numeric order.
 */
function appendIndexNumber(out: number[], f: number): void {
	// -0 and +0 have different bit patterns but compareValues calls them
	// equal, so they must encode identically or the index would order two rows
	// the comparator ties. `f === 0` is true for both; assigning 0 gives +0.
	scratch.setFloat64(0, f === 0 ? 0 : f);
	const negative = (scratch.getUint8(0) & 0x80) !== 0;
	for (let i = 0; i < 8; i++) {
		const b = scratch.getUint8(i);
		out.push(negative ? ~b & 0xff : i === 0 ? b | 0x80 : b);
	}
}

/**
 * Writes a 64-bit integer. A signed field biases by 2^63 to move it into
 * unsigned order; an unsigned one is already in order. Which applies is a
 * property of the field, so two entries in one index always agree.
 */
function appendIndexBigint(out: number[], v: bigint, unsigned: boolean): void {
	const MASK = (1n << 64n) - 1n;
	const u = (unsigned ? v : v + (1n << 63n)) & MASK;
	for (let shift = 56n; shift >= 0n; shift -= 8n) {
		out.push(Number((u >> shift) & 0xffn));
	}
}

/** One character per byte. Chunked so a long entry cannot blow the argument
 * limit on `String.fromCharCode`. */
function bytesToKey(bytes: number[]): string {
	let out = '';
	for (let i = 0; i < bytes.length; i += 4096) {
		out += String.fromCharCode(...bytes.slice(i, i + 4096));
	}
	return out;
}
