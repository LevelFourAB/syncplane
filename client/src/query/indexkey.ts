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
// A component whose path FANS OUT has no single value per row, so such a row
// takes one POSITION PER ELEMENT — the posting list that makes "some assignee is
// Maya" a range read instead of a scan. Two things fan out, and they compose:
//
// - a repeated field, one position per element; and
// - a map, written `*` in the declared path, one position per ENTRY. That is the
//   shape user-defined ("custom") fields take. The `*` is followed by `key` or
//   `value`, naming the side of the entry the component stores, so an index over
//   custom fields declares the components it wants in the order it wants them:
//   `[custom.*.key, custom.*.value.number]` makes a probe on ONE custom field a
//   contiguous range, and `[custom.*.value.number, custom.*.key]` makes "which
//   custom field holds 5" one.
//
// Four rules keep it honest:
//
// - Every component that fans out must fan out over the SAME chain, and they
//   fan out together, position by position. A cross product would pair one
//   element's id with another's role, and a query meant to describe one element
//   would match rows where no element does. A SHORTER chain is fine: an
//   entry-key component stops at the map and repeats across the positions below.
// - A `*` may be followed by a repeated field INSIDE one entry
//   (custom.*.value.labels.values, the multi-select shape). That is not a cross
//   product either: the second fan lives inside one position of the first.
// - A row whose collection is empty or unset still writes exactly one entry,
//   with the missing tag at each fan-out component. Every live row occupies at
//   least one position in every index, which is what lets a range that does NOT
//   constrain the fan-out component still see every row.
// - Duplicate positions collapse: they would produce identical keys, and an
//   entry is a position, not a count.

import type { DescMessage } from '@bufbuild/protobuf';

import { isTextField } from '../mutations.js';
import type { Path, Scalar } from './ast.js';
import { compareValues, resolvePath, resolveWithField } from './evaluate.js';
import {
	elementKind,
	type FanStep,
	type LeafKind,
	leafKind,
	localPathOf,
	MAP_FAN_SEGMENT,
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
	/** localName path segments — what the value is resolved through. A map fan
	 * is ONE segment (the field); the `*` is implicit in `fans`. */
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
	 * The scopes this component fans out over, outermost first. Empty when it
	 * yields one value per row. When the innermost fan is the LAST segment of
	 * `path` the elements themselves are the values (a repeated scalar);
	 * otherwise what follows resolves against each of them.
	 */
	fans: FanStep[];
	/**
	 * True when the value is the KEY of the entry the innermost map fan is at,
	 * rather than anything below it — the `key` selector, written
	 * `custom.*.key`.
	 */
	keyComponent: boolean;
	/**
	 * Makes this a COMPUTED component: its value is the row's match rank (a
	 * number) rather than anything read through a path, and `path` is empty.
	 * Only a relevance order key builds one, so it exists in query cursors and
	 * never in a stored index.
	 */
	rank?: {
		rank(row: unknown, schema: DescMessage | undefined): number;
		specString(): string;
	};
}

/**
 * Resolves one declared index key into the ONE component it contributes. It
 * rejects the same leaves an order key rejects — a component that could never
 * take part in a comparison would sort every row into one bucket — plus
 * collaborative text, which is comparable but would rewrite its entry on every
 * keystroke.
 *
 * A path ending at a single-key reference extends to that key field, exactly
 * as prepareQuery does, so an index and a query on the same reference agree —
 * including a to-many one, whose row then holds one entry per referenced key.
 *
 * Unlike a condition path, this one may FAN OUT: through a repeated field (and
 * may end at one), which is what an index over "some element's value" is made
 * of, and through `*` after a map, which is what an index over user-defined
 * fields is made of. At most one repeated field per component, since a second
 * list would need a cross product within the component itself; a `*` nests
 * inside one entry rather than crossing with it, so it composes with a list.
 *
 * A `*` is followed by `key` or `value`, so one declared path is one component
 * and an index states its own layout:
 *
 * ```
 * [ custom.*.key, custom.*.value.number ]   one custom field: equality, ranges
 *                                           and an ordering within it
 * [ custom.*.value.number, custom.*.key ]   which custom field holds 5:
 *                                           equality, cross-key or named-key
 * ```
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
	const innermost = leaf.fans.at(-1);
	const leafIsElement = innermost !== undefined && innermost.at === leaf.path.length - 1;
	if (leaf.entryKey) {
		// A map key is a proto scalar — string, integral or bool — so it is
		// always comparable and there is nothing to reject.
		return {
			path: leaf.path,
			protoPath: leaf.protoPath,
			desc,
			kind: indexKindOf(innermost?.keyKind ?? 'other', leaf.path),
			// A map KEY carries the signedness of its own type, not the field's:
			// an unsigned 64-bit key is already in byte order where a signed one
			// is biased, and the Go encoder reads it off the same descriptor.
			unsigned: isUnsigned64Key(leaf.field),
			fans: leaf.fans,
			keyComponent: true,
		};
	}
	if (
		leafIsElement && innermost.isMap && leaf.field.fieldKind === 'map'
		&& leaf.field.mapKind === 'message'
	) {
		// The component stops at `value` on a message-valued map, so it would
		// store something no comparison reaches. Point at the shape that works
		// rather than at isSet, which an index path cannot use.
		throw new Error(
			`index path ${path.join('.')}: map field "${
				leaf.path.at(-1)
			}" has a ${leaf.field.message.typeName} value, which is not comparable; index a scalar field inside it instead, e.g. ${
				path.join('.')
			}.<field>`,
		);
	}
	if (leafIsElement && !innermost.isMap) {
		// The leaf IS the repeated field, so the stored values are its elements
		// and it is the element kind that has to be comparable.
		requireComparableElement(leaf.path, leaf.field);
	} else {
		// A component ending at a map fan stores the entry's VALUE, which is what
		// a keyed leaf is; anything else is an ordinary singular leaf.
		requireComparable(leaf.path, { field: leaf.field, keyed: leafIsElement });
	}
	if (leaf.field.fieldKind === 'scalar' && isTextField(leaf.field)) {
		throw new Error(
			`index path ${
				leaf.path.join('.')
			}: a collaborative text field cannot be indexed; every keystroke rewrites its value, so the entry would be rewritten with it`,
		);
	}
	const kind = indexKindOf(
		leafIsElement
			? (innermost.isMap
				? leafKind({ field: leaf.field, keyed: true })
				: elementKind(leaf.field))
			: leafKind({ field: leaf.field, keyed: false }),
		leaf.path,
	);
	return {
		path: leaf.path,
		protoPath: leaf.protoPath,
		desc,
		kind,
		unsigned: isUnsigned64(leaf.field),
		fans: leaf.fans,
		keyComponent: false,
	};
}

function indexKindOf(kind: LeafKind, path: Path): IndexKind {
	if (kind === 'list' || kind === 'map' || kind === 'other') {
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

/** isUnsigned64 for the KEY type of a map field. */
function isUnsigned64Key(field: { fieldKind: string; mapKey?: number }): boolean {
	return field.fieldKind === 'map' && (field.mapKey === 4 || field.mapKey === 6);
}

/**
 * One place a row occupies in the index: one entry per level of the encoder's
 * fan chain, outermost first. A position SHORTER than the chain means the
 * collection at that depth was empty or absent, so every component reading below
 * it takes the missing tag.
 */
type Position = PosLevel[];

interface PosLevel {
	/** The entry's typed key; only set for a map level. */
	key?: Scalar;
	/** The element (a list level) or the entry's value (a map level). */
	val: unknown;
	/** The value's schema, when it is a message — what a deeper level resolves
	 * against. */
	schema: DescMessage | undefined;
}

/** Turns rows into the entries of one index. */
export class IndexEncoder {
	/**
	 * The chain of scopes the components fan out over, outermost first. Holding
	 * it once is what makes the components fan out TOGETHER: each collection is
	 * resolved a single time and every component reads the same position of it.
	 *
	 * It is a CHAIN rather than a single path because a `*` over a map may be
	 * followed by a repeated field inside one entry — the multi-select shape,
	 * `custom.*.labels.values` — and each level lives inside one position of the
	 * level above it, so nothing is crossed with anything.
	 */
	private readonly fan: FanLevel[] = [];

	/**
	 * Throws when two components fan out over different chains. Their cross
	 * product would be the only way to store both, and a cross product pairs
	 * values that never belonged to the same element — so such an index is
	 * refused at declaration rather than answering a subtly wrong question at
	 * read time. A SHORTER chain is fine, and is the normal case: the entry-key
	 * component of a `custom.*.labels.values` index stops at the map, and its
	 * value simply repeats across the positions below it.
	 */
	constructor(readonly components: IndexComponent[]) {
		for (const c of components) {
			const levels = fanLevelsOf(c);
			if (levels.length === 0) continue;
			const n = Math.min(levels.length, this.fan.length);
			for (let i = 0; i < n; i++) {
				const a = this.fan[i]!;
				const b = levels[i]!;
				if (a.isMap !== b.isMap || a.steps.join('.') !== b.steps.join('.')) {
					throw new Error(
						`an index cannot fan out over both ${fanPathString(this.fan)} and ${
							fanPathString(levels)
						}; one fan-out chain per index`,
					);
				}
			}
			if (levels.length > this.fan.length) {
				this.fan.length = 0;
				this.fan.push(...levels);
			}
		}
	}

	/**
	 * The entries `row` occupies in this index — one per position of the fan
	 * chain, or a single entry when no component fans out. `rowKey` is the row's
	 * KV key, which terminates every entry so entries are unique and tie exactly
	 * the way compareRows does.
	 *
	 * Never empty: a row whose collection is empty or unset still takes one
	 * position, with the missing tag at each fan-out component, so every live row
	 * is reachable through every index.
	 */
	encode(row: unknown, rowKey: string, schema?: DescMessage): string[] {
		const out: string[] = [];
		const seen = new Set<string>();
		for (const pos of this.positions(row, schema)) {
			const entry = this.encodeAt(row, rowKey, schema, pos);
			// Two positions can encode identically — duplicate list elements, or
			// two map entries no component distinguishes. An entry is a position,
			// not a count, so the second one would just overwrite the first.
			if (seen.has(entry)) continue;
			seen.add(entry);
			out.push(entry);
		}
		return out;
	}

	/** Enumerates the fan chain against one row, depth first. */
	private positions(row: unknown, schema: DescMessage | undefined): Position[] {
		if (this.fan.length === 0) return [ [] ];
		const out: Position[] = [];
		const walk = (
			scope: unknown,
			scopeSchema: DescMessage | undefined,
			level: number,
			cur: Position,
		) => {
			if (level === this.fan.length) {
				out.push(cur.slice());
				return;
			}
			const fl = this.fan[level]!;
			const r = resolveWithField(scope, fl.steps, scopeSchema);
			const valueSchema = r.field?.fieldKind === 'map'
				? (r.field.mapKind === 'message' ? r.field.message : undefined)
				: r.field?.fieldKind === 'list' && r.field.listKind === 'message'
				? r.field.message
				: undefined;
			if (!r.found) {
				out.push(cur.slice());
				return;
			}
			if (fl.isMap) {
				const rec = isRecord(r.value) ? r.value : undefined;
				const keys = rec ? sortedMapKeys(rec, fl.keyKind) : [];
				if (!rec || keys.length === 0) {
					out.push(cur.slice());
					return;
				}
				for (const [ raw, typed ] of keys) {
					cur.push({ key: typed, val: rec[raw], schema: valueSchema });
					walk(rec[raw], valueSchema, level + 1, cur);
					cur.pop();
				}
				return;
			}
			const list = Array.isArray(r.value) ? r.value : undefined;
			if (!list || list.length === 0) {
				out.push(cur.slice());
				return;
			}
			for (const el of list) {
				cur.push({ val: el, schema: valueSchema });
				walk(el, valueSchema, level + 1, cur);
				cur.pop();
			}
		};
		walk(row, schema, 0, []);
		return out;
	}

	/**
	 * One entry, reading each component from the position it belongs to. A
	 * position shorter than a component's fan depth means "no element there",
	 * which is how a row with an empty collection still gets its single entry.
	 */
	private encodeAt(
		row: unknown,
		rowKey: string,
		schema: DescMessage | undefined,
		pos: Position,
	): string {
		const out: number[] = [];
		for (const c of this.components) {
			if (c.rank) {
				appendComponent(out, c, true, c.rank.rank(row, schema));
				continue;
			}
			const depth = c.fans.length;
			if (depth === 0) {
				const r = resolvePath(row, c.path, schema);
				appendComponent(out, c, r.found, r.value);
				continue;
			}
			if (depth > pos.length) {
				appendComponent(out, c, false, undefined);
				continue;
			}
			const at = pos[depth - 1]!;
			const innermost = c.fans[depth - 1]!;
			if (c.keyComponent) {
				appendComponent(out, c, true, at.key);
				continue;
			}
			const rest = c.path.slice(innermost.at + 1);
			if (rest.length === 0) {
				// The fanned element (or map value) IS the value.
				appendComponent(out, c, true, at.val);
				continue;
			}
			const r = resolvePath(at.val, rest, at.schema);
			appendComponent(out, c, r.found, r.value);
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

	/**
	 * Cuts an entry of this index into its component half and its row-key
	 * suffix, by walking the components the encoder was built from. It throws on
	 * anything that is not an entry of this index — an unknown tag, a payload
	 * running off the end, an unterminated string, characters left over past the
	 * row key — which is what makes it usable on a cursor a caller supplied.
	 *
	 * A query cursor's seek bound is the component half alone: an index may carry
	 * components past the ordering's keys, and a bound running on into the row
	 * key would not be a prefix of any entry in such an index.
	 */
	splitEntry(entry: string): { components: string; rowKey: string } {
		let at = 0;
		this.components.forEach((c, i) => {
			try {
				at += componentLength(c, entry, at);
			} catch (err) {
				throw new Error(`index entry: component ${i}: ${(err as Error).message}`);
			}
		});
		let n: number;
		try {
			n = indexStringLength(entry, at, false);
		} catch (err) {
			throw new Error(`index entry: row key: ${(err as Error).message}`);
		}
		if (at + n !== entry.length) {
			throw new Error(`index entry: ${entry.length - at - n} characters left over past the row key`);
		}
		return { components: entry.slice(0, at), rowKey: entry.slice(at) };
	}
}

/**
 * The length of the component encoded at `at`, tag included. The component says
 * which payload shape to expect, so the walk needs no length prefixes in the
 * encoding itself.
 */
function componentLength(c: IndexComponent, entry: string, at: number): number {
	if (at >= entry.length) throw new Error('entry ends before the tag');
	const tag = entry.charCodeAt(at);
	if (tag === TAG_NAN || tag === TAG_MISSING) return 1;
	if (tag !== TAG_PRESENT) {
		throw new Error(
			`tag 0x${tag.toString(16).padStart(2, '0')} is none of 0x01, 0xfd, 0xff`,
		);
	}
	switch (c.kind) {
		case 'string':
			return 1 + indexStringLength(entry, at + 1, c.desc);
		case 'bool':
			if (at + 2 > entry.length) throw new Error('entry ends inside a bool payload');
			return 2;
		default:
			if (at + 9 > entry.length) throw new Error('entry ends inside an 8-byte payload');
			return 9;
	}
}

/**
 * The length of the CESU-8 string starting at `at`, terminator included.
 * `inverted` is set for a descending component, where every payload byte is
 * complemented — the escape and the terminator among them, so the two the walk
 * looks for flip with it.
 */
function indexStringLength(entry: string, at: number, inverted: boolean): number {
	const term = inverted ? 0xff : 0x00;
	const esc = inverted ? 0x00 : 0xff;
	for (let i = at; i < entry.length;) {
		if (entry.charCodeAt(i) !== term) {
			i++;
			continue;
		}
		if (i + 1 >= entry.length) break;
		const next = entry.charCodeAt(i + 1);
		if (next === term) return i + 2 - at;
		if (next !== esc) {
			throw new Error(`character 0x${next.toString(16).padStart(2, '0')} follows a string escape`);
		}
		i += 2;
	}
	throw new Error('unterminated string');
}

/** One scope a component fans out over: the segments from the enclosing scope
 * down to and including the collection itself. */
interface FanLevel {
	steps: Path;
	isMap: boolean;
	keyKind: IndexKind | undefined;
}

function fanLevelsOf(c: IndexComponent): FanLevel[] {
	let start = 0;
	return c.fans.map((f) => {
		const steps = c.path.slice(start, f.at + 1);
		start = f.at + 1;
		return { steps, isMap: f.isMap, keyKind: f.keyKind as IndexKind | undefined };
	});
}

function fanPathString(levels: FanLevel[]): string {
	if (levels.length === 0) return '(nothing)';
	return levels
		.map((l) => l.steps.join('.') + (l.isMap ? `.${MAP_FAN_SEGMENT}` : ''))
		.join(' → ');
}

function isRecord(v: unknown): v is Record<string, unknown> {
	return typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Uint8Array);
}

/**
 * A map's entries in the order the KEY COMPONENT stores them, as (record key,
 * typed key) pairs. Sorted rather than taken in property order: a JS object
 * orders integer-like keys ahead of the rest, Go randomizes map iteration, and
 * the entries a row writes have to be the same list in the same order on both
 * sides or the shared vectors could not pin them.
 */
function sortedMapKeys(
	rec: Record<string, unknown>,
	keyKind: IndexKind | undefined,
): [string, Scalar][] {
	const typed = (raw: string): Scalar => {
		switch (keyKind) {
			case 'bigint':
				return BigInt(raw);
			case 'number':
				return Number(raw);
			case 'bool':
				return raw === 'true';
			default:
				return raw;
		}
	};
	return Object.keys(rec)
		.map((raw): [string, Scalar] => [ raw, typed(raw) ])
		.sort((a, b) => compareValues(a[1], b[1]) ?? 0);
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
 * Mirrors engine/query/indexkey.go's SpecHash character for character, and has
 * to: a stored index only compares its own hash to its own declaration, but
 * query/cursor.ts folds this hash into a query cursor, and a cursor is minted
 * on one side and replayed on the other.
 *
 * It reads protoPath, not path: proto field names are what the two languages
 * state identically.
 */
export function specHash(comps: IndexComponent[]): string {
	let s = `v${ENCODING_VERSION}`;
	for (const c of comps) {
		if (c.rank) {
			// The whole matcher fingerprint, not a fixed word: a rank depends on
			// the query's terms and the declaration, so a relevance cursor minted
			// for one search must not bound a different one.
			s += `|relevance(${c.rank.specString()}):number:false:${c.desc}:-1`;
			continue;
		}
		// A map fan renders as `<field>.*`, matching how the path is declared.
		const mapFans = new Set(c.fans.filter((f) => f.isMap).map((f) => f.at));
		const path = c.protoPath
			.map((seg, i) => (mapFans.has(i) ? `${seg}.${MAP_FAN_SEGMENT}` : seg))
			.join('.');
		// The map-fan additions are APPENDED rather than folded in, so an index
		// shape that predates them fingerprints byte for byte as it did and its
		// stored entries stay valid.
		const first = c.fans.length > 0 ? c.fans[0]!.at : -1;
		s += `|${path}:${c.kind}:${c.unsigned}:${c.desc}:${first}`;
		for (const f of c.fans.slice(1)) s += `:+${f.at}`;
		if (c.keyComponent) s += ':key';
	}
	return fingerprint(s);
}

/**
 * FNV-1a/64 as 16 hex digits. It only has to CHANGE when what it summarizes
 * does, so a hash with no dependencies and a synchronous implementation beats a
 * cryptographic one (SubtleCrypto is async, which would infect every caller).
 * Nothing authenticates against it: a stored index whose hash moved rebuilds,
 * and a query cursor whose shape no longer matches is reissued.
 */
export function fingerprint(s: string): string {
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
export function appendIndexString(out: number[], s: string): void {
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
export function bytesToKey(bytes: number[]): string {
	let out = '';
	for (let i = 0; i < bytes.length; i += 4096) {
		out += String.fromCharCode(...bytes.slice(i, i + 4096));
	}
	return out;
}
