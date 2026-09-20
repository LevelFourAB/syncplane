// Movable ordered lists: a keyed `repeated Message` whose elements carry a
// fractional position field (order_field). Convergence is already handled by the
// per-item keyed-list merge (a move is an LWW write to `pos`, an insert/delete is
// an add/clear). This module is the pure client layer: allocate `pos` keys
// between neighbors, mint stable element ids, and sort by `pos` on read.

import { type DescField, type DescMessage, type Message } from '@bufbuild/protobuf';

import { generateKeyBetween as fracBetween } from './fractional.js';
import { encodeItemKey, fieldBehaviorOf, keyFieldsFor } from './mutations.js';

const DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

/**
 * Deterministic base-62 jitter (6 digits) derived from an actor id. Two actors
 * inserting into the same gap compute the same fractional midpoint; the jitter
 * (appended as extra fraction digits, so the key stays a valid order key and is
 * still always insertable-between) makes their positions distinct and stably
 * ordered. The last digit is forced non-zero to avoid a trailing-zero order key.
 */
function jitter(actor: string): string {
	// FNV-1a 32-bit seed, expanded to 6 base-62 digits with a small LCG.
	let h = 0x811c9dc5;
	for (let i = 0; i < actor.length; i++) {
		h ^= actor.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	let s = (h >>> 0) || 1;
	let out = '';
	for (let i = 0; i < 6; i++) {
		s = (Math.imul(s, 1103515245) + 12345) >>> 0;
		out += DIGITS[s % 62];
	}
	return out.endsWith('0') ? out.slice(0, -1) + '1' : out;
}

/**
 * Returns a fractional position strictly between a and b (null bounds open:
 * before-first / after-last), suffixed with actor-derived jitter so concurrent
 * same-gap inserts by different clients get distinct, deterministically-ordered
 * keys. Pass the existing `clientId` as the actor.
 */
export function generateKeyBetween(a: string | null, b: string | null, actor: string): string {
	const base = fracBetween(a, b); // strictly between a and b, a valid order key
	const candidate = base + jitter(actor);
	// Appending jitter grows the key, so it always stays > a; but when `base` is a
	// prefix of `b` (the gap is narrow) the jitter can reach/overflow `b`. In that
	// rare case drop the jitter and take a deeper deterministic midpoint, which is
	// strictly inside (base, b) ⊂ (a, b). Distinctness there falls back to the
	// element-id tie-break.
	if (b !== null && candidate >= b) return fracBetween(base, b);
	return candidate;
}

let counter = 0;

/**
 * Mints a stable, globally-unique, roughly time-sortable element id used as a
 * movable list's key_field value. The clientId makes it unique across devices;
 * a per-process counter makes it unique within a burst.
 */
export function newElementId(clientId: string): string {
	counter = (counter + 1) >>> 0;
	const t = Date.now().toString(36);
	const c = counter.toString(36).padStart(4, '0');
	return `${t}${c}-${clientId}`;
}

/** Orders two list elements by `pos`, breaking ties by encoded identity. */
export function comparePos(posA: string, idA: string, posB: string, idB: string): number {
	if (posA !== posB) return posA < posB ? -1 : 1;
	return idA < idB ? -1 : idA > idB ? 1 : 0;
}

/** Resolves the order_field name of a movable list, or undefined if the field
 * is not a movable (keyed message list with order_field) list. */
export function orderFieldOf(field: DescField): string | undefined {
	if (field.fieldKind !== 'list' || field.listKind !== 'message') return undefined;
	return fieldBehaviorOf(field)?.orderField || undefined;
}

/** Everything the sort needs about one movable list field, resolved from the schema. */
interface MovableList {
	/** Property the list is held under on a decoded message. */
	localName: string;
	/** Property an element's fractional position is held under. */
	posLocal: string;
	/** Identity fields the tie-break encodes (see keyFieldsFor). */
	keyFields: DescField[];
}

const movableLists = new WeakMap<DescMessage, readonly MovableList[]>();

/**
 * The movable list fields a message schema declares, in field order. Empty for
 * the common schema that declares none.
 */
function movableListsOf(schema: DescMessage): readonly MovableList[] {
	// Cached per descriptor: resolving this reads field options, and protobuf-es
	// re-parses the option message bytes on every read, which dominated the cost
	// of decoding a row on scans that sort each one.
	const cached = movableLists.get(schema);
	if (cached !== undefined) return cached;
	const out: MovableList[] = [];
	for (const field of schema.fields) {
		const order = orderFieldOf(field);
		if (!order || field.fieldKind !== 'list' || field.listKind !== 'message') continue;
		const posField = field.message.fields.find((f) => f.name === order);
		if (!posField) continue;
		out.push({
			localName: field.localName,
			posLocal: posField.localName,
			keyFields: keyFieldsFor(field),
		});
	}
	movableLists.set(schema, out);
	return out;
}

/**
 * Sorts every movable list field of `msg` in place by its `pos` field (ties
 * broken by element identity). The stored byte order stays canonical
 * (server-linearized, byte-equal across clients); this is a read-only logical
 * re-order for the app.
 */
export function sortOrderedFields(schema: DescMessage, msg: Message): void {
	for (const { localName, posLocal, keyFields } of movableListsOf(schema)) {
		const list = (msg as unknown as Record<string, unknown>)[localName] as
			| Message[]
			| undefined;
		if (!list || list.length < 2) continue;
		list.sort((x, y) => {
			const px = String((x as unknown as Record<string, unknown>)[posLocal] ?? '');
			const py = String((y as unknown as Record<string, unknown>)[posLocal] ?? '');
			return comparePos(px, encodeItemKey(x, keyFields), py, encodeItemKey(y, keyFields));
		});
	}
}
