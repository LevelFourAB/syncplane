import { type DescMessage, fromBinary, type MessageShape, toBinary } from '@bufbuild/protobuf';

import {
	compareWorkspaces,
	confirmedKey,
	confirmedPrefix,
	type ConfirmedRecord,
	confirmedWsPrefix,
	overlayKey,
	overlayPrefix,
	type OverlayRecord,
	overlayTextPrefix,
	overlayTextRowPrefix,
	overlayTextWsPrefix,
	overlayWsPrefix,
	splitOverlayTextSuffix,
	splitWsKey,
	wsFromSegment,
} from './keys.js';
import type { KVRead } from './kv/store.js';
import {
	type EntityDef,
	type EntityRegistry,
	setTextFieldBytes,
	textFieldBytes,
} from './mutations.js';
import { sortOrderedFields } from './ordering.js';
import type { Cond } from './query/ast.js';
import { reportError } from './report.js';
import {
	applyFragment,
	decodeFragment,
	decodeTextDoc,
	encodeTextDoc,
	type TextDoc,
} from './text/index.js';
import { textResidency } from './text/residency.js';
import {
	foldCacheKey,
	foldCacheOf,
	type FoldEntry,
	isFragmentPrefix,
	sameFragments,
} from './textfold.js';

/**
 * Folds one row's optimistic text log (`ot/`) into its bytes, so every reader
 * downstream of here sees whole documents — the same contract the confirmed
 * store and the wire keep. A text edit is stored as the fragment it added
 * rather than as a rewritten row (see keys.ts), and this is where that is put
 * back together.
 *
 * Free for entities that declare no text field: the registry settles that from
 * the schema alone, with no read.
 *
 * A fragment that will not apply is dropped rather than thrown: the local log
 * is derived state whose base can legitimately go missing (a mutation rejected
 * and dropped strands the edits queued behind it, which is the same case
 * `withoutUnapplicableText` absorbs on the write path). Losing an optimistic
 * edit here is a display regression that the next rebase corrects; failing the
 * read would take down every watcher bound to the entity.
 */
export async function foldTextLog(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	workspaceId: string,
	key: string,
	bytes: Uint8Array,
): Promise<Uint8Array> {
	if (registry.textPathsOf(entity).length === 0) return bytes;
	const fragments = new Map<string, Uint8Array[]>();
	for await (const [ k, v ] of tx.scan(overlayTextRowPrefix(entity, workspaceId, key))) {
		const field = fieldOfTextKey(entity, k);
		if (field === undefined) continue;
		const list = fragments.get(field);
		if (list === undefined) fragments.set(field, [ v as Uint8Array ]);
		else list.push(v as Uint8Array);
	}
	return applyTextLog(registry, entity, workspaceId, key, fragments, bytes);
}

/** The field segment of an `ot/` key, given the entity it was scanned under. */
function fieldOfTextKey(entity: string, k: string): string | undefined {
	return splitOverlayTextSuffix(k.slice(overlayTextPrefix(entity).length))?.field;
}

/**
 * Applies already-collected fragments, in the key order they were scanned in —
 * which is the order the edits were made, and therefore an order in which each
 * fragment's parents already exist.
 *
 * Served from the row's cached fold whenever that still matches what is on
 * disk, which is what keeps this off the O(document) path it would otherwise be
 * on every notification (see textfold.ts). The cache decides only how much work
 * to redo: every result below is what a cold fold would have produced.
 */
function applyTextLog(
	registry: EntityRegistry,
	entity: string,
	workspaceId: string,
	key: string,
	fragments: Map<string, Uint8Array[]>,
	bytes: Uint8Array,
): Uint8Array {
	if (fragments.size === 0) return bytes;
	const def = registry.get(entity);
	if (!def) return bytes;
	const cache = foldCacheOf(registry);
	const cacheKey = foldCacheKey(entity, workspaceId, key);
	const prior = cache.take(cacheKey, bytes);
	// Same row, same fragments: the fold is a pure function of the two, so
	// neither the row nor any document has to be decoded to know the answer.
	if (prior !== undefined && sameFragments(prior.frags, fragments)) {
		cache.put(cacheKey, prior);
		return prior.result;
	}
	let msg;
	try {
		msg = fromBinary(def.schema, bytes);
	} catch {
		return bytes;
	}
	const fields = new Map<string, Uint8Array>();
	let changed = false;
	for (const [ field, list ] of fragments) {
		// The field string is an instance path; it stops resolving when the
		// element it addresses is gone (removed under the edit, or a schema
		// this build does not know). The row keeps what it holds — delete-wins
		// — and the entry ages out with the overlay.
		let base: Uint8Array;
		try {
			base = textFieldBytes(def.schema, field, msg) ?? new Uint8Array();
		} catch {
			continue;
		}
		const folded = foldFragments(base, list, entity, field, reusable(prior, field, list));
		if (folded === undefined) continue;
		fields.set(field, folded.bytes);
		// The document this fold produced, left where the next fold of this
		// field and any reader rendering it will look for it.
		textResidency.put(folded.bytes, folded.doc);
		if (folded.bytes !== base) {
			try {
				setTextFieldBytes(def.schema, field, msg, folded.bytes);
			} catch {
				continue;
			}
			changed = true;
		}
	}
	const result = changed ? toBinary(def.schema, msg) : bytes;
	cache.put(cacheKey, { base: bytes, frags: fragments, fields, result });
	return result;
}

/**
 * The document an entry can be extended from for one field: the one the last
 * fold of this field produced, when the fragments already in it are a prefix of
 * the ones just scanned. Anything else — a field the entry never held, a list
 * that lost or reordered a fragment, a document since evicted from residency —
 * has to be folded from the base again.
 *
 * Taken rather than borrowed, because extending it mutates it (residency.ts).
 */
function reusable(
	prior: FoldEntry | undefined,
	field: string,
	list: readonly Uint8Array[],
): { doc: TextDoc; applied: number } | undefined {
	if (prior === undefined) return undefined;
	const at = prior.fields.get(field);
	const frags = prior.frags.get(field);
	if (at === undefined || frags === undefined || !isFragmentPrefix(frags, list)) return undefined;
	const doc = textResidency.take(at);
	return doc === undefined ? undefined : { doc, applied: frags.length };
}

/**
 * Applies a field's whole fragment list to its base document, or — given a
 * `prior` document already holding a prefix of that list — only the tail.
 *
 * Decoded ONCE and encoded once, however many fragments there are. Going
 * through the bytes-level entry point per fragment would decode and re-encode
 * the document each time, making a burst of N edits cost N whole-document round
 * trips — which is the shape this keyspace exists to avoid, reintroduced on the
 * read path. `prior` removes the remaining decode as well, so a keystroke
 * appended to a fold that is already cached costs the encode alone.
 *
 * Returns undefined when the base will not decode; the field is then left as
 * it is, and no document is cached for it.
 *
 * A fragment that will not apply is dropped rather than thrown (see foldTextLog
 * for why). `applyFragment` may mutate before it fails, so recovery rebuilds
 * from the base and the fragments already known to apply rather than trusting a
 * half-applied document — including `prior`, which is why a failure gives up on
 * it too. That path costs a re-decode per failure, which is the right trade for
 * something that only happens when a rejected mutation stranded the edits
 * queued behind it.
 */
function foldFragments(
	base: Uint8Array,
	list: readonly Uint8Array[],
	entity: string,
	field: string,
	prior?: { doc: TextDoc; applied: number },
): { doc: TextDoc; bytes: Uint8Array } | undefined {
	const applied: Uint8Array[] = prior === undefined ? [] : [ ...list.slice(0, prior.applied) ];
	let doc = prior?.doc;
	if (doc === undefined) {
		try {
			doc = decodeTextDoc(base, '');
		} catch {
			return undefined;
		}
	}
	for (const fragment of list.slice(applied.length)) {
		try {
			applyFragment(doc, decodeFragment(fragment));
			applied.push(fragment);
		} catch (err) {
			reportError(`text: a local edit to ${entity}/${field} could not be replayed`, err);
			try {
				doc = decodeTextDoc(base, '');
			} catch {
				return undefined;
			}
			for (const good of applied) applyFragment(doc, decodeFragment(good));
		}
	}
	return { doc, bytes: applied.length === 0 ? base : encodeTextDoc(doc) };
}

/**
 * Whether a row exists in the merged view, without decoding it or folding its
 * text log. The question a PATCH's delete-wins check asks, and all it needs:
 * the answer turns on the overlay's tombstone and the row's presence, neither
 * of which the text log can change (a log entry only ever edits a field of a
 * row that is already there).
 */
export async function rowPresent(
	tx: KVRead,
	entity: string,
	workspaceId: string,
	key: string,
): Promise<boolean> {
	const overlay = (await tx.get(overlayKey(entity, workspaceId, key))) as OverlayRecord | undefined;
	if (overlay !== undefined) return !overlay.del;
	return await tx.has(confirmedKey(entity, workspaceId, key));
}

/** Resolves a single row overlay-first. Returns the row bytes or undefined. */
export async function readMergedRow(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	workspaceId: string,
	key: string,
): Promise<Uint8Array | undefined> {
	const overlay = (await tx.get(overlayKey(entity, workspaceId, key))) as OverlayRecord | undefined;
	// The confirmed row only counts when no overlay shadows it, so an overlaid
	// row costs one read rather than two.
	const confirmed = overlay !== undefined
		? undefined
		: ((await tx.get(confirmedKey(entity, workspaceId, key))) as ConfirmedRecord | undefined);
	return await mergedRowFrom(tx, registry, entity, workspaceId, key, overlay, confirmed);
}

/**
 * Resolves a row overlay-first from records the caller already holds, folding
 * the row's optimistic text log in as readMergedRow does. Returns the row bytes,
 * or undefined when the row is absent or locally deleted.
 *
 * `confirmed` is consulted only when there is no overlay, so a caller that read
 * both may pass both. The records must be the row as of this call: read them in
 * `tx`, and pass them only before anything in it has written the row.
 */
export async function mergedRowFrom(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	workspaceId: string,
	key: string,
	overlay: OverlayRecord | undefined,
	confirmed: ConfirmedRecord | undefined,
): Promise<Uint8Array | undefined> {
	const base = overlay !== undefined ? (overlay.del ? undefined : overlay.v) : confirmed?.v;
	if (base === undefined) return undefined;
	return await foldTextLog(tx, registry, entity, workspaceId, key, base);
}

/** A merged-view row: its bytes plus the confirmed base version (see Row.version). */
export interface MergedRow {
	v: Uint8Array;
	version: bigint;
}

/**
 * Resolves rows of one entity overlay-first, in one getMany for all the overlay
 * and confirmed records together. The result is parallel to `keys`; a row that
 * is absent or locally deleted is undefined in its slot.
 *
 * Each row carries its confirmed base version — the workspace sequence it was
 * last server-acked at, or 0n when it exists only optimistically (a local
 * create not yet acked). The version reflects the confirmed base even when an
 * optimistic overlay shadows it, which is the base an expectedVersion CAS guard
 * wants.
 */
export async function readMergedRowsWithVersion(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	keys: readonly { workspaceId: string; key: string }[],
): Promise<(MergedRow | undefined)[]> {
	const records = await tx.getMany([
		...keys.map(({ workspaceId, key }) => overlayKey(entity, workspaceId, key)),
		...keys.map(({ workspaceId, key }) => confirmedKey(entity, workspaceId, key)),
	]);
	const out: (MergedRow | undefined)[] = new Array(keys.length);
	for (let i = 0; i < keys.length; i++) {
		const overlay = records[i] as OverlayRecord | undefined;
		const confirmed = records[keys.length + i] as ConfirmedRecord | undefined;
		const base = overlay !== undefined
			? (overlay.del ? undefined : overlay.v)
			: confirmed?.v;
		if (base === undefined) continue;
		const { workspaceId, key } = keys[i]!;
		out[i] = {
			v: await foldTextLog(tx, registry, entity, workspaceId, key, base),
			version: confirmed ? BigInt(confirmed.ver) : 0n,
		};
	}
	return out;
}

/** A merged-view row plus the workspace it belongs to. */
export interface ScopedRow extends MergedRow {
	workspaceId: string;
	key: string;
}

/**
 * Which workspace prefixes a scan covers: one, a named set, or — when absent —
 * every workspace in the store.
 *
 * A named set is not a shorthand for "all": an empty array names no workspace
 * and reads nothing.
 */
export type WorkspaceScope = string | readonly string[];

/**
 * Which workspaces one read spans, as the caller asked for it: one by id, a set
 * it named, or — when absent — every workspace the client holds.
 */
export type ReadScope = string | NamedWorkspaces | undefined;

/**
 * A read bounded to workspaces the caller named, optionally with a predicate
 * chosen per workspace.
 *
 * The set bounds what is read, so the cost is the named prefixes rather than the
 * entity across every workspace. A set of exactly one is read on that
 * workspace's prefix, shared rows and indexes — the paths a single-workspace
 * read already takes.
 */
export interface NamedWorkspaces {
	/**
	 * Workspaces the read spans, deduplicated and in key-segment order (see
	 * compareWorkspaces), which is the order the merged result's final tie-break
	 * resolves in. An empty set reads nothing.
	 */
	workspaces: readonly string[];
	/**
	 * The predicate each workspace is read under, replacing the read's own
	 * `where`. Only the predicate may vary: one result has one order.
	 *
	 * Every workspace in `workspaces` has an entry; a workspace to be excluded is
	 * left out of the set rather than mapped to nothing.
	 */
	predicates?: ReadonlyMap<string, Cond>;
}

/**
 * Scans live rows of an entity, overlay-first, in key order. One workspace
 * scans that workspace's contiguous prefix and a named set scans one prefix
 * each; with no scope it is a single prefix scan of every workspace in the
 * store, which is what the entity-first key order buys.
 *
 * Results are ordered by (workspace, key) wherever the scope spans more than
 * one workspace and by key within a single one — the order the underlying
 * prefix scans yield, with a named set's workspaces visited in key-segment
 * order so the two agree.
 */
export async function scanMergedRows(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	scope?: WorkspaceScope,
): Promise<ScopedRow[]> {
	const out: ScopedRow[] = [];
	await forEachMergedRow(tx, registry, entity, scope, (row) => {
		out.push(row);
	});
	return out;
}

/**
 * {@link scanMergedRows}, handing each row to `visit` as the scan reaches it
 * instead of collecting them. Rows arrive in the order scanMergedRows returns
 * them, and `visit` has run for every one of them by the time the promise
 * settles.
 *
 * `visit` runs between the store's chunk reads, so whatever it does — decoding
 * the row, above all — overlaps the read of the rows that follow. That is the
 * reason to prefer it over mapping the array.
 */
export async function forEachMergedRow(
	tx: KVRead,
	registry: EntityRegistry,
	entity: string,
	scope: WorkspaceScope | undefined,
	visit: (row: ScopedRow) => void,
): Promise<void> {
	if (typeof scope === 'object') {
		for (const ws of [ ...new Set(scope) ].sort(compareWorkspaces)) {
			await forEachMergedRow(tx, registry, entity, ws, visit);
		}
		return;
	}
	const workspaceId = scope;
	const cPrefix = workspaceId === undefined
		? confirmedPrefix(entity)
		: confirmedWsPrefix(entity, workspaceId);
	const oPrefix = workspaceId === undefined
		? overlayPrefix(entity)
		: overlayWsPrefix(entity, workspaceId);
	const split = (suffix: string): { workspaceId: string; key: string } =>
		workspaceId === undefined ? splitWsKey(suffix) : { workspaceId, key: suffix };
	// The overlay comes first so that what an unacked write shadows is settled
	// before the confirmed prefix — the scan that actually costs something — is
	// walked, which is what lets a confirmed row be handed on the moment it is
	// read. The overlay is bounded by the pending queue; the confirmed prefix is
	// the whole entity.
	//
	// Suffixes (`<wsSegment>/<key>` or `<key>`) are exactly what the two
	// prefixes share, so the overlay lines up with the confirmed row without
	// re-encoding the workspace, and both prefix scans yield them in the same
	// ascending order — hence one merging pass rather than a map and a sort.
	const overlay: { suffix: string; record: OverlayRecord }[] = [];
	for await (const [ k, v ] of tx.scan(oPrefix)) {
		overlay.push({ suffix: k.slice(oPrefix.length), record: v as OverlayRecord });
	}
	const texts = registry.textPathsOf(entity).length === 0
		? undefined
		: await readTextLogs(tx, entity, workspaceId);
	const emit = (suffix: string, row: ScopedRow): void => {
		const fields = texts?.get(suffix);
		if (fields === undefined) {
			visit(row);
			return;
		}
		visit({ ...row, v: applyTextLog(registry, entity, row.workspaceId, row.key, fields, row.v) });
	};
	let next = 0;
	const overlayOnly = (upto: string | undefined): void => {
		for (; next < overlay.length; next++) {
			const { suffix, record } = overlay[next]!;
			if (upto !== undefined && suffix >= upto) return;
			// A delete of a row with no confirmed base removes nothing.
			if (record.del) continue;
			emit(suffix, { ...split(suffix), v: record.v, version: 0n });
		}
	};
	for await (const [ k, v ] of tx.scan(cPrefix)) {
		const suffix = k.slice(cPrefix.length);
		overlayOnly(suffix);
		const version = BigInt((v as ConfirmedRecord).ver);
		if (next < overlay.length && overlay[next]!.suffix === suffix) {
			const { record } = overlay[next]!;
			next++;
			if (record.del) continue;
			// Keep the confirmed base version under an optimistic edit.
			emit(suffix, { ...split(suffix), v: record.v, version });
			continue;
		}
		emit(suffix, { ...split(suffix), v: (v as ConfirmedRecord).v, version });
	}
	overlayOnly(undefined);
}

/**
 * The workspaces holding rows of an entity — confirmed or overlay — in
 * key-segment order, which is the order an unscoped scanMergedRows visits them
 * in. A workspace the client holds but has no rows for is absent, and one it
 * no longer holds is present while its rows remain.
 *
 * Costs one seek per workspace found plus one per keyspace, never a walk of
 * the rows: it is what lets a read spanning every workspace be served as one
 * index leg per workspace (see NamedWorkspaces) and still cover exactly the
 * rows the scan would.
 */
export async function workspacesWithRows(tx: KVRead, entity: string): Promise<string[]> {
	const segments = new Set<string>();
	for (const prefix of [ confirmedPrefix(entity), overlayPrefix(entity) ]) {
		// Every key under the prefix is `<prefix><segment>/<key>`. A segment holds
		// no slash, and `0` is the code unit after `/`, so `<prefix><segment>0`
		// bounds the workspace's keys from above and resuming there lands on the
		// first key of the next one. The prefix ends in `/` too, which makes the
		// same step its own upper bound.
		const hi = `${prefix.slice(0, -1)}0`;
		let lo = prefix;
		for (;;) {
			const key = await tx.seek(lo, hi);
			if (key === undefined) break;
			// A key with no slash after the prefix is not a row key; taking its whole
			// tail as the segment still steps past it rather than seeking it again.
			const end = key.indexOf('/', prefix.length);
			const segment = end === -1 ? key.slice(prefix.length) : key.slice(prefix.length, end);
			segments.add(segment);
			lo = `${prefix}${segment}0`;
		}
	}
	return [ ...segments ].map(wsFromSegment).sort(compareWorkspaces);
}

/**
 * The scan's counterpart to foldTextLog: one prefix scan of the whole entity's
 * text log, grouped by the row each entry belongs to, rather than a scan per
 * row. Rows are keyed by the same `<wsSegment>/<key>` (or `<key>`) suffix the
 * row keyspaces share, so a log entry lines up with its row without re-encoding
 * the workspace. Undefined when the log holds nothing for the scope.
 */
async function readTextLogs(
	tx: KVRead,
	entity: string,
	workspaceId: string | undefined,
): Promise<Map<string, Map<string, Uint8Array[]>> | undefined> {
	const prefix = workspaceId === undefined
		? overlayTextPrefix(entity)
		: overlayTextWsPrefix(entity, workspaceId);
	const entityPrefix = overlayTextPrefix(entity);
	let byRow: Map<string, Map<string, Uint8Array[]>> | undefined;
	for await (const [ k, v ] of tx.scan(prefix)) {
		// Split from the entity prefix, not the scan prefix: an `ot/` key always
		// carries its workspace segment, and a cross-workspace scan needs it back
		// in the suffix to line up with the rows, which are keyed the same way.
		const rest = k.slice(entityPrefix.length);
		const parts = splitOverlayTextSuffix(rest);
		if (parts === undefined) continue;
		const suffix = workspaceId === undefined
			? `${rest.slice(0, rest.indexOf('/'))}/${parts.key}`
			: parts.key;
		byRow ??= new Map();
		let fields = byRow.get(suffix);
		if (fields === undefined) byRow.set(suffix, fields = new Map());
		const list = fields.get(parts.field);
		if (list === undefined) fields.set(parts.field, [ v as Uint8Array ]);
		else list.push(v as Uint8Array);
	}
	return byRow;
}

export interface Row<T = unknown> {
	key: string;
	value: T;
	/**
	 * Workspace the row lives in. Always set — a row is only ever inside one
	 * workspace partition — and it is what makes a cross-workspace result
	 * attributable (and routable back to the right `client.ws(id)` for a write).
	 */
	workspaceId: string;
	/**
	 * Server-confirmed version this row is based on — the workspace sequence
	 * that last wrote it. 0n for rows that exist only optimistically (a local
	 * create not yet acked). Pass as `expectedVersion` to a mutate call for a
	 * compare-and-swap that rejects if the row moved server-side since you read.
	 */
	version: bigint;
}

export async function scanDecoded<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	scope?: WorkspaceScope,
): Promise<Row<MessageShape<D>>[]> {
	const rows: Row<MessageShape<D>>[] = [];
	await forEachMergedRow(tx, registry, def.name, scope, (row) => {
		const value = fromBinary(schema, row.v);
		sortOrderedFields(schema, value);
		rows.push({ key: row.key, value, workspaceId: row.workspaceId, version: row.version });
	});
	return rows;
}

/**
 * scanDecoded, with each row decoded through `view` — a projection holding only
 * the fields the caller's query reads (see query/projection.ts) — instead of
 * through `schema`.
 *
 * `sources` gives each returned row the merged-view bytes it was decoded from,
 * so a caller keeping some of them can decode those in full with
 * {@link decodeWholeRow} rather than reading the store again. It is keyed by row
 * identity, which filtering, sorting and slicing the rows preserve.
 */
export async function scanProjected<D extends DescMessage>(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	schema: D,
	view: DescMessage,
	scope?: WorkspaceScope,
): Promise<{ rows: Row<MessageShape<D>>[]; sources: Map<Row<MessageShape<D>>, Uint8Array> }> {
	const rows: Row<MessageShape<D>>[] = [];
	const sources = new Map<Row<MessageShape<D>>, Uint8Array>();
	await forEachMergedRow(tx, registry, def.name, scope, (row) => {
		// Retaining unknown fields would keep exactly the bytes the projection
		// exists to skip.
		const value = fromBinary(view, row.v, {
			readUnknownFields: false,
		}) as unknown as MessageShape<D>;
		sortOrderedFields(schema, value);
		const out = { key: row.key, value, workspaceId: row.workspaceId, version: row.version };
		rows.push(out);
		sources.set(out, row.v);
	});
	return { rows, sources };
}

/**
 * One row decoded in full from the merged-view bytes it was projected from —
 * the value {@link scanDecoded} would have given it.
 */
export function decodeWholeRow<D extends DescMessage>(
	schema: D,
	row: Row<unknown>,
	bytes: Uint8Array,
): Row<MessageShape<D>> {
	const value = fromBinary(schema, bytes);
	sortOrderedFields(schema, value);
	return { key: row.key, value, workspaceId: row.workspaceId, version: row.version };
}
