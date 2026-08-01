import { fromBinary, toBinary } from '@bufbuild/protobuf';

import {
	type CrudMutation,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../gen/syncplane/v1/mutation_pb.js';
import type { RowChange } from '../gen/syncplane/v1/patch_pb.js';
import { confirmedKey, type ConfirmedRecord } from '../keys.js';
import type { KVRead } from '../kv/store.js';
import { type EntityRegistry, setTextFieldBytes, textFieldBytes } from '../mutations.js';
import { readMergedRow } from '../read.js';
import {
	applyFragment,
	applyFragmentBytes,
	decodeFragment,
	decodeTextDoc,
	encodeFragment,
	encodeTextDoc,
	isFragment,
	mergeFragments,
} from '../text/index.js';
import { textResidency } from '../text/residency.js';

/**
 * Delta state on the wire, both directions: a text edit travels as the fragment
 * it added rather than as the whole document, in a form that reconstructs to
 * exactly the bytes full state would have produced.
 *
 * Upstream (toWireMutations) makes every queued text PATCH carry exactly what
 * the writer holds beyond the row the server last confirmed. Downstream
 * (applyTextDeltas) is the mirror image: the server ships fragments cut against
 * the client's cursor position, and the client folds them back into the
 * confirmed row it already holds.
 *
 * Queued PATCHes reach this module in either form. A live text handle queues
 * the fragment for its one edit (text/handle.ts), so its keystroke costs
 * O(edit) in the pending queue; every other writer — string-valued text fields,
 * a re-send after `text_full_state_required` — queues full state. Upstream
 * normalizes both to the same thing: the cumulative fragment against the
 * confirmed row. For full state that is a diff; for the handle's fragments it
 * is their union, accumulated across the queue in id order, which is the same
 * state the diff would have produced without decoding a document to find it.
 *
 * Accumulating rather than shipping each fragment alone is deliberate. It
 * keeps every pushed mutation independently applicable, exactly as it was when
 * the queue held full state: shipping one edit's fragment alone would leave
 * every later one dangling behind a mid-queue rejection. The cost — a burst
 * ships each of its prefixes — is what queue compaction removes by folding the
 * burst into one mutation before it is ever pushed (optimistic/compact.ts).
 *
 * The substitution happens at push time, on a copy: every path that READS a row
 * — stored rows, rebase replay, the merged view — sees a whole document, so
 * nothing downstream has to understand fragments. (Optimistic text edits are
 * themselves stored as fragments, in the `ot/` log; read.ts folds them back
 * before anything here sees a row, which is what keeps that true.) Deferring to
 * push time also means the diff base is read at the last possible moment, when
 * it is as close to the server's state as this client can know.
 *
 * Soundness rests on one relation in each direction: the fragment's base must
 * be state the receiver already has. Confirmed rows (`c/`) are written only
 * from pulls and bootstraps, so the server's state always covers them, and the
 * server cuts its own fragments against the cursor those same pulls advanced.
 * `encodeFragment` separately refuses a base the local value is not itself
 * built on, which is the other half — and both failures fall back to full state
 * rather than to a wrong merge. Each side's guard (the server rejects a
 * fragment that does not apply and leaves the row untouched; the client
 * discards the page and re-bootstraps) is the backstop for the case neither
 * side can see.
 */

/**
 * How much smaller than the full document a fragment must be to be worth
 * sending. Below the threshold the saving does not pay for the extra decode
 * path on both sides, and a self-contained fragment of a fresh document is
 * roughly the size of the document itself. Byte-observable, but not
 * cross-language: only the client decides what to send.
 */
export const fragmentSizeRatio = 0.5;

/** Identity of a row pinned to full-state text writes. */
export function fullStateKey(workspaceId: string, entity: string, key: string): string {
	return `${workspaceId}\u0000${entity}\u0000${key}`;
}

/**
 * What one text field of one row has queued beyond the confirmed row, as a
 * fragment — or null once this pass can no longer express that, after which the
 * field's remaining writes go out as full state read back from the merged row.
 */
type Accumulated = Map<string, Uint8Array | null>;

const accKey = (entity: string, key: string, path: string): string =>
	`${entity}\u0000${key}\u0000${path}`;

/**
 * Returns the mutations to put on the wire for one workspace: the same list,
 * with each text field carrying everything queued for it beyond the confirmed
 * row. Mutations that change are deep-copied; the rest are passed through by
 * reference, so a queue with no text in it costs one type lookup per crud.
 *
 * The accumulation runs over `pending`, which is exactly what the server has
 * not acked — so a fragment built here always has a base the server holds,
 * whether that base is the confirmed row or an earlier mutation of this same
 * push.
 *
 * `pinned` names rows (fullStateKey) that must keep shipping full documents
 * because the server already refused a fragment for them.
 */
export async function toWireMutations(
	tx: KVRead,
	registry: EntityRegistry,
	workspaceId: string,
	pending: readonly Mutation[],
	pinned: ReadonlySet<string>,
): Promise<Mutation[]> {
	const out: Mutation[] = [];
	const acc: Accumulated = new Map();
	for (const m of pending) {
		const cruds = crudsOf(m);
		if (!cruds?.some((c) => textPathsIn(registry, c).length > 0)) {
			out.push(m);
			continue;
		}
		// Planned against the original, applied to a copy: the queue entry itself
		// must not change, and a later rebase replays from it.
		const planned = new Map<number, Uint8Array>();
		let index = -1;
		for (const crud of cruds) {
			index++;
			const value = await planCrud(tx, registry, workspaceId, crud, acc, pinned);
			if (value !== undefined) planned.set(index, value);
		}
		if (planned.size === 0) {
			out.push(m);
			continue;
		}
		const copy = fromBinary(MutationSchema, toBinary(MutationSchema, m));
		const copies = crudsOf(copy) ?? [];
		for (const [ i, value ] of planned) copies[i]!.value = value;
		out.push(copy);
	}
	return out;
}

function crudsOf(m: Mutation): CrudMutation[] | undefined {
	return m.payload.case === 'crud'
		? [ m.payload.value ]
		: m.payload.case === 'batch'
		? m.payload.value.mutations
		: undefined;
}

/**
 * The text fields one crud writes: everything the entity declares for a PUT
 * (which carries a whole row), only what the mask names outright for a PATCH.
 * A mask naming just an ancestor replaces that subtree whole, so the field is
 * not written through the text merger and carries no delta state.
 */
function textPathsIn(registry: EntityRegistry, crud: CrudMutation): readonly string[] {
	const paths = registry.textPathsOf(crud.entity);
	if (paths.length === 0 || crud.op === CrudOp.DELETE) return [];
	if (crud.op !== CrudOp.PATCH) return paths;
	const mask = crud.fieldMask?.paths ?? [];
	return paths.filter((p) => mask.includes(p));
}

/**
 * Whether this crud may carry delta state on the wire at all. Only a PATCH
 * runs its text fields through the server's merger; a PUT replaces the row and
 * a pinned row has already had a fragment refused, so both need whole
 * documents.
 */
function shipsFragments(
	workspaceId: string,
	crud: CrudMutation,
	pinned: ReadonlySet<string>,
): boolean {
	return crud.op === CrudOp.PATCH
		&& !pinned.has(fullStateKey(workspaceId, crud.entity, crud.key));
}

/**
 * Folds one crud into the accumulation and returns its replacement value bytes,
 * or undefined to ship it as queued. Every text field it writes is normalized
 * to the same meaning — the state queued for that field beyond the confirmed
 * row — whichever form the queue entry holds it in.
 *
 * Nothing here can make a write incorrect: on any failure the field falls back
 * to a whole document, which always applies.
 */
async function planCrud(
	tx: KVRead,
	registry: EntityRegistry,
	workspaceId: string,
	crud: CrudMutation,
	acc: Accumulated,
	pinned: ReadonlySet<string>,
): Promise<Uint8Array | undefined> {
	const paths = textPathsIn(registry, crud);
	const def = registry.get(crud.entity);
	if (paths.length === 0 || !def) {
		if (crud.op === CrudOp.DELETE) {
			// The row is gone; anything queued for it before is not a base for
			// anything queued after (a PATCH of a deleted row is rejected).
			for (const path of registry.textPathsOf(crud.entity)) {
				acc.delete(accKey(crud.entity, crud.key, path));
			}
		}
		return undefined;
	}
	const record = (await tx.get(confirmedKey(def.name, workspaceId, crud.key))) as
		| ConfirmedRecord
		| undefined;
	let confirmed;
	let local;
	try {
		confirmed = record === undefined ? undefined : fromBinary(def.schema, record.v);
		local = fromBinary(def.schema, crud.value);
	} catch {
		return undefined;
	}
	const asFragment = shipsFragments(workspaceId, crud, pinned);
	let changed = false;
	for (const path of paths) {
		const key = accKey(crud.entity, crud.key, path);
		const queued = textFieldBytes(def.schema, path, local);
		if (queued === undefined) continue;
		const confirmedBytes = confirmed && textFieldBytes(def.schema, path, confirmed);
		const total = isFragment(queued)
			? foldFragment(acc, key, queued)
			: diffFullState(acc, key, queued, confirmedBytes);
		if (total === null) {
			// The field can no longer be described as a delta of the confirmed row.
			// The merged row is the one state that is always both available and
			// self-contained; over-shipping later edits converges, since every text
			// write is a union.
			if (!isFragment(queued)) continue; // already full state — ship as queued
			const full = await readMergedTextField(tx, registry, workspaceId, crud, path);
			if (full === undefined) continue;
			setTextFieldBytes(def.schema, path, local, full);
			changed = true;
			continue;
		}
		if (asFragment) {
			// Worth it only when the delta is meaningfully smaller than the document
			// it stands in for; a fragment of a freshly created document is roughly
			// the document. The comparison needs a size for the whole state, which is
			// only known for free when the queue held it.
			if (!isFragment(queued) && total.length >= queued.length * fragmentSizeRatio) continue;
			if (isFragment(queued) && bytesEqual(total, queued)) continue; // already exact
			setTextFieldBytes(def.schema, path, local, total);
			changed = true;
			continue;
		}
		if (!isFragment(queued)) continue; // full state, shipped as queued
		const full = expandFragment(confirmedBytes, total);
		if (full === undefined) continue;
		setTextFieldBytes(def.schema, path, local, full);
		changed = true;
	}
	return changed ? toBinary(def.schema, local) : undefined;
}

/** Unions a queued fragment onto the field's accumulation. */
function foldFragment(acc: Accumulated, key: string, queued: Uint8Array): Uint8Array | null {
	const prior = acc.get(key);
	if (prior === null) return null;
	if (prior === undefined) {
		acc.set(key, queued);
		return queued;
	}
	try {
		const merged = mergeFragments(prior, queued);
		acc.set(key, merged);
		return merged;
	} catch {
		acc.set(key, null);
		return null;
	}
}

/**
 * Replaces the field's accumulation with the delta of a queued whole document
 * against the confirmed row — that document supersedes everything queued for
 * the field before it.
 */
function diffFullState(
	acc: Accumulated,
	key: string,
	queued: Uint8Array,
	confirmedBytes: Uint8Array | undefined,
): Uint8Array | null {
	try {
		const fragment = encodeFragment(
			decodeTextDoc(queued, ''),
			confirmedBytes === undefined ? null : decodeTextDoc(confirmedBytes, ''),
		);
		acc.set(key, fragment);
		return fragment;
	} catch {
		// The confirmed row is not an ancestor of the queued value (it moved on
		// under a pending edit), or one of them does not decode.
		acc.set(key, null);
		return null;
	}
}

/**
 * Applies one arriving fragment to the confirmed field's bytes.
 *
 * The residency-aware form of `applyFragmentBytes`: the confirmed document is
 * taken back out of text/residency.ts whenever it is still decoded there — which
 * it is if this client rendered, folded, or applied an earlier delta to this
 * field — so a remote keystroke costs an encode instead of a decode and an
 * encode. That is worth more than the latency it saves, because this runs inside
 * the KV write transaction, which is held exclusively across tabs for its whole
 * duration: it bounds every other writer, the way the server's write closure
 * bounds a workspace.
 *
 * The merged document is left registered under the bytes it produced, where the
 * next fragment for this field and any reader rendering the row will find it. A
 * throw registers nothing — `applyFragment` may already have mutated a document
 * that now encodes to bytes nobody holds — and the cost of that is a miss.
 */
function foldIntoConfirmed(base: Uint8Array, fragment: Uint8Array): Uint8Array {
	const doc = textResidency.take(base) ?? decodeTextDoc(base, '');
	applyFragment(doc, decodeFragment(fragment));
	const bytes = encodeTextDoc(doc);
	textResidency.put(bytes, doc);
	return bytes;
}

/** Expands a fragment back to the whole document it stands for. */
function expandFragment(
	confirmedBytes: Uint8Array | undefined,
	fragment: Uint8Array,
): Uint8Array | undefined {
	try {
		return applyFragmentBytes(confirmedBytes ?? new Uint8Array(), fragment);
	} catch {
		return undefined;
	}
}

/**
 * Reads one text field out of the merged row — the local state including every
 * pending edit. The last-resort source of a whole document for a field whose
 * queued value is a fragment.
 */
async function readMergedTextField(
	tx: KVRead,
	registry: EntityRegistry,
	workspaceId: string,
	crud: CrudMutation,
	path: string,
): Promise<Uint8Array | undefined> {
	const def = registry.get(crud.entity);
	if (!def) return undefined;
	const bytes = await readMergedRow(tx, registry, def.name, workspaceId, crud.key);
	if (bytes === undefined) return undefined;
	try {
		return textFieldBytes(def.schema, path, fromBinary(def.schema, bytes));
	} catch {
		return undefined;
	}
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * Thrown when a row change's delta state cannot be turned back into a full row.
 * It means this client's confirmed state is not what the server assumed when it
 * cut the fragments, so the page as a whole is untrustworthy: the only recovery
 * is to drop it and re-bootstrap. Never a normal outcome — the server declines
 * to send delta state wherever the client's prior state is not knowable.
 */
export class TextDeltaUnusableError extends Error {
	constructor(entity: string, key: string, field: string, cause?: unknown) {
		super(
			`cannot reconstruct text field "${field}" of ${entity}/${key} from delta state`,
			cause === undefined ? undefined : { cause },
		);
		this.name = 'TextDeltaUnusableError';
	}
}

/**
 * Rebuilds the full row bytes of a change that carries text delta state, using
 * the confirmed row this client already holds. Returns the change's value
 * unchanged when it carries no deltas, so callers can apply this to every row.
 *
 * A fragment applies to the confirmed field's bytes; an EMPTY fragment means
 * the field did not move over the span and the confirmed bytes carry over as
 * they are. Either way the reconstructed row is byte-identical to the full
 * snapshot the server would otherwise have sent — that equivalence is what lets
 * the rest of the client keep seeing whole documents everywhere.
 */
export async function applyTextDeltas(
	tx: KVRead,
	registry: EntityRegistry,
	workspaceId: string,
	change: RowChange,
	value: Uint8Array,
): Promise<Uint8Array> {
	if (change.textDeltas.length === 0) return value;
	const def = registry.get(change.entity);
	if (!def) {
		throw new TextDeltaUnusableError(change.entity, change.key, change.textDeltas[0]!.field);
	}
	const record = (await tx.get(confirmedKey(def.name, workspaceId, change.key))) as
		| ConfirmedRecord
		| undefined;
	let next;
	let confirmed;
	try {
		next = fromBinary(def.schema, value);
		confirmed = record === undefined ? undefined : fromBinary(def.schema, record.v);
	} catch (err) {
		throw new TextDeltaUnusableError(change.entity, change.key, change.textDeltas[0]!.field, err);
	}
	for (const delta of change.textDeltas) {
		const base = confirmed && textFieldBytes(def.schema, delta.field, confirmed);
		if (delta.fragment.length === 0) {
			// "Unchanged over the span." The server only says that about a field
			// it currently holds a document for, so an absent base means this
			// client's confirmed row is not the one the server diffed against.
			if (base === undefined) {
				throw new TextDeltaUnusableError(change.entity, change.key, delta.field);
			}
			setTextFieldBytes(def.schema, delta.field, next, base);
			continue;
		}
		try {
			// An absent base is legitimate for a row created inside the span: the
			// fragment is then self-contained and applies to the empty document.
			// One that is not self-contained fails here rather than silently
			// producing a document missing the text it was built on.
			setTextFieldBytes(
				def.schema,
				delta.field,
				next,
				foldIntoConfirmed(base ?? new Uint8Array(), delta.fragment),
			);
		} catch (err) {
			throw new TextDeltaUnusableError(change.entity, change.key, delta.field, err);
		}
	}
	return toBinary(def.schema, next);
}
