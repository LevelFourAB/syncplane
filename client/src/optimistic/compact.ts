import {
	clone,
	create,
	type DescField,
	fromBinary,
	isFieldSet,
	type Message,
	toBinary,
} from '@bufbuild/protobuf';

import { writeChangesAccess } from '../access.js';
import {
	type CrudMutation,
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../gen/syncplane/v1/mutation_pb.js';
import { confirmedKey, metaLastMutationId, pendingKey } from '../keys.js';
import type { KVRead, KVStore } from '../kv/store.js';
import {
	type EntityDef,
	type EntityMergers,
	type EntityRegistry,
	mergeMask,
	mergeTextFieldsInto,
	textFieldBytes,
	textInstancePaths,
} from '../mutations.js';
import { isFragment, mergeFragments, mergeText } from '../text/index.js';
import { TouchSet } from '../touch.js';
import {
	enqueuePending,
	moveUndoTokens,
	readPending,
	readPushedThrough,
	readUndoTokens,
	recountPendingAccess,
} from './queue.js';
import { clearOverlayAndReplay, type MirrorMap } from './rebase.js';

/**
 * Pending-queue compaction. Offline, every edit enqueues its own mutation, so a
 * long offline burst leaves a queue that costs O(pending × pages) to replay on
 * the first rebase after reconnect and ships in many push batches. Compaction
 * squashes successive CRUDs to the same (entity, key) — a burst of PATCHes to
 * the same fields, repeated PUTs or DELETEs, and under one compare-and-swap
 * guard also PATCHes with different masks, PUT+PATCHes and PUT/PATCH+DELETE —
 * so the queue the rebase replays and the push ships is short.
 *
 * The merge is overlay-preserving by construction (per-field LWW, applied in
 * order), so the merged view a reader sees is the same either side of it. The
 * overlay is still rebuilt from the renumbered queue, because the optimistic
 * text log is keyed by the mutation id that wrote it and the renumbering frees
 * those ids to be minted again — see compactPending.
 *
 * Safety rails (see planCompaction):
 *  - Only the never-pushed tail (id > pushedThrough) is touched. Anything the
 *    server may already have seen stays byte-for-byte, for idempotency. The
 *    watermark is the durable, cross-tab META_PUSHED_THROUGH (read inside the
 *    compaction transaction), advanced before a push goes out — so neither a
 *    lost push response nor a concurrent tab's push can leave us folding ids
 *    the server already applied.
 *  - The server requires per-client mutation ids to arrive gap-free, so the
 *    surviving tail is RENUMBERED contiguously from pushedThrough+1. Undo
 *    bookkeeping references ids, so a remap of old→new id is returned.
 *  - custom/batch mutations and mutations an undo/redo entry still references
 *    act as barriers: they are never merged away, so undo stays exact (its ids
 *    are merely remapped). A write that carries fields this build does not
 *    declare (queued by a newer build on a shared store) is a barrier too, so
 *    it reaches the server with those fields.
 *  - A write that can change what the caller holds — a write to a grant row,
 *    or the delete of a row that rows of another entity sit below — is a
 *    barrier across every other key. The server decides access to a row from
 *    other rows at the position of each write, so a later write folded onto a
 *    slot before such a write would be judged as if made before it.
 *  - Successive ops on one key fold only when their compare-and-swap guards
 *    AGREE — both absent, or both naming the same version, in which case the
 *    folded slot keeps that guard. Folding across disagreeing guards would
 *    rewrite what the caller asked for: onto a guard, and an unguarded write
 *    gains a rejection it never had; off one, and an explicit guard is silently
 *    dropped. Equal guards are the case that matters, because only a pull moves
 *    the confirmed version a caller reads — so a burst of edits between two
 *    pulls all name the same one. Folding them is what keeps the second from
 *    reaching the server after the first has already pushed the row past the
 *    guard, which is a conflict the client raised against nobody but itself.
 *  - Unguarded ops fold only when they are the same op and, for a PATCH, carry
 *    the same field mask (see sameWrite). The server can refuse a write for
 *    reasons the client cannot see — a permission hook, a field protected for
 *    this caller only, validation — and it gives the folded write one verdict.
 *    Folding a write it would accept with one it refuses therefore loses the
 *    accepted one, and nothing ever pushes it again. Writes of the same op to
 *    the same fields get the same verdict from every check that does not read
 *    values; for a check that does, the fold keeps the later value of each
 *    field, which is the one the user last asked for. Text has no "later
 *    value": a PUT unions its documents with the stored ones, so a PUT folds
 *    onto an earlier PUT only when it already carries all of that PUT's text.
 *  - A fold never makes a write past the engine's default write limits
 *    (MaxValueBytes, MaxFieldMaskPaths). The server refuses an over-limit
 *    write whole, which would lose every write in the slot.
 *  - An unguarded PUT never folds onto a PUT that may create the row (no
 *    confirmed row, or a DELETE before it). A refusal of the later value
 *    would otherwise take the row itself with it, not only an earlier value.
 */
export interface CompactionPlan {
	/** New queue in id order: the untouched pushed prefix + renumbered tail. */
	mutations: Mutation[];
	/** Old tail mutation id → new id of the slot that absorbed it. */
	remap: Map<bigint, bigint>;
	/** True when the plan drops and/or renumbers anything. */
	changed: boolean;
}

/** A merge slot under construction while folding the tail. */
interface Slot {
	/** The folded CRUD when this slot can still absorb ops; undefined for a barrier. */
	crud?: CrudMutation;
	/** A passthrough mutation (custom/batch/undo-referenced) reused verbatim. */
	passthrough?: Mutation;
	clientId: string;
	/** Old ids folded into this slot (all remap to the slot's new id). */
	sources: bigint[];
	/** The slot opened with a PUT on a row that may not exist before it. */
	creates?: boolean;
}

/**
 * Plans a compaction of the pending queue. Pure; does no I/O.
 *
 * `stored` tells whether the confirmed store holds the row of an entity and
 * key. When it is omitted, no row counts as held, which costs at most one
 * unfolded slot per row.
 */
export function planCompaction(
	registry: EntityRegistry,
	pending: Mutation[],
	pushedThrough: bigint,
	protectedIds: ReadonlySet<bigint>,
	stored: (entity: string, key: string) => boolean = () => false,
): CompactionPlan {
	const head: Mutation[] = [];
	const tail: Mutation[] = [];
	for (const m of pending) {
		if (m.mutationId <= pushedThrough) head.push(m);
		else tail.push(m);
	}

	// Whether each row is live after the queue so far, where an op on it says.
	// A custom or batch mutation can create or delete any row, so after one
	// every row the queue has not written since may be absent.
	const live = new Map<string, boolean>();
	let unknown = false;
	const mayBeAbsent = (entity: string, key: string): boolean => {
		const known = live.get(`${entity}\u0000${key}`);
		if (known !== undefined) return !known;
		return unknown || !stored(entity, key);
	};
	const track = (m: Mutation): void => {
		if (m.payload.case !== 'crud') {
			live.clear();
			unknown = true;
			return;
		}
		const crud = m.payload.value;
		const keyStr = `${crud.entity}\u0000${crud.key}`;
		if (crud.op === CrudOp.PUT) live.set(keyStr, true);
		else if (crud.op === CrudOp.DELETE) live.set(keyStr, false);
	};
	for (const m of head) track(m);

	const slots: Slot[] = [];
	// The mergeable slot currently open for each (entity, key). A barrier closes
	// the relevant slots so later ops cannot merge across it.
	const openByKey = new Map<string, Slot>();

	for (const m of tail) {
		const createsRow = m.payload.case === 'crud' && m.payload.value.op === CrudOp.PUT
			&& mayBeAbsent(m.payload.value.entity, m.payload.value.key);
		track(m);
		if (m.payload.case !== 'crud') {
			// custom/batch: a hard barrier across every key (a custom mutator may
			// touch any row; a batch spans many).
			openByKey.clear();
			slots.push({ passthrough: m, clientId: m.clientId, sources: [ m.mutationId ] });
			continue;
		}
		const crud = m.payload.value;
		const def = registry.get(crud.entity);
		const keyStr = `${crud.entity}\u0000${crud.key}`;
		// A write that can change what the caller holds (a grant row, the delete
		// of a row others sit below) changes what the server admits for other
		// keys, so no later write may fold back before it. It still folds onto
		// its own key's slot when that slot is the last one, as nothing sits
		// between the two.
		if (writeChangesAccess(registry, crud.entity, crud.op === CrudOp.DELETE)) {
			const own = openByKey.get(keyStr);
			openByKey.clear();
			if (own !== undefined && own === slots.at(-1)) openByKey.set(keyStr, own);
		}
		// An undo-referenced edit or an unknown entity is a single-key barrier:
		// kept distinct so its semantics (and any undo inverse computed against
		// it) survive untouched. So is a write a newer build queued with fields
		// this build has no descriptor for: a fold rebuilds the messages and
		// would drop them, and this build cannot tell whether such a field is a
		// guard that must stop the fold.
		if (protectedIds.has(m.mutationId) || !def || hasUnknownFields(m, crud)) {
			openByKey.delete(keyStr);
			slots.push({ passthrough: m, clientId: m.clientId, sources: [ m.mutationId ] });
			continue;
		}
		const open = openByKey.get(keyStr);
		// Guards must agree to fold, and without a guard the two must be the same
		// write; see the safety rails above. Neither is a barrier, though — the
		// incoming op opens a fresh slot that later ops can still extend.
		const foldable = open?.crud !== undefined
			&& open.crud.expectedVersion === crud.expectedVersion
			&& (crud.expectedVersion !== undefined
				|| (sameWrite(open.crud, crud) && !(open.creates && crud.op === CrudOp.PUT)));
		const folded = foldable ? foldCrud(registry, def, open!.crud!, crud) : undefined;
		if (open?.crud && folded && folded !== 'barrier' && !pastLimits(folded)) {
			open.crud = folded;
			open.sources.push(m.mutationId);
			continue;
		}
		// No open slot, or the fold refused (delete-wins, or a merger/key-touching
		// hazard): start a fresh slot that later same-key ops can extend.
		const slot: Slot = {
			crud: clone(CrudMutationSchema, crud),
			clientId: m.clientId,
			sources: [ m.mutationId ],
			creates: createsRow,
		};
		slots.push(slot);
		openByKey.set(keyStr, slot);
	}

	// Renumber: the pushed prefix keeps its ids; the tail slots take
	// pushedThrough+1, +2, … in creation order, staying gap-free for the server.
	const remap = new Map<bigint, bigint>();
	const out: Mutation[] = [ ...head ];
	let nextId = pushedThrough;
	for (const slot of slots) {
		nextId += 1n;
		// A passthrough is CLONED rather than renumbered where it lies: the
		// caller deletes the old pending keys through the very array it handed
		// over, so moving an id in place would leave that entry's key un-deleted
		// — a second copy of the mutation, under an id the server's watermark has
		// never seen and so cannot dedupe.
		const mutation = slot.passthrough
			? clone(MutationSchema, slot.passthrough)
			: create(MutationSchema, {
				clientId: slot.clientId,
				payload: { case: 'crud', value: slot.crud! },
			});
		mutation.mutationId = nextId;
		for (const src of slot.sources) remap.set(src, nextId);
		out.push(mutation);
	}
	return { mutations: out, remap, changed: slots.length !== tail.length };
}

/** Whether the envelope or the crud of `m` carries fields this build does not declare. */
function hasUnknownFields(m: Mutation, crud: CrudMutation): boolean {
	return (m.$unknown?.length ?? 0) > 0 || (crud.$unknown?.length ?? 0) > 0;
}

/** The engine's default Limits.MaxValueBytes. */
const MAX_VALUE_BYTES = 1 << 20;
/** The engine's default Limits.MaxFieldMaskPaths. */
const MAX_FIELD_MASK_PATHS = 64;

/**
 * Whether `folded` passes a write limit. The queued size of a text value can
 * be larger than what the push ships for it (a whole document, sent as a
 * delta), so this can keep apart writes the server would take folded. That
 * costs one unfolded slot.
 */
function pastLimits(folded: CrudMutation): boolean {
	return folded.value.length > MAX_VALUE_BYTES
		|| (folded.fieldMask?.paths.length ?? 0) > MAX_FIELD_MASK_PATHS;
}

/**
 * Folds `incoming` onto the open slot's `base`, returning the merged CRUD, or
 * "barrier" when the two must stay separate (the caller then opens a fresh
 * slot). Mirrors the server's apply order so the merged result yields the same
 * row state — and the same optimistic overlay — as applying both in sequence.
 *
 * The caller only folds ops whose compare-and-swap guards are equal, so the
 * result carries `base.expectedVersion` and that is `incoming`'s too: one
 * guarded mutation replaces a chain that named the same version throughout, and
 * it applies or rejects as a unit.
 */
function foldCrud(
	registry: EntityRegistry,
	def: EntityDef,
	base: CrudMutation,
	incoming: CrudMutation,
): CrudMutation | 'barrier' {
	switch (incoming.op) {
		case CrudOp.PUT:
			// A PUT after a DELETE re-creates the row, which the server treats
			// differently from a PUT on the live row: text does not union with the
			// deleted document, and protected fields do not keep their stored
			// values. Folded, the PUT would reach the live row.
			if (base.op === CrudOp.DELETE) return 'barrier';
			// A PUT that leaves a guarded unit unset keeps its stored value when
			// the caller may not read it, so it does not carry an earlier write of
			// that unit: a PATCH's clear, or a PUT's value.
			if (keepsGuarded(def, base, incoming.value)) return 'barrier';
			// Text fields are the other exception to "a PUT overwrites": a PUT
			// unions each document with the stored one and keeps a stored one it
			// carries no bytes for. So the earlier PUT's documents would have to
			// union into this one, as the server's sequential apply does. That
			// gives this PUT text it did not carry, and a check that reads the
			// text then refuses both writes where alone it refused only the
			// earlier one. The fold is kept only when the union adds nothing:
			// this PUT already carries every document the earlier one did. An
			// earlier PATCH may clear a document, which a PUT without one would
			// not carry.
			const textPaths = registry.textPathsOf(def.name);
			if (textPaths.length > 0) {
				if (base.op === CrudOp.PATCH && patchReachesText(textPaths, base)) return 'barrier';
				if (base.op === CrudOp.PUT) {
					const next = fromBinary(def.schema, incoming.value);
					const alone = toBinary(def.schema, next);
					try {
						mergeTextFieldsInto(def.schema, textPaths, fromBinary(def.schema, base.value), next);
					} catch {
						return 'barrier';
					}
					const value = toBinary(def.schema, next);
					if (!bytesEqual(alone, value)) return 'barrier';
					const out = clone(CrudMutationSchema, incoming);
					out.value = value;
					return out;
				}
			}
			// Otherwise a PUT overwrites whatever came before: the row ends at the
			// PUT value either way.
			return clone(CrudMutationSchema, incoming);
		case CrudOp.DELETE:
			// A DELETE wins over any prior PUT/PATCH on the key.
			return create(CrudMutationSchema, {
				entity: incoming.entity,
				key: incoming.key,
				op: CrudOp.DELETE,
				expectedVersion: base.expectedVersion,
			});
		case CrudOp.PATCH: {
			if (base.op === CrudOp.DELETE) {
				// delete-wins: a PATCH of a deleted row is rejected (and locally a
				// no-op), so merging it away would drop that rejection. Keep separate.
				return 'barrier';
			}
			const mergers = registry.mergersFor(def.name);
			if (base.op === CrudOp.PUT) {
				// Absorb the patch into the PUT value. Safe even with mergers (a single
				// merge against the PUT base, exactly as the server would do), UNLESS
				// the mask touches a key field — that would shift the PUT's derived key
				// away from crud.key and the server's PUT key-check would reject it.
				if (patchTouchesKey(def, incoming)) return 'barrier';
				const merged = fromBinary(def.schema, base.value);
				const src = fromBinary(def.schema, incoming.value);
				if (!tryMergeMask(def, merged, src, incoming, mergers)) return 'barrier';
				const value = toBinary(def.schema, merged);
				// A clear of a guarded unit folds into a PUT that leaves it unset,
				// which keeps the stored value for a caller who may not read it.
				if (keepsGuarded(def, incoming, value)) return 'barrier';
				// The same holds for a cleared text document: a PUT without one
				// keeps the stored document.
				if (clearsText(def, registry.textPathsOf(def.name), incoming, merged)) return 'barrier';
				return create(CrudMutationSchema, {
					entity: base.entity,
					key: base.key,
					op: CrudOp.PUT,
					value,
					expectedVersion: base.expectedVersion,
				});
			}
			// base.op === PATCH. Chaining two patches through a merger needs the
			// merge to be associative, which custom mergers need not be — bail
			// when the entity has any custom merger registered. The built-in
			// text merger is a set union (associative, commutative, idempotent),
			// so schema-derived text mergers fold safely:
			// base∪(s1∪s2) === (base∪s1)∪s2, exactly what the server computes
			// when the patches arrive separately.
			if (mergers && ![ ...mergers.values() ].every((fn) => fn === mergeText)) {
				return 'barrier';
			}
			// The server refuses a PATCH whose mask reaches a guarded unit the
			// caller may not write, and refuses the whole mask, so a patch it
			// accepts alone would go down with one it refuses. Two patches that
			// reach the same guards get the same verdict from the guards.
			if (!sameGuards(def, base, incoming)) return 'barrier';
			// A PATCH carrying text unions it with the stored document, so text
			// folded onto an earlier clear would bring the cleared text back.
			const textPaths = registry.textPathsOf(def.name);
			if (
				textPaths.length > 0
				&& patchReachesText(textPaths, incoming)
				&& clearsText(def, textPaths, base, fromBinary(def.schema, base.value))
			) {
				return 'barrier';
			}
			const merged = fromBinary(def.schema, base.value);
			const src = fromBinary(def.schema, incoming.value);
			if (!tryMergeMask(def, merged, src, incoming, mergers)) return 'barrier';
			return create(CrudMutationSchema, {
				entity: base.entity,
				key: base.key,
				op: CrudOp.PATCH,
				value: toBinary(def.schema, merged),
				fieldMask: { paths: unionPaths(base.fieldMask?.paths ?? [], incoming.fieldMask?.paths ?? []) },
				expectedVersion: base.expectedVersion,
			});
		}
		default:
			return 'barrier';
	}
}

/**
 * Merges the incoming PATCH's masked fields onto `merged`, returning false when
 * the two cannot be folded and must stay separate mutations. A merger refusing
 * a pair of values is an ordinary outcome here (see mergeQueuedText), not a
 * failure: it costs one unfolded queue slot, and compaction is an optimization
 * whose safe answer is always "leave them alone". `merged` may be partially
 * written when this returns false, which is why the caller discards it.
 */
function tryMergeMask(
	def: EntityDef,
	merged: Message,
	src: Message,
	incoming: CrudMutation,
	mergers: EntityMergers | undefined,
): boolean {
	try {
		mergeMask(def.schema, merged, src, incoming.fieldMask?.paths ?? [], foldMergers(mergers));
		return true;
	} catch {
		return false;
	}
}

/**
 * The text merger, adapted to folding two QUEUED text values rather than
 * applying one to a stored row. Both sides may be delta state here: a live text
 * handle queues the fragment for each edit (text/handle.ts), so folding a
 * typing burst folds fragment onto fragment — a union of two deltas cut against
 * the same confirmed row, which is the delta the folded slot must carry.
 *
 * mergeText itself deliberately refuses a fragment base: rows always hold whole
 * documents, and that refusal is pinned cross-language (testdata/text). Queue
 * entries are not rows, and nothing outside this module folds two of them.
 */
function mergeQueuedText(base: Uint8Array, incoming: Uint8Array): Uint8Array {
	if (!isFragment(base)) return mergeText(base, incoming);
	if (isFragment(incoming)) return mergeFragments(base, incoming);
	// A whole document queued after a fragment supersedes it — it was written
	// against local state that already carried the fragment's edits — but only
	// the writer knew the base the fragment was cut against, so nothing here can
	// prove that. Refusing keeps the two mutations separate instead.
	throw new Error('text: cannot fold full state onto a queued fragment');
}

/** mergers with every text merger swapped for its queue-folding form. */
function foldMergers(mergers: EntityMergers | undefined): EntityMergers | undefined {
	if (!mergers) return mergers;
	const out: EntityMergers = new Map(mergers);
	for (const [ path, fn ] of out) {
		if (fn === mergeText) out.set(path, mergeQueuedText);
	}
	return out;
}

/**
 * Whether the masks of the PATCHes `base` and `incoming` reach the same field
 * guards of `def`.
 */
function sameGuards(def: EntityDef, base: CrudMutation, incoming: CrudMutation): boolean {
	if (def.guards.length === 0) return true;
	const reached = (patch: CrudMutation): boolean[] => {
		const units = new Set(
			(patch.fieldMask?.paths ?? []).map((path) => {
				const head = path.split('.', 1)[0]!;
				return unitMembers(def, head)?.unit ?? head;
			}),
		);
		return def.guards.map((g) => g.units.some((u) => units.has(u)));
	};
	const a = reached(base);
	const b = reached(incoming);
	return a.every((hit, i) => hit === b[i]);
}

/**
 * Whether `incoming` is the same op as `base` and, for a PATCH, carries the
 * same set of mask paths — the writes a server check that does not read
 * values gives the same verdict.
 */
function sameWrite(base: CrudMutation, incoming: CrudMutation): boolean {
	if (base.op !== incoming.op) return false;
	if (incoming.op !== CrudOp.PATCH) return true;
	const a = new Set(base.fieldMask?.paths ?? []);
	const b = new Set(incoming.fieldMask?.paths ?? []);
	return a.size === b.size && [ ...b ].every((p) => a.has(p));
}

function patchTouchesKey(def: EntityDef, crud: CrudMutation): boolean {
	const keyNames = new Set(def.keyFields.map((f) => f.name));
	return (crud.fieldMask?.paths ?? []).some((p) => keyNames.has(p.split('.')[0]!));
}

/**
 * Whether `earlier` writes a unit that a field guard restricts and `put`, the
 * value of a later PUT, leaves unset. A PATCH writes each unit its mask
 * reaches, and a PUT each unit its value sets. The server keeps the stored
 * value of such a unit on a PUT from a caller who may not read it, so the PUT
 * does not carry the earlier write. The client does not know which guarded
 * units the caller reads, so every guarded unit counts.
 */
function keepsGuarded(def: EntityDef, earlier: CrudMutation, put: Uint8Array): boolean {
	if (def.guards.length === 0) return false;
	const guarded = new Set(def.guards.flatMap((g) => g.units));
	let written: DescField[][];
	if (earlier.op === CrudOp.PATCH) {
		written = (earlier.fieldMask?.paths ?? []).flatMap((path) => {
			const members = unitMembers(def, path.split('.', 1)[0]!);
			return members && guarded.has(members.unit) ? [ members.fields ] : [];
		});
	} else if (earlier.op === CrudOp.PUT) {
		const value = fromBinary(def.schema, earlier.value);
		written = [ ...guarded ].flatMap((unit) => {
			const members = unitMembers(def, unit);
			return members?.fields.some((f) => isFieldSet(value, f)) ? [ members.fields ] : [];
		});
	} else {
		return false;
	}
	if (written.length === 0) return false;
	const value = fromBinary(def.schema, put);
	return written.some((fields) => !fields.some((f) => isFieldSet(value, f)));
}

/**
 * Whether a mask path of `patch` reaches a top-level field that holds one of
 * the text fields `textPaths` names. Coarse on purpose: a false positive costs
 * one unfolded slot.
 */
function patchReachesText(textPaths: readonly string[], patch: CrudMutation): boolean {
	const heads = new Set(textPaths.map((p) => p.split('.', 1)[0]!));
	return (patch.fieldMask?.paths ?? []).some((p) => heads.has(p.split('.', 1)[0]!));
}

/**
 * Whether `patch` folded into a PUT leaves a text document its mask reaches
 * without bytes in `merged`, the folded PUT value. Applied alone, the PATCH
 * clears that document. A PUT without it keeps the stored one.
 */
function clearsText(
	def: EntityDef,
	textPaths: readonly string[],
	patch: CrudMutation,
	merged: Message,
): boolean {
	const mask = patch.fieldMask?.paths ?? [];
	return textPaths.some((textPath) =>
		textInstancePaths(def.schema, textPath, merged).some((path) =>
			mask.some((p) => path === p || path.startsWith(`${p}.`))
			&& textFieldBytes(def.schema, path, merged) === undefined
		)
	);
}

/**
 * The unit that the top-level field or oneof `name` belongs to, with the fields
 * of that unit: the field alone, or each member of its oneof.
 */
function unitMembers(
	def: EntityDef,
	name: string,
): { unit: string; fields: DescField[] } | undefined {
	const field = def.schema.fields.find((f) => f.name === name);
	const oneof = field?.oneof ?? def.schema.oneofs.find((o) => o.name === name);
	if (oneof !== undefined) return { unit: oneof.name, fields: oneof.fields };
	return field === undefined ? undefined : { unit: field.name, fields: [ field ] };
}

function unionPaths(a: string[], b: string[]): string[] {
	const seen = new Set(a);
	const out = [ ...a ];
	for (const p of b) {
		if (!seen.has(p)) {
			seen.add(p);
			out.push(p);
		}
	}
	return out;
}

export interface CompactionResult {
	/** Old tail mutation id → new id. */
	remap: ReadonlyMap<bigint, bigint>;
	/** Pending-queue length after compaction. */
	pendingCount: number;
	changed: boolean;
}

/**
 * Compacts ONE workspace's pending queue in one KV transaction: reads it AND
 * that workspace's durable pushed-through watermark, plans, and — when anything
 * changed — rewrites the tail, lowers its mutation-id counter to the new
 * high-water mark, and rebuilds the optimistic overlay from the renumbered
 * queue. Compaction never spans workspaces: ids are counted per
 * (workspace, client) and must stay gap-free within that pair. Reading the
 * watermark inside this transaction is what makes the safety invariant hold
 * across tabs: a peer that advanced it (before its own push) is observed here,
 * so we never renumber ids the server may already have seen. The protected
 * undo tokens are resolved through `readProtectedTokens` INSIDE the same
 * transaction for the same reason: the cross-tab union of every tab's undo
 * refs (IntentRegistry.unionUndoRefTokens) is then atomic with the fold. A
 * pending mutation is protected, and kept as its own slot, when it carries any
 * of those tokens. Every token of a folded mutation moves to the slot that
 * absorbed it, so an undo stack never has to learn the new ids. Run under the
 * kv mutex.
 */
export async function compactPending(
	kv: KVStore,
	registry: EntityRegistry,
	mirrors: MirrorMap,
	workspaceId: string,
	readProtectedTokens: (tx: KVRead) => ReadonlySet<bigint> | Promise<ReadonlySet<bigint>>,
): Promise<CompactionResult> {
	const tx = await kv.write();
	try {
		const pending = await readPending(tx, workspaceId);
		const pushedThrough = await readPushedThrough(tx, workspaceId);
		const protectedTokens = await readProtectedTokens(tx);
		const tokens = await readUndoTokens(tx, workspaceId);
		const protectedIds = new Set<bigint>();
		if (protectedTokens.size > 0) {
			for (const [ id, list ] of tokens) {
				if (list.some((t) => protectedTokens.has(t))) protectedIds.add(id);
			}
		}
		// Only a PUT asks whether its row is stored (see planCompaction).
		const stored = new Set<string>();
		for (const m of pending) {
			if (m.mutationId <= pushedThrough || m.payload.case !== 'crud') continue;
			const { entity, key, op } = m.payload.value;
			if (op !== CrudOp.PUT) continue;
			if (await tx.has(confirmedKey(entity, workspaceId, key))) stored.add(`${entity}\u0000${key}`);
		}
		const plan = planCompaction(
			registry,
			pending,
			pushedThrough,
			protectedIds,
			(entity, key) => stored.has(`${entity}\u0000${key}`),
		);
		if (!plan.changed) {
			tx.release();
			return { remap: new Map(), pendingCount: pending.length, changed: false };
		}
		for (const m of pending) await tx.del(pendingKey(workspaceId, m.mutationId));
		for (const m of plan.mutations) await enqueuePending(tx, workspaceId, m);
		await moveUndoTokens(tx, workspaceId, tokens, plan.remap);
		const maxId = plan.mutations.length
			? plan.mutations[plan.mutations.length - 1]!.mutationId
			: pushedThrough;
		await tx.put(metaLastMutationId(workspaceId), maxId.toString());
		await recountPendingAccess(tx, registry, workspaceId, plan.mutations, maxId);
		// The fold preserves the overlay ROW by construction, but the optimistic
		// text log does not ride on the row: an entry is keyed by the mutation id
		// that wrote it (keys.ts), and the renumbering above frees the ids above
		// the new high-water mark for the next local write to mint. Left alone,
		// the entry that write appends lands on top of an edit only the log still
		// holds, and the fold then drops that edit and everything queued behind
		// it. runResequence rebuilds both for the same reason; here the replay
		// costs a pass over the queue this call has already shortened.
		//
		// Nothing to notify: the replay reproduces the merged view the fold
		// preserved, which expectSameOverlay pins, so the TouchSet is filled and
		// dropped rather than fanned out to every watcher on every push cycle.
		await clearOverlayAndReplay(
			tx,
			registry,
			mirrors,
			workspaceId,
			plan.mutations,
			new TouchSet(),
		);
		await tx.commit();
		return { remap: plan.remap, pendingCount: plan.mutations.length, changed: true };
	} catch (err) {
		tx.release();
		throw err;
	}
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}
