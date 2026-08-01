import { clone, create, fromBinary, type Message, toBinary } from '@bufbuild/protobuf';

import {
	type CrudMutation,
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../gen/syncplane/v1/mutation_pb.js';
import { metaLastMutationId, metaPushedThrough, pendingKey } from '../keys.js';
import type { KVRead, KVStore, KVWrite } from '../kv/store.js';
import {
	type EntityDef,
	type EntityMergers,
	type EntityRegistry,
	mergeMask,
} from '../mutations.js';
import { isFragment, mergeFragments, mergeText } from '../text/index.js';
import { enqueuePending, readPending } from './queue.js';

/**
 * Pending-queue compaction. Offline, every edit enqueues its own mutation, so a
 * long offline burst leaves a queue that costs O(pending × pages) to replay on
 * the first rebase after reconnect and ships in many push batches. Compaction
 * squashes successive CRUDs to the same (entity, key) — merging PATCH field
 * masks/values, collapsing PUT+PATCHes into one PUT, and PUT/PATCH+DELETE into
 * one DELETE — so the queue the rebase replays and the push ships is short.
 *
 * The merge is overlay-preserving by construction (per-field LWW, applied in
 * order), so the optimistic overlay does NOT need rebuilding afterwards.
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
 *  - custom/batch mutations, compare-and-swap (expected_version) guards, and
 *    mutations an undo/redo entry still references act as barriers: they are
 *    never merged away, so undo stays exact (its ids are merely remapped).
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
	/** A passthrough mutation (custom/batch/guarded CRUD) reused verbatim. */
	passthrough?: Mutation;
	clientId: string;
	/** Old ids folded into this slot (all remap to the slot's new id). */
	sources: bigint[];
}

/** Plans a compaction of the pending queue. Pure; does no I/O. */
export function planCompaction(
	registry: EntityRegistry,
	pending: Mutation[],
	pushedThrough: bigint,
	protectedIds: ReadonlySet<bigint>,
): CompactionPlan {
	const head: Mutation[] = [];
	const tail: Mutation[] = [];
	for (const m of pending) {
		if (m.mutationId <= pushedThrough) head.push(m);
		else tail.push(m);
	}

	const slots: Slot[] = [];
	// The mergeable slot currently open for each (entity, key). A barrier closes
	// the relevant slots so later ops cannot merge across it.
	const openByKey = new Map<string, Slot>();

	for (const m of tail) {
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
		// A compare-and-swap guard, an undo-referenced edit, or an unknown entity
		// is a single-key barrier: kept distinct so its semantics (and any undo
		// inverse computed against it) survive untouched.
		if (crud.expectedVersion !== undefined || protectedIds.has(m.mutationId) || !def) {
			openByKey.delete(keyStr);
			slots.push({ passthrough: m, clientId: m.clientId, sources: [ m.mutationId ] });
			continue;
		}
		const open = openByKey.get(keyStr);
		const folded = open?.crud ? foldCrud(registry, def, open.crud, crud) : undefined;
		if (open?.crud && folded && folded !== 'barrier') {
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
		const mutation = slot.passthrough
			?? create(MutationSchema, {
				clientId: slot.clientId,
				payload: { case: 'crud', value: slot.crud! },
			});
		mutation.mutationId = nextId;
		for (const src of slot.sources) remap.set(src, nextId);
		out.push(mutation);
	}
	return { mutations: out, remap, changed: slots.length !== tail.length };
}

/**
 * Folds `incoming` onto the open slot's `base`, returning the merged CRUD, or
 * "barrier" when the two must stay separate (the caller then opens a fresh
 * slot). Mirrors the server's apply order so the merged result yields the same
 * row state — and the same optimistic overlay — as applying both in sequence.
 */
function foldCrud(
	registry: EntityRegistry,
	def: EntityDef,
	base: CrudMutation,
	incoming: CrudMutation,
): CrudMutation | 'barrier' {
	switch (incoming.op) {
		case CrudOp.PUT:
			// A PUT overwrites whatever came before (including a tombstone): the row
			// ends at the PUT value either way.
			return clone(CrudMutationSchema, incoming);
		case CrudOp.DELETE:
			// A DELETE wins over any prior PUT/PATCH on the key.
			return create(CrudMutationSchema, {
				entity: incoming.entity,
				key: incoming.key,
				op: CrudOp.DELETE,
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
				return create(CrudMutationSchema, {
					entity: base.entity,
					key: base.key,
					op: CrudOp.PUT,
					value: toBinary(def.schema, merged),
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
			const merged = fromBinary(def.schema, base.value);
			const src = fromBinary(def.schema, incoming.value);
			if (!tryMergeMask(def, merged, src, incoming, mergers)) return 'barrier';
			return create(CrudMutationSchema, {
				entity: base.entity,
				key: base.key,
				op: CrudOp.PATCH,
				value: toBinary(def.schema, merged),
				fieldMask: { paths: unionPaths(base.fieldMask?.paths ?? [], incoming.fieldMask?.paths ?? []) },
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

function patchTouchesKey(def: EntityDef, crud: CrudMutation): boolean {
	const keyNames = new Set(def.keyFields.map((f) => f.name));
	return (crud.fieldMask?.paths ?? []).some((p) => keyNames.has(p.split('.')[0]!));
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
	/** Old tail mutation id → new id, for remapping undo bookkeeping. */
	remap: ReadonlyMap<bigint, bigint>;
	/** Pending-queue length after compaction. */
	pendingCount: number;
	changed: boolean;
}

/**
 * Reads a workspace's durable pushed-through watermark. Absent — a workspace
 * that has never pushed — reads as 0. Compaction may only renumber/fold ids
 * strictly above it.
 */
export async function readPushedThrough(tx: KVRead, workspaceId: string): Promise<bigint> {
	const raw = (await tx.get(metaPushedThrough(workspaceId))) as string | undefined;
	return raw ? BigInt(raw) : 0n;
}

/** Advances a workspace's pushed-through watermark. Monotonic; callers raise it. */
export async function writePushedThrough(
	tx: KVWrite,
	workspaceId: string,
	id: bigint,
): Promise<void> {
	await tx.put(metaPushedThrough(workspaceId), id.toString());
}

/**
 * Compacts ONE workspace's pending queue in one KV transaction: reads it AND
 * that workspace's durable pushed-through watermark, plans, and — when anything
 * changed — rewrites the tail and lowers its mutation-id counter to the new
 * high-water mark. Compaction never spans workspaces: ids are counted per
 * (workspace, client) and must stay gap-free within that pair. The
 * optimistic overlay is left untouched (the merge preserves it). Reading the
 * watermark inside this transaction is what makes the safety invariant hold
 * across tabs: a peer that advanced it (before its own push) is observed here,
 * so we never renumber ids the server may already have seen. The protected
 * (undo-referenced) ids are resolved through `readProtectedIds` INSIDE the
 * same transaction for the same reason: the cross-tab union of every tab's
 * undo refs (IntentRegistry.unionUndoRefIds) is then atomic with the
 * renumber. Run under the kv mutex. Returns the undo id remap so the caller
 * can keep undo bookkeeping consistent.
 */
export async function compactPending(
	kv: KVStore,
	registry: EntityRegistry,
	workspaceId: string,
	readProtectedIds: (tx: KVRead) => ReadonlySet<bigint> | Promise<ReadonlySet<bigint>>,
): Promise<CompactionResult> {
	const tx = await kv.write();
	try {
		const pending = await readPending(tx, workspaceId);
		const pushedThrough = await readPushedThrough(tx, workspaceId);
		const protectedIds = await readProtectedIds(tx);
		const plan = planCompaction(registry, pending, pushedThrough, protectedIds);
		if (!plan.changed) {
			tx.release();
			return { remap: new Map(), pendingCount: pending.length, changed: false };
		}
		for (const m of pending) await tx.del(pendingKey(workspaceId, m.mutationId));
		for (const m of plan.mutations) await enqueuePending(tx, workspaceId, m);
		const maxId = plan.mutations.length
			? plan.mutations[plan.mutations.length - 1]!.mutationId
			: pushedThrough;
		await tx.put(metaLastMutationId(workspaceId), maxId.toString());
		await tx.commit();
		return { remap: plan.remap, pendingCount: plan.mutations.length, changed: true };
	} catch (err) {
		tx.release();
		throw err;
	}
}
