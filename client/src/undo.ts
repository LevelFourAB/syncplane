// In-memory undo/redo bookkeeping, every local CRUD mutation records its inverse;
// undo/redo enqueue those inverses as real mutations through the normal pipeline
// (pushed, idempotent, rebased). Nothing here is persisted — stacks die with
// the page, by design.
//
// Semantics the tests pin down:
//   - PUT      → inverse PUT(prev) or DELETE when the row did not exist
//   - PATCH    → inverse PATCH(prev snapshot, same mask) or DELETE
//   - DELETE   → inverse PUT(prev), or nothing when the row did not exist
//   - text-field edits (a state CRDT, where a snapshot inverse would merge to
//     a no-op) → a SYMBOLIC inverse (src/text/undo-ops.ts) materialised
//     against the then-current document at undo time; only local ops are
//     named, so a peer's concurrent edits survive the undo
//   - custom mutations are a barrier: both stacks clear (not invertible)
//   - a transact() batch is ONE undo group: undo/redo replays its inverses
//     together (as one atomic batch), in reverse application order
//   - typing coalesces: a text edit may join the previous undo group
//     (idle-timeout/word-boundary policy decided by the text handle)
//   - a new user edit clears the redo stack
//   - undoing a row a peer edited concurrently clobbers the peer's fields
//     named in the inverse mask — accepted per-field LWW semantics
//   - a rejected mutation drops its whole containing group from both stacks
//     (a rejected batch shares one mutation_id, so its group drops whole)

import { create } from '@bufbuild/protobuf';

import { type CrudMutation, CrudMutationSchema, CrudOp } from './gen/syncplane/v1/mutation_pb.js';
import { safeInvoke } from './report.js';
import type { TextInverseOp } from './text/undo-ops.js';

/**
 * One recorded inverse: an ordinary CRUD mutation replayed verbatim, or a
 * symbolic text inverse (entity/key/field name the row and the text field's
 * proto path; ops describe what to undo in op-level terms).
 */
export type Inverse =
	| { kind: 'crud'; crud: CrudMutation }
	| { kind: 'text'; entity: string; key: string; field: string; ops: TextInverseOp[] };

export interface UndoOptions {
	/** Default true when the `undo` option object is present. */
	enabled?: boolean;
	/** Maximum undo groups kept; oldest evict first. Default 100. */
	maxDepth?: number;
}

interface UndoEntry {
	groupId: number;
	/** Id of the forward mutation this entry inverts (rejection wiring). */
	mutationId: bigint;
	inverse: Inverse;
}

export type StackName = 'undo' | 'redo';

/**
 * Computes the inverse of a CRUD mutation given the merged row bytes as they
 * were BEFORE the mutation applied. Returns undefined for no-ops (deleting
 * a row that did not exist).
 */
export function invertCrud(
	crud: CrudMutation,
	prev: Uint8Array | undefined,
): CrudMutation | undefined {
	switch (crud.op) {
		case CrudOp.PUT:
			if (prev === undefined) {
				return create(CrudMutationSchema, { entity: crud.entity, key: crud.key, op: CrudOp.DELETE });
			}
			return create(CrudMutationSchema, {
				entity: crud.entity,
				key: crud.key,
				op: CrudOp.PUT,
				value: prev,
			});
		case CrudOp.PATCH:
			if (prev === undefined) {
				// Delete-wins: a PATCH of an absent/deleted row is a no-op (it never
				// resurrects the row), so there is nothing to undo.
				return undefined;
			}
			// Restoring the masked fields from the previous snapshot inverts the
			// patch; unmasked fields stay untouched either way.
			return create(CrudMutationSchema, {
				entity: crud.entity,
				key: crud.key,
				op: CrudOp.PATCH,
				value: prev,
				fieldMask: crud.fieldMask,
			});
		case CrudOp.DELETE:
			if (prev === undefined) {
				return undefined;
			}
			return create(CrudMutationSchema, {
				entity: crud.entity,
				key: crud.key,
				op: CrudOp.PUT,
				value: prev,
			});
		default:
			return undefined;
	}
}

export class UndoManager {
	private undoStack: UndoEntry[] = [];
	private redoStack: UndoEntry[] = [];
	private readonly maxDepth: number;
	private nextGroupId = 1;
	/** Listeners notified whenever canUndo()/canRedo() may have changed. */
	private listeners = new Set<() => void>();

	constructor(maxDepth = 100) {
		this.maxDepth = maxDepth;
	}

	/**
	 * Subscribes to stack changes; the callback fires after any method that
	 * mutates either stack. Use canUndo()/canRedo() to read the current state.
	 * Returns an unsubscribe function. Listeners may be invoked even when the
	 * observable (canUndo|canRedo) state is unchanged — callers that care about
	 * referential stability should derive a primitive snapshot and dedupe.
	 */
	onChange(cb: () => void): () => void {
		this.listeners.add(cb);
		return () => {
			this.listeners.delete(cb);
		};
	}

	/** Notifies listeners that the undo/redo stacks may have changed. */
	private emit(): void {
		for (const cb of [ ...this.listeners ]) safeInvoke(cb, undefined, 'onUndoChange listener');
	}

	/**
	 * Records the inverses of one forward mutation (a single CRUD, or every
	 * sub-mutation of a batch) as ONE undo group, after its transaction
	 * committed. Inverses are pushed in application order; takeGroup pops them
	 * newest-first, so a batch replays in reverse — the order an undo needs.
	 * All entries share the forward mutation_id so a rejection drops the whole
	 * group.
	 *
	 * `captureStack` is the stack to route to while undo()/redo() replays
	 * inverses: 'redo' for undo's replay, 'undo' for redo's. It is passed in by
	 * the specific enqueue replaying the group rather than read from instance
	 * state, so a concurrent user mutation committing during that replay's async
	 * window is not misrouted (it arrives with captureStack undefined). Undefined
	 * means a new user edit, which clears the redo stack (even when every inverse
	 * is a no-op).
	 *
	 * `coalesce` joins the entries onto the TOP undo group instead of minting
	 * a new one — how typing collapses into word-sized undo steps. It only
	 * applies to new user edits (captureStack undefined) with a non-empty
	 * undo stack; the redo stack still clears (a coalesced edit is still an
	 * edit).
	 */
	recordGroup(
		mutationId: bigint,
		inverses: (Inverse | undefined)[],
		captureStack?: StackName,
		coalesce?: boolean,
	): void {
		const target = captureStack ?? 'undo';
		if (captureStack === undefined) {
			this.redoStack = [];
		}
		const defined = inverses.filter((i): i is Inverse => i !== undefined);
		if (defined.length === 0) {
			// May still have cleared the redo stack above (captureStack undefined).
			this.emit();
			return;
		}
		const stack = target === 'undo' ? this.undoStack : this.redoStack;
		const top = this.undoStack[this.undoStack.length - 1];
		// One enqueue commits one group unless the caller asked to coalesce
		// onto the previous one.
		const groupId = coalesce && captureStack === undefined && top !== undefined
			? top.groupId
			: this.nextGroupId++;
		for (const inverse of defined) {
			stack.push({ groupId, mutationId, inverse });
		}
		if (target === 'undo') {
			this.trim();
		}
		this.emit();
	}

	/** Custom mutations cannot be inverted: both stacks clear. */
	barrier(): void {
		this.undoStack = [];
		this.redoStack = [];
		this.emit();
	}

	canUndo(): boolean {
		return this.undoStack.length > 0;
	}

	canRedo(): boolean {
		return this.redoStack.length > 0;
	}

	/** Pops the contiguous top group, newest entry first (replay order). */
	takeUndoGroup(): UndoEntry[] {
		const group = takeGroup(this.undoStack);
		if (group.length > 0) this.emit();
		return group;
	}

	takeRedoGroup(): UndoEntry[] {
		const group = takeGroup(this.redoStack);
		if (group.length > 0) this.emit();
		return group;
	}

	/**
	 * Re-pushes a group claimed by takeUndoGroup/takeRedoGroup when the replay
	 * that would have consumed it failed (closed client, KV error), so the step
	 * is not lost. `group` is newest-first as takeGroup returned it, so it is
	 * restored in reverse to recover application order. Best-effort on ordering:
	 * a user edit that committed during the failed replay's window sits below the
	 * restored group; acceptable on an error path.
	 */
	restoreUndoGroup(group: UndoEntry[]): void {
		for (let i = group.length - 1; i >= 0; i--) {
			this.undoStack.push(group[i] as UndoEntry);
		}
		if (group.length > 0) this.emit();
	}

	restoreRedoGroup(group: UndoEntry[]): void {
		for (let i = group.length - 1; i >= 0; i--) {
			this.redoStack.push(group[i] as UndoEntry);
		}
		if (group.length > 0) this.emit();
	}

	/**
	 * A rejected mutation never applied on the server: its containing groups
	 * drop from both stacks so undo/redo cannot replay state that builds on
	 * the rejected change.
	 */
	onRejected(mutationId: bigint): void {
		this.undoStack = dropContainingGroup(this.undoStack, mutationId);
		this.redoStack = dropContainingGroup(this.redoStack, mutationId);
		this.emit();
	}

	/**
	 * Mutation ids whose forward mutation pending-queue compaction must keep as
	 * its own slot. Only SNAPSHOT inverses need that: they restore the row as it
	 * was before one specific mutation, so that mutation has to stay the thing
	 * the queue replays.
	 *
	 * A symbolic text inverse (see text/undo-ops.ts) does not. It names ops, not
	 * state, and is materialised against whatever the document holds at undo
	 * time, so it is indifferent to how many forward mutations the queue folded
	 * its edit into. Excluding them is what lets a typing burst — one mutation
	 * per keystroke, each with an inverse — compact at all; every keystroke
	 * being its own barrier is what left an offline burst growing without bound.
	 *
	 * Ids the stacks merely reference are still tracked (see remapIds and
	 * onRejected): a folded slot's rejection must still drop every group that
	 * folded into it. Which ids may be folded and which ids must be heard about
	 * are different questions, and this answers only the first.
	 */
	foldBarrierIds(): Set<bigint> {
		const ids = new Set<bigint>();
		for (const stack of [ this.undoStack, this.redoStack ]) {
			for (const e of stack) {
				if (e.inverse.kind !== 'text') ids.add(e.mutationId);
			}
		}
		return ids;
	}

	/**
	 * Predicts what foldBarrierIds() will return AFTER recordGroup (or barrier)
	 * runs for the mutation currently being enqueued. The enqueue transaction
	 * persists the cross-tab intent record, but recordGroup only runs once that
	 * transaction has committed — so the in-transaction write must compute the
	 * post-commit set from the same inputs recordGroup will receive:
	 *
	 *  - a barrier (custom mutation) clears both stacks → empty set;
	 *  - a plain edit (captureStack undefined) clears the redo stack → current
	 *    undo-stack ids only;
	 *  - an undo/redo replay (captureStack set) keeps both stacks;
	 *  - the new mutation's own id joins when it captured a snapshot inverse.
	 *
	 * Deliberately ignores maxDepth trimming, so it may over-include ids a
	 * recordGroup-side trim would evict — over-protection only ever costs
	 * compaction efficiency, never correctness.
	 */
	predictedBarrierIds(
		mutationId: bigint,
		hasSnapshotInverse: boolean,
		captureStack: StackName | undefined,
		isBarrier: boolean,
	): Set<bigint> {
		if (isBarrier) return new Set();
		const ids = new Set<bigint>();
		for (const e of this.undoStack) {
			if (e.inverse.kind !== 'text') ids.add(e.mutationId);
		}
		if (captureStack !== undefined) {
			for (const e of this.redoStack) {
				if (e.inverse.kind !== 'text') ids.add(e.mutationId);
			}
		}
		if (hasSnapshotInverse) ids.add(mutationId);
		return ids;
	}

	/**
	 * Rewrites the forward-mutation ids both stacks reference after compaction
	 * renumbered the pending queue. Entries whose id is absent from the map keep
	 * it (the pushed prefix is never renumbered); every folded id maps to the
	 * slot that absorbed it, so no entry is ever left dangling. Several entries
	 * landing on one id is normal — a typing burst folds to a single slot — and
	 * is exactly what keeps a rejection of that slot dropping all of them.
	 */
	remapIds(remap: ReadonlyMap<bigint, bigint>): void {
		for (const stack of [ this.undoStack, this.redoStack ]) {
			for (const e of stack) {
				const next = remap.get(e.mutationId);
				if (next !== undefined) e.mutationId = next;
			}
		}
		// Renumbering does not change canUndo/canRedo, but a listener deriving a
		// primitive snapshot will simply no-op; emit for completeness.
		this.emit();
	}

	private trim(): void {
		let groups = countGroups(this.undoStack);
		while (groups > this.maxDepth && this.undoStack.length > 0) {
			const oldest = this.undoStack[0]?.groupId;
			this.undoStack = this.undoStack.filter((e) => e.groupId !== oldest);
			groups--;
		}
	}
}

function takeGroup(stack: UndoEntry[]): UndoEntry[] {
	const top = stack[stack.length - 1];
	if (top === undefined) {
		return [];
	}
	const group: UndoEntry[] = [];
	while (stack.length > 0 && stack[stack.length - 1]?.groupId === top.groupId) {
		group.push(stack.pop() as UndoEntry);
	}
	return group;
}

function dropContainingGroup(stack: UndoEntry[], mutationId: bigint): UndoEntry[] {
	const doomed = new Set(stack.filter((e) => e.mutationId === mutationId).map((e) => e.groupId));
	if (doomed.size === 0) {
		return stack;
	}
	return stack.filter((e) => !doomed.has(e.groupId));
}

function countGroups(stack: UndoEntry[]): number {
	const seen = new Set<number>();
	for (const e of stack) {
		seen.add(e.groupId);
	}
	return seen.size;
}
