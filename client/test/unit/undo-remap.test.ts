import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { CrudMutationSchema, CrudOp } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type Inverse, UndoManager } from '../../src/undo.js';

// Compaction renumbers the never-pushed tail, so the undo manager names
// forward mutations by undo token: it exposes which tokens compaction must
// keep from folding, and a rejection names the tokens the dead entry carried.

function inverse(key: string): Inverse {
	return {
		kind: 'crud',
		crud: create(CrudMutationSchema, { entity: 'todo', key, op: CrudOp.DELETE }),
	};
}

/** A symbolic text inverse — replayed against the document, not a snapshot. */
function textInverse(key: string): Inverse {
	return {
		kind: 'text',
		entity: 'todo',
		key,
		field: 'body',
		ops: [ { type: 'deleteItems', peer: 'c1', start: 1, count: 1 } ],
	};
}

/** The row key an entry's crud inverse targets (test convenience). */
function keyOf(inv: Inverse): string {
	if (inv.kind !== 'crud') throw new Error('expected a crud inverse');
	return inv.crud.key;
}

describe('UndoManager compaction hooks', () => {
	it('reports tokens referenced by both stacks', () => {
		const mgr = new UndoManager();
		mgr.recordGroup(1n, [ inverse('a') ]);
		mgr.recordGroup(2n, [ inverse('b') ]);
		// Undo moves group 2 to the redo stack (capturing under a new token).
		const group = mgr.takeUndoGroup();
		expect(group[0]!.token).toBe(2n);
		mgr.recordGroup(3n, [ inverse('b') ], 'redo');

		// undo stack still has token 1; redo stack has token 3.
		expect(mgr.foldBarrierTokens()).toEqual(new Set([ 1n, 3n ]));
	});

	it('does not pin a text edit, whose inverse is replayed not restored', () => {
		const mgr = new UndoManager();
		mgr.recordGroup(1n, [ inverse('a') ]);
		mgr.recordGroup(2n, [ textInverse('b') ]);
		mgr.recordGroup(3n, [ textInverse('b') ]);

		// Only the snapshot inverse pins its mutation; a typing burst is free to
		// fold, which is what keeps an offline burst from growing without bound.
		expect(mgr.foldBarrierTokens()).toEqual(new Set([ 1n ]));

		// Folding both text edits onto one slot gives the slot both tokens, so
		// its rejection drops both groups and keeps the other one.
		expect(mgr.canUndo()).toBe(true);
		mgr.onRejected([ 2n, 3n ]);
		expect(mgr.takeUndoGroup().map((e) => e.token)).toEqual([ 1n ]);
		expect(mgr.canUndo()).toBe(false);
	});

	it('drops only the groups whose tokens a rejection names', () => {
		const mgr = new UndoManager();
		mgr.recordGroup(5n, [ inverse('a') ]);
		mgr.recordGroup(6n, [ inverse('b') ]);

		mgr.onRejected([ 6n, 99n ]);
		expect(mgr.foldBarrierTokens()).toEqual(new Set([ 5n ]));

		// A rejection that carried no tokens drops nothing.
		mgr.onRejected([]);
		expect(mgr.foldBarrierTokens()).toEqual(new Set([ 5n ]));
	});

	it('restores a claimed group when the replay fails, preserving replay order', () => {
		const mgr = new UndoManager();
		// One group of two inverses; takeUndoGroup returns them newest-first.
		mgr.recordGroup(1n, [ inverse('a'), inverse('b') ]);
		const claimed = mgr.takeUndoGroup();
		expect(claimed.map((e) => keyOf(e.inverse))).toEqual([ 'b', 'a' ]);
		expect(mgr.canUndo()).toBe(false);

		// A failed enqueue must put the group back intact and in the same order.
		mgr.restoreUndoGroup(claimed);
		expect(mgr.canUndo()).toBe(true);
		expect(mgr.takeUndoGroup().map((e) => keyOf(e.inverse))).toEqual([ 'b', 'a' ]);
	});
});
