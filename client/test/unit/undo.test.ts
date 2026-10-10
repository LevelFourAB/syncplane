// Undo/redo over the local pipeline — no server needed: mutations apply
// optimistically and queue, which is exactly what undo manipulates.

import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { CompleteAllArgsSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	type CrudMutation,
	CrudMutationSchema,
	CrudOp,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { type Inverse, invertCrud, UndoManager } from '../../src/undo.js';

function newLocalClient(undo: { enabled?: boolean; maxDepth?: number } | undefined = {}) {
	return new SyncClient({
		workspaceId: 'ws-undo',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema ],
		undo,
	});
}

describe('undo/redo', () => {
	it('undoes a PUT that created a row by deleting it, and redoes it back', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'hello' });
		expect(c.canUndo()).toBe(true);

		expect(await c.undo()).toBe(true);
		expect(await c.get(TodoSchema, 't1')).toBeUndefined();
		expect(c.canUndo()).toBe(false);
		expect(c.canRedo()).toBe(true);

		expect(await c.redo()).toBe(true);
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('hello');
		expect(c.canUndo()).toBe(true);
		expect(c.canRedo()).toBe(false);
		await c.close();
	});

	it('undoes a PUT over an existing row by restoring the previous value', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await c.mutate.put(TodoSchema, { id: 't1', title: 'v2' });

		await c.undo();
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('v1');
		await c.redo();
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('v2');
		await c.close();
	});

	it('undoes a PATCH by restoring only the masked fields', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'original', completed: false });
		await c.mutate.patch(TodoSchema, 't1', { title: 'renamed' });
		// A separate edit to another field must survive the undo of the rename.
		await c.mutate.patch(TodoSchema, 't1', { completed: true });

		await c.undo(); // undoes { completed: true }
		expect((await c.get(TodoSchema, 't1'))?.completed).toBe(false);
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('renamed');

		await c.undo(); // undoes the rename
		const todo = await c.get(TodoSchema, 't1');
		expect(todo?.title).toBe('original');
		await c.close();
	});

	it('undoes a nested PATCH back to the container being unset', async () => {
		const c = newLocalClient();
		await c.mutate.put(WidgetSchema, { id: 'w1', label: 'original' });
		await c.mutate.patch(WidgetSchema, 'w1', { settings: { color: 'blue' } }, {
			paths: [ 'settings.color' ],
		});
		expect((await c.get(WidgetSchema, 'w1'))?.settings?.color).toBe('blue');

		// The inverse replays the pre-patch snapshot (settings absent) under the
		// forward mask, so the container the patch created goes away with it.
		await c.undo();
		expect((await c.get(WidgetSchema, 'w1'))?.settings).toBeUndefined();

		await c.redo();
		expect((await c.get(WidgetSchema, 'w1'))?.settings?.color).toBe('blue');
		await c.close();
	});

	it('PATCH of a missing row is a no-op with nothing to undo (delete-wins)', async () => {
		const c = newLocalClient();
		await c.mutate.patch(TodoSchema, 'ghost', { title: 'made by patch' });
		// Delete-wins: a PATCH never resurrects an absent row, so no ghost
		// materializes locally and the undo stack stays empty.
		expect(await c.get(TodoSchema, 'ghost')).toBeUndefined();
		expect(c.canUndo()).toBe(false);
		await c.close();
	});

	it('undoes a DELETE by restoring the row', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'keep me' });
		await c.mutate.del(TodoSchema, 't1');
		expect(await c.get(TodoSchema, 't1')).toBeUndefined();

		await c.undo();
		expect((await c.get(TodoSchema, 't1'))?.title).toBe('keep me');
		await c.close();
	});

	it('restores a deleted row as a guarded create, not an unguarded PUT', () => {
		const del = create(CrudMutationSchema, {
			entity: 'todo',
			key: 't1',
			op: CrudOp.DELETE,
		});
		const inv = invertCrud(del, new Uint8Array([ 1, 2, 3 ]));
		expect(inv?.op).toBe(CrudOp.PUT);
		// Version 0 means "row must not exist": the undo conflicts instead of
		// overwriting whatever took the key, and it is a provable create, so it
		// is legal where updates are trustedOnly.
		expect(inv?.expectedVersion).toBe(0n);
	});

	it('leaves a field the server did not send out of a PATCH inverse', () => {
		const patch = (paths: string[]) =>
			create(CrudMutationSchema, {
				entity: 'staff',
				key: 's1',
				op: CrudOp.PATCH,
				fieldMask: { paths },
			});
		const prev = new Uint8Array([ 1, 2, 3 ]);
		expect(invertCrud(patch([ 'name', 'pin' ]), prev, [ 'pin', 'salary' ])?.fieldMask?.paths).toEqual(
			[
				'name',
			],
		);
		// Nothing left to restore: the caller never read the value it wrote.
		expect(invertCrud(patch([ 'pin' ]), prev, [ 'pin' ])).toBeUndefined();
	});

	it('deleting a missing row records nothing to undo', async () => {
		const c = newLocalClient();
		await c.mutate.del(TodoSchema, 'never-existed');
		expect(c.canUndo()).toBe(false);
		await c.close();
	});

	it('transact() groups mutations into one undo step', async () => {
		const c = newLocalClient();
		await c.transact(async (tx) => {
			await tx.mutate.put(TodoSchema, { id: 'a', title: 'A' });
			await tx.mutate.put(TodoSchema, { id: 'b', title: 'B' });
		});
		await c.mutate.put(TodoSchema, { id: 'c', title: 'C' });

		await c.undo(); // pops only "c"
		expect(await c.get(TodoSchema, 'c')).toBeUndefined();
		expect(await c.get(TodoSchema, 'a')).toBeDefined();

		await c.undo(); // pops the whole group
		expect(await c.get(TodoSchema, 'a')).toBeUndefined();
		expect(await c.get(TodoSchema, 'b')).toBeUndefined();

		await c.redo(); // restores the whole group
		expect(await c.get(TodoSchema, 'a')).toBeDefined();
		expect(await c.get(TodoSchema, 'b')).toBeDefined();
		await c.close();
	});

	it('nested transact() is an independent undo group', async () => {
		const c = newLocalClient();
		await c.transact(async (outer) => {
			await outer.mutate.put(TodoSchema, { id: 'a', title: 'A' });
			await c.transact(async (inner) => {
				await inner.mutate.put(TodoSchema, { id: 'b', title: 'B' });
			});
		});
		// Inner commits first as its own group, then outer; undo pops outer's 'a'.
		await c.undo();
		expect(await c.get(TodoSchema, 'a')).toBeUndefined();
		expect(await c.get(TodoSchema, 'b')).toBeDefined();
		await c.close();
	});

	it('custom mutations clear both stacks (barrier)', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		await c.undo();
		expect(c.canRedo()).toBe(true);

		await c.mutate.custom('complete_all', CompleteAllArgsSchema, {});
		expect(c.canUndo()).toBe(false);
		expect(c.canRedo()).toBe(false);
		await c.close();
	});

	it('a new edit clears the redo stack', async () => {
		const c = newLocalClient();
		await c.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await c.undo();
		expect(c.canRedo()).toBe(true);

		await c.mutate.put(TodoSchema, { id: 't2', title: 'other' });
		expect(c.canRedo()).toBe(false);
		await c.close();
	});

	it('evicts the oldest groups past maxDepth', async () => {
		const c = newLocalClient({ maxDepth: 2 });
		await c.mutate.put(TodoSchema, { id: 't1', title: '1' });
		await c.mutate.put(TodoSchema, { id: 't2', title: '2' });
		await c.mutate.put(TodoSchema, { id: 't3', title: '3' });

		expect(await c.undo()).toBe(true);
		expect(await c.undo()).toBe(true);
		expect(await c.undo()).toBe(false); // t1's step was evicted
		expect(await c.get(TodoSchema, 't1')).toBeDefined();
		await c.close();
	});

	it('returns false with empty stacks and throws when undo is disabled', async () => {
		const enabled = newLocalClient();
		expect(await enabled.undo()).toBe(false);
		expect(await enabled.redo()).toBe(false);
		await enabled.close();

		const disabled = new SyncClient({
			workspaceId: 'ws-undo',
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema, WidgetSchema ],
		});
		await disabled.mutate.put(TodoSchema, { id: 't1', title: 'x' });
		expect(disabled.canUndo()).toBe(false);
		await expect(disabled.undo()).rejects.toThrow('undo is not enabled');
		await disabled.close();
	});
});

describe('UndoManager.predictedRefIds', () => {
	function inverse(key: string): Inverse {
		return {
			kind: 'crud',
			crud: create(CrudMutationSchema, { entity: 'todo', key, op: CrudOp.DELETE }),
		};
	}

	/** A manager whose undo stack references id 1 and redo stack id 2. */
	function seeded(): UndoManager {
		const mgr = new UndoManager();
		mgr.recordGroup(1n, [ inverse('t1') ]);
		mgr.recordGroup(2n, [ inverse('t2') ], 'redo');
		return mgr;
	}

	it('a plain edit drops the redo stack and adds its own id', () => {
		const mgr = seeded();
		const predicted = mgr.predictedBarrierIds(3n, true, undefined, false);
		expect(predicted).toEqual(new Set([ 1n, 3n ]));
		// The prediction must equal what referencedIds() reports post-commit.
		mgr.recordGroup(3n, [ inverse('t3') ]);
		expect(mgr.foldBarrierIds()).toEqual(predicted);
	});

	it('an undo replay (captureStack set) keeps both stacks', () => {
		const mgr = seeded();
		const predicted = mgr.predictedBarrierIds(3n, true, 'redo', false);
		expect(predicted).toEqual(new Set([ 1n, 2n, 3n ]));
		mgr.recordGroup(3n, [ inverse('t3') ], 'redo');
		expect(mgr.foldBarrierIds()).toEqual(predicted);
	});

	it('a barrier (custom mutation) predicts the empty set', () => {
		const mgr = seeded();
		const predicted = mgr.predictedBarrierIds(3n, false, undefined, true);
		expect(predicted).toEqual(new Set());
		mgr.barrier();
		expect(mgr.foldBarrierIds()).toEqual(predicted);
	});

	it('a mutation without inverses does not add its own id', () => {
		const mgr = seeded();
		const predicted = mgr.predictedBarrierIds(3n, false, undefined, false);
		expect(predicted).toEqual(new Set([ 1n ]));
		mgr.recordGroup(3n, [ undefined ]);
		expect(mgr.foldBarrierIds()).toEqual(predicted);
	});
});
