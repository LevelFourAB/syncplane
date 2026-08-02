import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

describe('undo/redo over sync', () => {
	it('do → sync → undo → sync converges identically on both clients', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await a.syncNow();
		await a.mutate.patch(TodoSchema, 't1', { title: 'v2' });
		await a.syncNow();
		await b.syncNow();
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('v2');

		// Undo travels as a real mutation: both clients converge on v1.
		await a.undo();
		await a.syncNow();
		await b.syncNow();
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('v1');
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('v1');

		// Redo converges back on v2.
		await a.redo();
		await a.syncNow();
		await b.syncNow();
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('v2');
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('v2');

		await a.close();
		await b.close();
	});

	it('documents the accepted clobber: undo overwrites a concurrent remote edit to the same field', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'base' });
		await a.syncNow();
		await b.syncNow();

		await a.mutate.patch(TodoSchema, 't1', { title: "A's edit" });
		await a.syncNow();
		await b.syncNow();

		// B edits the same field after A's edit synced…
		await b.mutate.patch(TodoSchema, 't1', { title: "B's edit" });
		await b.syncNow();

		// …then A undoes its own edit: the inverse patch (title := "base")
		// sequences AFTER B's edit and wins per-field LWW. Accepted semantics.
		await a.undo();
		await a.syncNow();
		await b.syncNow();
		await a.syncNow();
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('base');
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('base');

		await a.close();
		await b.close();
	});

	it('a rejected mutation drops its undo group', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 'ok', title: 'fine' });
		await a.syncNow();

		await a.mutate.put(TodoSchema, { id: 'bad', title: 'forbidden fruit' });
		expect(a.canUndo()).toBe(true);
		await a.syncNow(); // server rejects; rollback + undo-group drop

		// The rejected put's group is gone; the surviving group is the first put.
		expect(a.canUndo()).toBe(true);
		await a.undo();
		await a.syncNow();
		expect(await a.get(TodoSchema, 'ok')).toBeUndefined();
		expect(a.canUndo()).toBe(false);

		await a.close();
	});

	it('an unrelated mutation racing undo() is recorded as a user edit, not onto the redo stack', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'v1' });
		await a.syncNow();
		await a.mutate.patch(TodoSchema, 't1', { title: 'v2' });
		await a.syncNow();

		// Fire an unrelated put while undo() is mid-flight (its enqueueInverses is
		// suspended on the kv mutex). With the capture flag scoped per-call the put
		// must record its inverse on the undo stack and clear redo — not be misrouted
		// onto the redo stack under the undo's capture group.
		await Promise.all([
			a.undo(),
			a.mutate.put(TodoSchema, { id: 't2', title: 'unrelated' }),
		]);
		await a.syncNow();

		// Undo applied (t1 back to v1) and the unrelated put landed (t2 exists).
		expect((await a.get(TodoSchema, 't1'))?.title).toBe('v1');
		expect((await a.get(TodoSchema, 't2'))?.title).toBe('unrelated');

		// The unrelated put is on the UNDO stack: undoing now removes t2. With the
		// bug its inverse was pushed to the redo stack, leaving nothing to undo.
		expect(a.canUndo()).toBe(true);
		expect(await a.undo()).toBe(true);
		await a.syncNow();
		expect(await a.get(TodoSchema, 't2')).toBeUndefined();

		await a.close();
	});

	it("a rejection drops the group from an undo SCOPE's stack too", async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		await a.start();

		await a.mutate.put(TodoSchema, { id: 'ok', title: 'fine' });
		await a.syncNow();

		// The dead mutation is issued through a nested scope, so the stack that
		// has to hear about the rejection is the scope's, not the workspace's.
		const dialog = a.undoScope();
		await dialog.mutate.put(TodoSchema, { id: 'bad', title: 'forbidden fruit' });
		expect(dialog.canUndo()).toBe(true);
		await a.syncNow(); // server rejects; rollback + undo-group drop

		expect(dialog.canUndo()).toBe(false);
		expect(await dialog.undo()).toBe(false);
		// The workspace's own step is untouched by a rejection it had no part in.
		expect(a.canUndo()).toBe(true);

		await a.close();
	});

	it("undoes a scope's text edit without touching the workspace's stack", async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		await a.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'widget' });
		await a.syncNow();
		// The view renames the row; the dialog edits its collaborative body.
		await a.mutate.patch(WidgetSchema, 'w1', { label: 'renamed' });
		const dialog = a.undoScope();
		const body = await dialog.text(WidgetSchema, 'w1', 'body');
		await body.insert(0, 'hello');
		await a.syncNow();
		expect(body.value).toBe('hello');

		// Undo in the dialog replays the symbolic text inverse and nothing else:
		// the rename stays, and it is still the view's to undo.
		expect(await dialog.undo()).toBe(true);
		await a.syncNow();
		expect(body.value).toBe('');
		expect((await a.get(WidgetSchema, 'w1'))?.label).toBe('renamed');
		expect(await dialog.undo()).toBe(false);

		expect(await a.undo()).toBe(true);
		await a.syncNow();
		expect((await a.get(WidgetSchema, 'w1'))?.label).toBe('widget');

		body.close();
		await a.close();
	});
});
