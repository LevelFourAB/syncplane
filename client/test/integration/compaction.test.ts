// Pending-queue compaction against the real server. The fold must give the
// same stored row as the separate writes would, and only a server can show
// that. Two kinds of burst on one row show the difference:
//
//  - The server refuses one write for a reason the client cannot see (a
//    permission hook, a field protected for one subject only). The writes it
//    accepts must still be stored.
//  - The burst deletes the row and re-creates it. The server treats a PUT on a
//    tombstone differently from one on a live row (text does not union,
//    protected fields are not kept), so the re-created row must hold the new
//    values only.

import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { CodeConflict } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { GuardedSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { plainTextOf } from '../../src/text/index.js';
import { authenticatedTransport, freshWorkspace, newClient } from './helpers.js';

/** Keeps a burst of writes in the queue until the spec calls syncNow, so the
 * whole burst is in one compaction pass. */
const HOLD = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };

describe('compaction stores what the separate writes store', () => {
	it('a refused PATCH does not take an earlier accepted PATCH of the same row with it', async () => {
		// Given a todo row that the server holds
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'buy milk' });
		await a.syncNow();

		// And the user marks it done (the server accepts this write)
		await a.mutate.patch(TodoSchema, 't1', { completed: true });
		// And then the user sets a title that the server policy refuses
		await a.mutate.patch(TodoSchema, 't1', { title: 'forbidden plan' });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then only the title write is refused
		expect(rejections).toHaveLength(1);
		// And the stored row keeps the accepted write: completed, old title
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		const stored = await b.get(TodoSchema, 't1');
		expect(stored?.title).toBe('buy milk');
		// And must not lose the "completed" write that the server would accept alone
		expect(stored?.completed).toBe(true);
		expect((await a.get(TodoSchema, 't1'))?.completed).toBe(true);

		await a.close();
		await b.close();
	});

	it('a refused PATCH does not cancel the PUT that created the row', async () => {
		// Given a client that creates a new todo row while offline
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't2', title: 'draft' });
		// And then sets a title that the server policy refuses
		await a.mutate.patch(TodoSchema, 't2', { title: 'forbidden plan' });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then only the title write is refused
		expect(rejections).toHaveLength(1);
		// And the row that the PUT created exists on the server with its first title
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		// And must not be missing because the refusal also cancelled the PUT
		expect((await b.get(TodoSchema, 't2'))?.title).toBe('draft');

		await a.close();
		await b.close();
	});

	it('a PATCH of a field only the server protects does not take an allowed PATCH with it', async () => {
		// Given a guarded row, and a client authenticated as "viewer" (the server
		// policy protects `label` for this subject only, so the client cannot know)
		const ws = freshWorkspace();
		const editor = newClient(ws, {
			entities: [ GuardedSchema ],
			transport: authenticatedTransport('editor'),
		});
		await editor.start();
		await editor.mutate.put(GuardedSchema, { id: 'g1', label: 'made' });
		await editor.syncNow();

		const rejections: RejectionInfo[] = [];
		const viewer = newClient(ws, {
			...HOLD,
			entities: [ GuardedSchema ],
			transport: authenticatedTransport('viewer'),
			onMutationRejected: (info) => rejections.push(info),
		});
		await viewer.start();
		await viewer.syncNow();

		// And the viewer edits a field it may write
		await viewer.mutate.patch(GuardedSchema, 'g1', { settings: { color: 'dark' } });
		// And then edits the protected label
		await viewer.mutate.patch(GuardedSchema, 'g1', { label: 'edited' });

		// When the viewer pushes the two writes
		await viewer.syncNow();
		await viewer.syncNow();

		// Then only the label write is refused
		expect(rejections).toHaveLength(1);
		// And the allowed settings write is stored
		await editor.syncNow();
		const stored = await editor.get(GuardedSchema, 'g1');
		expect(stored?.label).toBe('made');
		// And must not be lost with the refused label write
		expect(stored?.settings?.color).toBe('dark');

		await editor.close();
		await viewer.close();
	});

	it('delete then re-create of a row does not bring back the deleted text', async () => {
		// Given a widget whose collaborative text "hello" the server holds
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', body: 'hello' });
		await a.syncNow();

		// And the user deletes the widget
		await a.mutate.del(WidgetSchema, 'w1');
		// And creates a new widget with the same key and the text "world"
		await a.mutate.put(WidgetSchema, { id: 'w1', body: 'world' });
		// And before the push, the local view shows only the new text
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('world');

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then the server stores the new text only, as DELETE then PUT gives
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body)).toBe('world');
		// And the writer must not show the deleted text merged into the new row
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('world');

		await a.close();
		await b.close();
	});

	it('delete then re-create of a row does not carry a protected field over', async () => {
		// Given a guarded row whose protected `owner` a trusted mutator set to "alice"
		const ws = freshWorkspace();
		const a = newClient(ws, { ...HOLD, entities: [ GuardedSchema ] });
		await a.start();
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'old' });
		await a.mutate.custom('set_owner', GuardedSchema, { id: 'g1', owner: 'alice' });
		await a.syncNow();
		await a.syncNow();
		expect((await a.get(GuardedSchema, 'g1'))?.owner).toBe('alice');

		// And the client deletes the row
		await a.mutate.del(GuardedSchema, 'g1');
		// And creates a new row with the same key
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'new' });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then the new row has no owner: a client cannot seed a protected field
		// on a row it creates, and DELETE then PUT creates the row
		const b = newClient(ws, { entities: [ GuardedSchema ] });
		await b.start();
		await b.syncNow();
		const stored = await b.get(GuardedSchema, 'g1');
		expect(stored?.label).toBe('new');
		// And must not keep the owner that trusted code gave the deleted row
		expect(stored?.owner).toBe('');

		await a.close();
		await b.close();
	});
	it('a guarded delete then re-create does not carry a protected field over', async () => {
		// Given a guarded row whose protected `owner` a trusted mutator set to "alice"
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, {
			...HOLD,
			entities: [ GuardedSchema ],
			onMutationRejected: (info) => rejections.push(info),
		});
		await a.start();
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'old' });
		await a.mutate.custom('set_owner', GuardedSchema, { id: 'g1', owner: 'alice' });
		await a.syncNow();
		await a.syncNow();
		const base = (await a.getRow(GuardedSchema, 'g1'))!.version;

		// And the client deletes the row and creates it again, both writes
		// guarded by the version it read
		await a.mutate.del(GuardedSchema, 'g1', { expectedVersion: base });
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'new' }, { expectedVersion: base });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then the delete applies, and the re-create conflicts with it: the row
		// is no longer at the version the PUT names
		expect(rejections).toHaveLength(1);
		expect(rejections[0]?.errorCode).toBe(CodeConflict);
		// And the row must not stay live with the owner that trusted code gave it
		const b = newClient(ws, { entities: [ GuardedSchema ] });
		await b.start();
		await b.syncNow();
		expect(await b.get(GuardedSchema, 'g1')).toBeUndefined();

		await a.close();
		await b.close();
	});

	it('control: the accepted PATCH is stored when it is pushed alone', async () => {
		// Given a todo row that the server holds
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'buy milk' });
		await a.syncNow();

		// When the accepted write and the refused write go in separate pushes
		await a.mutate.patch(TodoSchema, 't1', { completed: true });
		await a.syncNow();
		await a.mutate.patch(TodoSchema, 't1', { title: 'forbidden plan' });
		await a.syncNow();

		// Then the accepted write is stored
		const stored = await a.get(TodoSchema, 't1');
		expect(stored?.completed).toBe(true);
		expect(stored?.title).toBe('buy milk');
		await a.close();
	});

	it('control: delete then re-create with a push between gives the new text only', async () => {
		// Given a widget whose text "hello" the server holds
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', body: 'hello' });
		await a.syncNow();

		// When the delete and the re-create go in separate pushes
		await a.mutate.del(WidgetSchema, 'w1');
		await a.syncNow();
		await a.mutate.put(WidgetSchema, { id: 'w1', body: 'world' });
		await a.syncNow();

		// Then the row holds the new text only
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('world');
		await a.close();
	});

	it('control: delete then re-create with a push between clears the protected field', async () => {
		// Given a guarded row whose protected `owner` a trusted mutator set to "alice"
		const ws = freshWorkspace();
		const a = newClient(ws, { ...HOLD, entities: [ GuardedSchema ] });
		await a.start();
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'old' });
		await a.mutate.custom('set_owner', GuardedSchema, { id: 'g1', owner: 'alice' });
		await a.syncNow();
		await a.syncNow();

		// When the delete and the re-create go in separate pushes
		await a.mutate.del(GuardedSchema, 'g1');
		await a.syncNow();
		await a.mutate.put(GuardedSchema, { id: 'g1', label: 'new' });
		await a.syncNow();

		// Then the new row has no owner
		const stored = await a.get(GuardedSchema, 'g1');
		expect(stored?.label).toBe('new');
		expect(stored?.owner).toBe('');
		await a.close();
	});
});
