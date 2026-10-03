// Pending-queue compaction against the real server. The fold must give the
// same stored row as the separate writes would, and only a server can show
// that: it refuses writes for reasons the client cannot see (a permission hook,
// a field protected for one subject only). Each spec queues a burst on one row
// in which the server refuses one write, then checks that the writes it
// accepts are still stored.

import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { GuardedSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { authenticatedTransport, freshWorkspace, newClient } from './helpers.js';

/** Keeps a burst of writes in the queue until the spec calls syncNow, so the
 * whole burst is in one compaction pass. */
const HOLD = { pushDebounceMs: 60_000, pushMaxDelayMs: 60_000 };

describe('compaction keeps the writes the server accepts', () => {
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
});
