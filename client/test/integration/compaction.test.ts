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

	it('a refused PUT does not cancel an earlier PUT that created the row', async () => {
		// Given a client that creates a new todo row while offline
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't3', title: 'draft' });
		// And then writes the row again with a title that the server policy refuses
		await a.mutate.put(TodoSchema, { id: 't3', title: 'forbidden plan' });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then only the second write is refused
		expect(rejections).toHaveLength(1);
		// And the row that the first PUT created exists on the server with its title
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		// And must not be missing on the server or on the writer
		expect((await b.get(TodoSchema, 't3'))?.title).toBe('draft');
		expect((await a.get(TodoSchema, 't3'))?.title).toBe('draft');

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

	it('a PUT of a row read before the text was typed does not drop that text', async () => {
		// Given a widget whose text "base" the server holds, and a copy read now
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'a', body: 'base' });
		await a.syncNow();
		const snapshot = (await a.get(WidgetSchema, 'w1'))!;

		// And the user types, then saves the old copy with a new label
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'a', body: 'base typed offline' });
		await a.mutate.put(WidgetSchema, { ...snapshot, label: 'b' });
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('base typed offline');

		// When the client pushes the two writes in one compaction pass
		await a.syncNow();

		// Then the server keeps the typed text, as the separate writes would
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body)).toBe('base typed offline');
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('base typed offline');

		await a.close();
		await b.close();
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

	it('two guarded updates that fit the mask limit alone do not fold into one that does not', async () => {
		// Given a widget at a version the client read
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x' });
		await a.syncNow();
		const base = (await a.getRow(WidgetSchema, 'w1'))!.version;

		// And two updates guarded by that version, each adding 40 attrs entries
		// (the server takes at most 64 mask paths in one write)
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			for (let i = 0; i < 40; i++) w.attrs[`a${i}`] = 'v';
		}, { expectedVersion: base });
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			for (let i = 0; i < 40; i++) w.attrs[`b${i}`] = 'v';
		}, { expectedVersion: base });

		// When the client pushes the two writes in one compaction pass
		await a.syncNow();
		await a.syncNow();

		// Then the first update is stored, and the second is stored or refused
		// alone as a conflict with the first
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		const keys = Object.keys((await b.get(WidgetSchema, 'w1'))?.attrs ?? {});
		// And must not lose both to a refusal of the folded mask
		expect({
			first: keys.filter((k) => k.startsWith('a')).length,
			refusedOtherwise: rejections.map((r) => r.errorCode).filter((c) => c !== CodeConflict),
		}).toEqual({ first: 40, refusedOtherwise: [] });

		await a.close();
		await b.close();
	});

	it('offline text edits that fit the value limit alone are not lost together', async () => {
		// Given a widget with no text
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x' });
		await a.syncNow();

		// And three offline text edits that add 450 KiB each (the server takes
		// at most 1 MiB in one value). The limit counts UTF-8 bytes and the
		// text CRDT keeps one item per code point, so 4-byte code points reach
		// the limit with a quarter of the items 1-byte ones need, and keep the
		// spec inside its timeout on a loaded machine.
		const run = (i: number): string => String.fromCodePoint(0x1f600 + i).repeat(450 * 1024 / 4);
		let text = '';
		const afterTwo = run(0) + run(1);
		for (let i = 0; i < 3; i++) {
			text += run(i);
			await a.mutate.patch(WidgetSchema, 'w1', { body: text });
		}

		// When the client syncs
		for (let i = 0; i < 3; i++) await a.syncNow().catch(() => undefined);

		// Then the server holds all three edits: each one fits the value limit
		// alone, though all three together do not
		// And must not lose the first two with an edit that does not fit
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		const stored = plainTextOf((await b.get(WidgetSchema, 'w1'))?.body);
		expect({
			keepsFirstTwo: stored.startsWith(afterTwo),
			keepsAll: stored === text,
			rejections: rejections.map((r) => r.errorCode),
			pending: await a.pendingCount(),
		}).toEqual({ keepsFirstTwo: true, keepsAll: true, rejections: [], pending: 0 });

		await a.close();
		await b.close();
	}, 60_000);

	it('a later PUT without text is not refused for text an earlier refused PUT carried', async () => {
		// Given a widget with text, which another client changed after this
		// client read it
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one', body: 'hello' });
		await a.syncNow();
		const base = (await a.getRow(WidgetSchema, 'w1'))!.version;
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		await b.mutate.patch(WidgetSchema, 'w1', { label: 'two' });
		await b.syncNow();

		// And this client deletes the row guarded by the version it read (the
		// server refuses this), creates it again with new text, and then saves
		// a new label with a PUT that carries no text
		await a.mutate.del(WidgetSchema, 'w1', { expectedVersion: base });
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'three', body: 'world' });
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'four' });

		// When the client pushes the three writes in one compaction pass
		await a.syncNow();
		await a.syncNow();

		// Then the server stores the last label, as the separate writes give
		await b.syncNow();
		// And must not lose the last PUT with a refusal of the text it did not carry
		expect((await b.get(WidgetSchema, 'w1'))?.label).toBe('four');

		await a.close();
		await b.close();
	});

	it('a later PUT without text is not refused for text over the value limit that an earlier PUT carried', async () => {
		// Given a widget that the server holds
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one' });
		await a.syncNow();

		// And the client writes text that passes the server's value limit (the
		// server refuses this write), and then saves a new label with a PUT that
		// carries no text
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'big', body: 'x'.repeat(1_200_000) });
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'four' });

		// When the client pushes the two writes in one compaction pass
		await a.syncNow();
		await a.syncNow();

		// Then only the write with the large text is refused
		expect(rejections).toHaveLength(1);
		// And the server stores the last label, as the separate writes give
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))?.label).toBe('four');

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

	it('control: a PUT of an old copy with a push between keeps the typed text', async () => {
		// Given a widget whose text "base" the server holds, and a copy read now
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'a', body: 'base' });
		await a.syncNow();
		const snapshot = (await a.get(WidgetSchema, 'w1'))!;

		// When the typing and the save of the old copy go in separate pushes
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'a', body: 'base typed offline' });
		await a.syncNow();
		await a.mutate.put(WidgetSchema, { ...snapshot, label: 'b' });
		await a.syncNow();

		// Then the row keeps the typed text
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('base typed offline');
		await a.close();
	});

	it('control: two accepted PUTs of a new row store the later one', async () => {
		// Given a client that creates a new todo row while offline
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't3', title: 'draft' });
		// And then writes the row again with a title that the server accepts
		await a.mutate.put(TodoSchema, { id: 't3', title: 'final' });

		// When the client pushes the two writes
		await a.syncNow();
		await a.syncNow();

		// Then the server stores the later title
		expect(rejections).toHaveLength(0);
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(TodoSchema, 't3'))?.title).toBe('final');

		await a.close();
		await b.close();
	});

	it('control: a refused PUT pushed after the PUT that created the row leaves the row', async () => {
		// Given a client that creates a new todo row and pushes it
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(TodoSchema, { id: 't3', title: 'draft' });
		await a.syncNow();

		// When a write that the server policy refuses goes in a separate push
		await a.mutate.put(TodoSchema, { id: 't3', title: 'forbidden plan' });
		await a.syncNow();

		// Then the row keeps its first title
		expect((await a.get(TodoSchema, 't3'))?.title).toBe('draft');
		await a.close();
	});

	it('control: a refused delete, a text PUT and a PUT without text pushed one at a time store the last label', async () => {
		// Given a widget with text, which another client changed after this
		// client read it
		const ws = freshWorkspace();
		const a = newClient(ws, HOLD);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one', body: 'hello' });
		await a.syncNow();
		const base = (await a.getRow(WidgetSchema, 'w1'))!.version;
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		await b.mutate.patch(WidgetSchema, 'w1', { label: 'two' });
		await b.syncNow();

		// When the three writes go in separate pushes
		await a.mutate.del(WidgetSchema, 'w1', { expectedVersion: base });
		await a.syncNow();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'three', body: 'world' });
		await a.syncNow();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'four' });
		await a.syncNow();
		await a.syncNow();

		// Then the server stores the last label
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))?.label).toBe('four');

		await a.close();
		await b.close();
	});

	it('control: two offline text edits within the value limit are stored', async () => {
		// Given a widget with no text
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { ...HOLD, onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x' });
		await a.syncNow();

		// When two offline text edits add 450 KiB each, and the client syncs
		// (4-byte code points for the same reason as the spec above)
		const run = (i: number): string => String.fromCodePoint(0x1f600 + i).repeat(450 * 1024 / 4);
		const text = run(0) + run(1);
		await a.mutate.patch(WidgetSchema, 'w1', { body: run(0) });
		await a.mutate.patch(WidgetSchema, 'w1', { body: text });
		await a.syncNow();

		// Then the server holds both edits
		expect(rejections).toEqual([]);
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body) === text).toBe(true);

		await a.close();
		await b.close();
	}, 60_000);
});
