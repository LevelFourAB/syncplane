import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// Per-item merge end-to-end: two clients that each touch a different item of the
// same collection offline converge with both items after sync, instead of the
// last writer clobbering the whole collection.
describe('per-item collection merge (sync)', () => {
	it('two clients add disjoint list items offline → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', members: [ { uid: 'u0', name: 'base' } ] });
		await a.syncNow();
		await b.syncNow();

		// Each appends a distinct member while "offline" (no interleaved sync).
		const ar = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', {
			members: [ ...ar.members, { uid: 'ua', name: 'A' } ],
		});
		const br = (await b.get(WidgetSchema, 'w1'))!;
		await b.mutate.patch(WidgetSchema, 'w1', {
			members: [ ...br.members, { uid: 'ub', name: 'B' } ],
		});

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const uids = (await client.get(WidgetSchema, 'w1'))!.members.map((m) => m.uid).sort();
			expect(uids).toEqual([ 'u0', 'ua', 'ub' ]);
		}

		await a.close();
		await b.close();
	});

	it('two clients edit disjoint map entries offline → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', attrs: { base: '0' } });
		await a.syncNow();
		await b.syncNow();

		const ar = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', { attrs: { ...ar.attrs, a: '1' } });
		const br = (await b.get(WidgetSchema, 'w1'))!;
		await b.mutate.patch(WidgetSchema, 'w1', { attrs: { ...br.attrs, b: '2' } });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			expect((await client.get(WidgetSchema, 'w1'))!.attrs).toEqual({ base: '0', a: '1', b: '2' });
		}

		await a.close();
		await b.close();
	});

	it('one client edits the empty-key map entry, another a different entry → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', attrs: { k: 'old' }, tags: [ 'base' ] });
		await a.syncNow();
		await b.syncNow();

		const ar = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', {
			attrs: { ...ar.attrs, k: 'new' },
			tags: [ 'base', 'a' ],
		});
		await a.syncNow();
		// b has not seen a's edits, and adds the entry and the element "".
		const br = (await b.get(WidgetSchema, 'w1'))!;
		await b.mutate.patch(WidgetSchema, 'w1', {
			attrs: { ...br.attrs, '': 'x' },
			tags: [ ...br.tags, '' ],
		});
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const row = (await client.get(WidgetSchema, 'w1'))!;
			expect(row.attrs).toEqual({ k: 'new', '': 'x' });
			expect([ ...row.tags ].sort()).toEqual([ '', 'a', 'base' ]);
		}

		await a.close();
		await b.close();
	});

	it('two clients add disjoint scalar set values offline → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', tags: [ 'base' ] });
		await a.syncNow();
		await b.syncNow();

		const ar = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', { tags: [ ...ar.tags, 'a' ] });
		const br = (await b.get(WidgetSchema, 'w1'))!;
		await b.mutate.patch(WidgetSchema, 'w1', { tags: [ ...br.tags, 'b' ] });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			expect((await client.get(WidgetSchema, 'w1'))!.tags.sort()).toEqual([ 'a', 'b', 'base' ]);
		}

		await a.close();
		await b.close();
	});

	// A collection patched to the value it holds narrows to no item path. The
	// write changes nothing, so the server must not see it as a PATCH with an
	// empty field mask.
	it('a patch that sets a map to the entries it holds is not refused', async () => {
		// Given a widget whose attrs the server holds
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one', attrs: { k: 'v' } });
		await a.syncNow();

		// When the client patches attrs with the same entries and syncs
		await a.mutate.patch(WidgetSchema, 'w1', { attrs: { k: 'v' } });
		await a.syncNow();
		await a.syncNow();

		// Then nothing is refused and the queue drains
		expect(rejections).toEqual([]);
		expect(await a.pendingCount()).toBe(0);
		expect((await a.get(WidgetSchema, 'w1'))!.attrs).toEqual({ k: 'v' });

		await a.close();
	});

	it('a patch that sets an empty map to empty is not refused', async () => {
		// Given a widget with no attrs on the server
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one' });
		await a.syncNow();

		// When the client patches attrs to {} and syncs
		await a.mutate.patch(WidgetSchema, 'w1', { attrs: {} });
		await a.syncNow();
		await a.syncNow();

		// Then nothing is refused and the queue drains
		expect(rejections).toEqual([]);
		expect(await a.pendingCount()).toBe(0);

		await a.close();
	});

	it('a no-op collection patch does not roll back the transact() batch it is in', async () => {
		// Given a widget whose tags the server holds
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'one', tags: [ 'x' ] });
		await a.syncNow();

		// When one batch patches tags to the value they hold and creates a second row
		await a.transact(async (tx) => {
			await tx.mutate.patch(WidgetSchema, 'w1', { tags: [ 'x' ] });
			await tx.mutate.put(WidgetSchema, { id: 'w2', label: 'created in batch' });
		});
		expect((await a.get(WidgetSchema, 'w2'))?.label).toBe('created in batch');
		await a.syncNow();
		await a.syncNow();

		// Then the row the batch created is stored on the server
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w2'))?.label).toBe('created in batch');
		// And must not disappear from the writer when the batch is acked
		expect((await a.get(WidgetSchema, 'w2'))?.label).toBe('created in batch');
		// And the batch is not refused
		expect(rejections).toEqual([]);

		await a.close();
		await b.close();
	});

	it('control: a patch that adds a scalar set value is stored', async () => {
		// Given a widget whose tags the server holds
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', tags: [ 'x' ] });
		await a.syncNow();

		// When the client patches tags with one more value
		await a.mutate.patch(WidgetSchema, 'w1', { tags: [ 'x', 'y' ] });
		await a.syncNow();

		// Then the server stores both values
		expect(rejections).toEqual([]);
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))!.tags.sort()).toEqual([ 'x', 'y' ]);

		await a.close();
		await b.close();
	});

	it('undo of an added item clears it; undo of a removed item restores it', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { undo: {} });
		await a.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', members: [ { uid: 'u1', name: 'one' } ] });

		// Add u2, then undo → back to just u1.
		const cur = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', {
			members: [ ...cur.members, { uid: 'u2', name: 'two' } ],
		});
		expect(uids(await a.get(WidgetSchema, 'w1'))).toEqual([ 'u1', 'u2' ]);
		await a.undo();
		expect(uids(await a.get(WidgetSchema, 'w1'))).toEqual([ 'u1' ]);

		// Now with [u1, u2] confirmed, remove u2, then undo → u2 restored.
		const two = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', {
			members: [ ...two.members, { uid: 'u2', name: 'two' } ],
		});
		const both = (await a.get(WidgetSchema, 'w1'))!;
		await a.mutate.patch(WidgetSchema, 'w1', {
			members: both.members.filter((m) => m.uid !== 'u2'),
		});
		expect(uids(await a.get(WidgetSchema, 'w1'))).toEqual([ 'u1' ]);
		await a.undo();
		expect(uids(await a.get(WidgetSchema, 'w1'))).toEqual([ 'u1', 'u2' ]);

		await a.close();
	});
});

function uids(w: Widget | undefined): string[] {
	return (w?.members ?? []).map((m) => m.uid).sort();
}
