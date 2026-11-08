import { describe, expect, it } from 'vitest';

import type { RejectionInfo } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

// mutate.update() end-to-end against the real server. The claim under test is
// the one that makes the verb worth having: because the mask covers only the
// fields the recipe changed, two clients that edit DIFFERENT fields of the same
// row offline both keep their edit — where writing the whole edited row back
// (put) means the later arrival wins everything, stale fields included.
describe('update (sync)', () => {
	it('two clients update disjoint fields offline → both survive', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'base', count: 0 });
		await a.syncNow();
		await b.syncNow();

		// Each edits its own field while "offline" (no interleaved sync). Both
		// recipes read a draft carrying the OTHER field's pre-edit value.
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.label = 'from A';
		});
		await b.mutate.update(WidgetSchema, 'w1', (w) => {
			w.count = 7;
		});

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const w = (await client.get(WidgetSchema, 'w1'))!;
			expect(w.label).toBe('from A');
			expect(w.count).toBe(7);
		}

		await a.close();
		await b.close();
	});

	it('saves an edit made against a stale read without losing the newer field', async () => {
		// The hazard in its realistic form: B reads a row, something else changes
		// a different field of it, and only then does B save. A whole-row write
		// carries B's stale copy of that field and undoes the newer value —
		// asserted here as the contrast — while an update writes only what the
		// recipe touched.
		const staleThenPut = async (): Promise<{ label: string; count: number }> => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			const b = newClient(ws);
			await a.start();
			await b.start();
			await a.mutate.put(WidgetSchema, { id: 'w1', label: 'base', count: 0 });
			await a.syncNow();
			await b.syncNow();

			const stale = (await b.get(WidgetSchema, 'w1'))!; // held across the change below
			await a.mutate.update(WidgetSchema, 'w1', (w) => {
				w.count = 7;
			});
			await a.syncNow();
			await b.syncNow();

			await b.mutate.put(WidgetSchema, { ...stale, label: 'from B' });
			await b.syncNow();
			await a.syncNow();
			const w = (await a.get(WidgetSchema, 'w1'))!;
			await a.close();
			await b.close();
			return { label: w.label, count: w.count };
		};

		const staleThenUpdate = async (): Promise<{ label: string; count: number }> => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			const b = newClient(ws);
			await a.start();
			await b.start();
			await a.mutate.put(WidgetSchema, { id: 'w1', label: 'base', count: 0 });
			await a.syncNow();
			await b.syncNow();

			await a.mutate.update(WidgetSchema, 'w1', (w) => {
				w.count = 7;
			});
			await a.syncNow();
			await b.syncNow();

			await b.mutate.update(WidgetSchema, 'w1', (w) => {
				w.label = 'from B';
			});
			await b.syncNow();
			await a.syncNow();
			const w = (await a.get(WidgetSchema, 'w1'))!;
			await a.close();
			await b.close();
			return { label: w.label, count: w.count };
		};

		// The whole-row write undoes count; the update keeps both.
		expect(await staleThenPut()).toEqual({ label: 'from B', count: 0 });
		expect(await staleThenUpdate()).toEqual({ label: 'from B', count: 7 });
	});

	it('two clients update disjoint items of one collection → both survive', async () => {
		// The diff names the collection whole; the enqueue path narrows it to the
		// touched item, so per-item merge still applies to an update.
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', attrs: { base: '0' } });
		await a.syncNow();
		await b.syncNow();

		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.attrs.a = '1';
		});
		await b.mutate.update(WidgetSchema, 'w1', (w) => {
			w.attrs.b = '2';
		});

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			expect((await client.get(WidgetSchema, 'w1'))!.attrs).toEqual({ base: '0', a: '1', b: '2' });
		}

		await a.close();
		await b.close();
	});

	it('an update that changes nothing never reaches the server', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'same' });
		await a.syncNow();

		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.label = 'same';
		});
		expect(await a.pendingCount()).toBe(0);

		await a.close();
	});

	// The enqueue path narrows a collection to one mask path per changed item,
	// so the number of paths follows the data. The server refuses a mask of
	// more than 64 paths (MaxFieldMaskPaths); an update the app wrote as one
	// field must still reach it.
	it('an update that adds 65 values to a scalar set is stored', async () => {
		// Given a widget with no tags
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x', tags: [] });
		await a.syncNow();

		// When the client adds 65 tags in one update and syncs
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			for (let i = 0; i < 65; i++) w.tags.push(`t${i}`);
		});
		await a.syncNow();
		await a.syncNow();

		// Then the server stores all 65 tags
		// And the update must not be refused for the number of mask paths
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect({ tags: (await b.get(WidgetSchema, 'w1'))?.tags.length, rejections }).toEqual({
			tags: 65,
			rejections: [],
		});

		await a.close();
		await b.close();
	});

	it('an update that clears a 100-entry map and renames the label is stored', async () => {
		// Given a widget with 100 attrs entries
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		const attrs: Record<string, string> = {};
		for (let i = 0; i < 100; i++) attrs[`k${i}`] = 'v';
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x', attrs });
		await a.syncNow();

		// When the client clears attrs and renames the label in one update
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			w.attrs = {};
			w.label = 'cleared';
		});
		await a.syncNow();
		await a.syncNow();

		// Then the server stores both changes
		// And the label change must not be lost with the attrs change
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		const stored = await b.get(WidgetSchema, 'w1');
		expect({
			label: stored?.label,
			attrs: Object.keys(stored?.attrs ?? {}).length,
			rejections,
		}).toEqual({ label: 'cleared', attrs: 0, rejections: [] });

		await a.close();
		await b.close();
	});

	it('a patch whose explicit paths are over the limit rejects and writes nothing', async () => {
		// Given a widget with no attrs
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x' });

		// When the client patches 65 map entries by explicit paths
		const attrs: Record<string, string> = {};
		const paths: string[] = [];
		for (let i = 0; i < 65; i++) {
			attrs[`k${i}`] = 'v';
			paths.push(`attrs.k${i}`);
		}
		const write = a.mutate.patch(WidgetSchema, 'w1', { attrs }, { paths });

		// Then the patch rejects, and nothing of it reaches the local row
		await expect(write).rejects.toThrow(/over the limit of 64/);
		expect(Object.keys((await a.get(WidgetSchema, 'w1'))?.attrs ?? {})).toEqual([]);

		await a.close();
	});

	it('control: an update that adds 64 values to a scalar set is stored', async () => {
		// Given a widget with no tags
		const ws = freshWorkspace();
		const rejections: RejectionInfo[] = [];
		const a = newClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		await a.start();
		await a.mutate.put(WidgetSchema, { id: 'w1', label: 'x', tags: [] });
		await a.syncNow();

		// When the client adds 64 tags in one update and syncs
		await a.mutate.update(WidgetSchema, 'w1', (w) => {
			for (let i = 0; i < 64; i++) w.tags.push(`t${i}`);
		});
		await a.syncNow();

		// Then the server stores all 64 tags
		expect(rejections).toEqual([]);
		const b = newClient(ws);
		await b.start();
		await b.syncNow();
		expect((await b.get(WidgetSchema, 'w1'))?.tags.length).toBe(64);

		await a.close();
		await b.close();
	});
});
