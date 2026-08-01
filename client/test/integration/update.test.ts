import { describe, expect, it } from 'vitest';

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
});
