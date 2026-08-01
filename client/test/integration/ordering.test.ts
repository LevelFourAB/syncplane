import { describe, expect, it } from 'vitest';

import { type Widget, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { freshWorkspace, newClient } from './helpers.js';

const texts = (w: Widget | undefined): string[] => (w?.items ?? []).map((i) => i.text);
const idOf = (w: Widget, text: string): string => w.items.find((i) => i.text === text)!.id;

describe('movable ordered lists (sync)', () => {
	it('inserts then reorders, converging on a second client', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1' });
		await a.syncNow();
		await b.syncNow();

		const list = a.mutate.list(WidgetSchema, 'w1', 'items');
		await list.insertAt(0, { text: 'one' });
		await list.insertAt(1, { text: 'two' });
		await list.insertAt(2, { text: 'three' });
		expect(texts(await a.get(WidgetSchema, 'w1'))).toEqual([ 'one', 'two', 'three' ]);

		await a.syncNow();
		await b.syncNow();
		expect(texts(await b.get(WidgetSchema, 'w1'))).toEqual([ 'one', 'two', 'three' ]);

		// Move 'three' to the front (LWW on its pos, other fields preserved).
		const threeId = idOf((await a.get(WidgetSchema, 'w1'))!, 'three');
		await list.move(threeId, 0);
		await a.syncNow();
		await b.syncNow();
		expect(texts(await a.get(WidgetSchema, 'w1'))).toEqual([ 'three', 'one', 'two' ]);
		expect(texts(await b.get(WidgetSchema, 'w1'))).toEqual([ 'three', 'one', 'two' ]);

		await a.close();
		await b.close();
	});

	it('two clients insert into the same list offline → both survive, ordered', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1' });
		await a.syncNow();
		await b.syncNow();

		// Disjoint inserts while offline (no interleaved sync).
		await a.mutate.list(WidgetSchema, 'w1', 'items').insertAt(0, { text: 'from-A' });
		await b.mutate.list(WidgetSchema, 'w1', 'items').insertAt(0, { text: 'from-B' });

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			expect(texts(await client.get(WidgetSchema, 'w1')).sort()).toEqual([ 'from-A', 'from-B' ]);
		}
		// Both clients agree on the same order (positions are total + deterministic).
		expect(texts(await a.get(WidgetSchema, 'w1'))).toEqual(texts(await b.get(WidgetSchema, 'w1')));

		await a.close();
		await b.close();
	});

	it('concurrent move of one element → LWW, no duplication', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1' });
		const seed = a.mutate.list(WidgetSchema, 'w1', 'items');
		await seed.insertAt(0, { text: 'x' });
		await seed.insertAt(1, { text: 'y' });
		await seed.insertAt(2, { text: 'z' });
		await a.syncNow();
		await b.syncNow();

		const xId = idOf((await a.get(WidgetSchema, 'w1'))!, 'x');
		// Both clients move the same element concurrently to different slots.
		await a.mutate.list(WidgetSchema, 'w1', 'items').move(xId, 2);
		await b.mutate.list(WidgetSchema, 'w1', 'items').move(xId, 1);

		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		for (const client of [ a, b ]) {
			const w = (await client.get(WidgetSchema, 'w1'))!;
			expect(w.items).toHaveLength(3); // no duplication — identity is stable
			expect(w.items.map((i) => i.text).sort()).toEqual([ 'x', 'y', 'z' ]);
		}
		expect(texts(await a.get(WidgetSchema, 'w1'))).toEqual(texts(await b.get(WidgetSchema, 'w1')));

		await a.close();
		await b.close();
	});
});
