import { describe, expect, it } from 'vitest';

import type { MessageShape } from '@bufbuild/protobuf';
import { subscription } from '../../src/client.js';
import type { EntityApi } from '../../src/db.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { Query } from '../../src/query/index.js';
import { freshWorkspace, newClient } from './helpers.js';

type WidgetQuery = Query<MessageShape<typeof WidgetSchema>>;

describe('queryServer (push-down)', () => {
	it('returns rows outside the subscription, ordered like the local evaluator', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 30, label: 'x' });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 5, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w3', count: 20, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w4', count: 20, label: 'a' });
		await writer.syncNow();

		// The reader only syncs count >= 25 — most rows are out of its subset.
		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, (w) => w.count.gte(25)) ],
		});
		await reader.start();
		await reader.syncNow();
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w1' ]);

		const q: WidgetQuery = {
			where: (w) => w.count.gte(10),
			orderBy: (w) => [ w.count.desc(), w.label.asc() ],
		};
		const remote = await reader.queryServer(WidgetSchema, q);
		expect(remote.map((r) => r.key)).toEqual([ 'w1', 'w4', 'w3' ]);

		// Parity: the server's ordering matches the local evaluator on the
		// same data (the writer holds the full set locally).
		const local = await writer.query(WidgetSchema, q);
		expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));
		expect(remote.map((r) => r.value.count)).toEqual(local.map((r) => r.value.count));

		// The reader's synced subset is untouched by the server query.
		expect((await reader.scan(WidgetSchema)).map((r) => r.key)).toEqual([ 'w1' ]);

		await writer.close();
		await reader.close();
	});

	it('is reachable from the schema-bound db facade', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 30 });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 5 });
		await writer.syncNow();

		// Nothing is synced locally, so a local query sees nothing and the
		// server query sees the row — the whole point of the push-down path.
		const cold = newClient(ws);
		// newClient returns an unparameterized SyncClient, so its db facade is
		// not statically bound to the entity list; db typing is covered by the
		// unit suite, this is about the call reaching the server.
		const widgets = cold.db.widget as EntityApi<typeof WidgetSchema>;
		expect(await widgets.query({ where: (w) => w.count.gte(10) })).toEqual([]);

		const rows = await widgets.queryServer({ where: (w) => w.count.gte(10) });
		expect(rows.map((r) => r.key)).toEqual([ 'w1' ]);
		expect(rows[0]?.value.count).toBe(30);

		await writer.close();
		await cold.close();
	});

	it('applies limit and offset server-side and works before start()', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		for (
			const [ id, count ] of [
				[ 'w1', 1 ],
				[ 'w2', 2 ],
				[ 'w3', 3 ],
				[ 'w4', 4 ],
			] as const
		) {
			await writer.mutate.put(WidgetSchema, { id, count });
		}
		await writer.syncNow();

		const cold = newClient(ws); // never started: transport resolves lazily
		const rows = await cold.queryServer(WidgetSchema, {
			orderBy: (w) => [ w.count.desc() ],
			limit: 2,
			offset: 1,
		});
		expect(rows.map((r) => r.key)).toEqual([ 'w3', 'w2' ]);

		await writer.close();
		await cold.close();
	});
});
