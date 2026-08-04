import { describe, expect, it } from 'vitest';

import type { MessageShape } from '@bufbuild/protobuf';
import { subscription } from '../../src/client.js';
import type { EntityApi } from '../../src/db.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { and, not, type Query } from '../../src/query/index.js';
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

	it('answers a map-addressed predicate the same way both sides do', async () => {
		// The map key is a path segment on the wire, and one variant of an
		// app-defined map value is what a query names. Both are decisions the two
		// implementations have to agree on for the SAME condition to mean the
		// same thing, so this is the tier that can observe it.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, {
			id: 'w1',
			custom: {
				priority: { kind: { case: 'number', value: 5 } },
				team: { kind: { case: 'text', value: 'platform' } },
			},
		});
		await writer.mutate.put(WidgetSchema, {
			id: 'w2',
			// priority holds another variant, so `custom.priority.number` is not
			// found: the predicate is false and the ordering ranks it last.
			custom: { priority: { kind: { case: 'text', value: 'urgent' } } },
		});
		await writer.mutate.put(WidgetSchema, {
			id: 'w3',
			custom: { priority: { kind: { case: 'number', value: 1 } } },
		});
		await writer.syncNow();

		const cold = newClient(ws);
		const cases: WidgetQuery[] = [
			{ where: (w) => w.custom.at('priority').kind.number.gt(3) },
			{ where: (w) => w.custom.at('team').kind.text.eq('platform') },
			{ where: (w) => w.custom.at('priority').kind.number.isNotSet() },
			{ where: (w) => w.custom.isEmpty() },
			{ orderBy: (w) => [ w.custom.at('priority').kind.number.asc() ] },
		];
		for (const q of cases) {
			const remote = await cold.queryServer(WidgetSchema, q);
			const local = await writer.query(WidgetSchema, q);
			expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));
		}
		// Not merely equal to each other — equal to what the semantics say.
		expect(
			(await cold.queryServer(WidgetSchema, {
				where: (w) => w.custom.at('priority').kind.number.gt(3),
			})).map((r) => r.key),
		).toEqual([ 'w1' ]);
		expect(
			(await cold.queryServer(WidgetSchema, {
				orderBy: (w) => [ w.custom.at('priority').kind.number.asc() ],
			})).map((r) => r.key),
		).toEqual([ 'w3', 'w1', 'w2' ]);

		await writer.close();
		await cold.close();
	});

	it('answers a cross-key any() over a map the same way both sides do', async () => {
		// The MIRROR of the case above: the key is named inside the entry scope
		// rather than in the path, and one sub-condition may leave it open
		// entirely. The two implementations resolve `key` and `value` through
		// different machinery — Go through the map's MapKey/MapValue descriptors,
		// the client through a synthetic entry scope, because protobuf-es
		// surfaces no entry type — so agreeing is not automatic.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, {
			id: 'w1',
			custom: {
				priority: { kind: { case: 'number', value: 5 } },
				team: { kind: { case: 'text', value: 'platform' } },
			},
		});
		await writer.mutate.put(WidgetSchema, {
			id: 'w2',
			// A DIFFERENT key holding the same value, which is what makes "which
			// field holds 5" a question with two answers.
			custom: { size: { kind: { case: 'number', value: 5 } } },
		});
		await writer.mutate.put(WidgetSchema, {
			id: 'w3',
			custom: { priority: { kind: { case: 'number', value: 1 } } },
		});
		await writer.mutate.put(WidgetSchema, { id: 'w4', attrs: { color: 'red' } });
		await writer.syncNow();

		const cold = newClient(ws);
		const cases: WidgetQuery[] = [
			{ where: (w) => w.custom.any((e) => e.value.kind.number.eq(5)) },
			{ where: (w) => w.custom.any((e) => e.key.eq('team')) },
			{ where: (w) => w.custom.any((e) => e.key.in([ 'size', 'absent' ])) },
			{ where: (w) => w.custom.any((e) => e.key.startsWith('pri')) },
			{
				where: (w) => w.custom.any((e) => and(e.key.eq('priority'), e.value.kind.number.eq(5))),
			},
			{ where: (w) => not(w.custom.any((e) => e.value.kind.number.eq(5))) },
			// A scalar-valued map, where both sides of the entry are plain values.
			{ where: (w) => w.attrs.any((e) => e.value.eq('red')) },
			{ where: (w) => w.attrs.any((e) => and(e.key.eq('color'), e.value.eq('red'))) },
		];
		for (const q of cases) {
			const remote = await cold.queryServer(WidgetSchema, q);
			const local = await writer.query(WidgetSchema, q);
			expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));
		}
		// Not merely equal to each other — equal to what the semantics say.
		expect(
			(await cold.queryServer(WidgetSchema, {
				where: (w) => w.custom.any((e) => e.value.kind.number.eq(5)),
			})).map((r) => r.key),
		).toEqual([ 'w1', 'w2' ]);
		expect(
			(await cold.queryServer(WidgetSchema, {
				where: (w) => w.custom.any((e) => and(e.key.eq('team'), e.value.kind.number.eq(5))),
			})).map((r) => r.key),
			// Key and value from different entries describe no single entry.
		).toEqual([]);

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
