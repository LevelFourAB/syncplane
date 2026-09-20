import { describe, expect, it } from 'vitest';

import type { MessageShape } from '@bufbuild/protobuf';
import { subscription } from '../../src/client.js';
import type { EntityApi } from '../../src/db.js';
import {
	IssueSchema,
	PairSchema,
	ProjectSchema,
	UserSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
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

	describe('cursor paging', () => {
		/** Seven widgets with distinct counts, in a fresh workspace. */
		async function seeded() {
			const ws = freshWorkspace();
			const writer = newClient(ws);
			await writer.start();
			for (let i = 1; i <= 7; i++) {
				await writer.mutate.put(WidgetSchema, { id: `w${i}`, count: i });
			}
			await writer.syncNow();
			return { ws, writer };
		}

		const byCount: WidgetQuery = { orderBy: (w) => [ w.count.asc() ] };

		it('accepts a cursor the client minted and mints one the client accepts', async () => {
			// The whole cross-language claim in one spec: the two sides encode
			// the same position for the same row under the same ordering, so a
			// token from either is a valid bound for the other. Nothing in the
			// unit suite can prove it — both sides of every assertion there are
			// this implementation.
			const { ws, writer } = await seeded();
			const reader = newClient(ws);

			const localHead = await writer.query(WidgetSchema, { ...byCount, limit: 2 });
			expect(localHead.map((r) => r.key)).toEqual([ 'w1', 'w2' ]);

			// A cursor the CLIENT minted, replayed against the server.
			const fromServer = await reader.queryServer(WidgetSchema, {
				...byCount,
				limit: 2,
				after: localHead.endCursor,
			});
			expect(fromServer.map((r) => r.key)).toEqual([ 'w3', 'w4' ]);

			// And the SERVER's cursor replayed against the local evaluator.
			const fromLocal = await writer.query(WidgetSchema, {
				...byCount,
				limit: 2,
				after: fromServer.endCursor,
			});
			expect(fromLocal.map((r) => r.key)).toEqual([ 'w5', 'w6' ]);

			await writer.close();
			await reader.close();
		});

		it('pages forward and back over the wire', async () => {
			const { ws, writer } = await seeded();
			const reader = newClient(ws);

			const keys: string[] = [];
			let after = undefined as Awaited<ReturnType<typeof reader.queryServer>>['endCursor'];
			for (;;) {
				const page = await reader.queryServer(WidgetSchema, { ...byCount, limit: 3, after });
				keys.push(...page.map((r) => r.key));
				if (!page.hasMore) break;
				after = page.endCursor;
			}
			expect(keys).toEqual([ 'w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7' ]);

			const tail = await reader.queryServer(WidgetSchema, { ...byCount, limit: 2, offset: 5 });
			const prev = await reader.queryServer(WidgetSchema, {
				...byCount,
				limit: 2,
				before: tail.startCursor,
			});
			expect(prev.map((r) => r.key)).toEqual([ 'w4', 'w5' ]);

			await writer.close();
			await reader.close();
		});

		it('skips and repeats nothing while a second writer inserts ahead of the page', async () => {
			// The behaviour the offset path gets wrong, over the real wire: a
			// reader walks the ordering with cursors while another client keeps
			// inserting rows BEFORE the position it holds. Every original row
			// must come back exactly once.
			const { ws, writer } = await seeded();
			const reader = newClient(ws);

			const seen: string[] = [];
			let after = undefined as Awaited<ReturnType<typeof reader.queryServer>>['endCursor'];
			for (let page = 0;; page++) {
				const got = await reader.queryServer(WidgetSchema, { ...byCount, limit: 2, after });
				seen.push(...got.map((r) => r.key));
				if (!got.hasMore) break;
				after = got.endCursor;
				// Sorts ahead of everything already delivered, so an offset
				// continuation would re-serve a row the reader has.
				await writer.mutate.put(WidgetSchema, { id: `x${page}`, count: -1 - page });
				await writer.syncNow();
			}
			expect(seen).toEqual([ 'w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7' ]);

			// The same walk by rank, under the same interleaving, does repeat
			// rows — which is the comparison that makes the assertion above mean
			// something rather than just describing a quiet workspace.
			const byRank: string[] = [];
			for (let page = 0; page < 3; page++) {
				const got = await reader.queryServer(WidgetSchema, {
					...byCount,
					limit: 2,
					offset: page * 2,
				});
				byRank.push(...got.map((r) => r.key));
				await writer.mutate.put(WidgetSchema, { id: `y${page}`, count: -100 - page });
				await writer.syncNow();
			}
			expect(new Set(byRank).size).toBeLessThan(byRank.length);

			await writer.close();
			await reader.close();
		});

		it('rejects a cursor from a different ordering, and an offset beside one', async () => {
			const { ws, writer } = await seeded();
			const reader = newClient(ws);
			const head = await reader.queryServer(WidgetSchema, { ...byCount, limit: 1 });

			await expect(reader.queryServer(WidgetSchema, {
				orderBy: (w) => [ w.label.asc() ],
				after: head.endCursor,
			})).rejects.toThrow(/different entity or ordering/);

			await expect(reader.queryServer(WidgetSchema, {
				...byCount,
				offset: 1,
				after: head.endCursor,
			})).rejects.toThrow(/cannot be combined/);

			await writer.close();
			await reader.close();
		});
	});

	it('answers a matches() semi-join the same way both sides do', async () => {
		// The rel condition travels as RelCond and each side runs its own
		// two-phase rewrite — the client over its local rows, the server inside
		// its snapshot — so the SAME predicate producing the same rows is the
		// wire behavior only this tier can observe.
		const ws = freshWorkspace();
		const entities = [ IssueSchema, UserSchema, ProjectSchema, PairSchema ];
		const writer = newClient(ws, { entities });
		await writer.start();
		await writer.mutate.put(UserSchema, { id: 'u1', name: 'Maya' });
		await writer.mutate.put(UserSchema, { id: 'u2', name: 'Noor' });
		await writer.mutate.put(IssueSchema, { id: 'i1', state: 'open', assignee: { id: 'u1' } });
		await writer.mutate.put(IssueSchema, { id: 'i2', state: 'open', assignee: { id: 'u2' } });
		await writer.mutate.put(IssueSchema, { id: 'i3', state: 'done' });
		await writer.mutate.put(IssueSchema, {
			id: 'i4',
			state: 'done',
			blockedBy: [ { id: 'i1' }, { id: 'ghost' } ],
		});
		await writer.syncNow();

		type IssueQuery = Query<MessageShape<typeof IssueSchema>>;
		const cold = newClient(ws, { entities });
		const cases: IssueQuery[] = [
			{ where: (i) => i.assignee.matches((a) => a.name.eq('Maya')) },
			{ where: (i) => i.blockedBy.matches((b) => b.state.eq('open')) },
			{ where: (i) => not(i.assignee.matches((a) => a.name.eq('Maya'))) },
			{
				where: (i) => and(i.state.eq('open'), i.assignee.matches((a) => a.id.startsWith('u'))),
			},
		];
		for (const q of cases) {
			const remote = await cold.queryServer(IssueSchema, q);
			const local = await writer.query(IssueSchema, q);
			expect(remote.map((r) => r.key)).toEqual(local.map((r) => r.key));
		}
		// Not merely equal to each other — equal to what the semantics say.
		expect(
			(await cold.queryServer(IssueSchema, {
				where: (i) => i.assignee.matches((a) => a.name.eq('Maya')),
			})).map((r) => r.key),
		).toEqual([ 'i1' ]);
		expect(
			(await cold.queryServer(IssueSchema, {
				where: (i) => i.blockedBy.matches((b) => b.state.eq('open')),
			})).map((r) => r.key),
		).toEqual([ 'i4' ]);

		await writer.close();
		await cold.close();
	});
});

describe('queryServer facets', () => {
	it('answers the facets the local fold answers, over rows the client never synced', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w1', count: 30, label: 'x', tags: [ 'a', 'b' ] });
		await writer.mutate.put(WidgetSchema, { id: 'w2', count: 5, label: 'y' });
		await writer.mutate.put(WidgetSchema, { id: 'w3', count: 20, label: 'y', tags: [ 'b' ] });
		await writer.mutate.put(WidgetSchema, { id: 'w4', count: 20, label: 'a', tags: [ 'b', 'b' ] });
		await writer.syncNow();

		const reader = newClient(ws, {
			subscriptions: [ subscription(WidgetSchema, (w) => w.count.gte(25)) ],
		});
		await reader.start();
		await reader.syncNow();

		const q = {
			where: (w) => w.count.gte(10),
			orderBy: (w) => [ w.count.desc(), w.label.asc() ],
			limit: 1,
			facets: { label: (w) => w.label, tag: (w) => w.tags },
		} satisfies WidgetQuery;
		const remote = await reader.queryServer(WidgetSchema, q);
		expect(remote.map((r) => r.key)).toEqual([ 'w1' ]);
		// The facets describe the match set the page was cut from, keyed to the
		// workspace the read was made in — the shape the local fold produces.
		expect(remote.facets).toEqual({
			label: [
				{ value: 'a', count: 1, workspaceId: ws },
				{ value: 'x', count: 1, workspaceId: ws },
				{ value: 'y', count: 1, workspaceId: ws },
			],
			tag: [
				{ value: 'a', count: 1, workspaceId: ws },
				{ value: 'b', count: 3, workspaceId: ws },
			],
		});
		const local = await writer.query(WidgetSchema, q);
		expect(remote.facets).toEqual(local.facets);

		await writer.close();
		await reader.close();
	});
});
