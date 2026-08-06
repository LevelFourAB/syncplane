import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { subscription, type SubscriptionSpec, SyncClient } from '../../src/client.js';
import { coverageOfEntity, CoverageResolver, withCoverage } from '../../src/coverage.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { FieldHistorySchema } from '../../src/gen/syncplane/v1/history_pb.js';
import { SubscriptionSchema, WindowSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { CmpOp, ConditionSchema } from '../../src/gen/syncplane/v1/query_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

const anyPredicate = () =>
	create(ConditionSchema, {
		kind: {
			case: 'cmp',
			value: { path: [ 'count' ], op: CmpOp.GT, value: { kind: { case: 'doubleValue', value: 0 } } },
		},
	});

const sub = (
	entity: string,
	opts: { predicate?: boolean; limit?: number } = {},
) =>
	create(SubscriptionSchema, {
		entity,
		predicate: opts.predicate ? anyPredicate() : undefined,
		window: opts.limit === undefined ? undefined : create(WindowSchema, { limit: opts.limit }),
	});

describe('coverageOfEntity', () => {
	it('is complete under full sync (no subscriptions)', () => {
		expect(coverageOfEntity([], 'widget')).toEqual({ complete: true, bounds: [] });
	});

	it('reports unsubscribed for an entity the set does not name', () => {
		expect(coverageOfEntity([ sub('todo') ], 'widget')).toEqual({
			complete: false,
			bounds: [ 'unsubscribed' ],
		});
	});

	it('is complete for an entity subscribed without a bound', () => {
		expect(coverageOfEntity([ sub('widget') ], 'widget')).toEqual({
			complete: true,
			bounds: [],
		});
	});

	it('reports a predicate bound', () => {
		expect(coverageOfEntity([ sub('widget', { predicate: true }) ], 'widget')).toEqual({
			complete: false,
			bounds: [ 'predicate' ],
		});
	});

	it('reports a window bound with its row cap', () => {
		expect(coverageOfEntity([ sub('widget', { limit: 50 }) ], 'widget')).toEqual({
			complete: false,
			bounds: [ 'window' ],
			limit: 50,
		});
	});

	it('sums the caps of several windows on one entity', () => {
		const subs = [ sub('widget', { limit: 50 }), sub('widget', { limit: 25 }) ];
		expect(coverageOfEntity(subs, 'widget')).toEqual({
			complete: false,
			bounds: [ 'window' ],
			limit: 75,
		});
	});

	it('omits the cap when a predicate also bounds the entity', () => {
		const subs = [ sub('widget', { limit: 50 }), sub('widget', { predicate: true }) ];
		expect(coverageOfEntity(subs, 'widget')).toEqual({
			complete: false,
			bounds: [ 'window', 'predicate' ],
		});
	});

	it('is complete when any one subscription covers the whole entity', () => {
		// Subscriptions of an entity are a union, so an unbounded one wins.
		const subs = [ sub('widget', { limit: 5 }), sub('widget') ];
		expect(coverageOfEntity(subs, 'widget')).toEqual({ complete: true, bounds: [] });
	});
});

describe('CoverageResolver', () => {
	it('answers per workspace', () => {
		const r = new CoverageResolver(() =>
			new Map([
				[ 'a', [ sub('widget', { limit: 10 }) ] ],
				[ 'b', [ sub('widget') ] ],
			])
		);
		expect(r.coverageOf('widget', 'a').limit).toBe(10);
		expect(r.coverageOf('widget', 'b').complete).toBe(true);
	});

	it('is complete across workspaces only when complete in each', () => {
		const r = new CoverageResolver(() =>
			new Map([
				[ 'a', [ sub('widget') ] ],
				[ 'b', [ sub('widget', { predicate: true }) ] ],
			])
		);
		expect(r.coverageOf('widget')).toEqual({ complete: false, bounds: [ 'predicate' ] });
	});

	it('sums window caps across workspaces', () => {
		const r = new CoverageResolver(() =>
			new Map([
				[ 'a', [ sub('widget', { limit: 10 }) ] ],
				[ 'b', [ sub('widget', { limit: 5 }) ] ],
			])
		);
		expect(r.coverageOf('widget')).toEqual({ complete: false, bounds: [ 'window' ], limit: 15 });
	});

	it('treats an unknown workspace as complete rather than guessing', () => {
		const r = new CoverageResolver(() => new Map());
		expect(r.coverageOf('widget', 'nope').complete).toBe(true);
	});

	it('reports an entity still arriving in the first snapshot as hydrating', () => {
		const pending = new Set([ 'widget' ]);
		const r = new CoverageResolver(
			() => new Map([ [ 'a', [] ] ]),
			() => pending,
		);
		expect(r.coverageOf('widget', 'a')).toEqual({ complete: false, bounds: [ 'hydrating' ] });
		// The entity that already landed is complete even while its neighbour is
		// still downloading — the whole point of per-entity delivery.
		expect(r.coverageOf('todo', 'a').complete).toBe(true);
		pending.delete('widget');
		expect(r.coverageOf('widget', 'a').complete).toBe(true);
	});

	it('keeps the window cap while a windowed entity hydrates', () => {
		const r = new CoverageResolver(
			() => new Map([ [ 'a', [ sub('widget', { limit: 10 }) ] ] ]),
			() => new Set([ 'widget' ]),
		);
		// The cap still says what the entity converges to; hydrating says it has
		// not got there yet.
		expect(r.coverageOf('widget', 'a')).toEqual({
			complete: false,
			bounds: [ 'window', 'hydrating' ],
			limit: 10,
		});
	});

	it('unions hydration across workspaces', () => {
		const r = new CoverageResolver(
			() =>
				new Map([
					[ 'a', [] ],
					[ 'b', [] ],
				]),
			(ws) => (ws === 'b' ? new Set([ 'widget' ]) : undefined),
		);
		expect(r.coverageOf('widget')).toEqual({ complete: false, bounds: [ 'hydrating' ] });
		expect(r.coverageOf('widget', 'a').complete).toBe(true);
	});
});

describe('withCoverage', () => {
	it('leaves the array indistinguishable from a plain one', () => {
		const rows = withCoverage(
			[ { key: 'a', value: {}, workspaceId: 'ws', version: 1n } ],
			{ complete: false, bounds: [ 'window' ], limit: 1 },
		);
		expect(Array.isArray(rows)).toBe(true);
		expect(rows).toEqual([ { key: 'a', value: {}, workspaceId: 'ws', version: 1n } ]);
		expect(Object.keys(rows)).toEqual([ '0' ]);
		expect([ ...rows ]).toHaveLength(1);
		expect(rows.coverage.limit).toBe(1);
	});
});

describe('client reads carry coverage', () => {
	const newLocalClient = (subs?: SubscriptionSpec[]) =>
		new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ WidgetSchema, TodoSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
			subscriptions: subs,
		});

	it('claims complete under full sync', async () => {
		const client = newLocalClient();
		await client.db.widget.mutate.put({ id: 'w1', count: 1 });
		expect((await client.db.widget.scan()).coverage.complete).toBe(true);
		expect((await client.db.widget.query({})).coverage.complete).toBe(true);
		await client.close();
	});

	it('reports the window and its limit for a bounded subscription', async () => {
		const client = newLocalClient([
			subscription(WidgetSchema, undefined, { limit: 25, byRecency: true }),
		]);
		const rows = await client.db.widget.query({});
		expect(rows.coverage).toEqual({ complete: false, bounds: [ 'window' ], limit: 25 });
		await client.close();
	});

	it('reports a predicate bound, and unsubscribed for an entity the set omits', async () => {
		// The set names widget only, so the client holds nothing at all for todo —
		// an empty todo result means "nothing synced", not "nothing exists".
		const client = newLocalClient([ subscription(WidgetSchema, (w) => w.count.gt(0)) ]);
		expect((await client.db.widget.query({})).coverage.bounds).toEqual([ 'predicate' ]);
		expect((await client.db.todo.scan()).coverage).toEqual({
			complete: false,
			bounds: [ 'unsubscribed' ],
		});
		await client.close();
	});

	it('delivers coverage to watchers', async () => {
		const client = newLocalClient([
			subscription(WidgetSchema, undefined, { limit: 3, byRecency: true }),
		]);
		const seen: (boolean | undefined)[] = [];
		const unsub = client.db.widget.watch((rows) => seen.push(rows.coverage.complete));
		await new Promise((r) => setTimeout(r, 0));
		expect(seen.at(-1)).toBe(false);
		unsub();
		await client.close();
	});
});

describe('subscription window compilation', () => {
	it('rejects an ordering with no limit to bound it', () => {
		expect(() =>
			new SyncClient({
				workspaceId: 'ws1',
				kvStore: new MemoryKVStore(),
				entities: [ WidgetSchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
				subscriptions: [
					subscription(WidgetSchema, undefined, { orderBy: (w) => [ w.count.desc() ] }),
				],
			})
		).toThrow(/needs a limit/);
	});

	it('rejects a non-positive limit', () => {
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ WidgetSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		expect(() => client.setSubscriptions([ subscription(WidgetSchema, undefined, { limit: 0 }) ]))
			.toThrow(/positive integer/);
		void client.close();
	});

	// The server refuses such a subscription with invalid_argument, which reaches
	// the app as a sync failure long after the set was written. The option is on
	// the descriptor the client already holds, so it is refused where it is built.
	it('rejects a subscription naming a server-only entity', () => {
		expect(() =>
			new SyncClient({
				workspaceId: 'ws1',
				kvStore: new MemoryKVStore(),
				entities: [ WidgetSchema, FieldHistorySchema ],
				streamEnabled: false,
				pollIntervalMs: 0,
				subscriptions: [ subscription(WidgetSchema), subscription(FieldHistorySchema) ],
			})
		).toThrow(/syncplane_field_history.*server-only/);
	});

	it('rejects a server-only entity added by setSubscriptions', () => {
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ WidgetSchema, FieldHistorySchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		expect(() => client.setSubscriptions([ subscription(FieldHistorySchema) ]))
			.toThrow(/server-only/);
		void client.close();
	});

	it('rejects byRecency together with orderBy', () => {
		const client = new SyncClient({
			workspaceId: 'ws1',
			kvStore: new MemoryKVStore(),
			entities: [ WidgetSchema ],
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		expect(() =>
			client.setSubscriptions([
				subscription(WidgetSchema, undefined, {
					limit: 5,
					byRecency: true,
					orderBy: (w) => [ w.count.desc() ],
				}),
			])
		).toThrow(/byRecency and orderBy/);
		void client.close();
	});
});
