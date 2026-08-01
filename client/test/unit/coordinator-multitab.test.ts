import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it, vi } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	type RowChange,
	RowChangeSchema,
	type Subscription,
	SubscriptionSchema,
} from '../../src/gen/syncplane/v1/patch_pb.js';
import { MutationStatus, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { intentKey, metaCursor, metaLastMutationId, metaServerSeq } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { readMergedRow } from '../../src/read.js';
import { Coordinator, type CoordinatorOptions } from '../../src/sync/coordinator.js';
import { IntentRegistry, SUBS_TTL_MS } from '../../src/sync/intent.js';
import { StatusTracker } from '../../src/sync/status.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The single workspace these specs drive. */
const WS = 'ws';

/** A one-workspace scope map, the shape the registry and coordinator take. */
function scope(subs: Subscription[]): Map<string, Subscription[]> {
	return new Map([ [ WS, subs ] ]);
}

// Two full Coordinators ("tabs") over one shared MemoryKVStore + ChainMutex —
// the lean multi-tab model of issue 023: every tab syncs, correctness comes
// from shared intent records (subscription/undo-ref union) and the monotonic
// cursor gate. No locks, no channels: plain Node suffices.

function sub(entity: string): Subscription {
	return create(SubscriptionSchema, { entity });
}

function patch(id: bigint, key: string, paths: string[]): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: create(CrudMutationSchema, {
				entity: 'todo',
				key,
				op: CrudOp.PATCH,
				fieldMask: { paths },
			}),
		},
	});
}

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })) },
	});
}

interface PullPage {
	changes?: RowChange[];
	nextCursor: Uint8Array;
	seq: bigint;
}

describe('coordinator multi-tab (shared store, intent union)', () => {
	function harness() {
		const kv = new MemoryKVStore();
		const kvMutex = new ChainMutex();
		const registry = new EntityRegistry([ TodoSchema ]);
		const pushes: Mutation[][] = [];

		/** Acks every push; answers pulls with the given pages (last one repeats). */
		function transport(pulls: Subscription[][], pages: PullPage[] = []): Transport {
			let page = 0;
			return createRouterTransport(({ service }) => {
				service(SyncService, {
					push(req) {
						pushes.push(pushedMutations(req));
						return pushResults(
							req,
							pushedMutations(req).map((m) => ({
								mutationId: m.mutationId,
								status: MutationStatus.APPLIED,
								seq: m.mutationId,
							})),
						);
					},
					pull(req) {
						pulls.push(req.subscriptions);
						const p = pages[Math.min(page++, pages.length - 1)];
						return {
							patch: { changes: p?.changes ?? [], dropEntities: [] },
							nextCursor: p?.nextCursor ?? new Uint8Array([ 1 ]),
							lastAckedMutationId: 0n,
							reset: false,
							hasMore: false,
							seq: p?.seq ?? 0n,
						};
					},
					// eslint-disable-next-line require-yield
					async *bootstrap() {
						throw new Error('bootstrap should not run with a seeded cursor');
					},
					query() {
						return { rows: [] };
					},
				});
			});
		}

		async function seed(mutations: Mutation[] = []): Promise<void> {
			const tx = await kv.write();
			for (const m of mutations) await enqueuePending(tx, WS, m);
			await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
			if (mutations.length > 0) {
				await tx.put(
					metaLastMutationId(WS),
					mutations[mutations.length - 1]!.mutationId.toString(),
				);
			}
			await tx.commit();
		}

		function tab(t: Transport, opts: Partial<CoordinatorOptions> = {}): Coordinator {
			return new Coordinator({
				transport: t,
				kv,
				kvMutex,
				accountId: 'acct',
				clientId: 'c1',
				registry,
				mirrors: {},
				notify: async () => {},
				status: new StatusTracker(),
				scopes: scope([]),
				streamEnabled: false,
				pollIntervalMs: 0,
				...opts,
			});
		}

		async function pendingIds(): Promise<bigint[]> {
			const tx = await kv.read();
			try {
				return (await readPending(tx, WS)).map((m) => m.mutationId);
			} finally {
				tx.release();
			}
		}

		async function getMeta(key: string): Promise<unknown> {
			const tx = await kv.read();
			try {
				return await tx.get(key);
			} finally {
				tx.release();
			}
		}

		return { kv, kvMutex, registry, pushes, transport, seed, tab, pendingIds, getMeta };
	}

	function entities(subs: Subscription[]): string[] {
		return subs.map((s) => s.entity).sort();
	}

	it('both tabs pull the union of subscriptions; a full-sync tab widens everyone', async () => {
		const h = harness();
		await h.seed();
		const regA = new IntentRegistry(h.kv, h.kvMutex);
		const regB = new IntentRegistry(h.kv, h.kvMutex);
		const subsA = [ sub('todo') ];
		const subsB = [ sub('project') ];
		await regA.seed(scope(subsA), new Map());
		await regB.seed(scope(subsB), new Map());

		const pullsA: Subscription[][] = [];
		const pullsB: Subscription[][] = [];
		const tabA = h.tab(h.transport(pullsA), { intent: regA, scopes: scope(subsA) });
		const tabB = h.tab(h.transport(pullsB), { intent: regB, scopes: scope(subsB) });

		// The thrash regression: both tabs present the SAME scope (the union) to
		// the server, so it never delta-resyncs back and forth between scopes.
		await tabA.syncNow();
		await tabB.syncNow();
		expect(entities(pullsA.at(-1)!)).toEqual([ 'project', 'todo' ]);
		expect(entities(pullsB.at(-1)!)).toEqual([ 'project', 'todo' ]);

		// Identical subscriptions dedup rather than stack up.
		await regB.updateScopes(scope([ sub('todo') ]));
		await tabA.syncNow();
		expect(entities(pullsA.at(-1)!)).toEqual([ 'todo' ]);

		// A tab switching to full sync ([]) makes every tab full-sync.
		tabB.setSubscriptions(WS, []);
		await vi.waitFor(async () => {
			const tx = await h.kv.read();
			try {
				const rec = (await tx.get(intentKey(regB.tabId))) as {
					subs: Record<string, Uint8Array[]>;
				};
				expect(rec.subs[WS]).toHaveLength(0);
			} finally {
				tx.release();
			}
		});
		await tabA.syncNow();
		expect(pullsA.at(-1)!).toEqual([]);

		await tabA.stop();
		await tabB.stop();
	});

	it('reports the effective scope, flagging when a peer widens it to full sync', async () => {
		const h = harness();
		await h.seed();
		const status = new StatusTracker();
		const regA = new IntentRegistry(h.kv, h.kvMutex);
		const subsA = [ sub('todo') ];
		await regA.seed(scope(subsA), new Map());
		const tabA = h.tab(h.transport([]), { intent: regA, scopes: scope(subsA), status });

		// This tab's scope is in effect: own and effective agree, not widened.
		await tabA.syncNow();
		expect(status.current().workspaces[WS]!.scope).toEqual({ own: 1, effective: 1, widened: false });

		// A full-sync peer collapses the union to full sync — effective scope is
		// now broader than this tab asked for.
		const regB = new IntentRegistry(h.kv, h.kvMutex);
		await regB.seed(scope([]), new Map());
		await tabA.syncNow();
		expect(status.current().workspaces[WS]!.scope).toEqual({ own: 1, effective: 0, widened: true });

		await tabA.stop();
	});

	it("compaction protects a peer tab's undo-referenced mutation (union refs)", async () => {
		const h = harness();
		// Two same-key PATCHes — foldable to one mutation — but tab B's undo
		// stack references id 2.
		await h.seed([ patch(1n, 't1', [ 'title' ]), patch(2n, 't1', [ 'completed' ]) ]);
		const regA = new IntentRegistry(h.kv, h.kvMutex);
		const regB = new IntentRegistry(h.kv, h.kvMutex);
		await regA.seed(scope([]), new Map());
		await regB.seed(scope([]), new Map([ [ WS, new Set([ 2n ]) ] ]));

		const remaps: ReadonlyMap<bigint, bigint>[] = [];
		const tabA = h.tab(h.transport([]), {
			intent: regA,
			onCompacted: (_ws, remap) => remaps.push(remap),
		});

		// A compacts before pushing; B's ref acts as a barrier, so neither
		// mutation folds or renumbers — B's hypothetical undo replay of id 2
		// stays valid.
		await tabA.syncNow();
		expect(await h.pendingIds()).toEqual([ 1n, 2n ]);
		expect(remaps).toHaveLength(0);
		expect(h.pushes.at(-1)!.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		await tabA.stop();
	});

	it('compaction protects an own ref persisted after its in-memory snapshot', async () => {
		const h = harness();
		await h.seed([ patch(1n, 't1', [ 'title' ]), patch(2n, 't1', [ 'completed' ]) ]);
		const regA = new IntentRegistry(h.kv, h.kvMutex);
		await regA.seed(scope([]), new Map());

		// compact() reads this tab's undo refs from memory BEFORE taking the kv
		// mutex, so an undo() enqueuing in that window wins the mutex and its
		// mutation is missing from the snapshot. Reproduced here: the callback
		// queues the record write the enqueue transaction would have made (the
		// mutex is FIFO, so it lands first) and still returns the pre-enqueue set.
		// The union read INSIDE the compaction transaction is what closes the gap.
		let raced = false;
		const remaps: ReadonlyMap<bigint, bigint>[] = [];
		const tabA = h.tab(h.transport([]), {
			intent: regA,
			undoRefIds: () => {
				if (!raced) {
					raced = true;
					void regA.updateRefs(WS, new Set([ 2n ]));
				}
				return new Set();
			},
			onCompacted: (_ws, remap) => remaps.push(remap),
		});

		await tabA.syncNow();
		expect(raced).toBe(true);
		expect(await h.pendingIds()).toEqual([ 1n, 2n ]);
		expect(remaps).toHaveLength(0);
		expect(h.pushes.at(-1)!.map((m) => m.mutationId)).toEqual([ 1n, 2n ]);

		await tabA.stop();
	});

	it('a stale pull page (older seq) cannot regress rows or the shared cursor', async () => {
		const h = harness();
		await h.seed();

		// Tab A's server answer is current through seq 5; tab B's (a response
		// that raced A's, e.g. requested against the older cursor) only through
		// seq 3 and carries an older row state.
		const tabA = h.tab(h.transport([], [
			{ changes: [ serverRow('t1', 5n, 'fresh') ], nextCursor: new Uint8Array([ 5 ]), seq: 5n },
		]));
		const tabB = h.tab(h.transport([], [
			{ changes: [ serverRow('t1', 3n, 'stale') ], nextCursor: new Uint8Array([ 3 ]), seq: 3n },
		]));

		await tabA.syncNow();
		await tabB.syncNow(); // the whole page must be dropped: rows AND cursor

		const tx = await h.kv.read();
		try {
			const bytes = await readMergedRow(tx, h.registry, 'todo', WS, 't1');
			expect(bytes).toEqual(toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'fresh' })));
		} finally {
			tx.release();
		}
		expect(await h.getMeta(metaCursor(WS))).toEqual(new Uint8Array([ 5 ]));
		expect(await h.getMeta(metaServerSeq(WS))).toBe('5');

		await tabA.stop();
		await tabB.stop();
	});

	it("stop() deletes the intent record; a crashed tab's record ages out of the union", async () => {
		const h = harness();
		await h.seed();
		let now = 1_000_000;
		const clock = () => now;

		// "Crashed" tab: seeds a record, never stops.
		const regB = new IntentRegistry(h.kv, h.kvMutex, { now: clock });
		await regB.seed(scope([ sub('project') ]), new Map());

		// Clean tab: start() seeds its record, stop() removes it.
		const regA = new IntentRegistry(h.kv, h.kvMutex, { now: clock });
		const subsA = [ sub('todo') ];
		const pullsA: Subscription[][] = [];
		const tabA = h.tab(h.transport(pullsA), { intent: regA, scopes: scope(subsA) });
		await tabA.start();

		await tabA.syncNow();
		expect(entities(pullsA.at(-1)!)).toEqual([ 'project', 'todo' ]);

		// B goes silent past the subscription TTL: its scope stops widening A's.
		now += SUBS_TTL_MS + 60_000;
		await tabA.syncNow();
		expect(entities(pullsA.at(-1)!)).toEqual([ 'todo' ]);

		await tabA.stop();
		const tx = await h.kv.read();
		try {
			expect(await tx.get(intentKey(regA.tabId))).toBeUndefined();
			expect(await tx.get(intentKey(regB.tabId))).toBeDefined();
		} finally {
			tx.release();
		}
	});
});
