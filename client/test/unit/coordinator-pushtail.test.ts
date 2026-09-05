import { create } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MutationStatus, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import type { RebaseEvent, SyncEvent } from '../../src/sync/events.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushedMutations, pushResults } from './pushfake.js';

// A push cycle snapshots the queue before its requests leave, so writes landing
// while one is in flight fall outside it. Left for the next cycle they would
// first be replayed by this cycle's rebase, then pushed, then acked away — so
// the cycle keeps shipping while the queue grows, up to a bound that still lets
// it reach its pull.

/** The workspace these specs drive. */
const WS = 'ws';

/**
 * Extra rounds the coordinator runs beyond the first (MAX_EXTRA_PUSH_ROUNDS),
 * which is what caps a cycle whose writes never stop.
 */
const EXTRA_ROUNDS = 2;

describe('coordinator mid-push tail', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let events: SyncEvent[];
	/** Every RPC the cycle made, in order: mutation ids per push, or a pull. */
	let calls: ({ push: bigint[] } | { pull: true })[];

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		events = [];
		calls = [];
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.commit();
	});

	/**
	 * Queues local writes against distinct keys, exactly as the client's enqueue
	 * does. Distinct keys keep them out of each other's way in compaction, so a
	 * push carries one mutation per write rather than one merged mutation.
	 */
	async function enqueue(ids: bigint[]): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const tx = await kv.write();
		for (const id of ids) {
			const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: `t${id}`, title: 'x' }));
			const m = create(MutationSchema, {
				clientId: 'c1',
				mutationId: id,
				payload: { case: 'crud', value: crud },
			});
			await enqueuePending(tx, WS, m);
			await applyMutationLocally(tx, registry, {}, WS, m, new TouchSet());
		}
		await tx.put(metaLastMutationId(WS), String(ids[ids.length - 1]));
		await tx.commit();
	}

	async function pendingIds(): Promise<bigint[]> {
		const tx = await kv.read();
		try {
			return (await readPending(tx, WS)).map((m) => m.mutationId);
		} finally {
			tx.release();
		}
	}

	/**
	 * A server that records every call and acks every pushed mutation, reporting
	 * the watermark its pushes reached so the pull's rebase drops them. `onPush`
	 * runs while the RPC is in flight, which is where a spec queues the writes
	 * the cycle must still carry.
	 */
	function transport(onPush: (call: number) => Promise<void> | void): Transport {
		let acked = 0n;
		let pushes = 0;
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				async push(req) {
					const batch = pushedMutations(req);
					calls.push({ push: batch.map((m) => m.mutationId) });
					await onPush(++pushes);
					for (const m of batch) if (m.mutationId > acked) acked = m.mutationId;
					return pushResults(
						req,
						batch.map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.APPLIED,
							seq: m.mutationId,
						})),
					);
				},
				pull() {
					calls.push({ pull: true });
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: acked,
						reset: false,
						hasMore: false,
						seq: 0n,
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

	function newCoordinator(t: Transport, poisonThreshold = 0): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport: t,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			streamEnabled: false,
			pollIntervalMs: 0,
			poisonThreshold,
			onSyncEvent: (e) => void events.push(e),
		});
	}

	function rebases(): RebaseEvent[] {
		return events.filter((e): e is RebaseEvent => e.type === 'rebase');
	}

	it('ships writes that arrived mid-push before the cycle pulls', async () => {
		await enqueue([ 1n, 2n ]);
		const coord = newCoordinator(transport(async (call) => {
			if (call === 1) await enqueue([ 3n, 4n ]);
		}));

		await coord.syncNow();

		// Both rounds go out ahead of the pull, so the rebase has nothing left to
		// replay over the page it just applied.
		expect(calls).toEqual([
			{ push: [ 1n, 2n ] },
			{ push: [ 3n, 4n ] },
			{ pull: true },
		]);
		expect(rebases()).toEqual([
			expect.objectContaining({ replayed: 0 }),
		]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	it('never re-sends what an earlier round of the same cycle already sent', async () => {
		// An acked mutation only leaves the queue during the pull's rebase, so the
		// second round re-reads the first round's ids along with the new tail.
		await enqueue([ 1n ]);
		const coord = newCoordinator(transport(async (call) => {
			if (call === 1) await enqueue([ 2n ]);
		}));

		await coord.syncNow();

		expect(calls).toEqual([
			{ push: [ 1n ] },
			{ push: [ 2n ] },
			{ pull: true },
		]);

		await coord.stop();
	});

	it('still pulls when writes never stop arriving', async () => {
		await enqueue([ 1n ]);
		let next = 2n;
		const coord = newCoordinator(transport(async () => {
			await enqueue([ next++ ]);
		}));

		await coord.syncNow();

		expect(calls).toHaveLength(EXTRA_ROUNDS + 2);
		expect(calls[calls.length - 1]).toEqual({ pull: true });
		// The round the bound cut off is queued for the cycle the write's own
		// coalescing window schedules, not lost.
		expect(await pendingIds()).toEqual([ next - 1n ]);

		await coord.stop();
	});

	it('ships one mutation and stops while isolating a poison head', async () => {
		await enqueue([ 1n, 2n, 3n ]);
		const coord = newCoordinator(
			transport(async (call) => {
				// Cycle 1's push fails deterministically, which marks the head poison;
				// cycle 2 isolates it and the server takes it.
				if (call === 1) throw new ConnectError('unknown entity', Code.InvalidArgument);
				if (call === 2) await enqueue([ 4n ]);
			}),
			3,
		);

		await coord.syncNow().catch(() => undefined);
		calls = [];
		await coord.syncNow();

		// The isolated head goes out alone and the cycle stops there, even though
		// mutation 4 landed while that one request was in flight.
		expect(calls).toEqual([
			{ push: [ 1n ] },
			{ pull: true },
		]);
		expect(await pendingIds()).toEqual([ 2n, 3n, 4n ]);

		await coord.stop();
	});
});
