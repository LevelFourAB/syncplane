import { create } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	CrudMutationSchema,
	CrudOp,
	type Mutation,
	MutationSchema,
} from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MutationStatus, SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { Coordinator, type CoordinatorOptions } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { WatcherHub } from '../../src/watch.js';
import { pushedMutations, pushResults } from './pushfake.js';

// Exercises the write-to-push coalescing window: a burst of local writes should
// ship as one push, without an unbroken run of them deferring the push forever,
// and without any of it holding up a cycle triggered by anything else.

/** The workspace these specs drive. */
const WS = 'ws';

describe('coordinator push debounce', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let pushes: Mutation[][];
	let nextId: bigint;

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		pushes = [];
		nextId = 0n;
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	/**
	 * Records each push batch and acks every mutation as applied, with the pull
	 * reporting the watermark those pushes reached. Without the watermark moving,
	 * the rebase would never drop the acked mutations and every later push in a
	 * spec would re-ship them — which is exactly what these specs count.
	 */
	function transport(): Transport {
		let acked = 0n;
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					const batch = pushedMutations(req);
					pushes.push(batch);
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

	/** Gives the workspace a cursor, so a cycle pulls rather than bootstraps. */
	async function seedCursor(): Promise<void> {
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.commit();
	}

	/**
	 * Queues one local write against its own key, exactly as the client's
	 * enqueue does, and triggers the coalescing window. Distinct keys keep the
	 * mutations out of each other's way in compaction, so a coalesced push is
	 * visible as several mutations in one batch rather than one merged mutation.
	 */
	async function write(coord: Coordinator): Promise<void> {
		nextId += 1n;
		const id = nextId;
		const tx = await kv.write();
		await enqueuePending(
			tx,
			WS,
			create(MutationSchema, {
				clientId: 'c1',
				mutationId: id,
				payload: {
					case: 'crud',
					value: create(CrudMutationSchema, {
						entity: 'todo',
						key: `t${id}`,
						op: CrudOp.PUT,
					}),
				},
			}),
		);
		await tx.put(metaLastMutationId(WS), id.toString());
		await tx.commit();
		coord.syncAfterWrite();
	}

	function newCoordinator(opts: Partial<CoordinatorOptions> = {}): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport: transport(),
			kv,
			kvMutex,
			accountId: 'acct',
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			scopes: new Map([ [ WS, [] ] ]),
			streamEnabled: false,
			pollIntervalMs: 0,
			...opts,
		});
	}

	/** Lets any cycle the advanced timers released run to completion. */
	async function settle(): Promise<void> {
		await vi.advanceTimersByTimeAsync(0);
	}

	it('coalesces a burst of writes into one push', async () => {
		await seedCursor();
		const coord = newCoordinator({ pushDebounceMs: 50, pushMaxDelayMs: 250 });

		await write(coord);
		await vi.advanceTimersByTimeAsync(20);
		await write(coord);
		await vi.advanceTimersByTimeAsync(20);
		await write(coord);
		// 40ms into a burst whose last write was 0ms ago: still inside the window.
		expect(pushes).toHaveLength(0);

		await vi.advanceTimersByTimeAsync(50);
		await settle();
		expect(pushes).toHaveLength(1);
		expect(pushes[0]).toHaveLength(3);
	});

	it('pushes per write when the debounce is disabled', async () => {
		await seedCursor();
		const coord = newCoordinator({ pushDebounceMs: 0 });

		await write(coord);
		await settle();
		await write(coord);
		await settle();
		await write(coord);
		await settle();

		expect(pushes).toHaveLength(3);
		expect(pushes.flat()).toHaveLength(3);
	});

	it('pushes at the ceiling while writes keep coming', async () => {
		await seedCursor();
		const coord = newCoordinator({ pushDebounceMs: 50, pushMaxDelayMs: 250 });

		// A write every 20ms restarts the 50ms debounce every time, so only the
		// 250ms ceiling can release this push. Runs to 300ms: long enough for the
		// ceiling to fire once, short enough that the window re-armed by the
		// writes after it has not.
		for (let i = 0; i < 15; i++) {
			await write(coord);
			await vi.advanceTimersByTimeAsync(20);
		}
		await settle();

		expect(pushes).toHaveLength(1);
		expect(pushes[0]).not.toHaveLength(0);
	});

	it('subsumes a pending window into a cycle triggered elsewhere', async () => {
		await seedCursor();
		const coord = newCoordinator({ pushDebounceMs: 50, pushMaxDelayMs: 250 });

		await write(coord);
		// Stands in for a poke/reconnect/poll landing mid-window: it pushes the
		// same queue, so the armed window must not fire a second, empty cycle.
		await coord.syncNow();
		expect(pushes).toHaveLength(1);

		await vi.advanceTimersByTimeAsync(500);
		await settle();
		expect(pushes).toHaveLength(1);
	});

	it('drops an armed window on stop', async () => {
		await seedCursor();
		const coord = newCoordinator({ pushDebounceMs: 50, pushMaxDelayMs: 250 });

		await write(coord);
		await coord.stop();

		await vi.advanceTimersByTimeAsync(500);
		await settle();
		// The mutation stays queued for the next session rather than being pushed
		// by a coordinator that has already shut down.
		expect(pushes).toHaveLength(0);
	});
});
