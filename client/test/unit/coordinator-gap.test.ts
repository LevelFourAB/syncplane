import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { CodeMutationIdGap, ErrorCodeMetadataKey } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	MutationStatus,
	type PushRequest,
	type PushResponseSchema,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId, metaPushedThrough } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';

// A hole at the head of the pending queue — ids the server never saw, dropped
// locally by the poison backstop — used to wedge a workspace forever: the
// engine refuses every gapped batch, and no pull can close a hole ABOVE the
// watermark. These specs drive the recovery against a fake engine that enforces
// the same contiguity rule as engine/server/push.go.

const WS = 'ws';
const CLIENT = 'c1';

/**
 * The engine's watermark rule, in the small: ids must arrive contiguously from
 * `watermark + 1`, a first-seen client is adopted at whatever id it opens with,
 * and anything at or below the watermark is already applied. A gap fails the
 * whole batch with a workspace-scoped code and applies nothing.
 */
class FakeEngine {
	watermark = 0n;
	/** Mutation ids the server accepted, in arrival order. */
	readonly applied: bigint[] = [];

	push(req: PushRequest): MessageInitShape<typeof PushResponseSchema> {
		const batch = req.batches[0]!;
		const results = [];
		for (const m of batch.mutations) {
			if (m.mutationId <= this.watermark) {
				results.push({
					mutationId: m.mutationId,
					status: MutationStatus.ALREADY_APPLIED,
				});
				continue;
			}
			if (this.watermark !== 0n && m.mutationId !== this.watermark + 1n) {
				return {
					results: [ {
						workspaceId: batch.workspaceId,
						results: [],
						errorCode: CodeMutationIdGap,
						errorMessage: `mutation id gap: got ${m.mutationId}, want ${this.watermark + 1n}`,
					} ],
				};
			}
			this.watermark = m.mutationId;
			this.applied.push(m.mutationId);
			results.push({
				mutationId: m.mutationId,
				status: MutationStatus.APPLIED,
				seq: m.mutationId,
			});
		}
		return { results: [ { workspaceId: batch.workspaceId, results } ] };
	}

	transport(): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => this.push(req),
				pull: () => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: new Uint8Array([ 1 ]),
					lastAckedMutationId: this.watermark,
					reset: false,
					hasMore: false,
					seq: 0n,
				}),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not be called with a seeded cursor');
				},
				query: () => ({ rows: [] }),
			});
		});
	}
}

describe('coordinator mutation-id gap recovery', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let engine: FakeEngine;

	/** Seeds a queue holding `ids`, with the store's counters set to match. */
	async function seedQueue(ids: bigint[]): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const tx = await kv.write();
		for (const id of ids) {
			const crud = buildPut(
				def,
				TodoSchema,
				create(TodoSchema, { id: `t${id}`, title: `edit ${id}` }),
			);
			const mutation = create(MutationSchema, {
				clientId: CLIENT,
				mutationId: id,
				payload: { case: 'crud', value: crud },
			});
			await enqueuePending(tx, WS, mutation);
			await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		}
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaLastMutationId(WS), ids[ids.length - 1]!.toString());
		// Every queued id was marked pushed before its (refused) RPC, which is
		// what stops compaction from renumbering the hole away on its own.
		await tx.put(metaPushedThrough(WS), ids[ids.length - 1]!.toString());
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

	async function meta(key: string): Promise<string | undefined> {
		const tx = await kv.read();
		try {
			return (await tx.get(key)) as string | undefined;
		} finally {
			tx.release();
		}
	}

	function newCoordinator(status: StatusTracker): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport: engine.transport(),
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: CLIENT,
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		engine = new FakeEngine();
	});

	it('renumbers a queue stranded above the server watermark and drains it', async () => {
		// The wedge in the field: the server took 1..9, the poison backstop then
		// dropped 10 and 11 locally without the server ever seeing them, and every
		// push since has been refused for the hole.
		engine.watermark = 9n;
		await seedQueue([ 12n, 13n, 14n ]);
		const status = new StatusTracker();
		const coord = newCoordinator(status);

		// The gap must not pass silently. A cycle that fails is what puts a coded
		// error on the status surface (Coordinator.syncCycle), so the tagged
		// rejection here IS the "Online with a queue that never moves" fix.
		const err = await coord.syncNow().then(() => undefined, (e: unknown) => e);
		expect(err).toBeInstanceOf(ConnectError);
		expect((err as ConnectError).metadata.get(ErrorCodeMetadataKey)).toBe(CodeMutationIdGap);
		// The recovery renumbers onto the watermark; nothing reached the server yet.
		expect(await pendingIds()).toEqual([ 10n, 11n, 12n ]);
		expect(await meta(metaLastMutationId(WS))).toBe('12');
		// The ids were never taken, so the pushed-through mark has to come back
		// down with them or compaction stays frozen out of the whole queue.
		expect(await meta(metaPushedThrough(WS))).toBe('9');

		// The next cycle ships them contiguously and the queue drains.
		await coord.syncNow();
		expect(engine.applied).toEqual([ 10n, 11n, 12n ]);
		expect(await pendingIds()).toEqual([]);
		expect(status.current().pendingMutations).toBe(0);

		await coord.stop();
	});

	it('leaves a queue the server is merely ahead of to dropAcked', async () => {
		// The other direction — the server took ids this client still holds — is
		// not a hole and must not renumber: the pull acks them away instead.
		engine.watermark = 14n;
		await seedQueue([ 12n, 13n, 14n ]);
		const coord = newCoordinator(new StatusTracker());

		await coord.syncNow();

		expect(engine.applied).toEqual([]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	it('does not renumber a contiguous queue', async () => {
		engine.watermark = 11n;
		await seedQueue([ 12n, 13n ]);
		const coord = newCoordinator(new StatusTracker());

		await coord.syncNow();

		expect(engine.applied).toEqual([ 12n, 13n ]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});
});
