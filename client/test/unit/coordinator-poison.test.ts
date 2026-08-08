import { create } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { CodeInternal, CodePoisonDropped } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	MutationStatus,
	type PushRequest,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId, metaPushedThrough, pendingKey } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVWrite } from '../../src/kv/store.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator, type RejectionInfo } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

// Drives the Coordinator's push path against a fake server to exercise the
// poison-queue backstop: a mutation the server keeps failing — as an RPC error
// or as a workspace-scoped batch failure — must not wedge the queue forever.

/** A SyncService transport with the given push(); pull/bootstrap are no-ops so
 * a seeded cursor keeps sync from bootstrapping. */
function syncTransport(push: (req: PushRequest) => ReturnType<typeof pushResults>): Transport {
	return createRouterTransport(({ service }) => {
		service(SyncService, {
			push,
			pull() {
				return {
					patch: { changes: [], dropEntities: [] },
					nextCursor: new Uint8Array([ 1 ]),
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
				};
			},
			// eslint-disable-next-line require-yield
			async *bootstrap() {
				throw new Error('bootstrap should not be called with a seeded cursor');
			},
			query() {
				return { rows: [] };
			},
		});
	});
}

/** A transport whose push() runs the given hook and then answers emptily. */
function fakeTransport(onPush: () => void): Transport {
	return syncTransport((req) => {
		onPush();
		return pushResults(req, []);
	});
}

/** A transport whose push() fails the whole batch with a workspace-scoped code. */
function batchFailTransport(errorCode: string, errorMessage: () => string): Transport {
	return syncTransport((req) => pushFailed(req, errorCode, errorMessage()));
}

/** A transaction that calls `onDel` before each delete and `onCommit` after a commit. */
class HookedWrite implements KVWrite {
	constructor(
		private readonly inner: KVWrite,
		private readonly onDel: (key: string) => void,
		private readonly onCommit: () => void,
	) {}

	get(key: string): Promise<unknown | undefined> {
		return this.inner.get(key);
	}

	has(key: string): Promise<boolean> {
		return this.inner.has(key);
	}

	scan(prefix: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scan(prefix);
	}

	scanRange(lo?: string, hi?: string): AsyncIterable<[key: string, value: unknown]> {
		return this.inner.scanRange(lo, hi);
	}

	indexScan(lo?: string, hi?: string): AsyncIterable<string> {
		return this.inner.indexScan(lo, hi);
	}

	put(key: string, value: unknown): Promise<void> {
		return this.inner.put(key, value);
	}

	async del(key: string): Promise<void> {
		this.onDel(key);
		await this.inner.del(key);
	}

	async commit(): Promise<void> {
		await this.inner.commit();
		this.onCommit();
	}

	release(): void {
		this.inner.release();
	}
}

/**
 * A store whose transaction opened straight AFTER the one that deleted `key`
 * committed fails once — the window a dead-letter would leave open if it
 * dropped the mutation in one transaction and renumbered the survivors in the
 * next.
 */
class FailAfterDelete extends MemoryKVStore {
	private armed = false;

	constructor(private readonly key: string) {
		super();
	}

	override async write(): Promise<KVWrite> {
		if (this.armed) {
			this.armed = false;
			throw new Error('kv unavailable');
		}
		let deleted = false;
		return new HookedWrite(
			await super.write(),
			(k) => {
				if (k === this.key) deleted = true;
			},
			() => {
				if (deleted) this.armed = true;
			},
		);
	}
}

/** A store whose first delete of `key` throws, failing the dead-letter's own write. */
class FailOnDelete extends MemoryKVStore {
	private failed = false;

	constructor(private readonly key: string) {
		super();
	}

	override async write(): Promise<KVWrite> {
		return new HookedWrite(
			await super.write(),
			(k) => {
				if (k !== this.key || this.failed) return;
				this.failed = true;
				throw new Error('kv unavailable');
			},
			() => {},
		);
	}
}

describe('coordinator poison backstop', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let rejections: RejectionInfo[];

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		rejections = [];
		await seed();
	});

	/**
	 * Seeds `kv` with one pending mutation plus its optimistic overlay, a cursor
	 * (so the first sync skips bootstrap), and the id counter. A spec that needs
	 * a different store assigns `kv` and calls this again.
	 */
	async function seed(): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't1', title: 'poison' }));
		const mutation = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 1n,
			payload: { case: 'crud', value: crud },
		});
		const tx = await kv.write();
		await enqueuePending(tx, WS, mutation);
		await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		await tx.put(metaLastMutationId(WS), '1');
		await tx.commit();
	}

	/** Queues further pending mutations behind the seeded head. */
	async function seedTail(ids: bigint[]): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const tx = await kv.write();
		for (const id of ids) {
			const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: `t${id}`, title: 'keep' }));
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

	function newCoordinator(transport: Transport, poisonThreshold: number): Coordinator {
		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		return new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			onMutationRejected: (info) => rejections.push(info),
			poisonThreshold,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
	}

	/** Runs syncNow, swallowing the expected push error so we can step cycles. */
	async function cycle(coord: Coordinator): Promise<void> {
		await coord.syncNow().catch(() => undefined);
	}

	it('drops a head the server keeps deterministically rejecting', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		// First two cycles only count failures; the head is not dropped yet.
		await cycle(coord);
		await cycle(coord);
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		// The third deterministic failure crosses the threshold: dead-letter it.
		await cycle(coord);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		expect(await pendingIds()).toEqual([]);
		// Nothing is left to renumber, but the counter still named the dropped id.
		// Left there, the next local write would mint an id past the watermark the
		// server stopped at and gap the workspace all over again.
		const tx = await kv.read();
		try {
			expect(await tx.get(metaLastMutationId(WS))).toBe('0');
			expect(await tx.get(metaPushedThrough(WS))).toBe('0');
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it('drops a head whose batch the server keeps failing the same way', async () => {
		// A workspace-scoped batch failure is what a custom mutator returning a
		// plain error instead of Reject(...) produces. It is not an RPC error, so
		// nothing about it is deterministic on its face — but a head that fails
		// alone with the same verdict every time is one nothing can drain.
		const coord = newCoordinator(
			batchFailTransport(CodeInternal, () => 'workspace "ws": applying the batch failed'),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await cycle(coord);
		expect(rejections).toEqual([
			expect.objectContaining({
				mutationId: 1n,
				errorCode: CodePoisonDropped,
				errorMessage: 'workspace "ws": applying the batch failed',
			}),
		]);
		expect(await pendingIds()).toEqual([]);

		await coord.stop();
	});

	it('restarts the count when the batch failure changes', async () => {
		// A batch failure that reads differently each time is the shape of an
		// outage, not of a mutation nothing can apply: only a repeat of the same
		// verdict is evidence that retrying cannot help.
		let attempt = 0;
		const coord = newCoordinator(
			batchFailTransport(CodeInternal, () => `attempt ${++attempt} lost its connection`),
			3,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('never drops on transient (non-deterministic) errors', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('server down', Code.Unavailable);
			}),
			3,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		// The queue retries indefinitely on transient errors; it never gives up.
		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('honors poisonThreshold <= 0 by retrying forever', async () => {
		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('nope', Code.InvalidArgument);
			}),
			0,
		);

		for (let i = 0; i < 6; i++) await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n ]);

		await coord.stop();
	});

	it('keeps rejections from an answered chunk when a later chunk RPC fails', async () => {
		// Add a second pending mutation so the queue splits into two single-mutation
		// chunks (maxMutationsPerPush=1 below). Chunk 1 (id 1) is rejected by the
		// server; chunk 2 (id 2) then dies on a network error.
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't2', title: 'second' }));
		const m2 = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 2n,
			payload: { case: 'crud', value: crud },
		});
		const seedTx = await kv.write();
		await enqueuePending(seedTx, WS, m2);
		await applyMutationLocally(seedTx, registry, {}, WS, m2, new TouchSet());
		await seedTx.put(metaLastMutationId(WS), '2');
		await seedTx.commit();

		let calls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					calls += 1;
					if (calls === 1) {
						// Chunk 1: the server rejects mutation 1 (and bumps its
						// watermark, so a retry would ack it as ALREADY_APPLIED).
						// A rejection is per-MUTATION — the batch itself succeeded,
						// so it rides inside the workspace's results, not on it.
						return pushResults(req, [ {
							mutationId: 1n,
							status: MutationStatus.REJECTED,
							errorCode: 'BAD',
							errorMessage: 'no good',
						} ]);
					}
					// Chunk 2: a flaky connection drops the RPC mid-cycle.
					throw new ConnectError('connection reset', Code.Unavailable);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not be called with a seeded cursor');
				},
				query() {
					return { rows: [] };
				},
			});
		});

		const kvMutex = createKvMutex('test');
		const hub = new WatcherHub(kv, registry, kvMutex);
		const coord = new Coordinator({
			transport,
			kv,
			kvMutex,
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: (touched) => hub.notify(touched),
			status: new StatusTracker(),
			onMutationRejected: (info) => rejections.push(info),
			poisonThreshold: 3,
			streamEnabled: false,
			pollIntervalMs: 0,
			maxMutationsPerPush: 1,
		});

		await cycle(coord);

		// The rejection chunk 1 reported must NOT be dropped by chunk 2's throw:
		// it fired onMutationRejected and rolled the doomed mutation out of the
		// queue. Mutation 2 is untouched and will retry next cycle.
		expect(calls).toBe(2);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: 'BAD', errorMessage: 'no good' }),
		]);
		expect(await pendingIds()).toEqual([ 2n ]);

		await coord.stop();
	});

	it('renumbers the tail down over a dropped head', async () => {
		// A dead-letter discards an id the server never acked, so its watermark
		// stays below the tail. Left at their old ids, the survivors would gap
		// past it and the workspace would never push again.
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		// Cycle 1 pushes the whole queue, 2 and 3 isolate the head, and the third
		// confirmed failure dead-letters it.
		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		// The survivors close the hole rather than keeping ids 2 and 3.
		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		const tx = await kv.read();
		try {
			expect(await tx.get(metaLastMutationId(WS))).toBe('2');
			expect(await tx.get(metaPushedThrough(WS))).toBe('0');
		} finally {
			tx.release();
		}

		await coord.stop();
	});

	it('drops and renumbers in one transaction', async () => {
		// The drop is what makes the hole and the renumber is what closes it. A
		// store that dies between them would leave the queue numbered above a
		// watermark no ack can ever reach: every push refused for the gap, and the
		// gap recovery is this same renumber. One transaction is the only
		// arrangement in which the hole cannot outlive the drop.
		kv = new FailAfterDelete(pendingKey(WS, 1n));
		await seed();
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(await pendingIds()).toEqual([ 1n, 2n ]);
		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);

		await coord.stop();
	});

	it('keeps the mutation queued when the dead-letter cannot commit', async () => {
		// The other half of the same atomicity: a drop that fails writes nothing,
		// so the head stays queued with the survivors behind it and the backstop
		// simply counts up to the threshold again.
		kv = new FailOnDelete(pendingKey(WS, 1n));
		await seed();
		await seedTail([ 2n, 3n ]);

		const coord = newCoordinator(
			fakeTransport(() => {
				throw new ConnectError('unknown entity', Code.InvalidArgument);
			}),
			3,
		);

		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toHaveLength(0);
		expect(await pendingIds()).toEqual([ 1n, 2n, 3n ]);

		// The failure was the store's, not the mutation's: the next run of the
		// threshold dead-letters it for real.
		await cycle(coord);
		await cycle(coord);
		await cycle(coord);

		expect(rejections).toEqual([
			expect.objectContaining({ mutationId: 1n, errorCode: CodePoisonDropped }),
		]);
		expect(await pendingIds()).toEqual([ 1n, 2n ]);

		await coord.stop();
	});

	async function pendingIds(): Promise<bigint[]> {
		const tx = await kv.read();
		try {
			return (await readPending(tx, WS)).map((m) => m.mutationId);
		} finally {
			tx.release();
		}
	}
});
