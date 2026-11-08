import { create } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	BootstrapResponseSchema,
	type SubscribeResponse,
	SubscribeResponseSchema,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { type SchemaSkewAction, StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { WatcherHub } from '../../src/watch.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

/** The workspace these specs drive. */
const WS = 'ws';

// Drives the Coordinator against a fake server that advertises a configurable
// schema_revision, exercising the version-skew handshake: a client carrying its
// own revision must surface a mismatch and react per its policy.

interface FakeServer {
	/** Mutable revision the server advertises on every RPC. */
	revision: string;
	pushes: number;
	bootstraps: number;
	queries: number;
	/** When >0, the next N bootstrap attempts throw mid-stream (after Begin). */
	failBootstraps: number;
}

function fakeTransport(
	srv: FakeServer,
	subscribe?: (signal: AbortSignal) => AsyncIterable<SubscribeResponse>,
): Transport {
	return createRouterTransport(({ service }) => {
		service(SyncService, {
			async *subscribe(_req, ctx) {
				if (subscribe !== undefined) yield* subscribe(ctx.signal);
				await waitAbort(ctx.signal);
			},
			push(req) {
				srv.pushes++;
				// Push advertises the revision too, so a client learns of skew on
				// the WRITE path — the sync cycle pushes before it pulls, so
				// otherwise the first cycle after a deploy always writes against a
				// skewed server before a "block" policy could engage.
				return { ...pushResults(req, []), schemaRevision: srv.revision };
			},
			pull() {
				return {
					patch: { changes: [], dropEntities: [] },
					nextCursor: new Uint8Array([ 1 ]),
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
					schemaRevision: srv.revision,
				};
			},
			async *bootstrap() {
				srv.bootstraps++;
				yield create(BootstrapResponseSchema, {
					body: {
						case: 'begin',
						value: {
							cursor: new Uint8Array([ 1 ]),
							lastAckedMutationId: 0n,
							seq: 0n,
							schemaRevision: srv.revision,
						},
					},
				});
				if (srv.failBootstraps > 0) {
					srv.failBootstraps--;
					throw new Error('bootstrap stream dropped');
				}
				yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
			},
			query() {
				srv.queries++;
				// A Query is answered by the same deployment as everything else, so
				// it advertises the revision too — otherwise a client that only ever
				// queries the server never learns of the skew.
				return { rows: [], schemaRevision: srv.revision };
			},
		});
	});
}

function waitAbort(signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal.aborted) resolve();
		else signal.addEventListener('abort', () => resolve());
	});
}

// The server advertises its revision on the outer Subscribe frame only; the
// page inside it is a PullResponse that carries none.
function streamPage(revision: string, cursor: number): SubscribeResponse {
	return create(SubscribeResponseSchema, {
		workspaceId: WS,
		schemaRevision: revision,
		body: {
			case: 'page',
			value: {
				patch: { changes: [], dropEntities: [] },
				nextCursor: new Uint8Array([ cursor ]),
				hasMore: false,
				seq: 0n,
				schemaRevision: '',
			},
		},
	});
}

function streamHeartbeat(revision: string): SubscribeResponse {
	return create(SubscribeResponseSchema, {
		schemaRevision: revision,
		body: { case: 'heartbeat', value: true },
	});
}

describe('coordinator schema-revision handshake', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);

		// Seed one pending mutation (so push has work), its overlay, a cursor (so
		// sync skips the initial bootstrap), and the id counter.
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't1', title: 'x' }));
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
	});

	function newCoordinator(
		transport: Transport,
		status: StatusTracker,
		clientRevision: string | undefined,
		onSchemaSkew?: (info: { client: string; server: string }) => SchemaSkewAction,
		streamEnabled = false,
	): Coordinator {
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
			status,
			streamEnabled,
			pollIntervalMs: 0,
			clientRevision,
			onSchemaSkew,
		});
	}

	it('does nothing when no client revision is configured', async () => {
		const srv: FakeServer = {
			revision: 'server-rev',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const coord = newCoordinator(fakeTransport(srv), status, undefined);

		await coord.syncNow();
		expect(status.current().schemaSkew).toBeUndefined();
		expect(srv.pushes).toBeGreaterThan(0);
		await coord.stop();
	});

	it('warns but keeps syncing on a mismatch with the default policy', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const coord = newCoordinator(fakeTransport(srv), status, 'A');

		await coord.syncNow();
		await coord.syncNow();
		expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'warn' });
		// Warn does not suspend pushes: both cycles pushed.
		expect(srv.pushes).toBe(2);
		await coord.stop();
	});

	it('suspends pushes while blocked, and resumes when the revisions line up', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const coord = newCoordinator(fakeTransport(srv), status, 'A', () => 'block');

		// Cycle 1 pushes, then the pull observes the skew and engages the block.
		await coord.syncNow();
		expect(srv.pushes).toBe(1);
		expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'block' });

		// Cycle 2 is blocked: no further push.
		await coord.syncNow();
		expect(srv.pushes).toBe(1);

		// Server upgrades to match. The pull in this cycle clears the block, but
		// push already ran (blocked) earlier in the same cycle; the next cycle
		// resumes pushing. In production the live stream clears the block between
		// cycles, so the resume is prompt.
		srv.revision = 'A';
		await coord.syncNow();
		expect(status.current().schemaSkew).toBeUndefined();
		expect(srv.pushes).toBe(1);
		await coord.syncNow();
		expect(srv.pushes).toBe(2);
		await coord.stop();
	});

	it('forces one re-bootstrap on a resync decision without looping', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const coord = newCoordinator(fakeTransport(srv), status, 'A', () => 'resync');

		// Cycle 1: pull observes the skew and arms a resync.
		await coord.syncNow();
		expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'resync' });
		expect(srv.bootstraps).toBe(0);

		// Cycle 2: the armed resync bootstraps once. Its Begin re-advertises "B",
		// but the skew was already handled, so it does not re-arm.
		await coord.syncNow();
		expect(srv.bootstraps).toBe(1);

		// Cycle 3: no further bootstrap — the resync is not sticky.
		await coord.syncNow();
		expect(srv.bootstraps).toBe(1);
		await coord.stop();
	});

	it('retries the resync when the forced bootstrap fails', async () => {
		// The first bootstrap attempt drops mid-stream (after Begin), the next one
		// succeeds.
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 1,
		};
		const status = new StatusTracker();
		const coord = newCoordinator(fakeTransport(srv), status, 'A', () => 'resync');

		// Cycle 1: pull observes the skew and arms a resync.
		await coord.syncNow();
		expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'resync' });
		expect(srv.bootstraps).toBe(0);

		// The armed resync now drives a bootstrap that fails, then a retry that
		// succeeds — two attempts in all. observeServerRevision won't re-arm "B"
		// (it was already handled), so the only thing that keeps the resync alive
		// across the failed attempt is the flag staying armed on a throw. Before
		// the fix it was cleared up front, so the failed attempt stranded the
		// resync at a single bootstrap and the second, successful one never ran.
		await coord.syncNow();
		await coord.syncNow();
		await coord.syncNow();
		expect(srv.bootstraps).toBe(2);

		await coord.stop();
	});

	it('re-runs the policy when the server moves to a different revision', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const seen: string[] = [];
		const coord = newCoordinator(fakeTransport(srv), status, 'A', (info) => {
			seen.push(info.server);
			return 'warn';
		});

		await coord.syncNow();
		await coord.syncNow(); // same revision B: policy not re-run
		srv.revision = 'C';
		await coord.syncNow(); // new revision C: policy runs again

		expect(seen).toEqual([ 'B', 'C' ]);
		expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'C', action: 'warn' });
		await coord.stop();
	});

	it('falls back to warn when the policy itself throws', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const status = new StatusTracker();
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const coord = newCoordinator(fakeTransport(srv), status, 'A', () => {
				throw new TypeError('policy is broken');
			});

			// The policy is app code called from the push/pull path (and, in
			// production, from the stream frames too). A throw must not fail the
			// cycle that observed the revision; the skew still surfaces under the
			// same default action an unconfigured policy gets.
			await coord.syncNow();
			expect(status.current().state).not.toBe('error');
			expect(status.current().lastError).toBeUndefined();
			expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'warn' });
			expect(srv.pushes).toBe(1);
			expect(errSpy).toHaveBeenCalled();

			await coord.stop();
		} finally {
			errSpy.mockRestore();
		}
	});

	describe('over the subscribe stream', () => {
		/**
		 * Scripted stream frames, held back until `release` so the first sync
		 * cycle observes the skew before any frame arrives. `done` resolves once
		 * the stream has handed out its last frame.
		 */
		function scriptedStream(frames: SubscribeResponse[]) {
			let release!: () => void;
			const released = new Promise<void>((r) => (release = r));
			let done!: () => void;
			const finished = new Promise<void>((r) => (done = r));
			let opened = 0;
			async function* subscribe(): AsyncIterable<SubscribeResponse> {
				// A re-opened stream (after a bootstrap) has nothing new to say.
				if (opened++ > 0) return;
				await released;
				yield* frames;
				done();
			}
			return { subscribe, release, finished };
		}

		async function readCursor(): Promise<unknown> {
			const tx = await kv.read();
			try {
				return await tx.get(metaCursor(WS));
			} finally {
				tx.release();
			}
		}

		it('keeps a block engaged across a page whose frame carries the revision', async () => {
			const srv: FakeServer = {
				revision: 'B',
				pushes: 0,
				bootstraps: 0,
				queries: 0,
				failBootstraps: 0,
			};
			const stream = scriptedStream([ streamHeartbeat('B'), streamPage('B', 2) ]);
			const status = new StatusTracker();
			const seen: string[] = [];
			const coord = newCoordinator(fakeTransport(srv, stream.subscribe), status, 'A', (info) => {
				seen.push(info.server);
				return 'block';
			}, true);

			// Given a client blocked on server revision B
			await coord.start();
			await coord.syncNow();
			expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'block' });
			const pushes = srv.pushes;

			// When the stream delivers a page from the same server
			stream.release();
			await stream.finished;
			await vi.waitFor(async () => expect(await readCursor()).toEqual(new Uint8Array([ 2 ])));

			// Then the block stays, the policy is not asked again, and no push leaves
			expect(status.current().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'block' });
			expect(seen).toEqual([ 'B' ]);
			await coord.syncNow();
			expect(srv.pushes).toBe(pushes);
			await coord.stop();
		});

		it('re-bootstraps once on a resync decision while pages stream', async () => {
			const srv: FakeServer = {
				revision: 'B',
				pushes: 0,
				bootstraps: 0,
				queries: 0,
				failBootstraps: 0,
			};
			const frames: SubscribeResponse[] = [];
			for (let i = 0; i < 10; i++) frames.push(streamPage('B', i + 2), streamHeartbeat('B'));
			const stream = scriptedStream(frames);
			const status = new StatusTracker();
			const seen: string[] = [];
			const coord = newCoordinator(fakeTransport(srv, stream.subscribe), status, 'A', (info) => {
				seen.push(info.server);
				return 'resync';
			}, true);

			// Given a client that already ran the resync for server revision B
			await coord.start();
			await coord.syncNow();
			await coord.syncNow();
			expect(srv.bootstraps).toBe(1);

			// When the stream delivers 10 pages, each followed by a heartbeat
			stream.release();
			await stream.finished;
			await new Promise((r) => setTimeout(r, 30));
			await coord.syncNow();

			// Then no further re-bootstrap runs
			expect(srv.bootstraps).toBe(1);
			expect(seen).toEqual([ 'B' ]);
			await coord.stop();
		});

		it('reports no skew when neither frames nor pages carry a revision', async () => {
			const srv: FakeServer = {
				revision: '',
				pushes: 0,
				bootstraps: 0,
				queries: 0,
				failBootstraps: 0,
			};
			const stream = scriptedStream([ streamHeartbeat(''), streamPage('', 2), streamHeartbeat('') ]);
			const status = new StatusTracker();
			const seen: string[] = [];
			const coord = newCoordinator(fakeTransport(srv, stream.subscribe), status, 'A', (info) => {
				seen.push(info.server);
				return 'block';
			}, true);

			await coord.start();
			await coord.syncNow();
			stream.release();
			await stream.finished;
			await vi.waitFor(async () => expect(await readCursor()).toEqual(new Uint8Array([ 2 ])));

			expect(status.current().schemaSkew).toBeUndefined();
			expect(seen).toEqual([]);
			await coord.stop();
		});
	});
});

// queryServer runs outside the sync cycle — its own RPC, its own transport
// call — so the revision it carries reaches the skew handshake only if the
// query path feeds it there. A client that reads mostly through server queries
// would otherwise sit on a skewed server until its next pull.
describe('queryServer schema-revision handshake', () => {
	function newSyncClient(srv: FakeServer, clientRevision?: string): SyncClient {
		return new SyncClient({
			workspaceId: WS,
			kvStore: new MemoryKVStore(),
			entities: [ TodoSchema ],
			transport: fakeTransport(srv),
			streamEnabled: false,
			pollIntervalMs: 0,
			schemaRevision: clientRevision,
			onSchemaSkew: () => 'block',
		});
	}

	it('surfaces a skew the query response revealed', async () => {
		const srv: FakeServer = {
			revision: 'B',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const client = newSyncClient(srv, 'A');
		await client.start();

		await client.queryServer(TodoSchema, {});
		expect(srv.queries).toBe(1);
		expect(client.status().schemaSkew).toEqual({ client: 'A', server: 'B', action: 'block' });

		await client.close();
	});

	it('stays quiet when the revisions line up', async () => {
		const srv: FakeServer = {
			revision: 'A',
			pushes: 0,
			bootstraps: 0,
			queries: 0,
			failBootstraps: 0,
		};
		const client = newSyncClient(srv, 'A');
		await client.start();

		await client.queryServer(TodoSchema, {});
		expect(client.status().schemaSkew).toBeUndefined();

		await client.close();
	});
});
