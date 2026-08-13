import { create } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	BootstrapResponseSchema,
	MutationStatus,
	type PushRequest,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaAckedSeq, metaCursor, metaLastMutationId, metaPushedThrough } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { buildPut, EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import { applyMutationLocally } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { pushedMutations, pushResults } from './pushfake.js';

// The acked-seq freshness floor (keys.metaAckedSeq): an applied push's highest
// MutationResult.seq is persisted per workspace and sent back to the server as
// PullRequest.min_seq / WorkspaceScope.min_seq, so a replica-reading server
// never serves this store a snapshot older than its own acked writes.
//
// The integration tier is deliberately skipped: testserver runs sqlitestore,
// which has no replica capability, so the wire field would be received and
// ignored there. Cross-language floor behavior is pinned by the Go server
// suite (engine/server/replicaroute_test.go); what is local to this client —
// tracking, persistence, reset hygiene — is pinned here.

const WS = 'ws';
const CLIENT = 'c1';

describe('coordinator acked-seq floor', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
	});

	/** Seeds a synced workspace (cursor present) holding `ids` pending. */
	async function seed(ids: bigint[]): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
		for (const id of ids) {
			const mutation = create(MutationSchema, {
				clientId: CLIENT,
				mutationId: id,
				payload: {
					case: 'crud',
					value: buildPut(def, TodoSchema, create(TodoSchema, { id: `t${id}`, title: `edit ${id}` })),
				},
			});
			await enqueuePending(tx, WS, mutation);
			await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		}
		if (ids.length > 0) {
			await tx.put(metaLastMutationId(WS), ids[ids.length - 1]!.toString());
			await tx.put(metaPushedThrough(WS), '0');
		}
		await tx.commit();
	}

	async function meta(key: string): Promise<string | undefined> {
		const tx = await kv.read();
		try {
			return (await tx.get(key)) as string | undefined;
		} finally {
			tx.release();
		}
	}

	async function putMeta(key: string, value: string): Promise<void> {
		const tx = await kv.write();
		await tx.put(key, value);
		await tx.commit();
	}

	function newCoordinator(transport: Transport, streamEnabled = false): Coordinator {
		return new Coordinator({
			transport,
			kv,
			kvMutex: createKvMutex('test'),
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: CLIENT,
			registry,
			mirrors: {},
			notify: async () => {},
			status: new StatusTracker(),
			streamEnabled,
			pollIntervalMs: 0,
		});
	}

	/** A transport that acks pushes at `seqOf(mutationId)` and records pulls' min_seq. */
	function ackingTransport(
		pulledMinSeqs: bigint[],
		seqOf: (id: bigint) => bigint,
	): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) =>
					pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.APPLIED,
							seq: seqOf(m.mutationId),
						})),
					),
				pull(req) {
					pulledMinSeqs.push(req.minSeq);
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: req.cursor,
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 0n,
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query: () => ({ rows: [] }),
			});
		});
	}

	it("persists an applied push's highest seq and sends it as the pull floor", async () => {
		await seed([ 1n, 2n ]);
		const minSeqs: bigint[] = [];
		const coord = newCoordinator(ackingTransport(minSeqs, (id) => 40n + id));
		await coord.syncNow();
		await coord.stop();

		expect(await meta(metaAckedSeq(WS))).toBe('42');
		// The same cycle's pull already carries the floor the push just earned.
		expect(minSeqs).toContain(42n);
	});

	it('never regresses the floor', async () => {
		await seed([ 1n ]);
		await putMeta(metaAckedSeq(WS), '100');
		const minSeqs: bigint[] = [];
		const coord = newCoordinator(ackingTransport(minSeqs, (id) => 40n + id));
		await coord.syncNow();
		await coord.stop();

		expect(await meta(metaAckedSeq(WS))).toBe('100');
		expect(minSeqs).toContain(100n);
	});

	it('records nothing for a rejected-only push', async () => {
		await seed([ 1n ]);
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) =>
					pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: MutationStatus.REJECTED,
							errorCode: 'conflict',
						})),
					),
				pull: (req) => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: req.cursor,
					lastAckedMutationId: 1n,
					reset: false,
					hasMore: false,
					seq: 0n,
				}),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				query: () => ({ rows: [] }),
			});
		});
		const coord = newCoordinator(transport);
		await coord.syncNow();
		await coord.stop();

		expect(await meta(metaAckedSeq(WS))).toBeUndefined();
	});

	it('survives a coordinator restart', async () => {
		await seed([ 1n ]);
		const minSeqs: bigint[] = [];
		const transport = ackingTransport(minSeqs, () => 7n);
		const first = newCoordinator(transport);
		await first.syncNow();
		await first.stop();

		// A fresh coordinator over the same store: the floor is durable state,
		// not session state.
		minSeqs.length = 0;
		const second = newCoordinator(transport);
		await second.syncNow();
		await second.stop();
		expect(minSeqs).toContain(7n);
	});

	it('sends the floor on every subscribe scope', async () => {
		await seed([]);
		await putMeta(metaAckedSeq(WS), '9');

		const scopeMinSeqs: bigint[] = [];
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) => pushResults(req, []),
				pull: (req) => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: req.cursor,
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
				}),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				async *subscribe(req, ctx) {
					for (const scope of req.scopes) scopeMinSeqs.push(scope.minSeq);
					yield {
						workspaceId: WS,
						schemaRevision: '',
						body: { case: 'caughtUp' as const, value: true },
					};
					await new Promise<void>((resolve) => {
						if (ctx.signal.aborted) resolve();
						else ctx.signal.addEventListener('abort', () => resolve());
					});
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = newCoordinator(transport, true);
		await coord.start();
		await vi.waitFor(() => {
			expect(scopeMinSeqs).toContain(9n);
		});
		await coord.stop();
	});

	it('clears the floor when a reset re-bootstraps the workspace', async () => {
		await seed([]);
		await putMeta(metaAckedSeq(WS), '9');

		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) => pushResults(req, []),
				// The reset verdict: the client must discard and re-bootstrap.
				pull: () => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: new Uint8Array(),
					lastAckedMutationId: 0n,
					reset: true,
					hasMore: false,
					seq: 0n,
				}),
				async *bootstrap() {
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: { cursor: new Uint8Array([ 9 ]), lastAckedMutationId: 0n, seq: 3n },
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = newCoordinator(transport);
		await coord.syncNow();
		await coord.stop();

		// The swap installed a new cursor lineage; a floor named in the old one
		// could sit above the new head and pin every pull to the primary.
		expect(await meta(metaCursor(WS))).toBeDefined();
		expect(await meta(metaAckedSeq(WS))).toBeUndefined();
	});
});
