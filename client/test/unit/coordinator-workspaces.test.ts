import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it, vi } from 'vitest';

import { CodeWorkspaceAccessRevoked } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	MutationStatus,
	type PushRequest,
	SyncService,
	WorkspaceDropped_Reason,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaLastMutationId } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { enqueuePending, readPending } from '../../src/optimistic/queue.js';
import { readMergedRow } from '../../src/read.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator, type CoordinatorOptions } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { pushAppliedAll } from './pushfake.js';

// The multiplexed half of the coordinator: several workspaces on ONE Subscribe
// stream and ONE Push, with per-workspace cursors, queues and verdicts. In a
// Node runtime startStreamLeadership returns undefined, so a streamEnabled
// coordinator runs subscribeLoop directly — the no-Web-Locks fallback.

const A = 'ws-a';
const B = 'ws-b';

function put(id: bigint, key: string, title: string): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'todo',
				key,
				op: CrudOp.PUT,
				value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })),
			},
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

/** One page frame for `workspaceId`. Every stream frame names its workspace. */
function page(workspaceId: string, changes: RowChange[], cursor: Uint8Array, seq: bigint) {
	return {
		workspaceId,
		schemaRevision: '',
		body: {
			case: 'page' as const,
			value: {
				patch: { changes, dropEntities: [] },
				nextCursor: cursor,
				lastAckedMutationId: 0n,
				reset: false,
				hasMore: false,
				seq,
				schemaRevision: '',
			},
		},
	};
}

function caughtUp(workspaceId: string) {
	return {
		workspaceId,
		schemaRevision: '',
		body: { case: 'caughtUp' as const, value: true },
	};
}

function dropped(workspaceId: string, reason: WorkspaceDropped_Reason) {
	return {
		workspaceId,
		schemaRevision: '',
		body: { case: 'dropped' as const, value: { reason, message: '' } },
	};
}

function waitAbort(signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal.aborted) resolve();
		else signal.addEventListener('abort', () => resolve());
	});
}

/** An idempotent no-op pull (echoes the cursor; seq 0 bypasses the guard). */
const noopPull = (req: { cursor: Uint8Array }) => ({
	patch: { changes: [], dropEntities: [] },
	nextCursor: req.cursor,
	lastAckedMutationId: 0n,
	reset: false,
	hasMore: false,
	seq: 0n,
});

describe('coordinator across workspaces', () => {
	function harness() {
		const kv = new MemoryKVStore();
		const kvMutex = new ChainMutex();
		const registry = new EntityRegistry([ TodoSchema ]);

		/** Gives each workspace a cursor (so nothing bootstraps) and a queue. */
		async function seed(queues: Record<string, Mutation[]> = {}): Promise<void> {
			const tx = await kv.write();
			for (const ws of [ A, B ]) {
				await tx.put(metaCursor(ws), new Uint8Array([ 1 ]));
				const queue = queues[ws] ?? [];
				for (const m of queue) await enqueuePending(tx, ws, m);
				if (queue.length > 0) {
					await tx.put(metaLastMutationId(ws), queue[queue.length - 1]!.mutationId.toString());
				}
			}
			await tx.commit();
		}

		function coordinator(
			transport: Transport,
			opts: Partial<CoordinatorOptions> = {},
		): Coordinator {
			return new Coordinator({
				transport,
				kv,
				kvMutex,
				storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
				clientId: 'c1',
				registry,
				mirrors: {},
				notify: async () => {},
				status: new StatusTracker(),
				scopes: new Map([ [ A, [] ], [ B, [] ] ]),
				streamEnabled: false,
				pollIntervalMs: 0,
				...opts,
			});
		}

		async function title(ws: string, key: string): Promise<string | undefined> {
			const tx = await kv.read();
			try {
				const bytes = await readMergedRow(tx, registry, 'todo', ws, key);
				return bytes === undefined ? undefined : fromBinary(TodoSchema, bytes).title;
			} finally {
				tx.release();
			}
		}

		async function pendingIds(ws: string): Promise<bigint[]> {
			const tx = await kv.read();
			try {
				return (await readPending(tx, ws)).map((m) => m.mutationId);
			} finally {
				tx.release();
			}
		}

		return { kv, kvMutex, registry, seed, coordinator, title, pendingIds };
	}

	it('carries every workspace on ONE Subscribe stream and applies pages per workspace', async () => {
		const h = harness();
		await h.seed();

		const scopeSets: string[][] = [];
		let streams = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) => pushAppliedAll(req),
				pull: noopPull,
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with seeded cursors');
				},
				async *subscribe(req, ctx) {
					streams++;
					scopeSets.push(req.scopes.map((s) => s.workspaceId).sort());
					yield page(A, [ serverRow('t1', 5n, 'from A') ], new Uint8Array([ 5 ]), 5n);
					yield page(B, [ serverRow('t1', 7n, 'from B') ], new Uint8Array([ 7 ]), 7n);
					yield caughtUp(A);
					yield caughtUp(B);
					await waitAbort(ctx.signal);
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport, { streamEnabled: true });
		await coord.start();

		await vi.waitFor(async () => {
			expect(await h.title(A, 't1')).toBe('from A');
			expect(await h.title(B, 't1')).toBe('from B');
		});
		// One connection for both workspaces — the whole point of multiplexing.
		expect(streams).toBe(1);
		expect(scopeSets[0]).toEqual([ A, B ]);

		await coord.stop();
	});

	it('keeps streaming the other workspaces when one is dropped as revoked', async () => {
		const h = harness();
		await h.seed();

		let streams = 0;
		const status = new StatusTracker();
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) => pushAppliedAll(req),
				pull: noopPull,
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run');
				},
				async *subscribe(_req, ctx) {
					streams++;
					yield dropped(A, WorkspaceDropped_Reason.ACCESS_REVOKED);
					yield page(B, [ serverRow('t1', 7n, 'from B') ], new Uint8Array([ 7 ]), 7n);
					yield caughtUp(B);
					await waitAbort(ctx.signal);
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport, { streamEnabled: true, status });
		await coord.start();

		await vi.waitFor(() => {
			expect(status.current().workspaces[A]?.access).toBe('revoked');
		});
		// B keeps flowing on the SAME stream: a revoked scope ends one workspace,
		// not the connection.
		await vi.waitFor(async () => {
			expect(await h.title(B, 't1')).toBe('from B');
		});
		await new Promise((r) => setTimeout(r, 30));
		expect(streams).toBe(1);
		// The revoked workspace keeps its local rows — purging is the app's call.
		expect(status.current().workspaces[B]?.access).toBeUndefined();

		await coord.stop();
	});

	it('ships one batch per workspace in a single push', async () => {
		const h = harness();
		await h.seed({
			[A]: [ put(1n, 'a1', 'a one') ],
			[B]: [ put(1n, 'b1', 'b one'), put(2n, 'b2', 'b two') ],
		});

		const requests: { workspaceId: string; ids: bigint[] }[][] = [];
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					requests.push(
						req.batches.map((b) => ({
							workspaceId: b.workspaceId,
							ids: b.mutations.map((m) => m.mutationId),
						})),
					);
					return pushAppliedAll(req);
				},
				pull: noopPull,
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run');
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport);
		await coord.syncNow();

		expect(requests).toHaveLength(1);
		expect(requests[0]).toEqual([
			{ workspaceId: A, ids: [ 1n ] },
			{ workspaceId: B, ids: [ 1n, 2n ] },
		]);

		await coord.stop();
	});

	it('budgets the per-push mutation cap ACROSS workspaces', async () => {
		const h = harness();
		await h.seed({
			[A]: [ put(1n, 'a1', 'a1'), put(2n, 'a2', 'a2') ],
			[B]: [ put(1n, 'b1', 'b1'), put(2n, 'b2', 'b2') ],
		});

		const requests: { workspaceId: string; ids: bigint[] }[][] = [];
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					requests.push(
						req.batches.map((b) => ({
							workspaceId: b.workspaceId,
							ids: b.mutations.map((m) => m.mutationId),
						})),
					);
					return pushAppliedAll(req);
				},
				pull: noopPull,
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run');
				},
				query: () => ({ rows: [] }),
			});
		});

		// The engine counts its limit as a TOTAL over a request's batches, so a
		// cap of 3 must split 2+2 into 2+1 then 1 — not send two batches of 2.
		const coord = h.coordinator(transport, { maxMutationsPerPush: 3 });
		await coord.syncNow();

		expect(requests.map((r) => r.flatMap((b) => b.ids).length)).toEqual([ 3, 1 ]);
		expect(requests[0]).toEqual([
			{ workspaceId: A, ids: [ 1n, 2n ] },
			{ workspaceId: B, ids: [ 1n ] },
		]);
		expect(requests[1]).toEqual([ { workspaceId: B, ids: [ 2n ] } ]);

		await coord.stop();
	});

	it('a workspace-scoped push denial stops that workspace only', async () => {
		const h = harness();
		await h.seed({
			[A]: [ put(1n, 'a1', 'a one') ],
			[B]: [ put(1n, 'b1', 'b one') ],
		});

		const status = new StatusTracker();
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					// A is refused as a whole; B applies. Neither is an RPC error, so
					// one revoked partition cannot wedge the other's queue.
					return {
						results: req.batches.map((batch) =>
							batch.workspaceId === A
								? {
									workspaceId: A,
									results: [],
									errorCode: CodeWorkspaceAccessRevoked,
									errorMessage: 'removed from the project',
								}
								: {
									workspaceId: batch.workspaceId,
									results: batch.mutations.map((m) => ({
										mutationId: m.mutationId,
										status: MutationStatus.APPLIED,
										seq: m.mutationId,
									})),
								}
						),
					};
				},
				pull: (req) => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: req.cursor,
					lastAckedMutationId: req.workspaceId === B ? 1n : 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
				}),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run');
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport, { status });
		await coord.syncNow();

		expect(status.current().workspaces[A]?.access).toBe('revoked');
		// A's mutation stays queued (nothing applied), B's was acked and dropped.
		expect(await h.pendingIds(A)).toEqual([ 1n ]);
		expect(await h.pendingIds(B)).toEqual([]);

		await coord.stop();
	});
});
