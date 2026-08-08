import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	BootstrapResponseSchema,
	MutationStatus,
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
import type { BootstrapEvent, PullEvent, PushEvent, SyncEvent } from '../../src/sync/events.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { pushedMutations, pushResults } from './pushfake.js';

/**
 * The observability seam (onSyncEvent). These specs pin the events that report
 * what a successful RPC hides — a push whose mutations the server refused is a
 * 200 OK, and nothing else in the client says so — and that a failing handler
 * cannot take the sync cycle down with it.
 */

const WS = 'ws';

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })) },
	});
}

describe('coordinator sync events', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;
	let events: SyncEvent[];

	beforeEach(async () => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
		events = [];
		// A cursor already present makes sync pull rather than bootstrap.
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 0 ]));
		await tx.commit();
	});

	/** Seeds one pending mutation, the shape a push then ships. */
	async function seedPending(): Promise<void> {
		const def = registry.bySchema(TodoSchema);
		const crud = buildPut(def, TodoSchema, create(TodoSchema, { id: 't1', title: 'queued' }));
		const mutation = create(MutationSchema, {
			clientId: 'c1',
			mutationId: 1n,
			payload: { case: 'crud', value: crud },
		});
		const tx = await kv.write();
		await enqueuePending(tx, WS, mutation);
		await applyMutationLocally(tx, registry, {}, WS, mutation, new TouchSet());
		await tx.put(metaLastMutationId(WS), '1');
		await tx.commit();
	}

	function newCoordinator(
		transport: Transport,
		onSyncEvent = (e: SyncEvent) => void events.push(e),
	): Coordinator {
		return new Coordinator({
			transport,
			kv,
			kvMutex: createKvMutex('test'),
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: async () => {},
			status: new StatusTracker(),
			streamEnabled: false,
			pollIntervalMs: 0,
			onSyncEvent,
		});
	}

	/** A server that pulls one page and answers pushes with `results`. */
	function transportWith(pushStatus?: MutationStatus): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push(req) {
					if (pushStatus === undefined) return pushResults(req, []);
					return pushResults(
						req,
						pushedMutations(req).map((m) => ({
							mutationId: m.mutationId,
							status: pushStatus,
							...(pushStatus === MutationStatus.REJECTED
								? { errorCode: 'permission_denied', errorMessage: 'nope' }
								: { seq: m.mutationId }),
						})),
					);
				},
				pull() {
					return {
						patch: { changes: [ serverRow('t1', 1n, 'one') ], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 1n,
						schemaRevision: '',
					};
				},
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap not used');
				},
				query() {
					return { rows: [] };
				},
			});
		});
	}

	it('reports a pull with its change count and duration', async () => {
		const coord = newCoordinator(transportWith());
		await coord.syncNow();

		const pull = events.find((e): e is PullEvent => e.type === 'pull');
		expect(pull).toBeDefined();
		expect(pull!.workspaceId).toBe(WS);
		expect(pull!.changes).toBe(1);
		expect(pull!.reset).toBe(false);
		expect(pull!.outcome).toBe('ok');
		expect(pull!.durationMs).toBeGreaterThanOrEqual(0);
		expect(pull!.at).toBeGreaterThan(0);
	});

	it('separates applied from rejected mutations on a push that returned 200', async () => {
		await seedPending();
		const coord = newCoordinator(transportWith(MutationStatus.REJECTED));
		await coord.syncNow();

		const push = events.find((e): e is PushEvent => e.type === 'push');
		expect(push).toBeDefined();
		// The RPC succeeded and the mutation was refused: the whole point of the
		// event, since neither the transport nor the status surface says so.
		expect(push!.outcome).toBe('ok');
		expect(push!.mutations).toBe(1);
		expect(push!.rejected).toBe(1);
		expect(push!.applied).toBe(0);
		expect(push!.workspaceIds).toEqual([ WS ]);
	});

	it('counts an applied push as applied', async () => {
		await seedPending();
		const coord = newCoordinator(transportWith(MutationStatus.APPLIED));
		await coord.syncNow();

		const push = events.find((e): e is PushEvent => e.type === 'push');
		expect(push?.applied).toBe(1);
		expect(push?.rejected).toBe(0);
	});

	it('breaks a bootstrap into its stream, stage and swap phases', async () => {
		// No cursor: the cycle bootstraps rather than pulls.
		const tx = await kv.write();
		await tx.del(metaCursor(WS));
		await tx.commit();

		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				async *bootstrap() {
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: { cursor: new Uint8Array([ 1 ]), lastAckedMutationId: 0n, seq: 1n },
						},
					});
					yield create(BootstrapResponseSchema, {
						body: { case: 'chunk', value: { rows: [ serverRow('t1', 1n, 'one') ] } },
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				push(req) {
					return pushResults(req, []);
				},
				pull() {
					return {
						patch: { changes: [], dropEntities: [] },
						nextCursor: new Uint8Array([ 1 ]),
						lastAckedMutationId: 0n,
						reset: false,
						hasMore: false,
						seq: 1n,
						schemaRevision: '',
					};
				},
				query() {
					return { rows: [] };
				},
			});
		});

		const coord = newCoordinator(transport);
		await coord.syncNow();

		const boot = events.find((e): e is BootstrapEvent => e.type === 'bootstrap');
		expect(boot).toBeDefined();
		expect(boot!.rows).toBe(1);
		// The phases are the point: each is a real span, and together they are the
		// total up to the bookkeeping between them. A phase stuck at zero would
		// mean the clock was never taken, which is exactly the regression that
		// makes the split useless.
		expect(boot!.streamMs).toBeGreaterThan(0);
		expect(boot!.stageMs).toBeGreaterThan(0);
		expect(boot!.swapMs).toBeGreaterThan(0);
		const phases = boot!.streamMs + boot!.stageMs + boot!.swapMs;
		expect(phases).toBeLessThanOrEqual(boot!.durationMs + 1);
		// Slack in absolute milliseconds rather than as a fraction: the gap
		// between the phases is a few statements, so it stays small however small
		// the whole bootstrap is, and a percentage would only measure the clock.
		expect(phases).toBeGreaterThan(boot!.durationMs - 5);
		// One row lands as at least a put of the row plus the delete of its
		// staged copy; the exact number is the swap's business, that it is
		// counted at all is this event's.
		expect(boot!.swapWrites).toBeGreaterThanOrEqual(2);
	});

	it('survives a handler that throws', async () => {
		// The handler is adopter code running inside the sync cycle. A throw
		// there must not be mistaken for a failed pull — the page was already
		// committed by the time we notify.
		const coord = newCoordinator(transportWith(), () => {
			throw new Error('handler exploded');
		});
		await expect(coord.syncNow()).resolves.toBeUndefined();
	});

	it('emits nothing when no handler is configured', async () => {
		const coord = new Coordinator({
			transport: transportWith(),
			kv,
			kvMutex: createKvMutex('test'),
			storageNames: storageNames(DEFAULT_STORAGE_PREFIX, 'acct'),
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: async () => {},
			status: new StatusTracker(),
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		await expect(coord.syncNow()).resolves.toBeUndefined();
		expect(events).toHaveLength(0);
	});
});
