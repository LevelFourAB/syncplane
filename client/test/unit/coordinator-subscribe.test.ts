import { create, toBinary } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { describe, expect, it, vi } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	type RowChange,
	RowChangeSchema,
	type Subscription,
	SubscriptionSchema,
} from '../../src/gen/syncplane/v1/patch_pb.js';
import {
	BootstrapResponseSchema,
	MutationStatus,
	type SubscribeRequest,
	SyncService,
	WorkspaceDropped_Reason,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor, metaServerSeq } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { ChainMutex } from '../../src/mutex.js';
import { readMergedRow } from '../../src/read.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator, type CoordinatorOptions } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { pushApplied, pushedMutations, pushFailed, pushResults } from './pushfake.js';

// The coordinator's leader Subscribe stream: in a Node runtime (a server
// runtime), startStreamLeadership returns undefined, so a streamEnabled
// coordinator runs subscribeLoop directly on the global signal — exactly the
// no-Web-Locks fallback. These tests drive that path through a router
// transport whose subscribe() yields scripted pages.

function sub(entity: string): Subscription {
	return create(SubscriptionSchema, { entity });
}

function serverRow(key: string, version: bigint, title: string): RowChange {
	return create(RowChangeSchema, {
		entity: 'todo',
		key,
		version,
		body: { case: 'value', value: toBinary(TodoSchema, create(TodoSchema, { id: key, title })) },
	});
}

/** The workspace every scope in these specs names. */
const WS = 'w1';

/** One page frame for WS. Every stream frame names the workspace it concerns. */
function page(changes: RowChange[], nextCursor: Uint8Array, seq: bigint) {
	return {
		workspaceId: WS,
		schemaRevision: '',
		body: {
			case: 'page' as const,
			value: {
				patch: { changes, dropEntities: [] },
				nextCursor,
				lastAckedMutationId: 0n,
				reset: false,
				hasMore: false,
				seq,
				schemaRevision: '',
			},
		},
	};
}

/** The caught_up marker for WS: its backlog is drained, the live tail follows. */
function caughtUp() {
	return {
		workspaceId: WS,
		schemaRevision: '',
		body: { case: 'caughtUp' as const, value: true },
	};
}

/** A frame dropping WS from the stream for the given reason. */
function dropped(reason: WorkspaceDropped_Reason, message = '') {
	return {
		workspaceId: WS,
		schemaRevision: '',
		body: { case: 'dropped' as const, value: { reason, message } },
	};
}

/** The subscription entity names one scope asked for, sorted. */
function scopeEntities(req: SubscribeRequest): string[] {
	return (req.scopes[0]?.subscriptions ?? []).map((s) => s.entity).sort();
}

function waitAbort(signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal.aborted) resolve();
		else signal.addEventListener('abort', () => resolve());
	});
}

describe('coordinator subscribe stream', () => {
	function harness() {
		const kv = new MemoryKVStore();
		const kvMutex = new ChainMutex();
		const registry = new EntityRegistry([ TodoSchema ]);

		async function seed(): Promise<void> {
			const tx = await kv.write();
			await tx.put(metaCursor(WS), new Uint8Array([ 1 ]));
			await tx.commit();
		}

		function coordinator(
			transport: Transport,
			overrides: Partial<CoordinatorOptions> = {},
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
				streamEnabled: true, // run the subscribe stream
				pollIntervalMs: 0,
				scopes: new Map([ [ WS, [ sub('todo') ] ] ]),
				...overrides,
			});
		}

		async function getMeta(key: string): Promise<unknown> {
			const tx = await kv.read();
			try {
				return await tx.get(key);
			} finally {
				tx.release();
			}
		}

		return { kv, registry, seed, coordinator, getMeta };
	}

	it('applies stream pages and drops a stale page under the META_SERVER_SEQ guard', async () => {
		const h = harness();
		await h.seed();

		const transport = createRouterTransport(({ service }) => {
			let subCalls = 0;
			service(SyncService, {
				push: (req) => pushResults(req, []),
				// Idempotent no-op pull (echoes the cursor; seq 0 bypasses the guard).
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
				async *subscribe(_req, ctx) {
					subCalls++;
					if (subCalls === 1) {
						// Fresh (seq 5), caught up, then a STALE page (seq 3) that the
						// monotonic guard must drop whole — rows and cursor.
						yield page([ serverRow('t1', 5n, 'fresh') ], new Uint8Array([ 5 ]), 5n);
						yield caughtUp();
						yield page([ serverRow('t1', 3n, 'stale') ], new Uint8Array([ 3 ]), 3n);
					}
					await waitAbort(ctx.signal);
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport);
		await coord.start();

		await vi.waitFor(async () => {
			expect(await h.getMeta(metaServerSeq(WS))).toBe('5');
		});
		// Give the stale page a chance to (wrongly) apply, then assert it didn't.
		await new Promise((r) => setTimeout(r, 30));

		const tx = await h.kv.read();
		try {
			expect(await readMergedRow(tx, h.registry, 'todo', WS, 't1')).toEqual(
				toBinary(TodoSchema, create(TodoSchema, { id: 't1', title: 'fresh' })),
			);
		} finally {
			tx.release();
		}
		expect(await h.getMeta(metaServerSeq(WS))).toBe('5');

		await coord.stop();
	});

	it('reopens the stream with the new union on a subscription change', async () => {
		const h = harness();
		await h.seed();

		const subCalls: string[][] = [];
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => pushResults(req, []),
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
					throw new Error('bootstrap should not run');
				},
				async *subscribe(req, ctx) {
					subCalls.push(scopeEntities(req));
					yield caughtUp();
					await waitAbort(ctx.signal); // hold open until reopened
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport);
		await coord.start();

		await vi.waitFor(() => expect(subCalls).toContainEqual([ 'todo' ]));

		// A scope change re-scopes the stream: close + reopen with the new set.
		coord.setSubscriptions(WS, [ sub('todo'), sub('project') ]);
		await vi.waitFor(() => expect(subCalls).toContainEqual([ 'project', 'todo' ]));

		await coord.stop();
	});

	it('re-bootstraps and reopens the stream when a workspace is dropped for reset', async () => {
		const h = harness();
		await h.seed();

		let bootstraps = 0;
		let subCalls = 0;
		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => pushResults(req, []),
				pull: (req) => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: req.cursor,
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
				}),
				async *bootstrap() {
					bootstraps++;
					yield create(BootstrapResponseSchema, {
						body: {
							case: 'begin',
							value: {
								cursor: new Uint8Array([ 9 ]),
								lastAckedMutationId: 0n,
								seq: 0n,
								schemaRevision: '',
							},
						},
					});
					yield create(BootstrapResponseSchema, { body: { case: 'end', value: {} } });
				},
				async *subscribe(_req, ctx) {
					subCalls++;
					if (subCalls === 1) {
						// A reset now arrives as a DROPPED frame: it ends one workspace,
						// not the stream. The client re-bootstraps and reopens.
						yield dropped(WorkspaceDropped_Reason.RESET);
					}
					yield caughtUp();
					await waitAbort(ctx.signal);
				},
				query: () => ({ rows: [] }),
			});
		});

		const coord = h.coordinator(transport);
		await coord.start();

		await vi.waitFor(() => {
			expect(bootstraps).toBeGreaterThanOrEqual(1);
			expect(subCalls).toBeGreaterThanOrEqual(2);
		});
		// The re-bootstrap installed the fresh cursor.
		expect(await h.getMeta(metaCursor(WS))).toEqual(new Uint8Array([ 9 ]));

		await coord.stop();
	});

	it('reports a failure of its own apply path instead of taking it for a dropped stream', async () => {
		const h = harness();
		await h.seed();
		const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			const transport = createRouterTransport(({ service }) => {
				service(SyncService, {
					push: (req) => pushResults(req, []),
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
						throw new Error('bootstrap should not run');
					},
					async *subscribe(_req, ctx) {
						yield page([ serverRow('t1', 5n, 'fresh') ], new Uint8Array([ 5 ]), 5n);
						await waitAbort(ctx.signal);
					},
					query: () => ({ rows: [] }),
				});
			});

			// A throw from the library's own page-application path (here: notify)
			// reaches the same catch a dropped connection does. The reconnect would
			// hide it forever, so it has to be reported.
			const coord = h.coordinator(transport, {
				notify: async () => {
					throw new Error('watcher fanout exploded');
				},
			});
			await coord.start();

			await vi.waitFor(() => {
				expect(
					errSpy.mock.calls.some((c) => String(c[0]).includes('subscribe stream failed')),
				).toBe(true);
			});

			await coord.stop();
		} finally {
			errSpy.mockRestore();
		}
	});

	it('escalates the reconnect backoff when the stream flaps', async () => {
		vi.useFakeTimers();
		try {
			const h = harness();
			await h.seed();

			const opens: number[] = [];
			const transport = createRouterTransport(({ service }) => {
				service(SyncService, {
					push: (req) => pushResults(req, []),
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
						throw new Error('bootstrap should not run');
					},
					// Accepts the stream, delivers one frame, hangs up — an aggressive
					// idle timeout in front of the server, seen from the client.
					async *subscribe() {
						opens.push(Date.now());
						yield caughtUp();
					},
					query: () => ({ rows: [] }),
				});
			});

			const coord = h.coordinator(transport);
			await coord.start();
			await vi.advanceTimersByTimeAsync(60_000);
			await coord.stop();

			// 1s doubling to a 30s cap: under ten reconnects in the minute. Resetting
			// the backoff on the first frame would instead hold the ~0.75s floor —
			// some eighty connections a minute, from every client at once.
			expect(opens.length).toBeGreaterThanOrEqual(3);
			expect(opens.length).toBeLessThanOrEqual(12);
		} finally {
			vi.useRealTimers();
		}
	});

	it('resets the reconnect backoff after a stream that stays up', async () => {
		vi.useFakeTimers();
		try {
			const h = harness();
			await h.seed();

			const opens: number[] = [];
			const transport = createRouterTransport(({ service }) => {
				service(SyncService, {
					push: (req) => pushResults(req, []),
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
						throw new Error('bootstrap should not run');
					},
					async *subscribe() {
						opens.push(Date.now());
						yield caughtUp();
						// The first three opens flap (escalating the backoff); the fourth
						// is a healthy connection that lives well past STREAM_STABLE_MS
						// before dropping, which must put the next reconnect back on the
						// floor.
						if (opens.length === 4) await new Promise((r) => setTimeout(r, 15_000));
					},
					query: () => ({ rows: [] }),
				});
			});

			const coord = h.coordinator(transport);
			await coord.start();
			await vi.advanceTimersByTimeAsync(60_000);
			await coord.stop();

			expect(opens.length).toBeGreaterThanOrEqual(5);
			// Third flap: already escalated past the floor...
			expect(opens[3]! - opens[2]!).toBeGreaterThanOrEqual(2_000);
			// ...and after the 15s stream, the reconnect is a floor delay again.
			expect(opens[4]! - opens[3]! - 15_000).toBeLessThanOrEqual(1_100);
		} finally {
			vi.useRealTimers();
		}
	});
});
