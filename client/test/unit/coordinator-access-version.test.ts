import { create } from '@bufbuild/protobuf';
import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { encodeAccess } from '../../src/access.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	type CallerAccess,
	type CallerAccessDelta,
	CallerAccessDeltaSchema,
	CallerAccessSchema,
	type PullRequest,
	type PullResponse,
	PullResponseSchema,
	type PushRequest,
	SyncService,
} from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCallerAccessVersion, metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { finalizeRebase } from '../../src/optimistic/rebase.js';
import { DEFAULT_STORAGE_PREFIX, storageNames } from '../../src/storagenames.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker } from '../../src/sync/status.js';
import { TouchSet } from '../../src/touch.js';
import { pushResults } from './pushfake.js';

// A pull and a Subscribe scope name the CallerAccess the store holds
// (PullRequest.caller_access_version), and a CallerAccessDelta made for
// another summary leaves the stored one and gets another pull. What the
// server makes of the version is pinned by the Go suite
// (engine/server/access_sync_test.go) and the integration tier.

const WS = 'ws';

function summary(version: bigint, seq = 1n): CallerAccess {
	return create(CallerAccessSchema, {
		seq,
		subjects: [ 'u' ],
		anchorGroups: [ { permissions: [ 'view' ], anchors: [ { entity: 'project', keys: [ 'p1' ] } ] } ],
		version,
	});
}

function deltaOf(baseVersion: bigint, version: bigint): CallerAccessDelta {
	return create(CallerAccessDeltaSchema, {
		seq: 2n,
		baseVersion,
		version,
		anchorGroups: [ { permissions: [ 'edit' ], anchors: [ { entity: 'project', keys: [ 'p2' ] } ] } ],
	});
}

/** A page that changes no row, at seq 2. */
function page(
	cursor: Uint8Array,
	access: { callerAccess?: CallerAccess; callerAccessDelta?: CallerAccessDelta } = {},
): PullResponse {
	return create(PullResponseSchema, { patch: {}, nextCursor: cursor, seq: 2n, ...access });
}

describe('coordinator CallerAccess version', () => {
	let kv: MemoryKVStore;
	let registry: EntityRegistry;

	beforeEach(() => {
		kv = new MemoryKVStore();
		registry = new EntityRegistry([ TodoSchema ]);
	});

	/** Seeds a synced workspace that holds `access`. */
	async function seed(access: CallerAccess | undefined): Promise<void> {
		const tx = await kv.write();
		await finalizeRebase(tx, registry, {}, WS, {
			nextCursor: new Uint8Array([ 1 ]),
			lastAckedMutationId: 0n,
			...(access === undefined ? {} : { callerAccess: encodeAccess(access) }),
		}, new TouchSet());
		await tx.commit();
	}

	async function storedVersion(): Promise<string | undefined> {
		const tx = await kv.read();
		try {
			return (await tx.get(metaCallerAccessVersion(WS))) as string | undefined;
		} finally {
			tx.release();
		}
	}

	function newCoordinator(transport: Transport, streamEnabled = false): Coordinator {
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
			streamEnabled,
			pollIntervalMs: 0,
		});
	}

	/** A transport whose pulls answer with the pages of `answer`. */
	function transportOf(
		answer: (req: PullRequest) => PullResponse,
		subscribe?: (req: { scopes: { callerAccessVersion?: bigint }[] }) => AsyncIterable<unknown>,
	): Transport {
		return createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req: PushRequest) => pushResults(req, []),
				pull: (req) => answer(req),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				subscribe: subscribe as never,
				query: () => ({ rows: [] }),
			});
		});
	}

	it('sends the version of the stored summary on every pull', async () => {
		await seed(summary(5n));
		const versions: (bigint | undefined)[] = [];
		const coord = newCoordinator(transportOf((req) => {
			versions.push(req.callerAccessVersion);
			return page(req.cursor);
		}));
		await coord.syncNow();
		await coord.stop();

		expect(versions).toEqual([ 5n ]);
	});

	it('sends version 0 when it holds no summary', async () => {
		await seed(undefined);
		const versions: (bigint | undefined)[] = [];
		const coord = newCoordinator(transportOf((req) => {
			versions.push(req.callerAccessVersion);
			return page(req.cursor);
		}));
		await coord.syncNow();
		await coord.stop();

		// Set, not left out: a pull without the field gets no delta.
		expect(versions).toEqual([ 0n ]);
	});

	it('pulls again with its own version when a delta was made for another summary', async () => {
		await seed(summary(5n));
		const versions: (bigint | undefined)[] = [];
		const coord = newCoordinator(transportOf((req) => {
			versions.push(req.callerAccessVersion);
			if (versions.length === 1) return page(req.cursor, { callerAccessDelta: deltaOf(4n, 6n) });
			return page(req.cursor, { callerAccess: summary(6n, 2n) });
		}));
		await coord.syncNow();
		await coord.stop();

		// The stored summary stayed through the first page, so the second
		// pull names it again.
		expect(versions).toEqual([ 5n, 5n ]);
		expect(await storedVersion()).toBe('6');
	});

	it('sends the version on every subscribe scope, and pulls after a delta it cannot apply', async () => {
		await seed(summary(5n));
		const scopeVersions: (bigint | undefined)[] = [];
		const pullVersions: (bigint | undefined)[] = [];
		let afterStale: bigint | undefined;
		const transport = transportOf(
			(req) => {
				pullVersions.push(req.callerAccessVersion);
				return page(req.cursor);
			},
			async function*(req) {
				for (const scope of req.scopes) scopeVersions.push(scope.callerAccessVersion);
				yield {
					workspaceId: WS,
					schemaRevision: '',
					body: { case: 'caughtUp' as const, value: true },
				};
				// Lets the pulls a stream open starts finish first, so the one
				// counted below is the one the stale delta starts.
				await new Promise((resolve) => setTimeout(resolve, 100));
				const pulled = pullVersions.length;
				yield {
					workspaceId: WS,
					schemaRevision: '',
					body: {
						case: 'page' as const,
						value: page(new Uint8Array([ 3 ]), { callerAccessDelta: deltaOf(4n, 6n) }),
					},
				};
				await vi.waitFor(() => expect(pullVersions.length).toBeGreaterThan(pulled));
				afterStale = pullVersions.at(-1);
				await new Promise(() => {});
			},
		);

		const coord = newCoordinator(transport, true);
		await coord.start();
		await vi.waitFor(() => expect(afterStale).toBe(5n));
		expect(scopeVersions).toEqual([ 5n ]);
		expect(await storedVersion()).toBe('5');
		await coord.stop();
	});
});
