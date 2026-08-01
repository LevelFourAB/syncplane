import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { metaCursor } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { createKvMutex } from '../../src/mutex.js';
import { Coordinator } from '../../src/sync/coordinator.js';
import { StatusTracker, type SyncStatus } from '../../src/sync/status.js';
import { pushResults } from './pushfake.js';

/** The workspace this spec drives. */
const WS = 'ws';

// What a sync cycle costs the app in status notifications. Every listener is
// app code — usually a React state setter — so the budget is worth pinning:
// polling runs a cycle on a timer whether or not anything changed.

describe('coordinator status notifications', () => {
	it('costs a settled no-op cycle exactly one syncing/idle pair', async () => {
		const kv = new MemoryKVStore();
		const registry = new EntityRegistry([ TodoSchema ]);
		// A cursor already present makes the cycle a pull, not a bootstrap.
		const tx = await kv.write();
		await tx.put(metaCursor(WS), new Uint8Array([ 0 ]));
		await tx.commit();

		const transport = createRouterTransport(({ service }) => {
			service(SyncService, {
				push: (req) => pushResults(req, []),
				// Idempotent no-op pull: echoes the cursor, changes nothing.
				pull: (req) => ({
					patch: { changes: [], dropEntities: [] },
					nextCursor: req.cursor,
					lastAckedMutationId: 0n,
					reset: false,
					hasMore: false,
					seq: 0n,
					schemaRevision: '',
				}),
				// eslint-disable-next-line require-yield
				async *bootstrap() {
					throw new Error('bootstrap should not run with a seeded cursor');
				},
				async *subscribe() {},
				query: () => ({ rows: [] }),
			});
		});

		const status = new StatusTracker();
		const seen: SyncStatus[] = [];
		status.onChange((s) => seen.push(s));
		const coord = new Coordinator({
			transport,
			kv,
			kvMutex: createKvMutex('test'),
			accountId: 'acct',
			scopes: new Map([ [ WS, [] ] ]),
			clientId: 'c1',
			registry,
			mirrors: {},
			notify: async () => {},
			status,
			streamEnabled: false,
			pollIntervalMs: 0,
		});
		await coord.start();

		// The first cycle settles the standing fields (scope, pending count), so
		// measure the SECOND — the shape every later poll takes.
		const settle = (): Promise<void> => new Promise((r) => setTimeout(r, 50)) as Promise<void>;
		await settle();
		seen.length = 0;
		coord.syncSoon();
		await settle();
		await coord.stop();

		// Two notifications, both of them the state the app asked to be told about.
		// The fresh lastSyncedAt rides along on the 'idle' one rather than costing a
		// notification of its own — nothing here fires for an unchanged snapshot.
		expect(seen.map((s) => s.state)).toEqual([ 'syncing', 'idle' ]);
		expect(seen[1]?.lastSyncedAt).toBeInstanceOf(Date);
	});
});
