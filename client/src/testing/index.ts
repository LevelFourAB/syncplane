/**
 * In-process test kit for apps built on syncplane, importable as
 * `syncplane/testing`. {@link createTestClient} wires a real `SyncClient` to a
 * {@link TestServer} living in the same process, so the behaviors the client is
 * built around — optimistic apply, rebase, rejection rollback, offline
 * queueing, re-bootstrap — run inside an ordinary test in any runner, no
 * server binary involved.
 *
 * @example Reject a write while offline and assert the rollback the user sees.
 * ```ts
 * const t = await createTestClient({ entities: [ TodoSchema ] });
 * await t.net.offline();
 * await t.db.todo.mutate.put({ id: 't1', title: 'Buy milk' });
 * t.server.reject({ entity: TodoSchema, key: 't1' }, 'permission_denied');
 * await t.net.online();
 * expect(t.rejections[0]?.errorCode).toBe('permission_denied');
 * expect(await t.db.todo.get('t1')).toBeUndefined();
 * await t.close();
 * ```
 */

import type { DescMessage } from '@bufbuild/protobuf';
import { Code, ConnectError, type Transport } from '@connectrpc/connect';

import { SyncClient, type SyncClientOptions } from '../client.js';
import type { EntityApis } from '../db.js';
import { META_CLIENT_ID } from '../keys.js';
import { MemoryKVStore } from '../kv/memory.js';
import type { RejectionInfo } from '../sync/coordinator.js';
import { DEFAULT_WORKSPACE, TestServer } from './server.js';

export type { TestCallerAccess, TestPermission } from './access.js';
export {
	createTestServer,
	DEFAULT_WORKSPACE,
	type PushHold,
	Rejection,
	type RejectMatch,
	type RejectPredicate,
	type TestMutator,
	type TestMutatorTx,
	TestServer,
	type TestServerOptions,
} from './server.js';

/**
 * The simulated network between a test client and its server. Starts online.
 */
export interface TestNet {
	readonly isOffline: boolean;
	/**
	 * Fails every RPC from this client with `unavailable` until
	 * {@link TestNet.online}. Writes keep applying optimistically and queue.
	 */
	offline(): Promise<void>;
	/**
	 * Restores connectivity, and — when the kit started the client — runs one
	 * full sync cycle before resolving, so queued mutations are pushed and
	 * acks, rejections and remote changes are already applied when it returns.
	 * Rejects with the cycle's error when that sync fails.
	 */
	online(): Promise<void>;
}

export interface TestClientOptions<E extends readonly DescMessage[]>
	extends Omit<SyncClientOptions<E>, 'entities' | 'transport' | 'url' | 'wireFormat' | 'kvStore'>
{
	/** May be omitted when `server` is given — the server's entities are used. */
	entities?: E;
	/**
	 * Server to connect to. Give several clients the same one to test
	 * convergence between replicas. @defaultValue a fresh TestServer
	 */
	server?: TestServer<E>;
	/** @defaultValue 'test-client-<n>', numbered per process */
	clientId?: string;
	/**
	 * Whether to `start()` the client before returning. With `false` the test
	 * owns the lifecycle, and {@link TestNet.online} stops running sync cycles.
	 * @defaultValue true
	 */
	start?: boolean;
}

/** What {@link createTestClient} hands the test; see the fields' own docs. */
export interface TestClient<E extends readonly DescMessage[]> {
	client: SyncClient<E>;
	/** Shorthand for `client.db`. */
	db: EntityApis<E>;
	server: TestServer<E>;
	net: TestNet;
	clientId: string;
	/** Every rejection the client saw, oldest first — asserting rollbacks. */
	rejections: RejectionInfo[];
	/** One full push+pull cycle (`client.syncNow()`). */
	sync(): Promise<void>;
	/** Closes the client. The server outlives it. */
	close(): Promise<void>;
}

let nextOrdinal = 1;

// Max setTimeout delay. As a push debounce it means "not until a sync cycle
// asks": the production default of pushing moments after every write would
// let background pushes race the test's assertions.
const neverMs = 2 ** 31 - 1;

/**
 * A `SyncClient` on an in-memory store, wired to an in-process
 * {@link TestServer} through a network the test controls.
 *
 * The client is real and configured for determinism: polling and the
 * subscribe stream are off, presence is off, the client id is fixed, and the
 * push debounce is effectively infinite — writes apply optimistically and
 * queue, and reach the server only when {@link TestClient.sync} (or
 * `net.online()`) runs a cycle. Any `SyncClientOptions` field can be
 * overridden, including those defaults; `pushDebounceMs: 0` restores
 * production push-as-you-write, `streamEnabled: true` live server→client
 * pages — both at the cost of state changing between explicit sync calls.
 *
 * The client is started (see `start`) and must be closed via
 * {@link TestClient.close}.
 */
export async function createTestClient<const E extends readonly DescMessage[]>(
	opts: TestClientOptions<E>,
): Promise<TestClient<E>> {
	const { entities: entitiesOpt, server: serverOpt, clientId: clientIdOpt, start, ...clientOpts } =
		opts;
	if (!serverOpt && !entitiesOpt) {
		throw new Error('createTestClient needs entities or a server');
	}
	const server = serverOpt
		?? new TestServer({ entities: entitiesOpt!, mergers: clientOpts.mergers });
	const entities = (entitiesOpt ?? server.entities) as E;

	const clientId = clientIdOpt ?? `test-client-${nextOrdinal++}`;
	const kv = new MemoryKVStore();
	// Pinning the id up front keeps it deterministic where a fresh store would
	// mint a random UUID.
	const seed = await kv.write();
	await seed.put(META_CLIENT_ID, clientId);
	await seed.commit();

	let offline = false;
	const gate = () => {
		if (offline) {
			throw new ConnectError('network offline (syncplane/testing)', Code.Unavailable);
		}
	};
	const inner = server.transport();
	const transport = {
		unary: async (...args: unknown[]) => {
			gate();
			return (inner.unary as (...a: unknown[]) => Promise<unknown>)(...args);
		},
		stream: async (...args: unknown[]) => {
			gate();
			return (inner.stream as (...a: unknown[]) => Promise<unknown>)(...args);
		},
	} as Transport;

	const rejections: RejectionInfo[] = [];
	const scope = clientOpts.workspaceId === undefined && clientOpts.workspaces === undefined
		? { workspaceId: DEFAULT_WORKSPACE }
		: {};
	const client = new SyncClient<E>({
		streamEnabled: false,
		presenceEnabled: false,
		pollIntervalMs: 0,
		pushDebounceMs: neverMs,
		pushMaxDelayMs: neverMs,
		...scope,
		...clientOpts,
		entities,
		transport,
		kvStore: kv,
		onMutationRejected: (info) => {
			rejections.push(info);
			clientOpts.onMutationRejected?.(info);
		},
	});

	let started = false;
	if (start !== false) {
		await client.start();
		started = true;
	}

	const net: TestNet = {
		get isOffline() {
			return offline;
		},
		async offline() {
			offline = true;
		},
		async online() {
			offline = false;
			if (started) await client.syncNow();
		},
	};

	return {
		client,
		db: client.db,
		server,
		net,
		clientId,
		rejections,
		sync: () => client.syncNow(),
		close: () => client.close(),
	};
}
