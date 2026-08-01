import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVWrite } from '../../src/kv/store.js';

/**
 * A store whose first `failures` write transactions fail — stands in for a
 * flaky IndexedDB during the format check start() runs before anything else.
 */
class FlakyKVStore extends MemoryKVStore {
	constructor(private failures: number) {
		super();
	}

	override async write(): Promise<KVWrite> {
		if (this.failures > 0) {
			this.failures--;
			throw new Error('kv unavailable');
		}
		return super.write();
	}
}

const options = {
	workspaceId: 'ws1',
	entities: [ TodoSchema ],
	streamEnabled: false,
	pollIntervalMs: 0,
} as const;

/** The coordinator a successful start() leaves behind, or undefined. */
function coordinatorOf(client: SyncClient): unknown {
	return (client as unknown as { coordinator?: unknown }).coordinator;
}

describe('SyncClient.start', () => {
	it('is idempotent once it has succeeded', async () => {
		const client = new SyncClient({
			...options,
			kvStore: new MemoryKVStore(),
			transport: createRouterTransport(() => {}),
		});
		await client.start();
		const coordinator = coordinatorOf(client);
		await client.start();
		expect(coordinatorOf(client)).toBe(coordinator);
		await client.close();
	});

	it('shares one start between concurrent callers', async () => {
		const client = new SyncClient({
			...options,
			kvStore: new MemoryKVStore(),
			transport: createRouterTransport(() => {}),
		});
		await Promise.all([ client.start(), client.start(), client.start() ]);
		expect(coordinatorOf(client)).toBeDefined();
		await client.close();
	});

	it('rejects a client configured with neither url nor transport', async () => {
		const client = new SyncClient({ ...options, kvStore: new MemoryKVStore() });
		await expect(client.start()).rejects.toThrow(/needs url or transport/);
		// The failure must not latch: a second start() reports the same problem
		// rather than resolving into a client that silently never syncs.
		await expect(client.start()).rejects.toThrow(/needs url or transport/);
		await expect(client.syncNow()).rejects.toThrow(/start\(\) first/);
		await client.close();
	});

	it('can be retried after a start that failed on the store', async () => {
		const client = new SyncClient({
			...options,
			kvStore: new FlakyKVStore(1),
			transport: createRouterTransport(() => {}),
		});
		await expect(client.start()).rejects.toThrow(/kv unavailable/);
		// No half-started coordinator is left behind by the failure.
		expect(coordinatorOf(client)).toBeUndefined();
		await expect(client.syncNow()).rejects.toThrow(/start\(\) first/);

		await client.start();
		expect(coordinatorOf(client)).toBeDefined();
		await client.close();
	});
});

/**
 * Content types of every request a url-configured client sent, with fetch
 * stubbed out: the transport connect-web builds is opaque, so what it encodes
 * is only observable from the request it produces. The stubbed response fails
 * the sync, which is fine — the request has already been encoded by then.
 */
async function contentTypes(wireFormat?: 'binary' | 'json'): Promise<string[]> {
	const seen: string[] = [];
	vi.stubGlobal('fetch', async (input: unknown, init?: RequestInit) => {
		const headers = new Headers(init?.headers ?? (input as Request).headers);
		seen.push(headers.get('content-type') ?? '');
		return new Response(null, { status: 503 });
	});
	try {
		const client = new SyncClient({
			...options,
			kvStore: new MemoryKVStore(),
			url: 'http://sync.invalid',
			wireFormat,
		});
		await client.start();
		await client.syncNow().catch(() => {});
		await client.close();
	} finally {
		vi.unstubAllGlobals();
	}
	return seen;
}

describe('SyncClient wire format', () => {
	it('encodes as binary protobuf by default', async () => {
		// connect-web defaults the other way, so this asserts a deliberate
		// choice rather than a library default: JSON would base64 every
		// RowChange.value and repeat each field name per row.
		const types = await contentTypes();
		expect(types.length).toBeGreaterThan(0);
		for (const type of types) expect(type).toMatch(/proto/);
		for (const type of types) expect(type).not.toMatch(/json/);
	});

	it('encodes as JSON when asked, for readable traffic', async () => {
		const types = await contentTypes('json');
		expect(types.length).toBeGreaterThan(0);
		for (const type of types) expect(type).toMatch(/json/);
	});

	it('leaves a supplied transport alone', async () => {
		// wireFormat configures the transport built from `url`; one handed in
		// already carries its own encoding, and silently overriding it would
		// mean the option quietly did nothing here.
		const client = new SyncClient({
			...options,
			kvStore: new MemoryKVStore(),
			transport: createRouterTransport(() => {}),
			wireFormat: 'json',
		});
		await client.start();
		expect(coordinatorOf(client)).toBeDefined();
		await client.close();
	});
});
