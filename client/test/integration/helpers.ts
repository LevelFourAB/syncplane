import { createConnectTransport } from '@connectrpc/connect-node';
import { inject } from 'vitest';

import { SyncClient, type SyncClientOptions } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { PairSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';

/** Unique workspace per test so tests cannot observe each other (test files
 * run in parallel workers against the same spawned server). */
export function freshWorkspace(): string {
	return `ws-${crypto.randomUUID()}`;
}

export function newClient(
	workspaceId: string,
	overrides: Partial<SyncClientOptions> = {},
): SyncClient {
	return new SyncClient({
		transport: createConnectTransport({ baseUrl: inject('serverUrl'), httpVersion: '1.1' }),
		workspaceId,
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema, PairSchema ],
		// Integration tests drive sync explicitly unless a test opts in.
		pollIntervalMs: 0,
		streamEnabled: false,
		...overrides,
	});
}

/**
 * A client holding several workspaces at once: one store, one stream, one push.
 * `workspaces[0]` is the default, so the client-level API still works unqualified.
 */
export function newMultiClient(
	workspaces: string[],
	overrides: Partial<SyncClientOptions> = {},
): SyncClient {
	return new SyncClient({
		transport: createConnectTransport({ baseUrl: inject('serverUrl'), httpVersion: '1.1' }),
		accountId: `acct-${crypto.randomUUID()}`,
		workspaces,
		defaultWorkspace: workspaces[0],
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema, PairSchema ],
		pollIntervalMs: 0,
		streamEnabled: false,
		...overrides,
	});
}

/** Polls fn until it returns truthy or the deadline passes. */
export async function eventually<T>(fn: () => Promise<T>, timeoutMs = 5_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	let lastErr: unknown;
	for (;;) {
		try {
			const result = await fn();
			if (result) return result;
			lastErr = new Error('condition not met');
		} catch (err) {
			lastErr = err;
		}
		if (Date.now() > deadline) throw lastErr;
		await new Promise((r) => setTimeout(r, 50));
	}
}
