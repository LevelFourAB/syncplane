import 'fake-indexeddb/auto';
import { createRouterTransport } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	DEFAULT_STORAGE_PREFIX,
	storageNames,
	validateStoragePrefix,
} from '../../src/storagenames.js';

/** A transport that answers nothing: these specs never sync. */
const transport = createRouterTransport(() => {});

function newClient(opts: { workspaceId: string; storagePrefix?: string }): SyncClient<
	readonly [typeof TodoSchema]
> {
	return new SyncClient({
		transport,
		workspaceId: opts.workspaceId,
		storagePrefix: opts.storagePrefix,
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('storageNames', () => {
	// The default names are a compatibility contract: a client that derives
	// different ones from the same account opens an empty store and leaves
	// whatever the app already had on disk.
	it('names the store, locks and channels under the prefix', () => {
		expect(storageNames(DEFAULT_STORAGE_PREFIX, 'acct')).toEqual({
			store: 'sync:acct',
			kvLock: 'sync:acct',
			streamLock: 'sync:stream:acct',
			changeChannel: 'sync:change:acct',
			reopenChannel: 'sync:reopen:acct',
			presenceLock: 'sync:presencelock:acct',
			presenceChannel: 'sync:presence:acct',
			textPeerLocks: 'sync:textpeer',
			undoScopeLocks: 'sync:undoscope:acct',
			blobStore: 'sync:blobs:acct',
			blobLock: 'sync:bloblock:acct',
			blobChannel: 'sync:blob:acct',
		});
	});

	// The two planes are elected independently, and a tab holding one must not
	// be prevented from contending for the other.
	it('gives the data stream and the room stream different locks', () => {
		const names = storageNames(DEFAULT_STORAGE_PREFIX, 'acct');
		expect(names.presenceLock).not.toBe(names.streamLock);
		expect(names.presenceChannel).not.toBe(names.changeChannel);
		expect(names.presenceChannel).not.toBe(names.reopenChannel);
	});

	it('moves every name when the prefix changes', () => {
		const base = storageNames(DEFAULT_STORAGE_PREFIX, 'acct');
		const other = storageNames('app2', 'acct');
		for (const key of Object.keys(base) as (keyof typeof base)[]) {
			expect(other[key]).not.toBe(base[key]);
		}
	});
});

describe('validateStoragePrefix', () => {
	it('accepts letters, digits, underscores and dashes', () => {
		for (const prefix of [ 'sync', 'app2', 'my_app', 'my-app', '0' ]) {
			expect(() => validateStoragePrefix(prefix)).not.toThrow();
		}
	});

	it('refuses an empty prefix', () => {
		expect(() => validateStoragePrefix('')).toThrow(/empty/);
	});

	it('refuses a prefix that could spell another configuration’s name', () => {
		// `sync:stream` + account "a" would name the Web Lock that `sync` +
		// account "stream:a" holds.
		expect(() => validateStoragePrefix('sync:stream')).toThrow(/not valid/);
	});

	it('refuses a leading separator and an over-long prefix', () => {
		expect(() => validateStoragePrefix('-app')).toThrow(/not valid/);
		expect(() => validateStoragePrefix('a'.repeat(65))).toThrow(/limit is 64/);
	});
});

describe('SyncClient.storagePrefix', () => {
	it('defaults to the sync prefix', async () => {
		const client = newClient({ workspaceId: 'prefix-default' });
		expect(client.storageNames.store).toBe('sync:prefix-default');
		// clientId() is the first read of the store, so it is what creates the
		// database.
		await client.clientId();
		expect(await indexedDB.databases()).toContainEqual(
			expect.objectContaining({ name: 'sync:prefix-default' }),
		);
		await client.close();
	});

	it('opens a database of its own under a custom prefix', async () => {
		const ws = 'prefix-custom';
		const custom = newClient({ workspaceId: ws, storagePrefix: 'app2' });
		expect(custom.storageNames.store).toBe('app2:prefix-custom');
		const customId = await custom.clientId();
		await custom.close();

		// A different prefix is a different store, so the client mints a fresh
		// device identity rather than inheriting the one next door.
		const dflt = newClient({ workspaceId: ws });
		const defaultId = await dflt.clientId();
		await dflt.close();

		expect(defaultId).not.toBe(customId);
		const names = (await indexedDB.databases()).map((d) => d.name);
		expect(names).toContain('app2:prefix-custom');
		expect(names).toContain('sync:prefix-custom');
	});

	it('refuses an invalid prefix at construction', () => {
		expect(() => newClient({ workspaceId: 'prefix-bad', storagePrefix: 'a b' })).toThrow(
			/storagePrefix/,
		);
	});
});
