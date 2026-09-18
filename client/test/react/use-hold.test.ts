// The `hold` option of useRow and useText: a mounted component keeps its row in
// scope, and unmounting lets it go. No server is involved — the hold shows in
// the client's effective scope, which coverage reads before any sync runs.

import { act, renderHook } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { subscription, SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { db, SyncProvider } = createReactSync([ TodoSchema, WidgetSchema ]);

function newClient(): SyncClient {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		subscriptions: [ subscription(TodoSchema) ],
		holdGraceMs: 0,
	});
}

function wrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

function flush(): Promise<void> {
	return act(async () => {
		await new Promise((r) => setTimeout(r, 0));
	});
}

async function widgetBounds(client: SyncClient): Promise<string[]> {
	return (await client.query(WidgetSchema, {})).coverage.bounds;
}

describe('hold option', () => {
	it('useRow holds the row while mounted', async () => {
		const client = newClient();
		const { result, unmount, rerender } = renderHook(
			({ key }: { key: string }) => db.widget.useRow(key, { hold: true }),
			{ wrapper: wrapper(client), initialProps: { key: 'w1' } },
		);
		await flush();
		expect(result.current.row).toBeUndefined();
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);

		// A changed key moves the hold with it.
		rerender({ key: 'w2' });
		await flush();
		const subs = (client as unknown as {
			ownScopes(): Map<string, { keySet?: { keys: string[] } }[]>;
		}).ownScopes().get('ws1')!;
		expect(subs[1]!.keySet?.keys).toEqual([ 'w2' ]);

		unmount();
		await flush();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		await client.close();
	});

	it('useRow without the option takes no hold', async () => {
		const client = newClient();
		const { unmount } = renderHook(() => db.widget.useRow('w1'), { wrapper: wrapper(client) });
		await flush();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		unmount();
		await client.close();
	});

	it('useText holds the row and waits for it before opening', async () => {
		const client = newClient();
		const { result, unmount } = renderHook(
			() => db.widget.useText('w1', 'body', { hold: true }),
			{ wrapper: wrapper(client) },
		);
		await flush();
		// The row is not local and the client never syncs here, so the handle
		// stays unopened rather than failing on the absent row.
		expect(result.current.error).toBeUndefined();
		expect(result.current.handle).toBeUndefined();
		expect(await widgetBounds(client)).toEqual([ 'predicate' ]);

		unmount();
		await flush();
		expect(await widgetBounds(client)).toEqual([ 'unsubscribed' ]);
		await client.close();
	});
});
