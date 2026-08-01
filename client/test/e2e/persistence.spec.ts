import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * IndexedDB across a tab's lifetime. `IDBKVStore` buffers a logical write
 * outside any single IDB transaction and commits it at the end, so "did the
 * data actually land" is a question only a real store can answer — and the
 * payoff, a reopened tab that reads its own store instead of re-downloading the
 * workspace, is asserted from the server side.
 */
test.describe('store persistence across tabs', () => {
	test('a reopened tab reads its existing store and does not re-bootstrap', async ({ context }) => {
		const ws = freshWorkspace();
		const first = await Tab.open(context, { workspaceId: ws, start: true });
		await first.putTodo('t1', 'persisted');
		await first.syncNow();
		expect(await first.pendingCount()).toBe(0);

		// The client was given a url and nothing else, so it picked its own
		// storage: everything below is only meaningful because that pick was
		// IndexedDB, under the account-scoped database name.
		expect(await first.databases()).toContain(`sync:${ws}`);

		const bootstraps = await totalCalls('Bootstrap');
		await first.closeTab();

		const reopened = await Tab.open(context, { workspaceId: ws });
		// Before any network call at all: the row is local, which is the whole
		// promise of a local-first store surviving a tab.
		expect(await reopened.getTodo('t1')).toEqual({
			id: 't1',
			title: 'persisted',
			completed: false,
		});
		// And the clientId came out of the store rather than being minted afresh —
		// a new one would restart mutation ids from zero.
		const clientId = await reopened.clientId();
		expect(clientId).not.toBe('');

		await reopened.start();
		await reopened.syncNow();
		// It resumed from the stored cursor: a bootstrap here would mean the tab
		// re-downloaded a workspace it already held.
		expect(await totalCalls('Bootstrap')).toBe(bootstraps);
		expect(await reopened.todoIds()).toEqual([ 't1' ]);
	});

	test('writes made while a peer tab closes are not lost', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTodo('kept-1', 'from A');
		await b.putTodo('kept-2', 'from B');
		// B goes away with both writes still queued in the store they shared.
		await b.closeTab();

		expect(await a.todoIds()).toEqual([ 'kept-1', 'kept-2' ]);
		expect(await a.pendingCount()).toBe(2);

		await a.start();
		await a.syncNow();
		expect(await a.pendingCount()).toBe(0);
		expect(await a.rejections()).toEqual([]);
	});
});
