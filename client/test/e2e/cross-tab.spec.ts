import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Two tabs, one store. Watcher notification is per-tab in-memory while the data
 * lives in a shared IndexedDB, so the only thing that keeps a peer tab's views
 * fresh is the changeshare BroadcastChannel. Unit tests drive that channel
 * against in-process stand-ins; here it is the real one, between real tabs.
 */
test.describe('cross-tab change sharing', () => {
	test("a write in one tab reaches the other tab's watchers with no server involved", async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		// Same account (defaulted from the workspace) means one IndexedDB database,
		// and a clientId is a property of the store — so the tabs must agree on it.
		expect(await b.clientId()).toBe(await a.clientId());

		await b.watchTodos();
		const pushesBefore = await totalCalls('Push');

		await a.putTodo('t1', 'from A');

		await expect.poll(() => b.watched()).toContain('t1');
		expect(await b.getTodo('t1')).toEqual({ id: 't1', title: 'from A', completed: false });
		// Neither tab has started syncing: the row travelled through the shared
		// store and the broadcast, not through the server.
		expect(await totalCalls('Push')).toBe(pushesBefore);
	});

	test('a peer tab sees later edits to a row it is already watching', async ({ context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTodo('t1', 'first');
		await b.watchTodos();
		await expect.poll(() => b.watched()).toContain('t1');

		await a.patchTodo('t1', { title: 'edited', completed: true });

		await expect.poll(() => b.getTodo('t1')).toEqual({
			id: 't1',
			title: 'edited',
			completed: true,
		});
	});

	test('two tabs cold-bootstrapping one workspace end on one whole snapshot', async ({ context }) => {
		// A cold bootstrap writes its rows straight into confirmed state, so two
		// tabs racing one cannot both be allowed to write: one claims the
		// generation, the other stops mid-stream and leaves the workspace to it.
		// Node has no shared lock manager and no shared IndexedDB, so the unit tier
		// can only drive that with a stand-in — this is the real thing, two tabs
		// over one store.
		const ws = freshWorkspace();
		const ids = Array.from({ length: 120 }, (_, i) => `t${String(i).padStart(3, '0')}`);

		// Seeded from its own account, so the two tabs below start with a store
		// that has never held this workspace.
		const seeder = await Tab.open(context, { workspaceId: ws, accountId: `seed-${ws}`, start: true });
		await seeder.putTodos(ids);
		await seeder.syncNow();
		await seeder.closeTab();

		const account = `race-${ws}`;
		const a = await Tab.open(context, { workspaceId: ws, accountId: account, start: true });
		const b = await Tab.open(context, { workspaceId: ws, accountId: account, start: true });
		await Promise.all([ a.syncNow(), b.syncNow() ]);

		// Whichever bootstrap won, the shared store holds that snapshot entire:
		// no row of the loser's stream left behind, none of the winner's missing.
		await expect.poll(() => a.todoIds().then((got) => got.sort())).toEqual(ids);
		expect((await b.todoIds()).sort()).toEqual(ids);

		// And it is installed, not merely present: a workspace left without a
		// cursor re-bootstraps on the next cycle, so a second sync that downloads
		// nothing is what says the losing tab did not leave the store unusable.
		const bootstraps = await totalCalls('Bootstrap');
		await a.syncNow();
		expect(await totalCalls('Bootstrap')).toBe(bootstraps);
	});

	test('tabs on different accounts do not see each other, same workspace or not', async ({ context }) => {
		// The channel and the store are scoped to the account, so a second tab
		// with its own accountId is a separate app that happens to sync the same
		// workspace — it must learn nothing locally, only through the server.
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws });
		const other = await Tab.open(context, { workspaceId: ws, accountId: `other-${ws}` });

		await other.watchTodos();
		await a.putTodo('t1', 'from A');
		await expect.poll(() => a.todoIds()).toEqual([ 't1' ]);

		expect(await other.todoIds()).toEqual([]);
		expect(await other.watched()).toEqual([]);
	});
});
