import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';

/**
 * Holds across tabs. A hold lives in the tab that took it, but every tab on
 * the account syncs the union of all tabs' scopes, so a row one tab holds is
 * in scope for all of them. What no single tab can see is the other half: the
 * row must STAY when a tab that also held it closes, and go when the last
 * holder lets it go. Node has no shared store for two clients, so only real
 * tabs over one IndexedDB can show that.
 */
test.describe('holds across tabs', () => {
	test('a row two tabs hold survives one of them closing', async ({ context }) => {
		const ws = freshWorkspace();

		// Seeded from its own account, so the scoped tabs below start empty.
		const seeder = await Tab.open(context, { workspaceId: ws, accountId: `seed-${ws}`, start: true });
		await seeder.putTextString('w1', 'hello');
		await seeder.syncNow();
		await seeder.closeTab();

		const account = `held-${ws}`;
		const scoped = {
			workspaceId: ws,
			accountId: account,
			subscriptions: [ 'todo' as const ],
			start: true,
		};
		const a = await Tab.open(context, scoped);
		const b = await Tab.open(context, scoped);
		await a.syncNow();
		expect(await a.hasWidget('w1')).toBe(false);

		await a.holdWidget('w1');
		expect(await a.hasWidget('w1')).toBe(true);
		// The store is shared, so B reads the row A brought in without holding it.
		expect(await b.hasWidget('w1')).toBe(true);

		const held = await b.holdWidget('w1');
		await a.closeTab();

		// B's own hold keeps the row in the union A left behind.
		await b.syncNow();
		expect(await b.hasWidget('w1')).toBe(true);

		await b.releaseHold(held);
		await expect.poll(() => b.hasWidget('w1')).toBe(false);
		await b.closeTab();
	});

	test('a row only a peer tab holds stays in scope while that peer lives', async ({ context }) => {
		const ws = freshWorkspace();
		const seeder = await Tab.open(context, { workspaceId: ws, accountId: `seed-${ws}`, start: true });
		await seeder.putTextString('w1', 'hello');
		await seeder.syncNow();
		await seeder.closeTab();

		const account = `peer-${ws}`;
		const scoped = {
			workspaceId: ws,
			accountId: account,
			subscriptions: [ 'todo' as const ],
			start: true,
		};
		const a = await Tab.open(context, scoped);
		const b = await Tab.open(context, scoped);
		await a.holdWidget('w1');

		// B never held the row; its own cycles still carry A's hold through the
		// union, so the row is not tombstoned away by a tab that did not ask for it.
		await b.syncNow();
		expect(await b.hasWidget('w1')).toBe(true);

		await a.closeTab();
		await expect.poll(() => b.hasWidget('w1')).toBe(false);
		await b.closeTab();
	});
});
