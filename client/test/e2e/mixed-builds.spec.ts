import { expect, test } from '@playwright/test';

import { freshWorkspace, Tab } from './fixtures.js';

/**
 * Two tabs of DIFFERENT builds sharing one store — an SPA left open across a
 * deploy. The pending queue is one per workspace, not one per tab, so the older
 * build rebases over mutations naming entities it has never heard of. Node
 * cannot stage this honestly: it takes two real tabs on one IndexedDB to have
 * one build's queue entry replayed by the other build's registry.
 *
 * The tab opened with `entities: [ 'todo' ]` IS the older build; the default tab
 * registers the widget entity as well.
 */
test.describe('mixed builds sharing a queue', () => {
	test("an older build bootstraps past a newer build's queued mutation", async ({ context }) => {
		const ws = freshWorkspace();

		// The newer build writes a widget and a todo, and never syncs — so both are
		// still queued when the other tab starts.
		const newer = await Tab.open(context, { workspaceId: ws });
		await newer.putTextString('w1', 'hello');
		await newer.putTodo('t1', 'from the newer build');
		expect(await newer.pendingCount()).toBe(2);

		// The older build, on a store with no cursor: the first thing it does is
		// bootstrap, and the swap replays the whole shared queue. Replay that threw
		// on the widget mutation would wedge this workspace outright — a bootstrap
		// runs before the push, so nothing would ever drain the mutation.
		const older = await Tab.open(context, {
			workspaceId: ws,
			entities: [ 'todo' ],
			start: true,
		});
		await older.syncNow();

		expect(await older.pendingCount()).toBe(0);
		expect(await older.rejections()).toEqual([]);
		expect(await older.todoIds()).toEqual([ 't1' ]);

		// The older tab's rebase cleared its own overlay only, so the newer tab's
		// optimistic widget never blinked — even though the tab that rebased could
		// neither read the row nor replay the mutation behind it.
		expect(await newer.storedText('w1')).toBe('hello');

		// And the widget really did reach the server: the older build pushed bytes
		// it could not decode, so the newer tab gets its row back from a sync of
		// its own.
		await newer.start();
		await newer.syncNow();
		expect(await newer.storedText('w1')).toBe('hello');
	});

	test('the older build keeps writing and syncing afterwards', async ({ context }) => {
		const ws = freshWorkspace();
		const newer = await Tab.open(context, { workspaceId: ws });
		await newer.putTextString('w1', 'hello');

		const older = await Tab.open(context, {
			workspaceId: ws,
			entities: [ 'todo' ],
			start: true,
		});
		await older.syncNow();

		// A pull rebase after the bootstrap, with the older tab's own mutation in
		// the queue: the entity it cannot register must not cost it its own sync.
		await older.putTodo('t1', 'mine');
		await newer.putTextString('w2', 'more');
		await older.syncNow();

		expect(await older.pendingCount()).toBe(0);
		expect(await older.getTodo('t1')).toEqual({ id: 't1', title: 'mine', completed: false });
	});
});
