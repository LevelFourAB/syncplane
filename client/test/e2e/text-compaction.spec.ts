import { expect, test } from '@playwright/test';

import { collectGarbage, freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Server-side compaction of a collaborative text field, with an editor open.
 *
 * Compaction purges tombstones the server no longer needs and records a floor
 * per (row, field); a write measured against that floor either skips the
 * purged state or is refused as stale. The single-client shape of that loop is
 * covered by client/test/integration/text-compaction.test.ts. What is only
 * observable here is a LIVE HANDLE surviving it: a handle open across a
 * compaction anchors new typing to items the server has dropped, so it has to
 * notice the purge and rebuild onto the compacted document — while a second tab
 * of the same app writes through the store they share.
 *
 * The pass is triggered explicitly (/debug/gc) rather than waited for, and the
 * testserver's delta-log retention is a millisecond, so a row written a moment
 * ago is already quiet enough to compact.
 */
test.describe('collaborative text compaction', () => {
	test('a live handle rebuilds onto the compacted document and keeps typing', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, start: true });

		await a.putTextString('d1', 'hello world');
		await a.syncNow();
		const h = await a.openText('d1');
		await a.setTextString(h, 'hello');
		await a.syncNow();
		expect(await a.storedTombstones('d1'), 'the deleted run is still carried').toBe(6);

		await collectGarbage(ws);
		await a.syncNow();
		const bootstrapsAfterPurge = await totalCalls('Bootstrap');

		// The handle's document was built before the purge. It notices the row it
		// holds is no longer an ancestor of the server's, rebuilds on the
		// compacted state and re-applies what it had — with nothing visible
		// changing for whoever is typing.
		expect(await a.textValue(h)).toBe('hello');
		await expect.poll(() => a.storedTombstones('d1')).toBe(0);

		await a.setTextString(h, 'hello there');
		await a.syncNow();
		expect(await a.rejections()).toEqual([]);
		// A handle that had merged instead of rebuilding would still be anchoring
		// on purged items, and this write would have been refused and recovered
		// through a resync. It was not: the heal happened before the write.
		expect(await totalCalls('Bootstrap')).toBe(bootstrapsAfterPurge);

		const cold = await browser.newContext();
		try {
			const c = await Tab.open(cold, {
				workspaceId: ws,
				accountId: `cold-${ws}`,
				start: true,
			});
			await c.syncNow();
			expect(await c.storedText('d1')).toBe('hello there');
			expect(await c.storedTombstones('d1'), 'the purge held').toBe(0);
		} finally {
			await cold.close();
		}
	});

	test('an edit built on purged state is recovered, not lost', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, start: true });
		const b = await Tab.open(context, { workspaceId: ws });

		await a.putTextString('d1', 'hello world');
		await a.syncNow();
		const h = await a.openText('d1');
		await a.setTextString(h, 'hello');
		await a.syncNow();

		await collectGarbage(ws);

		const bootstrapsBefore = await totalCalls('Bootstrap');

		// b writes without pulling first, so its document still carries the
		// tombstones the server dropped, and its append attaches to one of them.
		// The server refuses the write rather than resurrecting them.
		await b.patchTextString('d1', 'hello again');
		// First cycle: the push is refused, the visible text is captured and a
		// resync armed. The next ones land the resync and re-apply the edit
		// against the row the server actually holds.
		await a.syncNow();
		await a.syncNow();
		await a.syncNow();

		// The rejection really happened: recovery re-reads the workspace from
		// scratch, which nothing else in this test would ask for.
		expect(await totalCalls('Bootstrap') - bootstrapsBefore).toBe(1);
		await expect.poll(() => a.storedText('d1')).toBe('hello again');
		// The recovery is invisible to the app: the edit was restored, not
		// reported as a rejected mutation.
		expect(await a.rejections()).toEqual([]);
		expect(await b.rejections()).toEqual([]);
		// And the handle that was open throughout agrees with the store.
		await expect.poll(() => a.textValue(h)).toBe('hello again');

		const cold = await browser.newContext();
		try {
			const c = await Tab.open(cold, {
				workspaceId: ws,
				accountId: `cold-${ws}`,
				start: true,
			});
			await c.syncNow();
			expect(await c.storedText('d1')).toBe('hello again');
			expect(await c.storedTombstones('d1')).toBe(0);
		} finally {
			await cold.close();
		}
	});
});
