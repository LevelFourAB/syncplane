import { expect, test } from '@playwright/test';

import { activeCalls, freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Leadership is visibility-aware: a tab holds the stream only while it is
 * visible, so a browser that suspends a background tab can never strand the
 * lock. The specs drive `document.visibilityState` and `visibilitychange`
 * directly — the pair the client reads — because no engine offers a portable
 * way to put a Playwright page into a real background state.
 *
 * Handover is asserted on the TOTAL Subscribe count, not the active one: after
 * a handover the account still holds one stream, so "active is 1" is equally
 * true if nothing happened. A second stream having been opened is the part that
 * only a real handover explains. To make the count attributable, the tab under
 * test is opened alone first, so it is unambiguously the leader.
 */
test.describe('leadership handover', () => {
	test.beforeEach(async () => {
		await expect.poll(() => activeCalls('Subscribe')).toBe(0);
	});

	test('hiding every tab releases the stream; showing one takes it back', async ({ context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, streamEnabled: true, start: true };
		const a = await Tab.open(context, opts);
		const b = await Tab.open(context, opts);
		await expect.poll(() => activeCalls('Subscribe')).toBe(1);

		// With no visible tab left, the account drops to the poll floor rather
		// than leaving a term parked in a tab the browser may freeze. The release
		// waits out the hide grace, hence the longer poll.
		await a.setVisibility('hidden');
		await b.setVisibility('hidden');
		await expect.poll(() => activeCalls('Subscribe'), { timeout: 20_000 }).toBe(0);

		// A tab coming back re-contends and wins, with no reload and no new client.
		await b.setVisibility('visible');
		await expect.poll(() => activeCalls('Subscribe')).toBe(1);
	});

	test('a hidden leader hands over to a visible peer, and both stay live', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, streamEnabled: true, start: true };
		// A is alone when it wins the lock, so it is the leader by construction.
		const a = await Tab.open(context, opts);
		await expect.poll(() => activeCalls('Subscribe')).toBe(1);
		const openedByA = await totalCalls('Subscribe');

		const b = await Tab.open(context, opts);
		await a.watchTodos();
		await b.watchTodos();

		// Hiding the leader ends its term; B has been waiting on the lock since it
		// opened, so it wins and opens a stream of its own — one more in total,
		// still one at a time.
		await a.setVisibility('hidden');
		await expect.poll(() => totalCalls('Subscribe'), { timeout: 20_000 })
			.toBe(openedByA + 1);
		expect(await activeCalls('Subscribe')).toBe(1);

		const writerContext = await browser.newContext();
		try {
			const writer = await Tab.open(writerContext, {
				workspaceId: ws,
				accountId: `writer-${ws}`,
				start: true,
			});
			await writer.putTodo('after-handover', 'written while A is hidden');
			await writer.syncNow();
		} finally {
			await writerContext.close();
		}

		// The visible tab is genuinely streaming...
		await expect.poll(() => b.watched()).toContain('after-handover');
		// ...and the hidden one, which holds no stream at all now, still keeps up
		// through the shared store.
		await expect.poll(() => a.watched()).toContain('after-handover');
	});
});
