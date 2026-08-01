import { expect, test } from '@playwright/test';

import { activeCalls, freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Stream leadership: however many tabs an account has open, exactly one of them
 * holds the Subscribe stream, and the rest see its rows through the shared
 * store. Elected over Web Locks, so this is the first tier that runs it at all.
 *
 * The assertions are server-side (testserver's /debug/rpc) because "how many
 * connections does this account hold" is precisely what no tab can see.
 */
test.describe('stream leadership', () => {
	test.beforeEach(async () => {
		// Streams from a previous spec's context are torn down asynchronously.
		await expect.poll(() => activeCalls('Subscribe')).toBe(0);
	});

	test('three tabs of one account hold one stream between them', async ({ context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, streamEnabled: true, start: true };
		const a = await Tab.open(context, opts);
		await expect.poll(() => activeCalls('Subscribe')).toBe(1);
		const opened = await totalCalls('Subscribe');

		await Tab.open(context, opts);
		await Tab.open(context, opts);

		// Two more tabs must not mean two more connections. `active` alone would
		// be satisfied the instant it is read, so give the newcomers time to do
		// the wrong thing and then assert that no second stream was ever OPENED.
		await a.page.waitForTimeout(1_000);
		expect(await activeCalls('Subscribe')).toBe(1);
		expect(await totalCalls('Subscribe')).toBe(opened);
		expect(await a.status()).toMatchObject({ online: true });
	});

	test('a separate account gets its own stream', async ({ context }) => {
		// Leadership is scoped to the account, not the origin: two apps in one
		// browser must not starve each other down to a single shared stream.
		const ws = freshWorkspace();
		await Tab.open(context, { workspaceId: ws, streamEnabled: true, start: true });
		await Tab.open(context, {
			workspaceId: ws,
			accountId: `other-${ws}`,
			streamEnabled: true,
			start: true,
		});

		await expect.poll(() => activeCalls('Subscribe')).toBe(2);
	});

	test('a follower tab sees rows the leader pulled down', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, streamEnabled: true, start: true };
		const a = await Tab.open(context, opts);
		const b = await Tab.open(context, opts);
		await expect.poll(() => activeCalls('Subscribe')).toBe(1);

		await a.watchTodos();
		await b.watchTodos();

		// A write from somewhere else entirely: it reaches whichever tab leads over
		// the stream, and the other tab through the shared store.
		const writerContext = await browser.newContext();
		try {
			const writer = await Tab.open(writerContext, {
				workspaceId: ws,
				accountId: `writer-${ws}`,
				start: true,
			});
			await writer.putTodo('remote-1', 'from elsewhere');
			await writer.syncNow();
		} finally {
			await writerContext.close();
		}

		await expect.poll(() => a.watched()).toContain('remote-1');
		await expect.poll(() => b.watched()).toContain('remote-1');
	});
});
