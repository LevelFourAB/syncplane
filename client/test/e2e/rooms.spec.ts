import { expect, test } from '@playwright/test';

import { activeCalls, freshWorkspace, Tab, totalCalls } from './fixtures.js';

/**
 * Rooms across real tabs: the ephemeral plane elected over Web Locks and shared
 * over BroadcastChannel, which is machinery Node has no equivalent of. The
 * integration tier covers the wire contract; what only this tier can show is
 * that N tabs of one account hold ONE Watch stream between them, and that
 * handing that stream over does not make everyone else see the account leave.
 *
 * The stream-count assertions are server-side (testserver's /debug/rpc) because
 * "how many connections does this account hold" is precisely what no tab can
 * see.
 */
test.describe('rooms', () => {
	test.beforeEach(async () => {
		// Streams from a previous spec's context are torn down asynchronously.
		await expect.poll(() => activeCalls('Watch')).toBe(0);
	});

	test('three tabs of one account hold one Watch stream between them', async ({ context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		await a.joinRoom('cursors');
		await expect.poll(() => activeCalls('Watch')).toBe(1);

		const b = await Tab.open(context, opts);
		const c = await Tab.open(context, opts);
		await b.joinRoom('cursors');
		await c.joinRoom('cursors');

		// Two more tabs must not mean two more connections. The leader does
		// reopen once per newcomer — the stream names the sessions it keeps
		// alive, so a new tab changes what it is opened with — but there is
		// never more than one open at a time.
		await a.page.waitForTimeout(3_000);
		expect(await activeCalls('Watch')).toBe(1);
	});

	// Re-announcements arrive from every tab every couple of seconds and say
	// nothing new; a leader that reopened on each of them would spend the
	// account's connection budget on churn.
	test('a settled account stops reopening its stream', async ({ context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		const b = await Tab.open(context, opts);
		await a.joinRoom('cursors');
		await b.joinRoom('cursors');
		await expect.poll(() => activeCalls('Watch')).toBe(1);

		// Let the union settle, then watch it stay settled across several
		// announce rounds.
		await a.page.waitForTimeout(3_000);
		const settled = await totalCalls('Watch');
		await a.page.waitForTimeout(5_000);
		expect(await totalCalls('Watch')).toBe(settled);
		expect(await activeCalls('Watch')).toBe(1);
	});

	// The union is what the leader watches, so a room only one follower is in
	// still has to reach the stream.
	test('a follower joining a new room reaches the leader', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		await a.joinRoom('shared');
		await expect.poll(() => activeCalls('Watch')).toBe(1);

		const b = await Tab.open(context, opts);
		const bRoom = await b.joinRoom('follower-only');
		await b.setPresence(bRoom, 'here');

		// An observer in a separate browser context is a separate account, so it
		// holds its own stream and sees only what really reached the server.
		const observerContext = await browser.newContext();
		try {
			const observer = await Tab.open(observerContext, {
				workspaceId: ws,
				accountId: `observer-${ws}`,
				start: true,
			});
			const room = await observer.joinRoom('follower-only');
			await expect.poll(() => observer.roomPeers(room).then((p) => p.length)).toBe(1);
			const peers = await observer.roomPeers(room);
			expect(peers[0]!.state).toBe('here');
			expect(peers[0]!.sessionId).toBe(await b.sessionId());
		} finally {
			await observerContext.close();
		}

		// Still one stream for the whole account, now covering both rooms.
		expect(await activeCalls('Watch')).toBe(1);
	});

	test('every tab of an account sees the same roster through the leader', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		const b = await Tab.open(context, opts);
		const roomA = await a.joinRoom('cursors');
		const roomB = await b.joinRoom('cursors');
		await expect.poll(() => activeCalls('Watch')).toBe(1);

		// Somebody else entirely publishes; both tabs must see it, including the
		// one that is not holding the stream.
		const outsiderContext = await browser.newContext();
		try {
			const outsider = await Tab.open(outsiderContext, {
				workspaceId: ws,
				accountId: `outsider-${ws}`,
				start: true,
			});
			const room = await outsider.joinRoom('cursors');
			await outsider.setPresence(room, 'from-outside');

			for (const [ tab, handle ] of [ [ a, roomA ], [ b, roomB ] ] as const) {
				await expect.poll(async () => {
					const peers = await tab.roomPeers(handle);
					return peers.find((p) => p.state === 'from-outside') !== undefined;
				}).toBe(true);
			}
		} finally {
			await outsiderContext.close();
		}
	});

	// A handover is invisible to everyone else by design: the outgoing leader
	// publishes no departure, and the incoming one's stream lands inside the
	// expiry window.
	test('closing the leader tab hands the stream over without anyone leaving', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		const b = await Tab.open(context, opts);
		const roomA = await a.joinRoom('cursors');
		const roomB = await b.joinRoom('cursors');
		await a.setPresence(roomA, 'a');
		await b.setPresence(roomB, 'b');
		await expect.poll(() => activeCalls('Watch')).toBe(1);

		const observerContext = await browser.newContext();
		try {
			const observer = await Tab.open(observerContext, {
				workspaceId: ws,
				accountId: `observer-${ws}`,
				start: true,
			});
			// The observer only watches — it publishes nothing, so it is not in
			// the roster itself. Both tabs of the other account are, including
			// the one that is not holding the stream: the leader names its
			// followers' sessions on the request, which is what keeps their
			// records from expiring under it.
			const room = await observer.joinRoom('cursors');
			await expect.poll(
				() => observer.roomPeers(room).then((p) => p.map((x) => x.state).sort()),
				{ timeout: 20_000 },
			).toEqual([ 'a', 'b' ]);

			// Closing one leaves the account with a stream — handed to the other
			// tab — and the observer with the survivor.
			await a.page.close();
			await expect.poll(() => activeCalls('Watch'), { timeout: 15_000 }).toBe(2);
			await expect.poll(
				() => observer.roomPeers(room).then((p) => p.map((x) => x.state)),
				{ timeout: 15_000 },
			).toEqual([ 'b' ]);
		} finally {
			await observerContext.close();
		}
	});

	test('a departure reaches the other tabs when a room is left', async ({ context }) => {
		const ws = freshWorkspace();
		const opts = { workspaceId: ws, start: true };
		const a = await Tab.open(context, opts);
		const roomA = await a.joinRoom('cursors');
		await a.setPresence(roomA, 'a');
		await expect.poll(() => a.roomPeers(roomA).then((p) => p.length)).toBe(1);

		await a.leaveRoom(roomA);
		// The last room on the account closed, so its Watch stream ends too.
		await expect.poll(() => activeCalls('Watch'), { timeout: 15_000 }).toBe(0);
	});

	test('a broadcast reaches a watcher in another browser', async ({ browser, context }) => {
		const ws = freshWorkspace();
		const a = await Tab.open(context, { workspaceId: ws, start: true });
		const roomA = await a.joinRoom('chat');
		await a.setPresence(roomA, 'a');

		const listenerContext = await browser.newContext();
		try {
			const listener = await Tab.open(listenerContext, {
				workspaceId: ws,
				accountId: `listener-${ws}`,
				start: true,
			});
			const room = await listener.joinRoom('chat');
			await listener.setPresence(room, 'l');
			// A broadcast is at-most-once, so it is worth sending only once both
			// streams are demonstrably up.
			await expect.poll(() => a.roomPeers(roomA).then((p) => p.length)).toBe(2);

			await a.sendRoomMessage(roomA, 'hello');
			await expect.poll(() => listener.roomMessages(room)).toEqual([ 'hello' ]);
		} finally {
			await listenerContext.close();
		}
	});
});
