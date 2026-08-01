import { expect, test } from '@playwright/test';

import {
	BenchTab,
	collectGarbage,
	freshAccount,
	largestTier,
	serverWrite,
	wireDelta,
	wireStats,
} from './fixtures.js';
import { report } from './report.js';

/**
 * The two costs that only show up in situations nobody plans for.
 *
 * **Falling behind the horizon.** Tombstones are collected eventually, and a
 * client whose cursor predates the collection cannot be told what it missed —
 * so it re-bootstraps. It is transparent, which is exactly why it is worth a
 * number: an app that goes quiet for a week pays a cold start it never asked
 * for, and nothing in the UI distinguishes it from a slow network.
 *
 * **A second tab.** Exactly one tab per account holds the stream; the rest read
 * its results out of the shared store. So the second tab should cost almost
 * nothing at the server — no bootstrap of its own, no second stream — and the
 * measurement is whether "almost nothing" is true.
 *
 * Last in the run order: this file deletes rows and advances the GC horizon,
 * which is the one change no later scenario could read past.
 */

const WINDOW = 500;

/** Calls of one Connect method between two /debug/stats snapshots. */
function callsOf(
	before: Awaited<ReturnType<typeof wireStats>>,
	after: Awaited<ReturnType<typeof wireStats>>,
	method: string,
): number {
	return (after.total[method] ?? 0) - (before.total[method] ?? 0);
}

test.describe('forced re-bootstrap', () => {
	test(`horizon overtakes a client — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		const tab = await BenchTab.open(context, {
			workspaceId: t.workspace,
			accountId: freshAccount('horizon'),
			scope: 'window',
			windowLimit: WINDOW,
		});
		try {
			const initial = await tab.start();
			await context.setOffline(true);

			// Deletes leave tombstones; the GC pass collects them. The client's
			// cursor now points before a deletion the server can no longer
			// describe, which is the definition of behind the horizon.
			await serverWrite(t.name, 'delete', 200);
			await collectGarbage(t.name);

			await context.setOffline(false);
			const before = await wireStats();
			const recovered = await tab.syncNow();
			const after = await wireStats();
			const recoveredWire = wireDelta(before, after);

			report({
				scenario: 'forced re-bootstrap',
				tier: t.name,
				scope: 'window',
				metrics: {
					firstBootstrapMs: initial.wallMs,
					recoveryMs: recovered.wallMs,
					// The ratio these two numbers make is the real finding: a
					// recovery that costs what the original cold start cost is
					// the honest expectation, and anything worse is a bug.
					resets: recovered.resets,
					bootstraps: recovered.bootstraps,
					bootstrapRows: recovered.bootstrapRows,
					bootstrapCalls: callsOf(before, after, 'Bootstrap'),
					localIssues: (await tab.counts()).issue ?? 0,
					bytesDown: recoveredWire.bytesDown,
					gzipDown: recoveredWire.gzipDown,
					roundTrips: recoveredWire.roundTrips,
				},
				// The expensive path priced on a real link: a client that fell
				// behind the horizon downloads its whole scope again, and on a
				// slow connection that is the difference between a hiccup and a
				// visibly broken app.
				wire: { ...recoveredWire, localMs: recovered.wallMs, of: 'recoveryMs' },
			});
			// If the horizon did not actually overtake the client, this
			// scenario measured an ordinary pull and the row above is
			// mislabelled — which is worse than a failure.
			expect(recovered.bootstraps + recovered.resets).toBeGreaterThan(0);
		} finally {
			await tab.closeTab();
		}
	});
});

test.describe('second tab', () => {
	test(`follower pays nothing at the server — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		// One account, two tabs: same origin, same IndexedDB, same Web Locks —
		// which is what makes them two tabs of one app rather than two clients.
		const account = freshAccount('tabs');
		const open = () =>
			BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: account,
				scope: 'window',
				windowLimit: WINDOW,
				streamEnabled: true,
			});

		const leader = await open();
		try {
			const first = await leader.start();

			const before = await wireStats();
			const follower = await open();
			try {
				const second = await follower.startWithFirstRead(20);
				const after = await wireStats();
				const followerWire = wireDelta(before, after);

				report({
					scenario: 'second tab',
					tier: t.name,
					scope: 'window',
					metrics: {
						firstTabMs: first.wallMs,
						firstTabBootstrapRows: first.bootstrapRows,
						secondTabFirstReadMs: second.firstReadMs,
						secondTabCompleteMs: second.completeMs,
						// Both expected to be zero: the follower reads the store
						// the leader already filled, and leadership is held by
						// one tab per account.
						secondTabBootstraps: second.summary.bootstraps,
						extraBootstrapCalls: callsOf(before, after, 'Bootstrap'),
						extraSubscribeCalls: callsOf(before, after, 'Subscribe'),
						extraPullCalls: callsOf(before, after, 'Pull'),
						// Near zero is the claim. On a slow link it is also the
						// difference between opening a second tab being free and
						// it costing another whole bootstrap.
						bytesDown: followerWire.bytesDown,
						gzipDown: followerWire.gzipDown,
						roundTrips: followerWire.roundTrips,
					},
					wire: {
						...followerWire,
						localMs: second.completeMs,
						of: 'secondTabCompleteMs',
					},
				});
				expect(second.summary.bootstraps).toBe(0);
				expect(callsOf(before, after, 'Bootstrap')).toBe(0);
			} finally {
				await follower.closeTab();
			}
		} finally {
			await leader.closeTab();
		}
	});
});
