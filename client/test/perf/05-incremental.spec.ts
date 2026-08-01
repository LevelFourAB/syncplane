import { expect, test } from '@playwright/test';

import {
	BenchTab,
	freshAccount,
	issueKey,
	largestTier,
	serverWrite,
	tiers,
	wireDelta,
	wireStats,
	withWire,
} from './fixtures.js';
import { report } from './report.js';

/**
 * What it costs to catch up with changes somebody else made.
 *
 * Bootstrap happens once; this happens all day. Three shapes of it:
 *
 *   - **catch-up** — the tab was closed, or the laptop was shut, and the
 *     workspace moved on. The cost should track the change set, not the corpus.
 *   - **realtime** — the tab is open with a live stream, and one row changes
 *     somewhere else. This is the number a user reads as "is it live".
 *   - **rebase** — the tab has its own unpushed writes when the server's changes
 *     arrive, so the queue is replayed over the new state. The cost that grows
 *     with what the user did while disconnected.
 *
 * These specs WRITE to the corpus, which is why they are numbered after the
 * read-only ones. The server database is a per-run copy, so what they change is
 * discarded when the run ends — but within a run, a scenario that reads a
 * pristine corpus has to have gone first.
 *
 * Offline is real offline: Playwright's context.setOffline cuts the browser's
 * network, not the test process's, so the specs can keep driving the server
 * while the tab under measurement cannot reach it. That is also the only way to
 * build up a pending queue, since every local write schedules a push.
 */

const WINDOW = 500;

test.describe('offline catch-up', () => {
	for (const t of tiers()) {
		test(`1000 server changes — tier ${t.name}`, async ({ context }) => {
			const changes = Math.min(1000, t.issues);
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`catchup-${t.name}`),
				scope: 'window',
				windowLimit: WINDOW,
			});
			try {
				await tab.start();
				await context.setOffline(true);
				// Touched newest-first, so every change lands inside the recency
				// window the tab is holding. Changes outside a client's scope
				// are the cheap case and would flatter the number.
				await serverWrite(t.name, 'patch', changes);
				await context.setOffline(false);

				const before = await wireStats();
				const caught = await tab.syncNow();
				const wire = wireDelta(before, await wireStats());

				report({
					scenario: 'offline catch-up',
					tier: t.name,
					scope: 'window',
					metrics: {
						serverChanges: changes,
						catchUpMs: caught.wallMs,
						pulls: caught.pulls,
						pullChanges: caught.pullChanges,
						bytesDown: wire.bytesDown,
						gzipDown: wire.gzipDown,
						bytesPerChange: caught.pullChanges === 0
							? 0
							: Math.round(wire.bytesDown / caught.pullChanges),
						roundTrips: wire.roundTrips,
						msPerChange: caught.pullChanges === 0
							? 0
							: caught.wallMs / caught.pullChanges,
						bootstraps: caught.bootstraps,
					},
					wire: { ...wire, localMs: caught.wallMs, of: 'catchUpMs' },
				});
				// A catch-up that re-bootstrapped is a different (much more
				// expensive) event wearing this one's name.
				expect(caught.bootstraps).toBe(0);
			} finally {
				await tab.closeTab();
			}
		});
	}

	// How far behind you are, against a fixed corpus. Cheap to read as a curve:
	// the per-change cost should be flat, and a knee in it is the finding.
	for (const changes of [ 100, 1000, 10_000 ]) {
		test(`${changes} changes behind — tier ${largestTier().name}`, async ({ context }) => {
			const t = largestTier();
			test.skip(changes > t.issues, 'corpus has fewer issues than this sweep point');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`behind-${changes}`),
				scope: 'window',
				windowLimit: WINDOW,
			});
			try {
				await tab.start();
				await context.setOffline(true);
				await serverWrite(t.name, 'patch', changes);
				await context.setOffline(false);
				const { result: caught, wire } = await withWire(() => tab.syncNow());
				report({
					scenario: 'catch-up depth',
					tier: t.name,
					scope: 'window',
					metrics: {
						serverChanges: changes,
						catchUpMs: caught.wallMs,
						pulls: caught.pulls,
						pullChanges: caught.pullChanges,
						msPerChange: caught.pullChanges === 0
							? 0
							: caught.wallMs / caught.pullChanges,
						bytesDown: wire.bytesDown,
						gzipDown: wire.gzipDown,
						roundTrips: wire.roundTrips,
					},
					wire: { ...wire, localMs: caught.wallMs, of: 'catchUpMs' },
				});
			} finally {
				await tab.closeTab();
			}
		});
	}
});

test.describe('realtime', () => {
	test(`one change over the live stream — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		const tab = await BenchTab.open(context, {
			workspaceId: t.workspace,
			accountId: freshAccount('poke'),
			scope: 'window',
			windowLimit: WINDOW,
			// The whole point of this scenario: the change must arrive because
			// the server pushed it, not because a poll went looking.
			streamEnabled: true,
			pollIntervalMs: 0,
		});
		try {
			await tab.start();
			// The newest issue — inside the recency window, so the change is
			// one the client is subscribed to.
			const issueId = issueKey(t.issues - 1);
			const latencies: number[] = [];
			for (let round = 0; round < 5; round++) {
				await tab.resetEvents();
				// A marker unique to this round. Without one, every round after
				// the first would match the title the PREVIOUS round wrote and
				// report a latency of nothing.
				const marker = `poke${round}`;
				// The wait starts before the write, so the interval it measures
				// contains the HTTP call that triggers the change. That makes
				// this an over-estimate by roughly one local round trip — which
				// is the right direction to be wrong in, since the alternative
				// (start waiting afterwards) can miss a change that has already
				// arrived and call it instant.
				const waiting = tab.waitForTitle(issueId, marker, 30_000);
				await serverWrite(t.name, 'patch', 1, marker);
				latencies.push(await waiting);
			}
			latencies.sort((a, b) => a - b);
			report({
				scenario: 'realtime latency',
				tier: t.name,
				scope: 'window',
				metrics: {
					rounds: latencies.length,
					// Median rather than mean: one scheduling hiccup in five
					// rounds would move a mean and says nothing about the path.
					medianMs: latencies[Math.floor(latencies.length / 2)] ?? 0,
					minMs: latencies[0] ?? 0,
					maxMs: latencies[latencies.length - 1] ?? 0,
				},
			});
		} finally {
			await tab.closeTab();
		}
	});
});

test.describe('rebase', () => {
	for (const pending of [ 10, 100, 1000 ]) {
		test(`${pending} pending writes over 500 server changes`, async ({ context }) => {
			const t = largestTier();
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`rebase-${pending}`),
				scope: 'window',
				windowLimit: WINDOW,
				measureWrites: true,
			});
			try {
				await tab.start();
				await context.setOffline(true);
				await tab.resetWriteBytes();

				// Queued while offline, so they stay queued: online, each of
				// these would have been pushed as it was made. Written against
				// the newest issues, which is where the recency window is, so
				// the replay is over rows the client actually holds.
				const queueMs = await tab.queueWrites(pending, t.issues - pending);
				expect(await tab.pendingCount()).toBe(pending);
				await serverWrite(t.name, 'patch', 500);

				await context.setOffline(false);
				const { result: synced, wire } = await withWire(() => tab.syncNow());
				const bytes = await tab.writeBytes();

				report({
					scenario: 'rebase',
					tier: t.name,
					scope: 'window',
					metrics: {
						pending,
						serverChanges: 500,
						queueMs,
						queueMsPerWrite: queueMs / pending,
						syncMs: synced.wallMs,
						pushMs: synced.pushMs,
						pushMutations: synced.pushMutations,
						rebaseMs: synced.rebaseMs,
						rebaseAcked: synced.rebaseAcked,
						// What the queue still had to replay after the server's
						// page landed. The work that makes a rebase cost more
						// than a pull.
						rebaseReplayed: synced.rebaseReplayed,
						overlayBytes: bytes.overlay,
						pendingBytes: bytes.pending,
						// The one scenario where the UPLOAD direction dominates:
						// a queue drained after a long offline stretch is the
						// client talking, not listening.
						bytesUp: wire.bytesUp,
						gzipUp: wire.gzipUp,
						bytesDown: wire.bytesDown,
						roundTrips: wire.roundTrips,
					},
					wire: { ...wire, localMs: synced.wallMs, of: 'syncMs' },
				});
				// The queue drained: a rebase measured against writes that are
				// still pending measured only half the work.
				expect(await tab.pendingCount()).toBe(0);
			} finally {
				await tab.closeTab();
			}
		});
	}
});
