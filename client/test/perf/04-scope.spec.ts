import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers, wireDelta, wireStats } from './fixtures.js';
import { report } from './report.js';

/**
 * What it costs to change what you are looking at.
 *
 * Scrolling a list, opening another project, joining a channel — in a
 * partial-sync app these are all the same operation: swap the subscription set
 * and wait. The engine delta-resyncs the visible-set difference rather than
 * re-bootstrapping, and this is where that claim is priced.
 *
 * Three directions, because they are not symmetric:
 *
 *   - **widening** a window has to deliver everything that entered,
 *   - **narrowing** it has to tombstone everything that left, and rows leaving
 *     is the direction people forget costs anything at all,
 *   - **switching** between two disjoint predicates does both at once.
 *
 * `bootstraps: 0` is asserted rather than reported alone: a resync that fell
 * back to a full re-bootstrap would still produce a plausible duration, and only
 * the event count says which of the two happened.
 *
 * Read-only against the server — every write here is the client changing its
 * own mind.
 */

test.describe('scope change', () => {
	for (const t of tiers()) {
		test(`widen and narrow a window — tier ${t.name}`, async ({ context }) => {
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`scope-${t.name}`),
				scope: 'window',
				windowLimit: 200,
			});
			try {
				await tab.start();
				const started = await tab.counts();

				let before = await wireStats();
				const widened = await tab.setScope('window', { windowLimit: 800 });
				let after = await wireStats();
				const widenWire = wireDelta(before, after);
				const wideCounts = await tab.counts();

				before = after;
				const narrowed = await tab.setScope('window', { windowLimit: 200 });
				after = await wireStats();
				const narrowWire = wireDelta(before, after);
				const narrowCounts = await tab.counts();

				report({
					scenario: 'scope change',
					tier: t.name,
					scope: 'window 200→800→200',
					metrics: {
						widenMs: widened.wallMs,
						widenChanges: widened.pullChanges,
						widenBytesDown: widenWire.bytesDown,
						widenGzipDown: widenWire.gzipDown,
						widenBootstraps: widened.bootstraps,
						narrowMs: narrowed.wallMs,
						// Rows leaving arrive as tombstones, so a narrowing
						// resync carries changes too — that it is not free is
						// the number worth having.
						narrowChanges: narrowed.pullChanges,
						narrowBytesDown: narrowWire.bytesDown,
						narrowGzipDown: narrowWire.gzipDown,
						narrowBootstraps: narrowed.bootstraps,
						issuesAt200: started.issue ?? 0,
						issuesAt800: wideCounts.issue ?? 0,
						issuesBack: narrowCounts.issue ?? 0,
					},
					// The widening: the direction that carries rows, and the one
					// a user waits on when they scroll.
					wire: { ...widenWire, localMs: widened.wallMs, of: 'widenMs' },
				});

				expect(widened.bootstraps).toBe(0);
				expect(narrowed.bootstraps).toBe(0);
				// The window is maintained exactly: rows that merely sank below
				// the boundary leave the local store. Without this, a narrowing
				// that quietly kept its rows would look like a very fast resync.
				expect(narrowCounts.issue ?? 0).toBeLessThanOrEqual(200);
			} finally {
				await tab.closeTab();
			}
		});
	}

	for (const t of tiers()) {
		test(`switch project — tier ${t.name}`, async ({ context }) => {
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`switch-${t.name}`),
				scope: 'project',
				projectId: 'project-00000000',
			});
			try {
				await tab.start();
				const before = await wireStats();
				// Disjoint from the first: every row held has to go, and every
				// row wanted has to arrive. The worst case a scope swap has.
				const switched = await tab.setScope('project', { projectId: 'project-00000001' });
				const wire = wireDelta(before, await wireStats());
				report({
					scenario: 'scope change',
					tier: t.name,
					scope: 'project→project',
					metrics: {
						switchMs: switched.wallMs,
						changes: switched.pullChanges,
						bytesDown: wire.bytesDown,
						gzipDown: wire.gzipDown,
						roundTrips: wire.roundTrips,
						bootstraps: switched.bootstraps,
						issues: (await tab.counts()).issue ?? 0,
					},
					wire: { ...wire, localMs: switched.wallMs, of: 'switchMs' },
				});
				expect(switched.bootstraps).toBe(0);
			} finally {
				await tab.closeTab();
			}
		});
	}
});
