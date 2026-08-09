import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, largestTier, serverWrite, type Tier } from './fixtures.js';
import type { WatchSpec } from './harness-api.js';
import { report } from './report.js';

/**
 * What holding live queries costs while changes arrive.
 *
 * A real app is not one query per render — it is a screenful of watches held
 * open while the coordinator applies other people's writes. Every applied
 * commit fans out through the watcher hub, and that maintenance cost lands
 * inside the sync cycle where nothing else itemizes it. So the measurement is
 * marginal: the same catch-up cycle at 0, 10 and 50 open watches, and the
 * difference is what the watches cost.
 *
 * Deliveries are counted per watch class. A callback only fires when the
 * delivered page MOVED, so the `flat` watches — an entity these writes never
 * touch — are asserted to zero deliveries: the receipt that maintenance is
 * row-granular at the delivery level. Work that ended in "nothing changed"
 * cannot fire a callback and shows up only in the marginal wall time, which is
 * exactly why the wall time is the number.
 *
 * Writes to the corpus, hence the number: after the read-only specs.
 */

const WINDOW = 500;
const CHANGES = 200;
const WATCH_COUNTS = [ 0, 10, 50 ];

/**
 * `count` watches in a fixed rotation of the four shapes, boards spread over
 * distinct projects and labels over distinct labels — fifty watches on one
 * board would all share the hub's maintained result and flatter the number.
 */
function mixedWatches(t: Tier, count: number): WatchSpec[] {
	const key = (prefix: string, i: number) => `${prefix}-${String(i).padStart(8, '0')}`;
	const specs: WatchSpec[] = [];
	for (let i = 0; specs.length < count; i++) {
		specs.push({ kind: 'board', projectId: key('project', i % t.projects), limit: 50 });
		if (specs.length < count) specs.push({ kind: 'triage', limit: 50 });
		if (specs.length < count) {
			specs.push({ kind: 'byLabel', label: key('label', i % t.labels), limit: 50 });
		}
		if (specs.length < count) specs.push({ kind: 'flat' });
	}
	return specs;
}

test.describe('live queries', () => {
	test(`catch-up under open watches — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		const tab = await BenchTab.open(context, {
			workspaceId: t.workspace,
			accountId: freshAccount(`watches-${t.name}`),
			scope: 'window',
			windowLimit: WINDOW,
		});
		try {
			await tab.start();
			await tab.waitForIndexes();
			// Changes other specs left behind would land in the first round's
			// pull and break the rounds' comparability.
			await tab.syncNow();
			for (const count of WATCH_COUNTS) {
				const specs = mixedWatches(t, count);
				if (count > 0) await tab.openWatches(specs);
				await tab.resetWatchStats();
				await tab.resetEvents();
				// Newest-first, so the changes land inside the window every watch
				// is looking at.
				await serverWrite(t.name, 'patch', CHANGES, `w${count}`);
				const sync = await tab.syncNow();
				const stats = await tab.watchStats();
				const delivered = (kind: WatchSpec['kind']) =>
					stats.reduce((sum, s, i) => specs[i]?.kind === kind ? sum + s.callbacks : sum, 0);
				const flats = delivered('flat');
				if (count > 0) {
					expect(flats, 'a watch on an untouched entity must not deliver').toBe(0);
				}
				report({
					scenario: 'catch-up under watches',
					tier: t.name,
					scope: 'window',
					metrics: {
						watches: count,
						changes: sync.pullChanges,
						pulls: sync.pulls,
						wallMs: sync.wallMs,
						pullMs: sync.pullMs,
						deliveries: stats.reduce((sum, s) => sum + s.callbacks, 0),
						boardDeliveries: delivered('board'),
						triageDeliveries: delivered('triage'),
						labelDeliveries: delivered('byLabel'),
					},
				});
				await tab.closeWatches();
			}
		} finally {
			await tab.closeTab();
		}
	});

	test(`watch-observed change latency — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		const tab = await BenchTab.open(context, {
			workspaceId: t.workspace,
			accountId: freshAccount(`watch-latency-${t.name}`),
			scope: 'window',
			windowLimit: WINDOW,
			streamEnabled: true,
		});
		try {
			await tab.start();
			await tab.waitForIndexes();
			// An entity watch fires on any issue change, so it is the waiter that
			// needs no knowledge of which row the write hit. 05's waitForTitle
			// polls get() for the same moment; the difference between the two
			// numbers is what the watch pipeline adds over polling.
			await tab.openWatches([ { kind: 'issues' }, { kind: 'flat' } ]);
			const rounds: number[] = [];
			for (let round = 0; round < 3; round++) {
				await tab.resetWatchStats();
				await serverWrite(t.name, 'patch', 1, `live${round}`);
				rounds.push(await tab.waitForWatch(0, 1));
			}
			const flat = (await tab.watchStats())[1];
			expect(flat?.callbacks, 'a watch on an untouched entity must not deliver').toBe(0);
			report({
				scenario: 'watch latency',
				tier: t.name,
				scope: 'window',
				metrics: {
					latency1Ms: rounds[0] ?? 0,
					latency2Ms: rounds[1] ?? 0,
					latency3Ms: rounds[2] ?? 0,
				},
			});
		} finally {
			await tab.closeTab();
		}
	});
});
