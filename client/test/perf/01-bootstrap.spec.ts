import type { BrowserContext } from '@playwright/test';
import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, issueKey, tier, tiers, wireDelta, wireStats } from './fixtures.js';
import { report } from './report.js';

/**
 * What it costs a cold client to become usable.
 *
 * The suite's headline pair of numbers. `firstReadMs` is when a reader could
 * paint something; `completeMs` is when the local store provably held the whole
 * scope. An app's felt latency is the first; its correctness guarantees rest on
 * the second, and the gap between them is the thing worth watching as a corpus
 * grows.
 *
 * Full sync stops before the xl tier on purpose. A full-sync client at half a
 * million rows is the case partial sync exists to answer, and pricing it here
 * would spend twenty minutes measuring a configuration nobody should ship. The
 * xl row of the windowed table is the answer to what it would have cost.
 *
 * Run order matters across this directory: these specs only read, so they come
 * first and measure a corpus in exactly the state the seeder left it. Later
 * files write.
 */

/** Rows a windowed client caps each entity at. Under the server's default 1000. */
const WINDOW = 500;

async function measureBootstrap(
	context: BrowserContext,
	tierName: string,
	scope: 'full' | 'window' | 'project' | 'thread',
	label: string,
) {
	const t = tier(tierName);
	const before = await wireStats();
	const tab = await BenchTab.open(context, {
		workspaceId: t.workspace,
		// A fresh account is a fresh IndexedDB database, which is what makes
		// this a COLD start: the workspace itself has been synced by every
		// earlier scenario.
		accountId: freshAccount(`${label}-${tierName}`),
		scope,
		windowLimit: WINDOW,
		// The NEWEST issue, not the first. Comments are skewed onto the newest
		// fifth of the corpus, so a thread rooted at issue 0 follows to an empty
		// set and prices a closure nobody would ever compute.
		issueId: issueKey(t.issues - 1),
	});
	try {
		const result = await tab.startWithFirstRead(20);
		const [ counts, storage, heap, after ] = await Promise.all([
			tab.counts(),
			tab.storage(),
			tab.heapBytes(),
			wireStats(),
		]);
		const wire = wireDelta(before, after);
		const localRows = Object.values(counts).reduce((a, b) => a + b, 0);

		report({
			scenario: 'cold bootstrap',
			tier: tierName,
			scope,
			metrics: {
				firstReadMs: result.firstReadMs,
				completeMs: result.completeMs,
				bootstrapMs: result.summary.bootstrapMs,
				// The three phases bootstrapMs is made of. Reported beside it
				// because a bootstrap that is slow at the network and one that is
				// slow at the store are the same total and different problems, and
				// a change that moves time between phases nets out to nothing in
				// the total alone.
				streamMs: result.summary.bootstrapStreamMs,
				stageMs: result.summary.bootstrapStageMs,
				swapMs: result.summary.bootstrapSwapMs,
				bootstrapRows: result.summary.bootstrapRows,
				// KV entries the swap wrote, and how many that is per row. Row
				// count alone hides write amplification — the number that says
				// whether the swap is writing a snapshot or writing an index.
				swapWrites: result.summary.bootstrapSwapWrites,
				swapWritesPerRow: result.summary.bootstrapRows === 0
					? 0
					: result.summary.bootstrapSwapWrites / result.summary.bootstrapRows,
				pulls: result.summary.pulls,
				localRows,
				// What the client ended up holding, per entity: a windowed run
				// that pulled in far more than its window did so by following
				// references, and this is where that shows up.
				issues: counts.issue ?? 0,
				comments: counts.comment ?? 0,
				rowsPerSec: localRows === 0
					? 0
					: Math.round((localRows / result.completeMs) * 1000),
				bytesDown: wire.bytesDown,
				gzipDown: wire.gzipDown,
				bytesPerRow: localRows === 0 ? 0 : Math.round(wire.bytesDown / localRows),
				gzipPerRow: localRows === 0 ? 0 : Math.round(wire.gzipDown / localRows),
				roundTrips: wire.roundTrips,
				storageBytes: storage.usage,
				heapBytes: heap,
			},
			wire: { ...wire, localMs: result.completeMs, of: 'completeMs' },
		});
		return { result, counts, localRows };
	} finally {
		await tab.closeTab();
	}
}

test.describe('cold bootstrap', () => {
	for (const t of tiers()) {
		test(`full sync — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(
				t.name === 'xl',
				'full sync at 500k rows is the case partial sync exists for; the windowed row prices the alternative',
			);
			const { localRows } = await measureBootstrap(context, t.name, 'full', 'full');
			// Not a performance assertion — a completeness one. A number taken
			// from a client that only got half the corpus is not a bootstrap
			// measurement at all, and without this it would still be reported.
			expect(localRows).toBe(t.total);
		});
	}

	for (const t of tiers()) {
		test(`windowed (${WINDOW}/entity) — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			const { counts } = await measureBootstrap(context, t.name, 'window', 'window');
			// The claim the window makes: the working set is a policy choice, so
			// what lands locally tracks the cap and not the corpus. Followed rows
			// (a window's issues drag in their projects and assignees) are extra,
			// which is why this bounds the windowed entities rather than the total.
			//
			// Bounded on BOTH sides: an upper bound alone is satisfied by a client
			// that synced nothing at all, which is the one outcome that would make
			// every number in the row meaningless while looking like a pass.
			expect(counts.issue ?? 0).toBe(Math.min(WINDOW, t.issues));
			expect(counts.comment ?? 0).toBe(Math.min(WINDOW, t.comments));
		});
	}

	for (const t of tiers()) {
		test(`one project — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			const { counts } = await measureBootstrap(context, t.name, 'project', 'project');
			// A predicate scope is unbounded in principle; what it buys is
			// selectivity. Recording what it actually selected is the point —
			// this only catches a scope that selected nothing at all.
			expect(counts.issue ?? 0).toBeGreaterThan(0);
		});
	}

	for (const t of tiers()) {
		test(`one thread — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			// Few rows, several hops: the cost here is the server computing a
			// follow closure, not the client writing rows. It is the shape that
			// scales worst against corpus size while looking smallest locally,
			// which is exactly why it is measured at every tier.
			const { counts } = await measureBootstrap(context, t.name, 'thread', 'thread');
			expect(counts.issue ?? 0).toBeGreaterThan(0);
		});
	}
});
