import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers } from './fixtures.js';
import { report } from './report.js';

/**
 * What reads cost while the cold bootstrap is still streaming.
 *
 * 02-reads deliberately waits the snapshot out, so the state it measures is the
 * one an app reaches. This spec measures the state a user actually sees on first
 * launch: rows are arriving into confirmed state chunk by chunk, no index-built
 * marker exists yet, and every indexed query falls back to a scan over whatever
 * has landed so far. Both sides of that are reported — what the scan fallback
 * costs a read against a store that is filling, and how long it takes before a
 * read is answered by an index at all.
 *
 * Read-only, like every spec numbered below 05.
 */

const LIMIT = 50;

test.describe('reads during bootstrap', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is not a state 02 prices either');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`build-reads-${t.name}`),
				scope: 'full',
			});
			try {
				// Not awaited: the reads below have to overlap the snapshot, which is
				// the whole scenario. `settled` is what keeps the loop from spinning
				// forever on a bootstrap that failed — the install is what claims the
				// markers, so once start() is over one more read decides it.
				let settled = false;
				const starting = tab.start().finally(() => {
					settled = true;
				});
				// Hammer the board shape until an index answers one. Every read before
				// that prices the scan fallback plus whatever lock contention the
				// incoming chunks inflict; the loop stops the moment a read was
				// answered by an index, so nothing indexed pollutes the during-
				// bootstrap numbers.
				const projectId = 'project-00000000';
				const scanMs: number[] = [];
				let firstIndexedMs: number | undefined;
				for (;;) {
					const done = settled;
					const read = await tab.read({ kind: 'board', projectId, limit: LIMIT });
					if (read.path !== undefined && !read.path.startsWith('scan:')) {
						firstIndexedMs = read.ms;
						break;
					}
					scanMs.push(read.ms);
					if (done) break;
				}
				await starting;
				const build = await tab.waitForIndexes();
				expect(firstIndexedMs).toBeDefined();
				scanMs.sort((a, b) => a - b);
				const at = (q: number) =>
					scanMs[Math.min(scanMs.length - 1, Math.floor(q * scanMs.length))] ?? 0;
				report({
					scenario: 'reads during bootstrap',
					tier: t.name,
					scope: 'full',
					metrics: {
						scanReads: scanMs.length,
						scanP50Ms: at(0.5),
						scanMaxMs: at(1),
						// The first read an index answered, which on the cold path is the
						// first one issued after the install: the markers are claimed by
						// the transaction that writes the cursor.
						firstIndexedMs: firstIndexedMs ?? 0,
						// What the build pass then found to do. Zero rows is the cold
						// path having already indexed them; 02's `index build` row for
						// the same tier is the same number without the read contention.
						buildMs: build.buildMs,
						rows: build.rows,
					},
				});
			} finally {
				await tab.closeTab();
			}
		});
	}
});
