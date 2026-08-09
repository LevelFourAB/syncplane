import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers } from './fixtures.js';
import { report } from './report.js';

/**
 * What reads cost while the background index build is still running.
 *
 * 02-reads deliberately waits the build out, so the state it measures is the
 * one an app reaches eventually. This spec measures the state a user actually
 * sees on first launch: the bootstrap has installed its snapshot unindexed,
 * every indexed query falls back to a scan, and the build competes with those
 * reads for the store. Both sides of that contention are reported — what the
 * scan fallback costs a read, and what concurrent reads cost the build
 * (against the uncontended `index build` number 02 reports for the same tier).
 *
 * Read-only, like every spec numbered below 05.
 */

const LIMIT = 50;

test.describe('reads during index build', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is not a state 02 prices either');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`build-reads-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				// Hammer the board shape until the build claims its indexes. Every
				// read before that prices the scan fallback plus whatever lock
				// contention the build inflicts; the loop stops the moment a read
				// was answered by an index, so nothing indexed pollutes the
				// during-build numbers.
				const projectId = 'project-00000000';
				const scanMs: number[] = [];
				let firstIndexedMs: number | undefined;
				for (;;) {
					const read = await tab.read({ kind: 'board', projectId, limit: LIMIT });
					if (read.path !== undefined && !read.path.startsWith('scan:')) {
						firstIndexedMs = read.ms;
						break;
					}
					scanMs.push(read.ms);
				}
				const build = await tab.waitForIndexes();
				expect(firstIndexedMs).toBeDefined();
				scanMs.sort((a, b) => a - b);
				const at = (q: number) =>
					scanMs[Math.min(scanMs.length - 1, Math.floor(q * scanMs.length))] ?? 0;
				report({
					scenario: 'reads during build',
					tier: t.name,
					scope: 'full',
					metrics: {
						scanReads: scanMs.length,
						scanP50Ms: at(0.5),
						scanMaxMs: at(1),
						firstIndexedMs: firstIndexedMs ?? 0,
						// The build under read contention; 02's `index build` row for
						// the same tier is the uncontended baseline.
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
