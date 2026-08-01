import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers, withWire } from './fixtures.js';
import { report } from './report.js';

/**
 * What it costs to open an app that has synced before.
 *
 * The common case, and the one a cold-bootstrap number says nothing about. A
 * returning client already holds the rows; what it pays is opening IndexedDB,
 * reading its cursor, and asking the server whether anything happened. On a
 * quiet workspace the answer is no, and a pull whose cursor already sits at the
 * head of the workspace skips recomputation entirely — so this number should
 * stay flat as the corpus grows, and finding it does not is the point.
 *
 * The bootstrap count is reported alongside: a warm start that bootstrapped is
 * not a warm start, and the duration alone could not tell you.
 */

test.describe('warm restart', () => {
	for (const t of tiers()) {
		test(`tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			// The same account across both tabs — that is what makes the second
			// one warm, since the account names the local database.
			const account = freshAccount(`restart-${t.name}`);
			const open = () =>
				BenchTab.open(context, {
					workspaceId: t.workspace,
					accountId: account,
					scope: 'window',
					windowLimit: 500,
				});

			const first = await open();
			const cold = await first.start();
			await first.closeTab();

			const second = await open();
			try {
				const { result: warm, wire } = await withWire(() => second.startWithFirstRead(20));
				report({
					scenario: 'warm restart',
					tier: t.name,
					scope: 'window',
					metrics: {
						coldCompleteMs: cold.wallMs,
						warmFirstReadMs: warm.firstReadMs,
						warmCompleteMs: warm.completeMs,
						// Zero is the expected value. Anything else means the
						// store was rebuilt rather than reopened, and every
						// other number in this row describes a different event.
						warmBootstraps: warm.summary.bootstraps,
						warmPullChanges: warm.summary.pullChanges,
						// A warm open still talks to the server: the interesting
						// number is how little it says, because on a high-RTT
						// link the round trips are the whole cost.
						bytesDown: wire.bytesDown,
						gzipDown: wire.gzipDown,
						roundTrips: wire.roundTrips,
					},
					wire: { ...wire, localMs: warm.completeMs, of: 'warmCompleteMs' },
				});
				expect(warm.summary.bootstraps).toBe(0);
			} finally {
				await second.closeTab();
			}
		});
	}
});
