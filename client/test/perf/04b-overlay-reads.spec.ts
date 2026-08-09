import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, issueKey, type Tier, tiers } from './fixtures.js';
import type { ReadOp } from './harness-api.js';
import { report } from './report.js';

/**
 * What a pending queue costs every read.
 *
 * A tab that has been writing offline holds an optimistic overlay, and every
 * query must merge it: overlay rows shadow their confirmed versions, which
 * disables the fast paths a clean store gets (a shadowed row cannot be counted
 * as an index position, so settled walks go back to walking). 02-reads prices
 * the clean store; this is the same set of shapes read through 100 and then
 * 1000 pending mutations — the users most likely to have a big overlay are the
 * offline-heavy ones, who pay this price exactly when they can least afford
 * re-syncing.
 *
 * The writes here are LOCAL and stay local: the tab is offline for the whole
 * spec, so nothing reaches the server and every spec after this one still
 * reads the corpus the seeder left. Read-only from the corpus's point of view,
 * which is why it can sit below 05.
 */

const LIMIT = 50;
const PENDING = [ 100, 1000 ];

function readOps(t: Tier): { name: string; op: ReadOp }[] {
	const projectId = 'project-00000000';
	return [
		{ name: 'get', op: { kind: 'get', issueId: issueKey(t.issues - 1) } },
		{ name: 'board', op: { kind: 'board', projectId, limit: LIMIT } },
		{ name: 'triage', op: { kind: 'triage', limit: LIMIT } },
		{ name: 'byLabel', op: { kind: 'byLabel', label: 'label-00000003', limit: LIMIT } },
		{ name: 'scanIssues', op: { kind: 'scanIssues' } },
	];
}

test.describe('reads under overlay pressure', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			test.skip(t.issues < PENDING[PENDING.length - 1]!, 'tier holds fewer issues than the queue');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`overlay-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				await tab.waitForIndexes();
				await context.setOffline(true);

				let queued = 0;
				for (const pending of PENDING) {
					// Newest rows: the ones the board and window shapes are looking
					// at, so the shadowing actually intersects the reads.
					await tab.queueWrites(pending - queued, t.issues - pending);
					queued = pending;
					expect(await tab.pendingCount()).toBe(pending);
					for (const { name, op } of readOps(t)) {
						const cold = await tab.read(op);
						const warm = await tab.read(op);
						report({
							scenario: 'overlay read',
							tier: t.name,
							scope: 'full',
							metrics: {
								pending,
								query: name,
								coldMs: cold.ms,
								warmMs: warm.ms,
								rows: warm.rows,
								path: warm.path ?? '',
								candidates: warm.candidates ?? '',
							},
						});
					}
				}
			} finally {
				// Still offline on purpose: the queue must never reach the server.
				// The account is fresh, so the orphaned queue dies with the tab.
				await tab.closeTab();
			}
		});
	}
});
