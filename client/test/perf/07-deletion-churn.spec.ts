import { expect, test } from '@playwright/test';

import {
	BenchTab,
	freshAccount,
	largestTier,
	serverWrite,
	wireDelta,
	wireStats,
} from './fixtures.js';
import { report } from './report.js';

/**
 * What a wave of deletions costs to apply, and what it leaves behind.
 *
 * Deletes are the change shape nothing above measures at volume: a tombstone
 * per row crosses the wire, and applying one removes the row, unwinds its
 * index positions and notifies watchers — different work from the patch waves
 * 05 catches up over. The churn is this spec's own: it inserts fresh rows,
 * syncs them in, deletes exactly those rows again, and syncs the deletions —
 * so the corpus's seeded rows are untouched and the row count provably returns
 * to where it started.
 *
 * The reads afterwards price the store the churn leaves: same shapes as 02, on
 * an entity that grew by 2000 rows and shrank back. Against 02's numbers for
 * the same tier, the difference is what churn costs a store.
 *
 * Runs after 06 on purpose — that spec deletes seeded rows and advances the GC
 * horizon, and this one must not change what IT measures.
 */

const CHURN = 2000;
const LIMIT = 50;

test.describe('deletion churn', () => {
	test(`insert, delete, read — tier ${largestTier().name}`, async ({ context }) => {
		const t = largestTier();
		const scope = t.name === 'xl' ? 'window' : 'full';
		const tab = await BenchTab.open(context, {
			workspaceId: t.workspace,
			accountId: freshAccount(`churn-${t.name}`),
			scope,
			windowLimit: 500,
		});
		try {
			await tab.start();
			await tab.waitForIndexes();
			const before = (await tab.counts())['issue'] ?? 0;

			const { first } = await serverWrite(t.name, 'insert', CHURN);
			const insertWire = await wireStats();
			const inserts = await tab.syncNow();
			report({
				scenario: 'churn: apply inserts',
				tier: t.name,
				scope,
				metrics: {
					changes: inserts.pullChanges,
					pulls: inserts.pulls,
					pullMs: inserts.pullMs,
					requestMs: inserts.pullRequestMs,
					applyMs: inserts.pullApplyMs,
					wallMs: inserts.wallMs,
				},
				wire: {
					...wireDelta(insertWire, await wireStats()),
					localMs: inserts.wallMs,
					of: 'wallMs',
				},
			});

			await serverWrite(t.name, 'delete', CHURN, undefined, first);
			const deleteWire = await wireStats();
			const deletes = await tab.syncNow();
			report({
				scenario: 'churn: apply deletes',
				tier: t.name,
				scope,
				metrics: {
					changes: deletes.pullChanges,
					pulls: deletes.pulls,
					pullMs: deletes.pullMs,
					requestMs: deletes.pullRequestMs,
					applyMs: deletes.pullApplyMs,
					wallMs: deletes.wallMs,
				},
				wire: {
					...wireDelta(deleteWire, await wireStats()),
					localMs: deletes.wallMs,
					of: 'wallMs',
				},
			});
			// The receipt that the churn deleted what it inserted, not the corpus.
			expect((await tab.counts())['issue'] ?? 0).toBe(before);

			for (
				const [ name, op ] of [
					[ 'board', { kind: 'board', projectId: 'project-00000000', limit: LIMIT } ],
					[ 'triage', { kind: 'triage', limit: LIMIT } ],
					[ 'scanIssues', { kind: 'scanIssues' } ],
				] as const
			) {
				const cold = await tab.read(op);
				const warm = await tab.read(op);
				report({
					scenario: 'churn: read after',
					tier: t.name,
					scope,
					metrics: {
						query: name,
						coldMs: cold.ms,
						warmMs: warm.ms,
						rows: warm.rows,
						path: warm.path ?? '',
					},
				});
			}
		} finally {
			await tab.closeTab();
		}
	});
});
