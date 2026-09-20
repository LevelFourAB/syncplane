import { test } from '@playwright/test';

import { BenchTab, freshAccount, tiers } from './fixtures.js';
import type { ReadOp } from './harness-api.js';
import { report } from './report.js';

/**
 * What the facets beside a list cost, against the same page without them.
 *
 * A facet folds from every row the predicate matches, so a page that an index
 * would answer in `limit` steps has to read its whole match set instead. The
 * pairs below put the two numbers side by side: `triage` against
 * `triageFacets`, `board` against `boardFacets`. `allFacets` has no predicate
 * and prices the sidebar over the whole entity — the ceiling.
 */

const LIMIT = 50;

function readOps(): { name: string; op: ReadOp }[] {
	const projectId = 'project-00000000';
	return [
		{ name: 'triage', op: { kind: 'triage', limit: LIMIT } },
		{ name: 'triageFacets', op: { kind: 'triageFacets', limit: LIMIT } },
		{ name: 'board', op: { kind: 'board', projectId, limit: LIMIT } },
		{ name: 'boardFacets', op: { kind: 'boardFacets', projectId, limit: LIMIT } },
		{ name: 'allFacets', op: { kind: 'allFacets', limit: LIMIT } },
	];
}

test.describe('facets', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`facets-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				await tab.waitForIndexes();
				for (const { name, op } of readOps()) {
					const cold = await tab.read(op);
					const warm = await tab.read(op);
					report({
						scenario: 'facet read',
						tier: t.name,
						scope: 'full',
						metrics: {
							query: name,
							coldMs: cold.ms,
							warmMs: warm.ms,
							rows: warm.rows,
							path: warm.path ?? '',
							candidates: warm.candidates ?? '',
						},
					});
				}
			} finally {
				await tab.closeTab();
			}
		});
	}
});
