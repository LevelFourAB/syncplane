import { expect, test } from '@playwright/test';

import { BenchTab, freshAccount, tiers } from './fixtures.js';
import { report } from './report.js';

/**
 * What reaching a deep rank costs, and what a total costs.
 *
 * 02-reads only ever takes page one. The paths this spec exists to pin are the
 * ones past it: walking pages by cursor (each page seeks to its bound and
 * reads one page of rows), jumping by `offset` (which re-does the work of
 * every page it skips), and the match `total` every paged result carries —
 * computed by counting index positions rather than reading rows whenever the
 * plan allows, which is exactly the optimization nothing else would notice
 * regressing: the answer stays right, just slow.
 *
 * Read-only, like every spec numbered below 05. Runs after the index build so
 * every number prices the index, not the scan standing in for it; `path` is
 * the receipt.
 */

const LIMIT = 50;
const PAGES = 20;

test.describe('pagination', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`pages-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				await tab.waitForIndexes();

				for (
					const [ name, op ] of [
						[ 'board', {
							kind: 'boardPages',
							projectId: 'project-00000000',
							limit: LIMIT,
							pages: PAGES,
						} ],
						[ 'triage', { kind: 'triagePages', limit: LIMIT, pages: PAGES } ],
						// The same walk with the count opted out — the difference
						// against the row above is what `total` costs a deep page.
						[ 'triage total:false', {
							kind: 'triagePages',
							limit: LIMIT,
							pages: PAGES,
							total: false,
						} ],
					] as const
				) {
					const walk = await tab.paginate(op);
					// A walk that scanned is measuring a different plan — see 02.
					expect(walk.path, `${name} walk fell back: ${walk.path}`).not.toMatch(/^scan:/);
					const last = walk.pageMs[walk.pageMs.length - 1] ?? 0;
					report({
						scenario: 'page walk',
						tier: t.name,
						scope: 'full',
						metrics: {
							query: name,
							pages: walk.pageMs.length,
							firstPageMs: walk.pageMs[0] ?? 0,
							lastPageMs: last,
							walkMs: walk.pageMs.reduce((a, b) => a + b, 0),
							// The same rank in one query: what an app that jumps
							// instead of walking pays.
							offsetMs: walk.offsetMs,
							rows: walk.rows,
							total: walk.total,
							path: walk.path ?? '',
						},
					});
				}

				// The total alone: a limit-1 page still counts the whole match
				// set, and on the triage shape (no residual, one range) that count
				// should cost index positions, not row reads.
				const count = await tab.paginate({ kind: 'triagePages', limit: 1, pages: 1 });
				report({
					scenario: 'total count',
					tier: t.name,
					scope: 'full',
					metrics: {
						ms: count.pageMs[0] ?? 0,
						total: count.total,
						path: count.path ?? '',
					},
				});
			} finally {
				await tab.closeTab();
			}
		});
	}
});
