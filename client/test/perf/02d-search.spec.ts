import { expect, test } from '@playwright/test';

import {
	BenchTab,
	freshAccount,
	midTermRows,
	rareTermRows,
	searchTerms,
	type Tier,
	tiers,
} from './fixtures.js';
import type { ReadOp } from './harness-api.js';
import { report } from './report.js';

/**
 * What a full-text search costs against a local store.
 *
 * Search is answered the same way everywhere: the term-postings index proposes
 * candidates, and every candidate is re-read and re-analyzed to decide whether
 * it matches and how well. So a search has two costs that scale differently —
 * how many index positions the walk reads, and how many documents it analyzes
 * — and the only thing that moves either is how many rows carry the query's
 * terms.
 *
 * That is why these queries vary by SELECTIVITY rather than by syntax. The
 * corpus plants three terms for it (see fixtures.searchTerms): one in ten rows
 * whatever the tier's size, one in a hundredth of them, and one in every one.
 *
 *   - `rare` should be flat in the corpus size. It is the case the index
 *     exists for, and a number that slopes is the regression to look for.
 *   - `mid` is where candidate re-checks start to dominate the walk.
 *   - `common` is the ceiling: a term every row carries leaves nothing to
 *     narrow, so the search costs what walking the entity costs.
 *   - `common+rare` is what a search box produces the moment a user types a
 *     second word. It belongs next to `rare`, not next to `common` — the two
 *     terms bound each other, and the plan is only entitled to the cheaper of
 *     them.
 *   - `prefix` is search-as-you-type: the final term matches a RANGE of terms
 *     rather than one, so what it costs over `rare` is the width of that range.
 *
 * `ranked` doubles the interesting ones because relevance ordering is priced
 * separately: a rank is computed per row from the row alone, so nothing about
 * a limit bounds it — twenty results out of ten thousand matches still means
 * ten thousand ranked rows.
 *
 * Read-only, like every spec numbered below 05. Reads are taken after the
 * background index build pass, which the spec waits for and reports: a search
 * issued before it reports may still be scanning, which is a real state but a
 * different measurement. The `path` column is the receipt.
 */

const LIMIT = 50;

/**
 * The searches, with the rows each must return. A trailing space is what makes
 * a final term exact rather than a prefix, so it is load-bearing rather than
 * formatting.
 */
function searchOps(): { name: string; op: ReadOp; rows: (t: Tier) => number }[] {
	const { rare, mid, common } = searchTerms;
	const all = () => rareTermRows;
	return [
		{ name: 'rare', op: { kind: 'search', terms: `${rare} `, limit: LIMIT }, rows: all },
		{
			name: 'rare/ranked',
			op: { kind: 'search', terms: `${rare} `, limit: LIMIT, ranked: true },
			rows: all,
		},
		{
			name: 'prefix',
			op: { kind: 'search', terms: rare.slice(0, 4), limit: LIMIT },
			rows: all,
		},
		{
			name: 'mid',
			op: { kind: 'search', terms: `${mid} `, limit: LIMIT },
			rows: (t) => Math.min(LIMIT, midTermRows(t)),
		},
		{
			name: 'common',
			op: { kind: 'search', terms: `${common} `, limit: LIMIT },
			rows: (t) => Math.min(LIMIT, t.issues),
		},
		{
			name: 'common/ranked',
			op: { kind: 'search', terms: `${common} `, limit: LIMIT, ranked: true },
			rows: (t) => Math.min(LIMIT, t.issues),
		},
		{
			name: 'common+rare',
			op: { kind: 'search', terms: `${common} ${rare} `, limit: LIMIT },
			rows: all,
		},
	];
}

test.describe('local search', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.issues} issues)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`search-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				const build = await tab.waitForIndexes();
				report({
					scenario: 'index build',
					tier: t.name,
					scope: 'full',
					metrics: { rows: build.rows, buildMs: build.buildMs, waitedMs: build.waitedMs },
				});
				for (const { name, op, rows } of searchOps()) {
					// Twice, as in 02-reads: an app issues the same query on
					// every keystroke, so the warm number is the one it lives
					// with, and reporting both says whether the cold one matters.
					const cold = await tab.read(op);
					const warm = await tab.read(op);
					// A row count that moved means the query stopped selecting
					// what its name claims, so its timing describes something
					// else.
					expect(warm.rows, `${name} matched a different row set`).toBe(rows(t));
					report({
						scenario: 'local search',
						tier: t.name,
						scope: 'full',
						metrics: {
							query: name,
							coldMs: cold.ms,
							warmMs: warm.ms,
							rows: warm.rows,
							complete: warm.complete,
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
