import { test } from '@playwright/test';

import { BenchTab, freshAccount, issueKey, tier, tiers } from './fixtures.js';
import type { ReadOp } from './harness-api.js';
import { report } from './report.js';

/**
 * What a local read costs once the data is there.
 *
 * Sync latency is only half of what a local-first app feels: every render is a
 * query against the local store, and a store that took four seconds to fill and
 * then answers in 300ms is not a fast app. These numbers are the other half.
 *
 * Each shape is here because it exercises a different plan:
 *
 *   - `board` and `byLabel` are served by declared indexes (the second through a
 *     fan-out component), so they should cost the limit, not the entity.
 *   - `triage` filters on one field and orders by another — the composite index's
 *     reason to exist.
 *   - `unindexed` deliberately has no index to serve it, so it walks the entity.
 *     It is the ceiling the indexed shapes are read against.
 *   - `boardHydrated` is `board` plus a point lookup per row per reference: the
 *     price of `include`.
 *   - `scanIssues` / `scanFlat` decode every row of an entity. Comparing them
 *     separates decode cost that is the row's shape from decode cost that is the
 *     row's size.
 *
 * Reads are taken AFTER the background index build has finished (which the
 * spec waits for, and reports as its own scenario): a bootstrap promotes its
 * snapshot unindexed, so until the build claims the markers every one of these
 * queries scans — a real state, but a different measurement. The `path` column
 * is the receipt: it names the index that served each read, or `scan:<reason>`
 * when one did not.
 *
 * Read-only, like every spec numbered below 05.
 */

const LIMIT = 50;

/**
 * One cell naming how the store answered: the serving index, or `scan:<reason>`
 * for a fallback (see ReadResult.path). The cold and warm reads can legitimately
 * differ — the background index build claims an index between them — and a cell
 * like `scan:not-built>by_project_updated` is that moment caught in the act:
 * the cold number priced the fallback, the warm one the index.
 */
function pathLabel(cold?: string, warm?: string): string {
	if (cold === warm) return warm ?? '';
	return `${cold ?? ''}>${warm ?? ''}`;
}

function readOps(t: ReturnType<typeof tier>): { name: string; op: ReadOp }[] {
	const projectId = 'project-00000000';
	return [
		{ name: 'get', op: { kind: 'get', issueId: issueKey(0) } },
		{ name: 'board', op: { kind: 'board', projectId, limit: LIMIT } },
		{ name: 'boardHydrated', op: { kind: 'boardHydrated', projectId, limit: LIMIT } },
		{ name: 'triage', op: { kind: 'triage', limit: LIMIT } },
		{ name: 'byLabel', op: { kind: 'byLabel', label: 'label-00000003', limit: LIMIT } },
		{ name: 'thread', op: { kind: 'thread', issueId: issueKey(t.issues - 1) } },
		{ name: 'unindexed', op: { kind: 'unindexed', limit: LIMIT } },
		{ name: 'scanIssues', op: { kind: 'scanIssues' } },
		{ name: 'scanFlat', op: { kind: 'scanFlat' } },
	];
}

test.describe('local reads', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`reads-${t.name}`),
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
				for (const { name, op } of readOps(t)) {
					// Twice: the first call pays for whatever the store, the
					// index and the decoder cache lazily. An app issues the same
					// query on every render, so the warm number is the one it
					// lives with — and reporting both is what shows whether the
					// cold one is worth caring about.
					const cold = await tab.read(op);
					const warm = await tab.read(op);
					report({
						scenario: 'local read',
						tier: t.name,
						scope: 'full',
						metrics: {
							query: name,
							coldMs: cold.ms,
							warmMs: warm.ms,
							rows: warm.rows,
							complete: warm.complete,
							path: pathLabel(cold.path, warm.path),
						},
					});
				}
			} finally {
				await tab.closeTab();
			}
		});
	}

	for (const t of tiers()) {
		test(`windowed store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`reads-win-${t.name}`),
				scope: 'window',
				windowLimit: 500,
			});
			try {
				await tab.start();
				const build = await tab.waitForIndexes();
				report({
					scenario: 'index build',
					tier: t.name,
					scope: 'window',
					metrics: { rows: build.rows, buildMs: build.buildMs, waitedMs: build.waitedMs },
				});
				for (const { name, op } of readOps(t)) {
					const cold = await tab.read(op);
					const warm = await tab.read(op);
					report({
						scenario: 'local read',
						tier: t.name,
						scope: 'window',
						metrics: {
							query: name,
							coldMs: cold.ms,
							warmMs: warm.ms,
							rows: warm.rows,
							// The number that makes the windowed row honest: a
							// query answered fast off 500 local rows may be
							// answering a question the store cannot fully see,
							// and coverage is how an app finds that out.
							complete: warm.complete,
							bounds: warm.bounds.join('+'),
							path: pathLabel(cold.path, warm.path),
						},
					});
				}
			} finally {
				await tab.closeTab();
			}
		});
	}
});
