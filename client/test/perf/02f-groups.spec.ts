import { test } from '@playwright/test';

import { BenchTab, freshAccount, tiers } from './fixtures.js';
import type { ReadOp } from './harness-api.js';
import { report } from './report.js';

/**
 * What a grouped read costs on each path it can take: bucketing a scan when no
 * index partitions the group value, and walking an index whose positions
 * cannot settle the predicate, so every row of every group is read. `rows` is
 * the number of groups the read produced.
 */

function readOps(): { name: string; op: ReadOp }[] {
	return [
		{ name: 'groupProject', op: { kind: 'groupProject' } },
		{ name: 'groupStateResidual', op: { kind: 'groupStateResidual' } },
		{ name: 'distinctAssignee', op: { kind: 'distinctAssignee' } },
		{ name: 'groupRowsResidual', op: { kind: 'groupRowsResidual', perGroup: 5 } },
	];
}

test.describe('grouped reads', () => {
	for (const t of tiers()) {
		test(`full store — tier ${t.name} (${t.total} rows)`, async ({ context }) => {
			test.skip(t.name === 'xl', 'a full local store at 500k rows is priced by the windowed row');
			const tab = await BenchTab.open(context, {
				workspaceId: t.workspace,
				accountId: freshAccount(`groups-${t.name}`),
				scope: 'full',
			});
			try {
				await tab.start();
				await tab.waitForIndexes();
				for (const { name, op } of readOps()) {
					const cold = await tab.read(op);
					const warm = await tab.read(op);
					report({
						scenario: 'grouped read',
						tier: t.name,
						scope: 'full',
						metrics: {
							query: name,
							coldMs: cold.ms,
							warmMs: warm.ms,
							groups: warm.rows,
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
