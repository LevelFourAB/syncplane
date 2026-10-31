import { test } from '@playwright/test';

import type { Kind, MaintainResult } from './app/maintain.js';
import { report } from './report.js';

/**
 * What keeping the full-text search index current costs, apart from the
 * network: a cold bootstrap of 2000 text-heavy rows, pull pages that insert,
 * edit and delete them, and a background build over rows that carry no
 * positions. See app/maintain.ts for the corpus and the write paths it drives.
 *
 * Each scenario runs on a searchable entity (`doc`) and on the same rows with
 * no search declaration (`plain`); `doc` minus `plain` is the search
 * maintenance. The warm searches at the end run against the `doc` store the
 * pulls left behind.
 *
 * It needs no corpus from the server, so the cheapest tier is enough:
 *
 *   TIERS=s node_modules/.bin/playwright test --config playwright.bench.config.ts \
 *     --project=chrome 08-search-maintenance
 *
 * MAINTAIN_RUNS=n sets the runs per entity (default 3); the table shows the
 * median and the range.
 */

const RUNS = Number(process.env['MAINTAIN_RUNS'] ?? 3);

const METRICS = [
	'bootstrapMs',
	'insert500Ms',
	'edit500Ms',
	'edit20Ms',
	'delete500Ms',
	'buildMs',
	'buildLockMs',
] as const;

function median(xs: number[]): number {
	const s = [ ...xs ].sort((a, b) => a - b);
	return s[Math.floor(s.length / 2)]!;
}

test('search maintenance on a text-heavy store', async ({ page }) => {
	test.setTimeout(20 * 60_000);
	await page.goto('/maintain.html');
	await page.waitForFunction(() => window.maintainBench !== undefined);
	const results: Record<Kind, MaintainResult[]> = { doc: [], plain: [] };
	for (let run = 1; run <= RUNS; run++) {
		for (const kind of [ 'doc', 'plain' ] as const) {
			const r = await page.evaluate(
				([ k, n ]) => window.maintainBench.run(k, n),
				[ kind, run ] as const,
			);
			results[kind].push(r);
			const { search, ...times } = r;
			const searchMs: Record<string, number> = {};
			for (const [ name, s ] of Object.entries(search ?? {})) searchMs[`search ${name}Ms`] = s.ms;
			report({
				scenario: 'search maintenance',
				tier: 'text',
				scope: kind,
				metrics: { ...times, ...searchMs },
			});
		}
	}
	const lines = [
		`\nsearch maintenance (${test.info().project.name}), median [min..max] of ${RUNS}`,
	];
	for (const metric of METRICS) {
		const cells = ([ 'doc', 'plain' ] as const).map((kind) => {
			const xs = results[kind].map((r) => r[metric]);
			return `${kind} ${median(xs).toFixed(1)} [${Math.min(...xs).toFixed(1)}..${
				Math.max(...xs).toFixed(1)
			}]`;
		});
		lines.push(`  ${metric.padEnd(12)} ${cells.join('   ')}`);
	}
	const searches = results.doc.map((r) => r.search ?? {});
	for (const name of Object.keys(searches[0] ?? {})) {
		const xs = searches.map((s) => s[name]!.ms);
		const one = searches[0]![name]!;
		lines.push(
			`  search ${name.padEnd(11)} ${median(xs).toFixed(2)}ms [${Math.min(...xs).toFixed(2)}..${
				Math.max(...xs).toFixed(2)
			}] rows ${one.rows} total ${one.total}`,
		);
	}
	console.log(lines.join('\n'));
});
