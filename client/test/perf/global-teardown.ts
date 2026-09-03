import { fileURLToPath } from 'node:url';

import { RUN_ID_ENV, SERVER_URL_ENV, stopServer } from './global-setup.js';
import {
	describeBrowser,
	formatResults,
	formatWireProjection,
	readBrowser,
	readResults,
	RESULTS_ENV,
	writeSummary,
} from './report.js';

/**
 * Kills the benchserver and prints the run's numbers. Setup and teardown share
 * this process, so importing the setup module reaches the same server handle
 * rather than a second copy of it.
 *
 * The table is the deliverable: `task bench:browser` is run to read it, and the
 * pass/fail Playwright reports says nothing beyond "the scenarios ran".
 */
export default async function globalTeardown(): Promise<void> {
	// Read the server's run-wide totals BEFORE killing it: this is the only
	// view that says which RPC the bytes went to, and it dies with the process.
	const byMethod = await methodTotals();
	stopServer();
	if (byMethod !== undefined) console.log(formatByMethod(byMethod));

	const path = process.env[RESULTS_ENV];
	if (path === undefined) return;
	const results = readResults(path);
	if (results.length === 0) {
		console.log('[bench] no results recorded');
		return;
	}
	const runId = process.env[RUN_ID_ENV] ?? 'latest';
	const summary = fileURLToPath(new URL(`./results/${runId}.json`, import.meta.url));
	// The browser comes from the workers, which are the only processes that
	// launched one — these numbers do not compare across engines.
	const browser = readBrowser(path);
	writeSummary(summary, { browser, results });
	console.log(formatResults(results));
	console.log(formatWireProjection(results));
	const on = browser === undefined ? '' : ` on ${describeBrowser(browser)}`;
	console.log(`\n[bench] ${results.length} results${on} — ${summary}`);
}

/**
 * The server's per-method totals for the whole run, fetched while it is still
 * alive.
 *
 * Worth a table of its own because the aggregate hides the thing that decides
 * how a deployment should be tuned: which RPC the bytes went to, and how well
 * each one's payload compresses. A workload dominated by Bootstrap and one
 * dominated by Pull want different answers even at identical total volume.
 */
async function methodTotals(): Promise<WireTotals | undefined> {
	const url = process.env[SERVER_URL_ENV];
	if (url === undefined) return undefined;
	try {
		const res = await fetch(new URL('/debug/stats', url));
		if (!res.ok) return undefined;
		return await res.json() as WireTotals;
	} catch {
		// The server may already be gone if setup failed; the rest of the
		// teardown is still worth running.
		return undefined;
	}
}

interface WireTotals {
	total: Record<string, number>;
	bytesIn: Record<string, number>;
	bytesOut: Record<string, number>;
	gzipIn: Record<string, number>;
	gzipOut: Record<string, number>;
	encodedOut: Record<string, number>;
	wireIn: Record<string, number>;
	wireOut: Record<string, number>;
}

function formatByMethod(totals: WireTotals): string {
	const methods = Object.keys(totals.total).sort();
	if (methods.length === 0) return '';
	const rows = methods.map((m) => {
		const calls = totals.total[m] ?? 0;
		const down = totals.bytesOut[m] ?? 0;
		const wireDown = totals.wireOut[m] ?? 0;
		const up = totals.bytesIn[m] ?? 0;
		const wireUp = totals.wireIn[m] ?? 0;
		const encoded = totals.encodedOut[m] ?? 0;
		return [
			m,
			String(calls),
			// Whether the engine compressed its own responses. Connect does it
			// for unary methods and cannot for streaming ones, so this column is
			// what explains why two rows below compress so differently.
			encoded === 0 ? 'no' : encoded === calls ? 'yes' : `${encoded}/${calls}`,
			(down / 1024).toFixed(0),
			(wireDown / 1024).toFixed(0),
			wireDown === 0 ? '-' : `${(down / wireDown).toFixed(2)}x`,
			(up / 1024).toFixed(0),
			(wireUp / 1024).toFixed(0),
			wireUp === 0 ? '-' : `${(up / wireUp).toFixed(2)}x`,
		];
	});
	const header = [
		'method',
		'calls',
		'selfZip',
		'rawKB',
		'wireKB',
		'ratio',
		'upKB',
		'wireKB',
		'ratio',
	];
	const widths = header.map((h, i) =>
		Math.max(h.length, ...rows.map((cells) => cells[i]?.length ?? 0))
	);
	const line = (cells: string[]) => cells.map((c, i) => c.padStart(widths[i] ?? 0)).join('  ');
	return [
		'\nwhole run, by RPC (raw = engine output, wire = what a link carries)',
		line(header),
		widths.map((w) => '-'.repeat(w)).join('  '),
		...rows.map(line),
	].join('\n');
}
