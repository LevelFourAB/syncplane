import { fileURLToPath } from 'node:url';

import { SERVER_URL_ENV } from '../perf/global-setup.js';
import { RESULTS_ENV } from '../perf/report.js';
import { type BenchServer, startBenchServer } from '../support/benchserver.js';

/** Environment variable carrying the seeded workspace id to the spec. */
export const WORKSPACE_ENV = 'SYNC_IDBSIZE_WORKSPACE';

/** Environment variable carrying the path the spec writes its summary to. */
export const SUMMARY_ENV = 'SYNC_IDBSIZE_SUMMARY';

/** Held for global teardown, which runs in this same process. */
let server: BenchServer | undefined;

/**
 * Starts a benchserver on an empty corpus and seeds the text-heavy search
 * corpus into it (benchserver's /debug/seed-search). The seed is a few
 * seconds, so nothing is cached.
 */
export default async function globalSetup(): Promise<void> {
	server = await startBenchServer({
		tiers: [],
		seed: 1,
		log: (message) => console.log(`[idbsize] ${message}`),
	});
	process.env[SERVER_URL_ENV] = server.url;

	const res = await fetch(new URL('/debug/seed-search', server.url), { method: 'POST' });
	if (!res.ok) throw new Error(`/debug/seed-search responded ${res.status}: ${await res.text()}`);
	const seeded = await res.json() as {
		workspace: string;
		counts: { total: number };
		tookMs: number;
	};
	console.log(
		`[idbsize] seeded ${seeded.counts.total} rows into ${seeded.workspace} in ${seeded.tookMs}ms`,
	);
	process.env[WORKSPACE_ENV] = seeded.workspace;

	// The perf fixtures record the browser beside a results file, so they need
	// one; the summary goes beside it, under the perf tier's ignored results.
	const runId = new Date().toISOString().replace(/[:.]/g, '-');
	const results = new URL(`../perf/results/idb-size-${runId}`, import.meta.url);
	process.env[RESULTS_ENV] = fileURLToPath(`${results.href}.jsonl`);
	process.env[SUMMARY_ENV] = fileURLToPath(`${results.href}.json`);
}

export function stopServer(): void {
	server?.stop();
	server = undefined;
}
