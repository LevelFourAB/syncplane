import { fileURLToPath } from 'node:url';

import { type BenchServer, startBenchServer } from '../support/benchserver.js';
import { RESULTS_ENV } from './report.js';

/**
 * Environment variables carrying this run's context to the specs. Playwright
 * forks its workers after global setup has run, so they inherit them.
 */
export const SERVER_URL_ENV = 'SYNC_BENCH_SERVER_URL';
export const TIERS_ENV = 'SYNC_BENCH_TIERS';
export const RUN_ID_ENV = 'SYNC_BENCH_RUN_ID';

/**
 * Which corpus sizes this run seeds. Every tier costs seeding time on a cold
 * cache and nothing afterwards, so the default is all of them; narrow it with
 * TIERS=s,m for a quick pass while iterating on a scenario.
 */
function requestedTiers(): string[] {
	const raw = process.env.TIERS;
	if (raw === undefined || raw.trim() === '') return [ 's', 'm', 'l', 'xl' ];
	return raw.split(',').map((t) => t.trim()).filter((t) => t !== '');
}

/** Held for global teardown, which runs in this same process. */
let server: BenchServer | undefined;

export default async function globalSetup(): Promise<void> {
	const tiers = requestedTiers();
	server = await startBenchServer({
		tiers,
		// Fixed, not random: the corpus is a benchmark input, and a run that
		// generated a different one would not be comparable to the last.
		// Override with SEED=n to check a result is not an artifact of this one.
		seed: Number(process.env.SEED ?? 1),
		log: (message) => console.log(`[bench] ${message}`),
	});
	process.env[SERVER_URL_ENV] = server.url;

	const res = await fetch(new URL('/debug/tiers', server.url));
	if (!res.ok) throw new Error(`/debug/tiers responded ${res.status}`);
	const all = await res.json() as { name: string }[];
	// Only the seeded tiers: a spec asking for one this run did not seed should
	// fail saying so, rather than measure an empty workspace.
	process.env[TIERS_ENV] = JSON.stringify(all.filter((t) => tiers.includes(t.name)));

	// Timestamped so runs accumulate rather than overwrite each other. Taken
	// once here so every worker appends to the same file.
	const runId = new Date().toISOString().replace(/[:.]/g, '-');
	process.env[RUN_ID_ENV] = runId;
	process.env[RESULTS_ENV] = fileURLToPath(new URL(`./results/${runId}.jsonl`, import.meta.url));
	console.log(`[bench] run ${runId}, tiers ${tiers.join(', ')}`);
}

export function stopServer(): void {
	server?.stop();
	server = undefined;
}
