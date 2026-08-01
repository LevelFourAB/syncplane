import { execFileSync, spawn } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, renameSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

export interface BenchServer {
	/** Base URL the server is listening on. */
	url: string;
	/** Kills the process. Safe to call more than once. */
	stop(): void;
}

export interface BenchServerOptions {
	/** Tiers to seed, as benchapp names them ('s', 'm', 'l', 'xl'). */
	tiers: string[];
	/** Corpus PRNG seed. Part of the cache fingerprint. */
	seed: number;
	/** Extra flags for the server process. */
	args?: string[];
	/** Progress lines, e.g. through Playwright's reporter. */
	log?: (message: string) => void;
}

const repoRoot = fileURLToPath(new URL('../../..', import.meta.url));
/** Cached corpora, one database per fingerprint. Gitignored; safe to delete. */
const cacheDir = fileURLToPath(new URL('../perf/.corpus', import.meta.url));

/**
 * Builds `cmd/benchserver`, makes sure a corpus for `tiers` exists, and spawns
 * a server on a private copy of it.
 *
 * Seeding is the expensive part — half a million rows is minutes of writes — so
 * a seeded database is cached under a fingerprint of everything that decides
 * its contents: the generator version, the tier list and the PRNG seed. Change
 * any of them and the next run regenerates rather than comparing against a
 * corpus that is no longer the one the code would produce.
 *
 * The server always runs on a COPY. Benchmarks write — they push mutations,
 * they ask the server to touch rows, they force a GC pass — and a run that
 * mutated the cache would silently change what every later run measures.
 */
export async function startBenchServer(opts: BenchServerOptions): Promise<BenchServer> {
	const log = opts.log ?? (() => {});
	const bin = join(mkdtempSync(join(tmpdir(), 'syncbench-')), 'benchserver');
	execFileSync('go', [ 'build', '-o', bin, './cmd/benchserver' ], {
		cwd: repoRoot,
		stdio: 'inherit',
	});

	const version = execFileSync(bin, [ '-corpus-version' ], { encoding: 'utf8' }).trim();
	const tiers = [ ...opts.tiers ].sort();
	const cached = join(cacheDir, `corpus-v${version}-${tiers.join('')}-s${opts.seed}.db`);

	if (!existsSync(cached)) {
		log(`seeding corpus (${tiers.join(', ')}) — this is a one-time cost per fingerprint`);
		await seedCorpus(bin, cached, tiers, opts.seed, log);
	} else {
		log(`reusing cached corpus ${cached}`);
	}

	const runDb = join(mkdtempSync(join(tmpdir(), 'syncbench-run-')), 'bench.db');
	copyDatabase(cached, runDb);
	const server = await spawnServer(bin, [ `-db=${runDb}`, ...(opts.args ?? []) ]);
	return {
		url: server.url,
		stop: () => {
			server.stop();
			rmSync(runDb, { force: true });
		},
	};
}

/**
 * Seeds every tier into a fresh database and moves it into place.
 *
 * The server is stopped before the copy: SQLite runs in WAL mode, so an open
 * database's most recent writes live in the -wal file, and closing is what
 * folds them back. Copying a live database would cache a corpus missing
 * whatever had not been checkpointed yet.
 *
 * Seeding into a temp path and renaming last is what keeps a cancelled run
 * (Ctrl-C halfway through the xl tier) from leaving a half-corpus behind under
 * a fingerprint that claims to be complete.
 */
async function seedCorpus(
	bin: string,
	cached: string,
	tiers: string[],
	seed: number,
	log: (message: string) => void,
): Promise<void> {
	mkdirSync(cacheDir, { recursive: true });
	const staging = join(mkdtempSync(join(tmpdir(), 'syncbench-seed-')), 'corpus.db');
	const server = await spawnServer(bin, [ `-db=${staging}` ]);
	try {
		for (const tier of tiers) {
			const started = Date.now();
			const res = await fetch(new URL('/debug/seed', server.url), {
				method: 'POST',
				body: JSON.stringify({ tier, seed }),
			});
			if (!res.ok) throw new Error(`seeding tier ${tier} failed: ${await res.text()}`);
			const body = await res.json() as { counts: { total: number } };
			log(`  tier ${tier}: ${body.counts.total} rows in ${Date.now() - started}ms`);
		}
	} finally {
		server.stop();
	}
	// The process has to be gone, not merely asked to go, before its WAL is
	// safe to read: kill() returns as soon as the signal is delivered.
	await waitForExit(server);
	copyDatabase(staging, cached, { checkpointed: true });
	rmSync(staging, { force: true });
}

/**
 * Copies a SQLite database and its sidecars. A database closed cleanly has
 * folded its WAL back into the main file, but an open one has not, so the
 * sidecars come along whenever they exist rather than only when they are known
 * to matter.
 */
function copyDatabase(from: string, to: string, opts?: { checkpointed?: boolean }): void {
	// Rename rather than copy for the main file when it is safe to move: a
	// half-written cache entry is worse than a slow one.
	if (opts?.checkpointed === true) {
		renameSync(from, to);
	} else {
		copyFileSync(from, to);
	}
	for (const suffix of [ '-wal', '-shm' ]) {
		if (existsSync(from + suffix)) copyFileSync(from + suffix, to + suffix);
	}
}

interface SpawnedServer extends BenchServer {
	exited: Promise<void>;
}

async function spawnServer(bin: string, args: string[]): Promise<SpawnedServer> {
	const proc = spawn(bin, args, { stdio: [ 'ignore', 'pipe', 'inherit' ] });
	const exited = new Promise<void>((resolve) => proc.on('exit', () => resolve()));

	const url = await new Promise<string>((resolve, reject) => {
		let buf = '';
		proc.stdout.on('data', (chunk: Buffer) => {
			buf += chunk.toString();
			const match = buf.match(/^LISTENING (\S+)$/m);
			if (match?.[1]) resolve(match[1]);
		});
		proc.on('error', reject);
		proc.on(
			'exit',
			(code) => reject(new Error(`benchserver exited with ${code} before listening`)),
		);
	});

	return { url, exited, stop: () => void proc.kill() };
}

function waitForExit(server: SpawnedServer): Promise<void> {
	return server.exited;
}
