/**
 * How many bytes the client's IndexedDB keeps on disk for a synced text-heavy
 * workspace, and how many of them are index data.
 *
 * Run from client/ (the browsers need the macOS sandbox off):
 *
 *   node_modules/.bin/playwright test --config playwright.idbsize.config.ts --project=webkit --project=chrome
 *
 * `--project=chromium` measures Playwright's bundled Chromium instead of the
 * installed Google Chrome. IDBSIZE_RUNS=n sets the runs per engine (default
 * 2). The run prints one block per engine and writes every number to
 * test/perf/results/idb-size-<run>-<project>.json. IDBSIZE_KEEP=1 keeps the
 * profiles and prints where they are.
 *
 * The corpus is the one engine/storage/sqlitestore/searchsize_test.go
 * measures on the server: 2000 issues with an 8-word title and a 300-word body
 * of Zipf-distributed words from a 20,000-word vocabulary, plus 20 projects
 * and 50 users. The text is the same word for word; the keys and the
 * workspace id are shaped like `newId()` ids. See benchapp.SeedSearchSize.
 *
 * Each run opens a fresh persistent profile, bootstraps the whole workspace,
 * waits for the client's `indexing` event (every value and search index
 * built), counts the store from inside the page, and closes the browser. Then
 * it sizes the profile's IndexedDB files:
 *
 *   - `closed`: right after that first close.
 *   - `reopen N`: after a launch that only opens the database, waits and
 *     closes again, repeated until the size stops moving (at most
 *     IDBSIZE_REOPENS times, default 3). LevelDB (Chromium) compacts lazily,
 *     and these launches are what give it the chance. The last one is the
 *     `settled` size.
 *
 * The `logical` block depends only on the corpus and the key layout, not on
 * the engine or on compaction. It is the number to iterate against; the disk
 * numbers say what one logical character costs in each engine.
 */
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { type BrowserContext, expect, test } from '@playwright/test';

import { BenchTab } from '../perf/fixtures.js';
import { type IdbDisk, measureIdb } from './disk.js';
import { SUMMARY_ENV, WORKSPACE_ENV } from './global-setup.js';
import { type LogicalBreakdown, logicalBreakdown } from './logical.js';

const ACCOUNT = 'idbsize';
const DATABASE = `sync:${ACCOUNT}`;
/** Rows the corpus holds: 2000 issues, 20 projects, 50 users. */
const ROWS = { issue: 2000, project: 20, user: 50 };

interface RunResult {
	run: number;
	bootstrapMs: number;
	indexWaitMs: number;
	/** navigator.storage.estimate().usage while the client was open. */
	estimateBytes: number;
	logical: LogicalBreakdown;
	closed: IdbDisk;
	/** One measurement per reopen; the last is the settled size. */
	reopened: IdbDisk[];
}

function kb(bytes: number): string {
	return `${(bytes / 1024).toFixed(0)} KB`;
}

function settled(r: RunResult): IdbDisk {
	return r.reopened[r.reopened.length - 1] ?? r.closed;
}

function formatDisk(d: IdbDisk, positions: number): string[] {
	const lines: string[] = [];
	for (const f of d.files.slice(0, 4)) lines.push(`    ${kb(f.bytes).padStart(9)}  ${f.path}`);
	if (d.files.length > 4) lines.push(`    … ${d.files.length - 4} more files`);
	const s = d.sqlite;
	if (s !== undefined) {
		lines.push(
			`  sqlite: page ${s.pageSize}, ${s.pageCount} pages, ${s.freePages} free`,
			`    Records: ${s.records.rows} rows, keys ${kb(s.records.keyBytes)}, values ${
				kb(s.records.valueBytes)
			}`,
			`    IndexRecords: ${s.indexRecords.rows} rows, keys ${kb(s.indexRecords.keyBytes)}, `
				+ `values ${kb(s.indexRecords.valueBytes)}`,
		);
		for (const o of s.objects.filter((o) => o.pgsize > 8192)) {
			lines.push(
				`    ${kb(o.pgsize).padStart(9)}  ${o.name} (payload ${kb(o.payload)}, ${o.cells} cells, `
					+ `${(o.pgsize / positions).toFixed(1)} B/position)`,
			);
		}
	}
	const l = d.leveldb;
	if (l !== undefined) {
		const kinds = Object.entries(l.kinds).sort(([ a ], [ b ]) => a.localeCompare(b));
		const live = kinds.reduce((n, [ , k ]) => n + k.diskBytes, 0);
		lines.push(
			`  leveldb: ${l.tables} tables ${kb(l.tableBytes)}, `
				+ `log ${kb(l.logBytes)} (${l.logEntries} entries), `
				+ `${l.orphans} orphan files ${kb(l.orphanBytes)}, `
				+ `data blocks ${kb(l.dataBlockBytes)} on disk / ${kb(l.rawBlockBytes)} raw, `
				+ `compression ${JSON.stringify(l.compression)}`,
			`  live ${kb(live)}:`,
		);
		for (const [ kind, k ] of kinds) {
			lines.push(
				`    ${kb(k.diskBytes).padStart(9)}  ${kind.padEnd(11)} ${
					String(k.entries).padStart(6)
				} entries, `
					+ `keys ${kb(k.keyBytes)}, values ${kb(k.valueBytes)} raw, `
					+ `${(k.diskBytes / Math.max(1, k.entries)).toFixed(1)} B/entry on disk`,
			);
		}
		lines.push(`    ${kb(l.dead.diskBytes).padStart(9)}  dead        ${l.dead.entries} entries`);
	}
	return lines;
}

function formatRun(r: RunResult): string {
	const positions = r.logical.totals.positions;
	const last = settled(r);
	return [
		`run ${r.run}: bootstrap ${r.bootstrapMs.toFixed(0)}ms, index wait ${
			r.indexWaitMs.toFixed(0)
		}ms, `
		+ `estimate ${kb(r.estimateBytes)}`,
		`  on disk: closed ${kb(r.closed.totalBytes)}`
		+ r.reopened.map((d, i) => `, reopen ${i + 1} ${kb(d.totalBytes)}`).join(''),
		`  settled ${kb(last.totalBytes)} (${
			(last.totalBytes / positions).toFixed(1)
		} B/position overall)`,
		...formatDisk(last, positions),
	].join('\n');
}

function formatLogical(l: LogicalBreakdown): string {
	const lines = [
		`logical: ${l.totals.records} records, keys ${l.totals.keyChars} chars, `
		+ `values ~${
			kb(l.totals.valueBytes)
		}, ${l.totals.positions} positions (${l.totals.positionChars} chars)`,
	];
	for (const [ space, k ] of Object.entries(l.keyspaces)) {
		lines.push(
			`  ${space.padEnd(18)} ${String(k.records).padStart(5)} records, keys ${k.keyChars} chars, `
				+ `values ~${kb(k.valueBytes)}, rows ${kb(k.rowBytes)}`,
		);
	}
	for (const [ kind, p ] of Object.entries(l.positions)) {
		lines.push(
			`  ${
				kind.padEnd(6)
			} positions: ${p.positions} on ${p.records} records, ${p.positionChars} chars `
				+ `(${
					(p.positionChars / p.positions).toFixed(1)
				}/position, ${p.minPositionChars}..${p.maxPositionChars}), `
				+ `record keys ${p.keyChars} chars, key chars per position ${p.keyCharsPerPosition}, `
				+ `${p.highChars} chars > 0x7f`,
		);
	}
	return lines.join('\n');
}

test('IndexedDB size of a text-heavy workspace', async ({ playwright, browserName, baseURL, channel }) => {
	const workspaceId = process.env[WORKSPACE_ENV];
	if (workspaceId === undefined) {
		throw new Error(`${WORKSPACE_ENV} unset — global setup did not run`);
	}
	const runs = Number(process.env['IDBSIZE_RUNS'] ?? 2);
	const reopens = Number(process.env['IDBSIZE_REOPENS'] ?? 3);
	const engine = playwright[browserName];
	const launch = (dir: string): Promise<BrowserContext> =>
		engine.launchPersistentContext(dir, { baseURL, channel, headless: true });

	const results: RunResult[] = [];
	for (let run = 1; run <= runs; run++) {
		const profile = mkdtempSync(join(tmpdir(), `idbsize-${browserName}-`));
		try {
			let context = await launch(profile);
			const tab = await BenchTab.open(context, { workspaceId, accountId: ACCOUNT, scope: 'full' });
			const started = await tab.start();
			const indexes = await tab.waitForIndexes();
			expect(await tab.counts()).toMatchObject(ROWS);
			const estimate = await tab.storage();
			const logical = await tab.page.evaluate(logicalBreakdown, DATABASE);
			await tab.closeTab();
			await context.close();
			const closed = measureIdb(profile, browserName);

			// These launches only open the database, so nothing is written but
			// what the engine decides to fold or compact on its own.
			const reopened: IdbDisk[] = [];
			for (let i = 0; i < reopens; i++) {
				context = await launch(profile);
				const page = await context.newPage();
				await page.goto('/');
				await page.evaluate(async (name) => {
					const db = await new Promise<IDBDatabase>((resolve, reject) => {
						const req = indexedDB.open(name);
						req.onsuccess = () => resolve(req.result);
						req.onerror = () => reject(req.error);
					});
					db.close();
				}, DATABASE);
				await page.waitForTimeout(5_000);
				await page.close();
				await context.close();
				const disk = measureIdb(profile, browserName);
				const before = reopened[reopened.length - 1] ?? closed;
				reopened.push(disk);
				if (disk.totalBytes === before.totalBytes) break;
			}

			results.push({
				run,
				bootstrapMs: started.wallMs,
				indexWaitMs: indexes.waitedMs,
				estimateBytes: estimate.usage,
				logical,
				closed,
				reopened,
			});
		} finally {
			if (process.env['IDBSIZE_KEEP'] === undefined) rmSync(profile, { recursive: true, force: true });
			else console.log(`[idbsize] kept profile ${profile}`);
		}
	}

	const project = test.info().project.name;
	const first = results[0];
	if (first === undefined) throw new Error('no runs');
	const totals = results.map((r) => settled(r).totalBytes);
	console.log(
		[
			`\n[idbsize] ${project} (${browserName}${channel === undefined ? '' : `, ${channel}`})`,
			formatLogical(first.logical),
			...results.map(formatRun),
			`settled across runs: ${totals.map(kb).join(', ')} `
			+ `(spread ${kb(Math.max(...totals) - Math.min(...totals))})`,
		].join('\n'),
	);
	// The logical counts are a function of the corpus and the layout alone, so
	// two runs that disagree mean the measurement is not what it claims.
	for (const r of results) expect(r.logical).toEqual(first.logical);

	const summary = process.env[SUMMARY_ENV];
	if (summary !== undefined) {
		writeFileSync(
			summary.replace(/\.json$/, `-${project}.json`),
			JSON.stringify(results, null, '\t'),
		);
	}
});
