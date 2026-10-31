import { execFileSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdtempSync, readdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';

import { analyzeLevelDb, type LevelDbBreakdown } from './leveldb.js';

/** One file of a browser profile, with its size in bytes. */
export interface ProfileFile {
	path: string;
	bytes: number;
}

/** Every regular file under `dir`, with paths relative to it. */
export function listFiles(dir: string): ProfileFile[] {
	const out: ProfileFile[] = [];
	const walk = (at: string) => {
		for (const entry of readdirSync(at, { withFileTypes: true })) {
			const full = join(at, entry.name);
			if (entry.isDirectory()) walk(full);
			else if (entry.isFile()) out.push({ path: relative(dir, full), bytes: statSync(full).size });
		}
	};
	walk(dir);
	return out.sort((a, b) => a.path.localeCompare(b.path));
}

/** What IndexedDB keeps on disk for one profile. */
export interface IdbDisk {
	/** The files that hold IndexedDB data, largest first. */
	files: ProfileFile[];
	totalBytes: number;
	/** Chromium only: the LevelDB directory, broken down by entry kind. */
	leveldb?: LevelDbBreakdown;
	/** WebKit only: the SQLite database, broken down per table and index. */
	sqlite?: SqliteBreakdown;
}

/**
 * Finds the IndexedDB files of a closed profile and sizes them. Chromium keeps
 * an origin's databases in one LevelDB directory, `<origin>.indexeddb.leveldb`,
 * under `Default/IndexedDB/`; WebKit keeps one SQLite file per database,
 * `IndexedDB.sqlite3`, under the profile's `IndexedDB/` directory. Anything else
 * with `indexeddb` in its path counts too, so a layout change shows up as
 * files rather than as a smaller number.
 */
export function measureIdb(profile: string, engine: string): IdbDisk {
	const files = listFiles(profile)
		.filter((f) => /indexeddb/i.test(f.path))
		.sort((a, b) => b.bytes - a.bytes);
	const totalBytes = files.reduce((n, f) => n + f.bytes, 0);
	const disk: IdbDisk = { files, totalBytes };
	if (engine === 'chromium') {
		const current = files.find((f) => /\.indexeddb\.leveldb\/CURRENT$/.test(f.path));
		if (current !== undefined) disk.leveldb = analyzeLevelDb(join(profile, dirname(current.path)));
	}
	if (engine === 'webkit') {
		// The client's database is the large one; the others are small or empty.
		const main = files.find((f) => f.path.endsWith('IndexedDB.sqlite3'));
		if (main !== undefined) disk.sqlite = analyzeSqlite(join(profile, main.path));
	}
	return disk;
}

/** Bytes of one SQLite table or index, from the `dbstat` virtual table. */
export interface SqliteObject {
	name: string;
	/** Pages times page size: what the object takes in the file. */
	pgsize: number;
	/** Bytes of cell content, without page headers and free space. */
	payload: number;
	unused: number;
	cells: number;
}

export interface SqliteColumnSums {
	rows: number;
	keyBytes: number;
	valueBytes: number;
}

export interface SqliteBreakdown {
	pageSize: number;
	pageCount: number;
	freePages: number;
	objects: SqliteObject[];
	records: SqliteColumnSums;
	indexRecords: SqliteColumnSums;
	/** Hex of one record key and one index-record key, to read the key encoding. */
	sampleRecordKey: string;
	sampleIndexKey: string;
	sampleIndexValue: string;
}

/**
 * Breaks a WebKit IndexedDB database down per table and index. Works on a
 * copy, with its WAL folded in, so the profile stays as the browser left it.
 * Needs the `sqlite3` command with the `dbstat` table (the macOS one has it).
 */
export function analyzeSqlite(path: string): SqliteBreakdown {
	const dir = mkdtempSync(join(tmpdir(), 'idbsize-sqlite-'));
	const copy = join(dir, 'copy.sqlite3');
	try {
		copyFileSync(path, copy);
		for (const suffix of [ '-wal', '-shm' ]) {
			if (existsSync(path + suffix)) copyFileSync(path + suffix, copy + suffix);
		}
		const q = (sql: string): string[][] =>
			execFileSync('sqlite3', [ '-separator', '\t', copy, sql ], { encoding: 'utf8' })
				.trim()
				.split('\n')
				.filter((l) => l !== '')
				.map((l) => l.split('\t'));
		const one = (sql: string): string => q(sql)[0]?.[0] ?? '';
		q('PRAGMA wal_checkpoint(TRUNCATE)');
		const objects = q(
			`SELECT name, SUM(pgsize), SUM(payload), SUM(unused), SUM(ncell)
			 FROM dbstat GROUP BY name ORDER BY SUM(pgsize) DESC`,
		).map(([ name, pgsize, payload, unused, cells ]) => ({
			name: name ?? '',
			pgsize: Number(pgsize),
			payload: Number(payload),
			unused: Number(unused),
			cells: Number(cells),
		}));
		const sums = (table: string): SqliteColumnSums => {
			const [ rows, keyBytes, valueBytes ] = q(
				`SELECT COUNT(*), SUM(length(CAST(key AS BLOB))), SUM(length(CAST(value AS BLOB)))
				 FROM ${table}`,
			)[0] ?? [];
			return { rows: Number(rows), keyBytes: Number(keyBytes), valueBytes: Number(valueBytes) };
		};
		return {
			pageSize: Number(one('PRAGMA page_size')),
			pageCount: Number(one('PRAGMA page_count')),
			freePages: Number(one('PRAGMA freelist_count')),
			objects,
			records: sums('Records'),
			indexRecords: sums('IndexRecords'),
			sampleRecordKey: one(
				`SELECT hex(key) FROM Records WHERE rowid = (SELECT MAX(rowid) FROM Records)`,
			),
			sampleIndexKey: one(`SELECT hex(key) FROM IndexRecords LIMIT 1 OFFSET 1000`),
			sampleIndexValue: one(`SELECT hex(value) FROM IndexRecords LIMIT 1 OFFSET 1000`),
		};
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
}
