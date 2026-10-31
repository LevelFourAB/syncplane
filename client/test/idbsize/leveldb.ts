import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Bytes of one kind of Chromium IndexedDB entry in a LevelDB directory's
 * tables and logs. Only the newest version of each key counts as live; older
 * versions and deletions wait for compaction and count as dead.
 */
export interface EntryKind {
	/** Live entries. */
	entries: number;
	/** Uncompressed bytes of the live entries' keys, LevelDB's 8-byte tag included. */
	keyBytes: number;
	/** Uncompressed bytes of the live entries' values. */
	valueBytes: number;
	/**
	 * Bytes of the files these entries take: each data block's size on disk,
	 * split over its entries by their uncompressed size, and a log entry's own
	 * size. Dead entries, block indexes and filters are left out, so the kinds
	 * sum to less than the files.
	 */
	diskBytes: number;
}

export interface LevelDbBreakdown {
	/** Tables the current MANIFEST uses. */
	tables: number;
	/**
	 * Tables and logs the current MANIFEST does not use: left behind by a
	 * compaction or a flush the browser did not finish before it exited.
	 * LevelDB deletes them when it next opens the directory. Not read.
	 */
	orphans: number;
	orphanBytes: number;
	/** Bytes of the tables the current MANIFEST uses. */
	tableBytes: number;
	/** Bytes of the data blocks on disk, block trailers included. */
	dataBlockBytes: number;
	/** Bytes of the data blocks before compression. */
	rawBlockBytes: number;
	/** Compression types seen, by LevelDB's block type byte, in blocks. */
	compression: Record<string, number>;
	/**
	 * Live entries by what Chromium keeps in them: `record c` (an object store
	 * record, by the client keyspace of its key), `exists` (the version entry
	 * beside each record), `index x` (one entry per index key of a record, by
	 * the keyspace of that record), `blob`, `scopes` (Chromium's transaction
	 * undo and cleanup logs) and `meta` for the rest of the database and
	 * global metadata.
	 */
	kinds: Record<string, EntryKind>;
	/** Bytes of the `.log` files, and the entries they hold. */
	logBytes: number;
	logEntries: number;
	/** Entries shadowed by a newer version of their key, or deletions. */
	dead: { entries: number; diskBytes: number };
}

/** One version of a key: its sequence number, kind and sizes. */
interface Version {
	seq: number;
	deleted: boolean;
	kind: string;
	/** Key and value bytes, uncompressed. */
	k: number;
	v: number;
	/** Disk bytes attributed to it. */
	d: number;
}

/** Reads a LevelDB varint at `at`; returns the value and the next offset. */
function varint(buf: Uint8Array, at: number): [number, number] {
	let result = 0;
	let shift = 0;
	for (;;) {
		const b = buf[at++]!;
		result += (b & 0x7f) * 2 ** shift;
		if ((b & 0x80) === 0) return [ result, at ];
		shift += 7;
	}
}

/** Snappy block decompression (the raw format, not the framing format). */
function unsnappy(src: Uint8Array): Uint8Array {
	let [ length, at ] = varint(src, 0);
	const out = new Uint8Array(length);
	let o = 0;
	while (at < src.length) {
		const tag = src[at++]!;
		const type = tag & 3;
		if (type === 0) {
			let len = tag >>> 2;
			if (len >= 60) {
				const bytes = len - 59;
				len = 0;
				for (let i = 0; i < bytes; i++) len |= src[at++]! << (8 * i);
			}
			len += 1;
			out.set(src.subarray(at, at + len), o);
			at += len;
			o += len;
			continue;
		}
		let len: number;
		let offset: number;
		if (type === 1) {
			len = 4 + ((tag >>> 2) & 7);
			offset = ((tag >>> 5) << 8) | src[at++]!;
		} else if (type === 2) {
			len = (tag >>> 2) + 1;
			offset = src[at]! | (src[at + 1]! << 8);
			at += 2;
		} else {
			len = (tag >>> 2) + 1;
			offset = (src[at]! | (src[at + 1]! << 8) | (src[at + 2]! << 16) | (src[at + 3]! << 24)) >>> 0;
			at += 4;
		}
		// Byte by byte: a copy may overlap the bytes it is writing.
		for (let i = 0; i < len; i++, o++) out[o] = out[o - offset]!;
	}
	return out;
}

/** The entries of one decompressed LevelDB block, as [key, value] pairs. */
function blockEntries(block: Uint8Array): [Uint8Array, Uint8Array][] {
	const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
	const restarts = view.getUint32(block.length - 4, true);
	const end = block.length - 4 - restarts * 4;
	const out: [Uint8Array, Uint8Array][] = [];
	let key = new Uint8Array(0);
	let at = 0;
	while (at < end) {
		let shared: number;
		let unshared: number;
		let valueLen: number;
		[ shared, at ] = varint(block, at);
		[ unshared, at ] = varint(block, at);
		[ valueLen, at ] = varint(block, at);
		const next = new Uint8Array(shared + unshared);
		next.set(key.subarray(0, shared), 0);
		next.set(block.subarray(at, at + unshared), shared);
		at += unshared;
		key = next;
		out.push([ key, block.subarray(at, at + valueLen) ]);
		at += valueLen;
	}
	return out;
}

/** Reads the block a handle points at, returning its contents and type. */
function readBlock(
	file: Uint8Array,
	offset: number,
	size: number,
): { raw: Uint8Array; type: number } {
	const type = file[offset + size]!;
	const stored = file.subarray(offset, offset + size);
	if (type === 0) return { raw: stored, type };
	if (type === 1) return { raw: unsnappy(stored), type };
	throw new Error(`leveldb: unsupported block compression type ${type}`);
}

/**
 * The keyspace of the client key an encoded Chromium IndexedDB string key
 * starting at `at` holds: its text up to the first `/` (`c`, `x`, `meta`). A
 * string key is the type byte 1, a varint length in characters and the
 * characters as big-endian UTF-16.
 */
function clientKeyspace(buf: Uint8Array, at: number): string {
	if (buf[at] !== 1) return '?';
	const [ len, start ] = varint(buf, at + 1);
	let out = '';
	for (let i = 0; i < len; i++) {
		const c = (buf[start + 2 * i]! << 8) | buf[start + 2 * i + 1]!;
		if (c === 0x2f) break;
		out += String.fromCharCode(c);
	}
	return out;
}

/**
 * The kind of a Chromium IndexedDB entry, from its key's KeyPrefix: one byte
 * giving the byte lengths of the database, object store and index ids, then
 * the ids as little-endian integers. Index id 1 is the object store's
 * records, 2 their exists entries, 3 their blob entries, and 30 and up a
 * declared index. Database id 0 is global metadata.
 *
 * Records and index entries also name the client keyspace of their record,
 * as `record c` or `index x`: a record's key is its primary key, and an index
 * entry's value is a varint version and then the primary key.
 */
function kindOf(key: Uint8Array, value?: Uint8Array): string {
	const first = key[0]!;
	const dbLen = (first >>> 5) + 1;
	const osLen = ((first >>> 2) & 7) + 1;
	const ixLen = (first & 3) + 1;
	const int = (at: number, len: number) => {
		let v = 0;
		for (let i = 0; i < len; i++) v += key[at + i]! * 2 ** (8 * i);
		return v;
	};
	const db = int(1, dbLen);
	const os = int(1 + dbLen, osLen);
	const ix = int(1 + dbLen + osLen, ixLen);
	const prefix = 1 + dbLen + osLen + ixLen;
	// Global metadata type 50 is the LevelDB scopes keyspace: the undo and
	// cleanup logs Chromium writes for a large transaction.
	if (db === 0 && key[prefix] === 50) return 'scopes';
	if (db === 0 || os === 0) return 'meta';
	if (ix === 1) return `record ${clientKeyspace(key, prefix)}`;
	if (ix === 2) return 'exists';
	if (ix === 3) return 'blob';
	if (ix >= 30) {
		if (value === undefined) return 'index';
		return `index ${clientKeyspace(value, varint(value, 0)[1])}`;
	}
	return 'meta';
}

/**
 * The records of a LevelDB log-format file (a write-ahead log or a
 * MANIFEST), in order. The file is 32 KiB blocks; a record is one FULL
 * fragment or a run of FIRST, MIDDLE and LAST fragments.
 */
function logRecords(file: Uint8Array): Uint8Array[] {
	const records: Uint8Array[] = [];
	const BLOCK = 32 * 1024;
	let pending: Uint8Array[] = [];
	let at = 0;
	while (at + 7 <= file.length) {
		const left = BLOCK - (at % BLOCK);
		if (left < 7) {
			at += left;
			continue;
		}
		const len = file[at + 4]! | (file[at + 5]! << 8);
		const type = file[at + 6]!;
		const data = file.subarray(at + 7, at + 7 + len);
		at += 7 + len;
		if (type === 0 && len === 0) break; // preallocated, never written
		if (type === 1) records.push(data);
		else if (type === 2) pending = [ data ];
		else if (type === 3) pending.push(data);
		else if (type === 4) {
			pending.push(data);
			records.push(Buffer.concat(pending));
			pending = [];
		}
	}
	return records;
}

/**
 * One write batch of a LevelDB log: the sequence number of its first entry,
 * and its entries. An entry with no value is a deletion.
 */
function writeBatch(
	b: Uint8Array,
): { seq: number; entries: { key: Uint8Array; value?: Uint8Array }[] } {
	const view = new DataView(b.buffer, b.byteOffset, b.byteLength);
	const seq = Number(view.getBigUint64(0, true));
	const count = view.getUint32(8, true);
	const entries: { key: Uint8Array; value?: Uint8Array }[] = [];
	let at = 12;
	for (let i = 0; i < count; i++) {
		const tag = b[at++]!;
		let len: number;
		[ len, at ] = varint(b, at);
		const key = b.subarray(at, at + len);
		at += len;
		if (tag === 1) {
			[ len, at ] = varint(b, at);
			entries.push({ key, value: b.subarray(at, at + len) });
			at += len;
		} else {
			entries.push({ key });
		}
	}
	return { seq, entries };
}

/**
 * The files the current version of a LevelDB directory uses, from the
 * MANIFEST that CURRENT names: the table numbers its edits added and did not
 * delete, and the oldest log number it still needs.
 */
function liveFiles(dir: string): { tables: Set<number>; minLog: number } {
	const manifest = readFileSync(join(dir, 'CURRENT'), 'utf8').trim();
	const tables = new Set<number>();
	let log = 0;
	let prevLog = 0;
	for (const edit of logRecords(new Uint8Array(readFileSync(join(dir, manifest))))) {
		let at = 0;
		const num = () => {
			const [ v, next ] = varint(edit, at);
			at = next;
			return v;
		};
		const skipSlice = () => {
			// Not `at += num()`: that reads `at` before num() moves it.
			const len = num();
			at += len;
		};
		while (at < edit.length) {
			const tag = num();
			switch (tag) {
				case 1: // comparator name
					skipSlice();
					break;
				case 2:
					log = num();
					break;
				case 3: // next file number
				case 4: // last sequence
					num();
					break;
				case 5: // compact pointer: level, key
					num();
					skipSlice();
					break;
				case 6: // deleted file: level, number
					num();
					tables.delete(num());
					break;
				case 7: // new file: level, number, size, smallest, largest
					num();
					tables.add(num());
					num();
					skipSlice();
					skipSlice();
					break;
				case 9:
					prevLog = num();
					break;
				default:
					throw new Error(`leveldb: unknown MANIFEST tag ${tag}`);
			}
		}
	}
	return { tables, minLog: prevLog === 0 ? log : Math.min(log, prevLog) };
}

/**
 * Breaks down the tables and logs of one Chromium IndexedDB LevelDB
 * directory that no process has open. A log entry counts its raw size as its
 * disk bytes, since logs are not compressed.
 */
export function analyzeLevelDb(dir: string): LevelDbBreakdown {
	const live = liveFiles(dir);
	const number = (name: string) => Number.parseInt(name, 10);
	const files = readdirSync(dir).sort();
	const tables = files.filter((f) => f.endsWith('.ldb') && live.tables.has(number(f)));
	const logs = files.filter((f) => f.endsWith('.log') && number(f) >= live.minLog);
	const orphans = files.filter((f) =>
		(f.endsWith('.ldb') && !live.tables.has(number(f)))
		|| (f.endsWith('.log') && number(f) < live.minLog)
	);
	const out: LevelDbBreakdown = {
		tables: tables.length,
		orphans: orphans.length,
		orphanBytes: orphans.reduce((n, f) => n + statSync(join(dir, f)).size, 0),
		tableBytes: 0,
		dataBlockBytes: 0,
		rawBlockBytes: 0,
		compression: {},
		kinds: {},
		logBytes: 0,
		logEntries: 0,
		dead: { entries: 0, diskBytes: 0 },
	};
	// The newest version of each user key, keyed by a digest to bound memory.
	const newest = new Map<string, Version>();
	const shadowed = (prev: { d: number } | undefined) => {
		if (prev === undefined) return;
		out.dead.entries++;
		out.dead.diskBytes += prev.d;
	};
	const offer = (user: Uint8Array, entry: Version) => {
		const digest = createHash('sha1').update(user).digest('base64');
		const prev = newest.get(digest);
		if (prev === undefined || prev.seq < entry.seq) {
			shadowed(prev);
			newest.set(digest, entry);
		} else {
			shadowed(entry);
		}
	};
	for (const name of tables) {
		const file = new Uint8Array(readFileSync(join(dir, name)));
		out.tableBytes += file.length;
		const footer = file.subarray(file.length - 48);
		let at = 0;
		[ , at ] = varint(footer, at); // metaindex offset
		[ , at ] = varint(footer, at); // metaindex size
		let indexOffset: number;
		let indexSize: number;
		[ indexOffset, at ] = varint(footer, at);
		[ indexSize, at ] = varint(footer, at);
		const index = readBlock(file, indexOffset, indexSize).raw;
		for (const [ , handle ] of blockEntries(index)) {
			const [ offset, next ] = varint(handle, 0);
			const [ size ] = varint(handle, next);
			const { raw, type } = readBlock(file, offset, size);
			out.compression[String(type)] = (out.compression[String(type)] ?? 0) + 1;
			const disk = size + 5;
			out.dataBlockBytes += disk;
			out.rawBlockBytes += raw.length;
			const entries = blockEntries(raw);
			const total = entries.reduce((n, [ k, v ]) => n + k.length + v.length, 0);
			for (const [ k, v ] of entries) {
				const user = k.subarray(0, k.length - 8);
				const tag = new DataView(k.buffer, k.byteOffset + k.length - 8, 8);
				const seq = Number(tag.getBigUint64(0, true) >> 8n);
				const deleted = (tag.getUint8(0) & 0xff) === 0;
				offer(user, {
					seq,
					deleted,
					kind: kindOf(user, v),
					k: k.length,
					v: v.length,
					d: total === 0 ? 0 : (disk * (k.length + v.length)) / total,
				});
			}
		}
	}
	for (const name of logs) {
		const file = new Uint8Array(readFileSync(join(dir, name)));
		out.logBytes += file.length;
		for (const batch of logRecords(file).map(writeBatch)) {
			batch.entries.forEach((e, i) => {
				out.logEntries++;
				const size = e.key.length + (e.value?.length ?? 0);
				offer(e.key, {
					seq: batch.seq + i,
					deleted: e.value === undefined,
					kind: kindOf(e.key, e.value),
					k: e.key.length + 8,
					v: e.value?.length ?? 0,
					d: size,
				});
			});
		}
	}
	for (const e of newest.values()) {
		if (e.deleted) {
			shadowed(e);
			continue;
		}
		const kind = out.kinds[e.kind] ??= { entries: 0, keyBytes: 0, valueBytes: 0, diskBytes: 0 };
		kind.entries++;
		kind.keyBytes += e.k;
		kind.valueBytes += e.v;
		kind.diskBytes += e.d;
	}
	out.dead.diskBytes = Math.round(out.dead.diskBytes);
	for (const kind of Object.values(out.kinds)) kind.diskBytes = Math.round(kind.diskBytes);
	return out;
}
