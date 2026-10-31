/**
 * What the client's KV store holds, counted from inside the page. These counts
 * depend only on the corpus and the client's key layout, never on the
 * engine's storage format or on when it compacts, so two runs of one build
 * give the same numbers.
 */
export interface LogicalBreakdown {
	/**
	 * Records per keyspace: the key up to its second `/` (`c/issue`, `meta/ws`),
	 * with every `intent/<tabId>` key under `intent`.
	 */
	keyspaces: Record<string, KeyspaceCounts>;
	/** Index positions by kind: `value` on `c/` records, `search` on `x/` records. */
	positions: Record<string, PositionCounts>;
	totals: {
		records: number;
		keyChars: number;
		valueBytes: number;
		positions: number;
		positionChars: number;
	};
}

export interface KeyspaceCounts {
	records: number;
	/** Characters of the primary keys. */
	keyChars: number;
	/**
	 * Bytes of the value, counted as the sum of its binary fields' bytes and its
	 * strings' characters (positions included). An estimate of the structured
	 * clone, which the browser does not expose: it leaves out the clone's
	 * per-field tags and lengths.
	 */
	valueBytes: number;
	/** Bytes of the serialized row (`ConfirmedRecord.v`) alone. */
	rowBytes: number;
}

export interface PositionCounts {
	/** Records that hold at least one position of this kind. */
	records: number;
	/** Characters of the primary keys of those records. */
	keyChars: number;
	/** Positions: one index entry each. */
	positions: number;
	/** Characters of all positions. */
	positionChars: number;
	/**
	 * The primary key's characters once per position. Each index entry stores
	 * the primary key of its record, so this is how many key characters the
	 * index repeats.
	 */
	keyCharsPerPosition: number;
	/** Code units above 0x7f in positions, which some encodings store wider. */
	highChars: number;
	minPositionChars: number;
	maxPositionChars: number;
}

/**
 * Counts every record of the `kv` object store of database `name`. Runs in the
 * page through `page.evaluate`, so it is self-contained: it reads IndexedDB
 * directly, with no import from the client. It opens a second connection and
 * only reads, so the client may stay open.
 */
export async function logicalBreakdown(name: string): Promise<LogicalBreakdown> {
	const db = await new Promise<IDBDatabase>((resolve, reject) => {
		const req = indexedDB.open(name);
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
		req.onupgradeneeded = () => reject(new Error(`database ${name} does not exist`));
	});
	const keyspaces: Record<string, KeyspaceCounts> = {};
	const positions: Record<string, PositionCounts> = {};

	const sizeOf = (v: unknown): number => {
		if (typeof v === 'string') return v.length;
		if (v instanceof Uint8Array) return v.byteLength;
		if (typeof v === 'number' || typeof v === 'boolean') return 8;
		if (Array.isArray(v)) return v.reduce((n: number, e) => n + sizeOf(e), 0);
		if (v !== null && typeof v === 'object') {
			let n = 0;
			for (const [ k, e ] of Object.entries(v)) n += k.length + sizeOf(e);
			return n;
		}
		return 0;
	};

	try {
		await new Promise<void>((resolve, reject) => {
			const tx = db.transaction('kv', 'readonly');
			const req = tx.objectStore('kv').openCursor();
			req.onerror = () => reject(req.error);
			req.onsuccess = () => {
				const cursor = req.result;
				if (cursor === null) return resolve();
				const key = String(cursor.primaryKey);
				const value = cursor.value as Record<string, unknown> | unknown;
				const first = key.indexOf('/');
				const second = first < 0 ? -1 : key.indexOf('/', first + 1);
				// The tab id after `intent/` is random, and would make two runs
				// disagree.
				const space = key.startsWith('intent/')
					? 'intent'
					: second < 0
					? key
					: key.slice(0, second);
				const ks = keyspaces[space] ??= { records: 0, keyChars: 0, valueBytes: 0, rowBytes: 0 };
				ks.records++;
				ks.keyChars += key.length;
				ks.valueBytes += sizeOf(value);
				const fields = typeof value === 'object' && value !== null
					? value as Record<string, unknown>
					: {};
				if (fields['v'] instanceof Uint8Array) ks.rowBytes += fields['v'].byteLength;

				const ix = fields['ix'];
				if (Array.isArray(ix) && ix.length > 0) {
					const kind = key.startsWith('x/') ? 'search' : key.startsWith('c/') ? 'value' : 'other';
					const pc = positions[kind] ??= {
						records: 0,
						keyChars: 0,
						positions: 0,
						positionChars: 0,
						keyCharsPerPosition: 0,
						highChars: 0,
						minPositionChars: Number.POSITIVE_INFINITY,
						maxPositionChars: 0,
					};
					pc.records++;
					pc.keyChars += key.length;
					for (const p of ix as string[]) {
						pc.positions++;
						pc.positionChars += p.length;
						pc.keyCharsPerPosition += key.length;
						pc.minPositionChars = Math.min(pc.minPositionChars, p.length);
						pc.maxPositionChars = Math.max(pc.maxPositionChars, p.length);
						for (let i = 0; i < p.length; i++) if (p.charCodeAt(i) > 0x7f) pc.highChars++;
					}
				}
				cursor.continue();
			};
		});
	} finally {
		db.close();
	}

	const totals = { records: 0, keyChars: 0, valueBytes: 0, positions: 0, positionChars: 0 };
	for (const ks of Object.values(keyspaces)) {
		totals.records += ks.records;
		totals.keyChars += ks.keyChars;
		totals.valueBytes += ks.valueBytes;
	}
	for (const pc of Object.values(positions)) {
		totals.positions += pc.positions;
		totals.positionChars += pc.positionChars;
	}
	return { keyspaces, positions, totals };
}
