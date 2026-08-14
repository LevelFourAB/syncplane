/**
 * What the optimistic text log trades: the CPU it takes off the write path
 * against the CPU it puts on the read path.
 *
 * A text edit used to be folded into the overlay row as it was written — decode
 * the row, union the fragment into the document, re-encode both — and reads then
 * returned the stored bytes for free. It is now appended to the `ot/` log
 * instead, and the fold happens on every read that resolves the row (read.ts).
 * 109 measured the BYTES that moved (149,052 → 585 per keystroke); this measures
 * the work, which did not disappear so much as change ends.
 *
 * Read it as two questions rather than six numbers:
 *
 *   write — is appending cheaper than merging the whole document? (two arms)
 *   read  — is what the fold costs in return bounded?             (four arms)
 *
 * The write arms are the old whole-row merge against the append that replaced
 * it. On the read side the floor is a row with no log to fold at all, and the
 * rest are measured against that.
 *
 * A fold is flat in the number of logged edits — 1 and 10 land within noise of
 * each other, because it decodes and encodes the document once however many
 * fragments it applies, and what N costs is the apply, which is O(edit). N is
 * how many edits are unacked, since the log holds exactly what the pending queue
 * does and a rebase empties both: 1 is a keystroke that raced a push, 10 a burst
 * pushed as one. Anything that reintroduces a decode per fragment shows up here
 * as those two numbers separating.
 *
 * What a fold is NOT flat in is the document, so the last arm is whether the
 * row's fold is already CACHED (textfold.ts). Both states are real: within one
 * keystroke's cycle several paths resolve the same row and only the first of
 * them can miss, while the first read after a commit starts from what the last
 * one left behind. A cold arm gets a fresh EntityRegistry per iteration, that
 * being what the cache is keyed by — which also puts its ~65 µs of construction
 * inside the timing: visible at 5000cp, noise above it.
 *
 * Two things about the harness. Every store is built once, outside the timed
 * region, and the write arms reuse theirs across iterations — sound because both
 * writes are overwrites at a fixed key: the same mutation id logs to the same
 * key, so the log does not grow under the benchmark. The read arms all go
 * through `cloningRead`, for the reason given there. The disk cost that all of
 * this replaces is 109's measurement, in the browser.
 */

import { create, toBinary } from '@bufbuild/protobuf';
import { FieldMaskSchema } from '@bufbuild/protobuf/wkt';
import { bench, describe } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	overlayKey,
	type OverlayRecord,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVRead } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import {
	applyCrudLocally,
	applyMutationLocally,
	type MirrorMap,
} from '../../src/optimistic/rebase.js';
import { readMergedRow } from '../../src/read.js';
import { decodeTextDoc, encodeFragmentFrom, encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws';
const registry = new EntityRegistry([ WidgetSchema ]);
const noMirrors: MirrorMap = {};
const SIZES = [ 5000, 20000, 50000 ];

function widgetRow(body: Uint8Array): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label: '', body }));
}

function document(size: number): Uint8Array {
	const d = new TextDoc('p0');
	d.insert(0, 'a'.repeat(size));
	return encodeTextDoc(d);
}

/** One edit's new state and the fragment standing for it, as a handle produces. */
function typeInto(state: Uint8Array, index: number, s: string) {
	const d = decodeTextDoc(state, 'p1');
	const range = d.insert(index, s);
	const ids = Array.from(
		{ length: range.count },
		(_, i) => ({ peer: range.peer, counter: range.start + i }),
	);
	return { state: encodeTextDoc(d), fragment: encodeFragmentFrom(d, ids, []) };
}

function bodyPatch(id: bigint, body: Uint8Array): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'widget',
				key: 'w1',
				op: CrudOp.PATCH,
				fieldMask: create(FieldMaskSchema, { paths: [ 'body' ] }),
				value: toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body })),
			},
		},
	});
}

const crudOf = (m: Mutation) =>
	m.payload.case === 'crud' ? m.payload.value : (() => {
		throw new Error('not a crud');
	})();

/** A store holding the document confirmed, plus `edits` logged fragments. */
async function seeded(base: Uint8Array, edits: number): Promise<MemoryKVStore> {
	const kv = new MemoryKVStore();
	const tx = await kv.write();
	const rec: ConfirmedRecord = { v: widgetRow(base), ver: '1' };
	await tx.put(confirmedKey('widget', WS, 'w1'), rec);
	let state = base;
	for (let i = 0; i < edits; i++) {
		const edit = typeInto(state, 0, 'x');
		state = edit.state;
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			bodyPatch(BigInt(i + 1), edit.fragment),
			new TouchSet(),
		);
	}
	await tx.commit();
	return kv;
}

/** The same store as it looked before the log: the whole document in the overlay. */
async function seededWholeRow(base: Uint8Array): Promise<MemoryKVStore> {
	const kv = new MemoryKVStore();
	const tx = await kv.write();
	const rec: ConfirmedRecord = { v: widgetRow(base), ver: '1' };
	await tx.put(confirmedKey('widget', WS, 'w1'), rec);
	const record: OverlayRecord = { v: widgetRow(base) };
	await tx.put(overlayKey('widget', WS, 'w1'), record);
	await tx.commit();
	return kv;
}

/**
 * A read view that hands back a fresh copy of every record, the way a real
 * IndexedDB read does (structured clone) and MemoryKVStore, which returns its
 * stored objects by reference, does not.
 *
 * The read arms all go through this. Without it the fold cache validates a hit
 * by an identity check that no browser would ever satisfy, so the cached arm
 * would report a number the shipped client cannot reach — and the cold arms
 * would be measured against a cheaper read than the cached one, which is not a
 * comparison.
 */
function cloningRead(tx: KVRead): KVRead {
	const copy = (v: unknown): unknown => {
		if (v instanceof Uint8Array) return new Uint8Array(v);
		if (v !== null && typeof v === 'object') {
			return Object.fromEntries(Object.entries(v).map(([ k, x ]) => [ k, copy(x) ]));
		}
		return v;
	};
	return {
		get: async (key) => copy(await tx.get(key)),
		getMany: async (keys) => (await tx.getMany(keys)).map(copy),
		has: (key) => tx.has(key),
		async *scan(prefix) {
			for await (const [ k, v ] of tx.scan(prefix)) yield [ k, copy(v) ];
		},
		indexScan: (lo, hi) => tx.indexScan(lo, hi),
		indexCount: (lo, hi) => tx.indexCount(lo, hi),
		indexSeek: (lo, hi, dir) => tx.indexSeek(lo, hi, dir),
		async *scanRange(lo, hi) {
			for await (const [ k, v ] of tx.scanRange(lo, hi)) yield [ k, copy(v) ];
		},
		release: () => tx.release(),
	};
}

for (const size of SIZES) {
	const base = document(size);
	const edit = typeInto(base, 0, 'x');
	const mutation = bodyPatch(1n, edit.fragment);

	const wholeRowStore = await seededWholeRow(base);
	const logStore = await seeded(base, 0);
	const readStores = {
		stored: await seededWholeRow(base),
		one: await seeded(base, 1),
		ten: await seeded(base, 10),
	};

	describe(`one keystroke, write path (${size}cp)`, () => {
		// What a keystroke used to do: fold the fragment into the row, store the
		// merged document. Reached by giving applyCrudLocally no log slot, which is
		// also what every non-text-delta write still does.
		bench('whole-row merge', async () => {
			const tx = await wholeRowStore.write();
			await applyCrudLocally(tx, registry, WS, crudOf(mutation), new TouchSet());
			await tx.commit();
		});

		bench('append to the log', async () => {
			const tx = await logStore.write();
			await applyMutationLocally(tx, registry, noMirrors, WS, mutation, new TouchSet());
			await tx.commit();
		});
	});

	describe(`reading the row (${size}cp)`, () => {
		bench('stored bytes, nothing to fold', async () => {
			const tx = cloningRead(await readStores.stored.read());
			await readMergedRow(tx, registry, 'widget', WS, 'w1');
			tx.release();
		});

		bench('fold 1 logged edit, cold', async () => {
			const tx = cloningRead(await readStores.one.read());
			await readMergedRow(tx, new EntityRegistry([ WidgetSchema ]), 'widget', WS, 'w1');
			tx.release();
		});

		bench('fold 10 logged edits, cold', async () => {
			const tx = cloningRead(await readStores.ten.read());
			await readMergedRow(tx, new EntityRegistry([ WidgetSchema ]), 'widget', WS, 'w1');
			tx.release();
		});

		// The same read again, with nothing changed under it: the fold is a pure
		// function of the row bytes and the fragments, so the cached one still
		// stands — and proving that is a comparison over the row plus one per
		// fragment, where redoing it is a decode and an encode of the document.
		bench('fold 10 logged edits, cached', async () => {
			const tx = cloningRead(await readStores.ten.read());
			await readMergedRow(tx, registry, 'widget', WS, 'w1');
			tx.release();
		});
	});
}
