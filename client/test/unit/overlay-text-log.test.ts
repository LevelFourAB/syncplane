import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { FieldMaskSchema } from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { CrudOp, type Mutation, MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import {
	confirmedKey,
	type ConfirmedRecord,
	metaLastMutationId,
	overlayKey,
	overlayTextRowPrefix,
} from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import type { KVWrite } from '../../src/kv/store.js';
import { EntityRegistry } from '../../src/mutations.js';
import { compactPending } from '../../src/optimistic/compact.js';
import { enqueuePending } from '../../src/optimistic/queue.js';
import {
	applyMutationLocally,
	clearOverlayAndReplay,
	type MirrorMap,
} from '../../src/optimistic/rebase.js';
import { readMergedRow, scanMergedRows } from '../../src/read.js';
import { decodeTextDoc, encodeFragmentFrom, encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

/**
 * The optimistic text log (`ot/`) — the keyspace a collaborative-text edit is
 * appended to instead of rewriting the whole overlay row.
 *
 * The browser tier owns the claim this exists for (what a keystroke costs in
 * real IndexedDB, text-local-cost.spec.ts). What is pinned here is the
 * behavior that has to hold for that to be a legitimate trade: the fold puts
 * back exactly the document a rewritten row would have held, the fast path
 * declines every write that is not a pure text delta, and the log's lifetime
 * stays tied to the overlay it is part of.
 */

const WS = 'ws';
const registry = new EntityRegistry([ WidgetSchema ]);
const noMirrors: MirrorMap = {};

function widgetRow(body: Uint8Array, label = ''): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label, body }));
}

/**
 * A document, plus the fragment for one insert appended to it — the pair a live
 * handle produces per edit. The fragment names every item the insert minted,
 * one per code point, which is what makes it stand for the whole edit.
 */
function typeInto(state: Uint8Array, peer: string, index: number, s: string) {
	const d = decodeTextDoc(state, peer);
	const range = d.insert(index, s);
	const ids = Array.from(
		{ length: range.count },
		(_, i) => ({ peer: range.peer, counter: range.start + i }),
	);
	return { state: encodeTextDoc(d), fragment: encodeFragmentFrom(d, ids, []) };
}

/** A text PATCH carrying `body` — a fragment on the keystroke path. */
function bodyPatch(id: bigint, body: Uint8Array, extra?: { label: string }): Mutation {
	return create(MutationSchema, {
		clientId: 'c1',
		mutationId: id,
		payload: {
			case: 'crud',
			value: {
				entity: 'widget',
				key: 'w1',
				op: CrudOp.PATCH,
				fieldMask: create(FieldMaskSchema, {
					paths: extra === undefined ? [ 'body' ] : [ 'body', 'label' ],
				}),
				value: toBinary(
					WidgetSchema,
					create(WidgetSchema, { id: 'w1', body, label: extra?.label ?? '' }),
				),
			},
		},
	});
}

async function seedConfirmed(tx: KVWrite, body: Uint8Array, label = ''): Promise<void> {
	const rec: ConfirmedRecord = { v: widgetRow(body, label), ver: '1' };
	await tx.put(confirmedKey('widget', WS, 'w1'), rec);
}

/** Visible text of the merged row, the way every reader downstream sees it. */
async function mergedText(tx: KVWrite): Promise<string> {
	const bytes = await readMergedRow(tx, registry, 'widget', WS, 'w1');
	if (bytes === undefined) throw new Error('row is gone');
	const row = fromBinary(WidgetSchema, bytes);
	return decodeTextDoc(row.body, '').plainText();
}

async function countLog(tx: KVWrite): Promise<number> {
	let n = 0;
	for await (const _ of tx.scan(overlayTextRowPrefix('widget', WS, 'w1'))) n++;
	return n;
}

describe('optimistic text log', () => {
	it('folds an appended fragment back into a whole document on read', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'hello');
			return encodeTextDoc(d);
		})();
		await seedConfirmed(tx, base);

		const edit = typeInto(base, 'p1', 5, ' world');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			bodyPatch(1n, edit.fragment),
			new TouchSet(),
		);

		// The edit is visible, and it cost a log entry rather than an overlay row.
		expect(await mergedText(tx)).toBe('hello world');
		expect(await countLog(tx)).toBe(1);
		expect(await tx.get(overlayKey('widget', WS, 'w1'))).toBeUndefined();

		// A scan resolves it the same way a point read does — the two share a
		// merged view, and a watcher rebuilding a snapshot goes through the scan.
		const rows = await scanMergedRows(tx, registry, 'widget', WS);
		expect(rows).toHaveLength(1);
		expect(decodeTextDoc(fromBinary(WidgetSchema, rows[0]!.v).body, '').plainText())
			.toBe('hello world');
		tx.release();
	});

	it('folds a burst in the order it was typed', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		let state = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'x');
			return encodeTextDoc(d);
		})();
		await seedConfirmed(tx, state);

		// Each fragment names items the one before it minted, so this also pins
		// that the fold applies them in an order where their parents exist.
		let id = 1n;
		for (const ch of [ ...'abc' ]) {
			const edit = typeInto(state, 'p1', 0, ch);
			state = edit.state;
			await applyMutationLocally(
				tx,
				registry,
				noMirrors,
				WS,
				bodyPatch(id++, edit.fragment),
				new TouchSet(),
			);
		}
		expect(await countLog(tx)).toBe(3);
		expect(await mergedText(tx)).toBe('cbax');
		tx.release();
	});

	it('declines a mask that also names a non-text field', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'hi');
			return encodeTextDoc(d);
		})();
		await seedConfirmed(tx, base);

		const edit = typeInto(base, 'p1', 2, '!');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			bodyPatch(1n, edit.fragment, { label: 'renamed' }),
			new TouchSet(),
		);

		// The label has to reach the row, so the row is written — and then there is
		// nothing left for a log entry to be a delta of.
		expect(await countLog(tx)).toBe(0);
		const bytes = await readMergedRow(tx, registry, 'widget', WS, 'w1');
		const row = fromBinary(WidgetSchema, bytes!);
		expect(row.label).toBe('renamed');
		expect(decodeTextDoc(row.body, '').plainText()).toBe('hi!');
		tx.release();
	});

	it('declines a whole document, and drops the log it supersedes', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'a');
			return encodeTextDoc(d);
		})();
		await seedConfirmed(tx, base);

		const first = typeInto(base, 'p1', 1, 'b');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			bodyPatch(1n, first.fragment),
			new TouchSet(),
		);
		expect(await countLog(tx)).toBe(1);

		// A whole-document write (a string-valued write, or a re-send after the
		// server asked for full state) already contains everything the log carried.
		const whole = typeInto(first.state, 'p1', 2, 'c').state;
		await applyMutationLocally(tx, registry, noMirrors, WS, bodyPatch(2n, whole), new TouchSet());
		expect(await countLog(tx)).toBe(0);
		expect(await mergedText(tx)).toBe('abc');
		tx.release();
	});

	it('clears with the overlay and is rebuilt by replay', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'a');
			return encodeTextDoc(d);
		})();
		await seedConfirmed(tx, base);

		const edit = typeInto(base, 'p1', 1, 'b');
		const mutation = bodyPatch(1n, edit.fragment);
		await applyMutationLocally(tx, registry, noMirrors, WS, mutation, new TouchSet());
		expect(await mergedText(tx)).toBe('ab');

		// Still queued: a rebase clears the overlay and replays it, and the log has
		// to come back with it or the edit disappears from the merged view.
		await clearOverlayAndReplay(tx, registry, noMirrors, WS, [ mutation ], new TouchSet());
		expect(await countLog(tx)).toBe(1);
		expect(await mergedText(tx)).toBe('ab');

		// Acked and dropped: the confirmed row now carries the text itself.
		await seedConfirmed(tx, edit.state);
		await clearOverlayAndReplay(tx, registry, noMirrors, WS, [], new TouchSet());
		expect(await countLog(tx)).toBe(0);
		expect(await mergedText(tx)).toBe('ab');
		tx.release();
	});

	it('names the row a cleared fragment belongs to, slashes in the key and all', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const key = 'nested/w1';
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'a');
			return encodeTextDoc(d);
		})();
		const seeded: ConfirmedRecord = {
			v: toBinary(WidgetSchema, create(WidgetSchema, { id: key, body: base })),
			ver: '1',
		};
		await tx.put(confirmedKey('widget', WS, key), seeded);

		const patch = (id: bigint, body: Uint8Array): Mutation =>
			create(MutationSchema, {
				clientId: 'c1',
				mutationId: id,
				payload: {
					case: 'crud',
					value: {
						entity: 'widget',
						key,
						op: CrudOp.PATCH,
						fieldMask: create(FieldMaskSchema, { paths: [ 'body' ] }),
						value: toBinary(WidgetSchema, create(WidgetSchema, { id: key, body })),
					},
				},
			});

		// Two keystrokes, so the row carries two fragments: the clear has to name
		// the ROW they belong to, and name it once.
		const first = typeInto(base, 'p1', 1, 'b');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			patch(1n, first.fragment),
			new TouchSet(),
		);
		const second = typeInto(first.state, 'p1', 2, 'c');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			patch(2n, second.fragment),
			new TouchSet(),
		);

		const touched = new TouchSet();
		await clearOverlayAndReplay(tx, registry, noMirrors, WS, [], touched);
		expect([ ...touched.pairs() ]).toEqual([
			{ workspaceId: WS, entity: 'widget', keys: new Set([ key ]) },
		]);
		tx.release();
	});

	it('survives the renumbering a compaction does', async () => {
		const kv = new MemoryKVStore();
		let state = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'x');
			return encodeTextDoc(d);
		})();

		// Three keystrokes, each its own queued mutation and its own log entry.
		{
			const tx = await kv.write();
			await seedConfirmed(tx, state);
			let id = 1n;
			for (const ch of [ ...'abc' ]) {
				const edit = typeInto(state, 'p1', 0, ch);
				state = edit.state;
				const m = bodyPatch(id++, edit.fragment);
				await enqueuePending(tx, WS, m);
				await applyMutationLocally(tx, registry, noMirrors, WS, m, new TouchSet());
			}
			await tx.put(metaLastMutationId(WS), '3');
			await tx.commit();
		}

		// The push cycle folds the burst into one slot and brings the mutation-id
		// counter down with it, so ids 2 and 3 are free to be minted again.
		const result = await compactPending(kv, registry, noMirrors, WS, () => new Set<bigint>());
		expect(result.changed).toBe(true);

		const tx = await kv.write();
		expect(await tx.get(metaLastMutationId(WS))).toBe('1');
		expect(await mergedText(tx)).toBe('cbax');

		// The next keystroke mints id 2 — an id the log used before the fold. Its
		// entry must not land on top of an edit that is still only in the log.
		const edit = typeInto(state, 'p1', 0, 'd');
		const next = bodyPatch(2n, edit.fragment);
		await enqueuePending(tx, WS, next);
		await applyMutationLocally(tx, registry, noMirrors, WS, next, new TouchSet());
		expect(await mergedText(tx)).toBe('dcbax');
		tx.release();
	});

	it('does not resurrect a row a PATCH cannot apply to', async () => {
		const kv = new MemoryKVStore();
		const tx = await kv.write();
		const base = (() => {
			const d = new TextDoc('p1');
			d.insert(0, 'a');
			return encodeTextDoc(d);
		})();
		// No confirmed row at all: delete-wins says a PATCH is a no-op, and the
		// fast path has to honour that as much as the whole-row path does.
		const edit = typeInto(base, 'p1', 1, 'b');
		await applyMutationLocally(
			tx,
			registry,
			noMirrors,
			WS,
			bodyPatch(1n, edit.fragment),
			new TouchSet(),
		);
		expect(await countLog(tx)).toBe(0);
		expect(await readMergedRow(tx, registry, 'widget', WS, 'w1')).toBeUndefined();
		tx.release();
	});

	// A row key may hold '/', so the log prefix of row "a" is also a prefix of
	// every fragment of row "a/b".
	describe('rows whose keys nest ("a" and "a/b")', () => {
		async function seedRow(tx: KVWrite, key: string, body: Uint8Array): Promise<void> {
			const rec: ConfirmedRecord = {
				v: toBinary(WidgetSchema, create(WidgetSchema, { id: key, body })),
				ver: '1',
			};
			await tx.put(confirmedKey('widget', WS, key), rec);
		}
		function rowPatch(
			id: bigint,
			key: string,
			value: { body?: Uint8Array; label?: string },
		): Mutation {
			return create(MutationSchema, {
				clientId: 'c1',
				mutationId: id,
				payload: {
					case: 'crud',
					value: {
						entity: 'widget',
						key,
						op: CrudOp.PATCH,
						fieldMask: create(FieldMaskSchema, { paths: value.body ? [ 'body' ] : [ 'label' ] }),
						value: toBinary(WidgetSchema, create(WidgetSchema, { id: key, ...value })),
					},
				},
			});
		}
		async function textOf(tx: KVWrite, key: string): Promise<string> {
			const bytes = await readMergedRow(tx, registry, 'widget', WS, key);
			return decodeTextDoc(fromBinary(WidgetSchema, bytes!).body, '').plainText();
		}
		const docOf = (peer: string, s: string): Uint8Array => {
			const d = new TextDoc(peer);
			if (s) d.insert(0, s);
			return encodeTextDoc(d);
		};

		it('does not fold the fragments of "a/b" into "a"', async () => {
			const kv = new MemoryKVStore();
			const tx = await kv.write();
			await seedRow(tx, 'a', docOf('p1', 'A'));
			// An empty document: the first keystroke has no origin, so it would
			// apply to any document it was folded into.
			const child = docOf('p2', '');
			await seedRow(tx, 'a/b', child);

			const edit = typeInto(child, 'p3', 0, 'leak');
			await applyMutationLocally(
				tx,
				registry,
				noMirrors,
				WS,
				rowPatch(1n, 'a/b', { body: edit.fragment }),
				new TouchSet(),
			);

			expect(await textOf(tx, 'a/b')).toBe('leak');
			expect(await textOf(tx, 'a')).toBe('A');
			tx.release();
		});

		it('keeps the log of "a/b" when "a" is written', async () => {
			const kv = new MemoryKVStore();
			const tx = await kv.write();
			await seedRow(tx, 'a', docOf('p1', 'A'));
			const child = docOf('p2', 'B');
			await seedRow(tx, 'a/b', child);

			const edit = typeInto(child, 'p3', 1, ' typed');
			await applyMutationLocally(
				tx,
				registry,
				noMirrors,
				WS,
				rowPatch(1n, 'a/b', { body: edit.fragment }),
				new TouchSet(),
			);
			await applyMutationLocally(
				tx,
				registry,
				noMirrors,
				WS,
				rowPatch(2n, 'a', { label: 'renamed' }),
				new TouchSet(),
			);

			expect(await textOf(tx, 'a/b')).toBe('B typed');
			tx.release();
		});
	});
});
