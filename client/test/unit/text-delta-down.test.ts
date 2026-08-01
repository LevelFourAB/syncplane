// Downstream delta state: what the client does with a pull page whose
// collaborative-text fields arrive as fragments instead of documents.
//
// The invariant every spec here circles is the mirror of the upstream one:
// folding the shipped fragment into the confirmed row must reach the same bytes
// the equivalent full-state page would have written. Everything downstream of
// confirmed storage keeps seeing whole documents.

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { confirmedKey, type ConfirmedRecord } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { applyServerChanges, stageServerChanges } from '../../src/optimistic/rebase.js';
import { TextDeltaUnusableError } from '../../src/sync/textdelta.js';
import {
	decodeTextDoc,
	encodeFragment,
	encodeTextDoc,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws';
const registry = new EntityRegistry([ WidgetSchema ]);

function doc(peer: string, text: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, text);
	return encodeTextDoc(d);
}

function edit(state: Uint8Array, peer: string, fn: (d: TextDoc) => void): Uint8Array {
	const d = decodeTextDoc(state, peer);
	fn(d);
	return encodeTextDoc(d);
}

/** The fragment carrying what `next` holds beyond `base`. */
function fragmentOf(next: Uint8Array, base: Uint8Array | undefined): Uint8Array {
	return encodeFragment(
		decodeTextDoc(next, ''),
		base === undefined ? null : decodeTextDoc(base, ''),
	);
}

function widgetRow(body: Uint8Array | undefined, label = ''): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', label, body }));
}

/** A row change shaped the way the server ships delta state: the document
 * cleared from the value, and carried as a fragment beside it. */
function deltaChange(fragment: Uint8Array, label = '', version = 2n): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key: 'w1',
		version,
		body: { case: 'value', value: widgetRow(undefined, label) },
		textDeltas: [ { field: 'body', fragment } ],
	});
}

describe('downstream text delta state', () => {
	let kv: MemoryKVStore;

	beforeEach(() => {
		kv = new MemoryKVStore();
	});

	async function seedConfirmed(body: Uint8Array | undefined): Promise<void> {
		const tx = await kv.write();
		await tx.put(
			confirmedKey('widget', WS, 'w1'),
			{
				v: widgetRow(body),
				ver: '1',
			} satisfies ConfirmedRecord,
		);
		await tx.commit();
	}

	async function apply(changes: RowChange[]): Promise<void> {
		const tx = await kv.write();
		await applyServerChanges(tx, registry, WS, changes, new TouchSet());
		await tx.commit();
	}

	async function confirmedBody(): Promise<Uint8Array | undefined> {
		const tx = await kv.read();
		const record = (await tx.get(confirmedKey('widget', WS, 'w1'))) as ConfirmedRecord;
		tx.release();
		return fromBinary(WidgetSchema, record.v).body;
	}

	it('folds a fragment into the confirmed row, reaching the full-state bytes', async () => {
		const base = doc('seed', 'the quick brown fox');
		await seedConfirmed(base);
		const next = edit(base, 'alice', (d) => d.setString('the quick brown fox jumps'));

		await apply([ deltaChange(fragmentOf(next, base)) ]);

		expect(await confirmedBody()).toEqual(next);
		expect(plainTextOf((await confirmedBody())!)).toBe('the quick brown fox jumps');
	});

	it('keeps the confirmed document when the fragment is empty', async () => {
		const base = doc('seed', 'unchanged');
		await seedConfirmed(base);

		// "Only other fields moved": the value carries no document at all.
		await apply([ deltaChange(new Uint8Array(), 'renamed') ]);

		expect(await confirmedBody()).toEqual(base);
		const tx = await kv.read();
		const record = (await tx.get(confirmedKey('widget', WS, 'w1'))) as ConfirmedRecord;
		tx.release();
		expect(fromBinary(WidgetSchema, record.v).label).toBe('renamed');
	});

	it('applies a self-contained fragment to a row it has never held', async () => {
		const fresh = doc('seed', 'brand new');
		await apply([ deltaChange(fragmentOf(fresh, undefined)) ]);
		expect(await confirmedBody()).toEqual(fresh);
	});

	it('applies deltas in page order, so a later one sees the earlier one', async () => {
		const base = doc('seed', 'one');
		await seedConfirmed(base);
		const second = edit(base, 'a', (d) => d.insert(3, ' two'));
		const third = edit(second, 'b', (d) => d.insert(7, ' three'));

		await apply([
			deltaChange(fragmentOf(second, base), '', 2n),
			deltaChange(fragmentOf(third, second), '', 3n),
		]);

		expect(await confirmedBody()).toEqual(third);
	});

	it('leaves rows without delta state untouched', async () => {
		const body = doc('seed', 'whole');
		await apply([
			create(RowChangeSchema, {
				entity: 'widget',
				key: 'w1',
				version: 1n,
				body: { case: 'value', value: widgetRow(body) },
			}),
		]);
		expect(await confirmedBody()).toEqual(body);
	});

	describe('when the fold cannot be trusted', () => {
		it('rejects a fragment whose base this client does not hold', async () => {
			// Confirmed state diverged from what the server diffed against, so
			// the fragment's parents resolve to nothing.
			await seedConfirmed(doc('other', 'a different history'));
			const base = doc('seed', 'the quick brown fox');
			const next = edit(base, 'alice', (d) => d.insert(19, ' jumps'));

			await expect(apply([ deltaChange(fragmentOf(next, base)) ])).rejects.toThrow(
				TextDeltaUnusableError,
			);
		});

		it('rejects an unchanged-marker for a field it holds nothing for', async () => {
			await seedConfirmed(undefined);
			await expect(apply([ deltaChange(new Uint8Array()) ])).rejects.toThrow(
				TextDeltaUnusableError,
			);
		});

		it('rejects delta state on a bootstrap chunk', async () => {
			const tx = await kv.write();
			await expect(
				stageServerChanges(tx, registry, WS, 1, [
					deltaChange(fragmentOf(doc('seed', 'x'), undefined)),
				]),
			).rejects.toThrow(TextDeltaUnusableError);
			tx.release();
		});
	});
});
