import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import {
	KeyedNoteSchema,
	KeyedTextEntitySchema,
	TextHolderSchema,
	WidgetSchema,
} from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MutationSchema } from '../../src/gen/syncplane/v1/mutation_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import {
	EntityRegistry,
	expandCollectionMask,
	mergeMask,
	mergeTextFieldsInto,
	textFieldBytes,
	textInstancePaths,
	textSchemaPathOf,
} from '../../src/mutations.js';
import { readPending } from '../../src/optimistic/queue.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	isFragment,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';

function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

function editDoc(state: Uint8Array, peer: string, fn: (d: TextDoc) => void): Uint8Array {
	const d = decodeTextDoc(state, peer);
	fn(d);
	return encodeTextDoc(d);
}

const schema = KeyedTextEntitySchema;

function newClient(kvStore: MemoryKVStore = new MemoryKVStore()) {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore,
		entities: [ KeyedTextEntitySchema, WidgetSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

describe('instance paths', () => {
	it('enumerates one path per element, keys escaped as the mask carries them', () => {
		const msg = create(schema, {
			id: 'k1',
			notes: [
				{ id: 'n1', body: bodyDoc('p', 'a') },
				{ id: 'n.2' },
			],
			sections: { intro: {}, 'a.b': {} },
		});
		expect(textInstancePaths(schema, 'notes.body', msg))
			.toEqual([ 'notes.n1.body', 'notes.n\\.2.body' ]);
		expect(textInstancePaths(schema, 'notes.deep.body', msg)).toEqual([]);
		expect(textInstancePaths(schema, 'sections.body', msg))
			.toEqual([ 'sections.a\\.b.body', 'sections.intro.body' ]);
	});

	it('resolves a mask path back to the text field it names outright', () => {
		expect(textSchemaPathOf(schema, 'notes.n1.body')).toBe('notes.body');
		expect(textSchemaPathOf(schema, 'notes.n\\.2.deep.body')).toBe('notes.deep.body');
		expect(textSchemaPathOf(schema, 'sections.intro.body')).toBe('sections.body');
		expect(textSchemaPathOf(schema, 'title')).toBeUndefined();
		expect(textSchemaPathOf(schema, 'notes.n1')).toBeUndefined();
		expect(textSchemaPathOf(schema, 'notes.n1.label')).toBeUndefined();
	});
});

describe('mask expansion around element text', () => {
	const base = () =>
		create(schema, {
			id: 'k1',
			notes: [
				{ id: 'n1', label: 'a', body: bodyDoc('p', 'one') },
				{ id: 'n2', label: 'b', body: bodyDoc('p', 'two') },
			],
		});

	it('splits a changed text-bearing element into per-unit paths', () => {
		const prior = base();
		const next = base();
		next.notes[0]!.label = 'a2';
		next.notes[0]!.body = editDoc(next.notes[0]!.body, 'p', (d) => d.insert(3, '!'));
		expect(expandCollectionMask(schema, prior, next, [ 'notes' ]).sort())
			.toEqual([ 'notes.n1.body', 'notes.n1.label' ]);
	});

	it('keeps whole-element paths for additions and removals', () => {
		const prior = base();
		const next = base();
		next.notes = [ next.notes[0]!, create(KeyedNoteSchema, { id: 'n3' }) ];
		expect(expandCollectionMask(schema, prior, next, [ 'notes' ]).sort())
			.toEqual([ 'notes.n2', 'notes.n3' ]);
	});

	it('reaches a document below a singular message inside the element', () => {
		const prior = base();
		prior.notes[0]!.deep = create(TextHolderSchema, { body: bodyDoc('p', 'deep') });
		const next = fromBinary(schema, toBinary(schema, prior));
		next.notes[0]!.deep!.body = editDoc(next.notes[0]!.deep!.body, 'p', (d) => d.insert(0, 'X'));
		expect(expandCollectionMask(schema, prior, next, [ 'notes' ]))
			.toEqual([ 'notes.n1.deep.body' ]);
	});
});

describe('merge semantics for element documents', () => {
	it('a mask naming the element document merges instead of replacing', () => {
		const registry = new EntityRegistry([ KeyedTextEntitySchema ]);
		const mergers = registry.mergersFor('keyed_text');
		const seed = bodyDoc('seed', '[]');
		const dst = create(schema, {
			id: 'k1',
			notes: [ {
				id: 'n1',
				label: 'keep',
				body: editDoc(seed, 'alice', (d) => d.insert(1, 'alice')),
			} ],
		});
		const src = create(schema, {
			notes: [ { id: 'n1', body: editDoc(seed, 'bob', (d) => d.insert(1, 'bob')) } ],
		});
		mergeMask(schema, dst, src, [ 'notes.n1.body' ], mergers);
		expect(plainTextOf(dst.notes[0]!.body)).toBe('[alicebob]');
		expect(dst.notes[0]!.label).toBe('keep');
	});

	it('mergeTextFieldsInto pairs documents by element and drops removed ones', () => {
		const seed = bodyDoc('seed', 'one');
		const stored = create(schema, {
			id: 'k1',
			notes: [
				{ id: 'n1', body: editDoc(seed, 'client', (d) => d.insert(3, ' two')) },
				{ id: 'gone', body: bodyDoc('seed', 'bye') },
			],
		});
		const put = create(schema, {
			id: 'k1',
			notes: [ { id: 'n1', label: 'v2', body: editDoc(seed, 'editor', (d) => d.insert(0, 'ONE ')) } ],
		});
		mergeTextFieldsInto(schema, [ 'notes.body', 'notes.deep.body', 'sections.body' ], stored, put);
		expect(plainTextOf(put.notes[0]!.body)).toBe('ONE one two');
		expect(put.notes).toHaveLength(1);
	});

	it('mergeTextFieldsInto keeps a stored document the PUT carries none of', () => {
		const stored = create(schema, {
			id: 'k1',
			notes: [ { id: 'n1', label: 'a', body: bodyDoc('client', 'typed') } ],
		});
		const put = create(schema, { id: 'k1', notes: [ { id: 'n1', label: 'a2' } ] });
		mergeTextFieldsInto(schema, [ 'notes.body', 'notes.deep.body', 'sections.body' ], stored, put);
		expect(put.notes[0]!.label).toBe('a2');
		expect(plainTextOf(put.notes[0]!.body)).toBe('typed');
	});

	it('a local PUT without a document keeps the one the row holds', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'n1', label: 'a', body: bodyDoc('client', 'typed') } ],
		});
		await client.db.keyedTextEntity.mutate.put({ id: 'k1', notes: [ { id: 'n1', label: 'a2' } ] });

		const row = await client.db.keyedTextEntity.get('k1');
		expect(row?.notes[0]?.label).toBe('a2');
		expect(plainTextOf(row!.notes[0]!.body)).toBe('typed');
	});

	it('a local PUT of an empty string clears the document', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'n1', body: bodyDoc('client', 'typed') } ],
		});
		await client.db.keyedTextEntity.mutate.put({ id: 'k1', notes: [ { id: 'n1', body: '' } ] });

		const row = await client.db.keyedTextEntity.get('k1');
		expect(plainTextOf(row!.notes[0]!.body)).toBe('');
	});
});

describe('text handles on element documents', () => {
	it('edits one element locally without touching its siblings', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [
				{ id: 'n1', body: bodyDoc('seed', 'hello') },
				{ id: 'n2', body: bodyDoc('seed', 'other') },
			],
		});
		const handle = await client.db.keyedTextEntity.text('k1', 'notes.n1.body');
		expect(handle.value).toBe('hello');
		await handle.insert(5, ' world');
		expect(handle.value).toBe('hello world');

		const again = await client.db.keyedTextEntity.text('k1', 'notes.n1.body');
		expect(again.value).toBe('hello world');
		const sibling = await client.db.keyedTextEntity.text('k1', 'notes.n2.body');
		expect(sibling.value).toBe('other');

		const row = await client.db.keyedTextEntity.get('k1');
		expect(row?.notes.find((n) => n.id === 'n1')?.label).toBe('');

		handle.close();
		again.close();
		sibling.close();
		await client.close();
	});

	it('queues the edit as a fragment under the instance-path mask', async () => {
		const kv = new MemoryKVStore();
		const client = newClient(kv);
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'n1', body: bodyDoc('seed', 'hi') } ],
		});
		const handle = await client.db.keyedTextEntity.text('k1', 'notes.n1.body');
		await handle.insert(2, '!');
		handle.close();

		const tx = await kv.read();
		let pending;
		try {
			pending = await readPending(tx, 'ws1');
		} finally {
			tx.release();
		}
		const patch = pending.at(-1)!;
		expect(patch.payload.case).toBe('crud');
		const crud = patch.payload.case === 'crud' ? patch.payload.value : undefined;
		expect(crud?.fieldMask?.paths).toEqual([ 'notes.n1.body' ]);
		const value = fromBinary(schema, crud!.value);
		expect(isFragment(textFieldBytes(schema, 'notes.n1.body', value)!)).toBe(true);
		await client.close();
	});

	it('logs element edits in the overlay text log, id slashes and all', async () => {
		const kv = new MemoryKVStore();
		const client = newClient(kv);
		// An element id carrying '/' exercises the log key's field escaping: the
		// key layout parses from the right on '/', so the field segment must
		// never leak one.
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'a/b', body: bodyDoc('seed', 'hi') } ],
		});
		const handle = await client.db.keyedTextEntity.text('k1', 'notes.a/b.body');
		await handle.insert(2, '!');
		expect(handle.value).toBe('hi!');
		handle.close();

		const tx = await kv.read();
		const logKeys: string[] = [];
		try {
			for await (const [ k ] of tx.scan('ot/keyed_text/')) logKeys.push(k);
		} finally {
			tx.release();
		}
		expect(logKeys).toHaveLength(1);

		// The fold round-trips the escaped field: a fresh read sees the edit.
		const row = await client.db.keyedTextEntity.get('k1');
		expect(plainTextOf(row!.notes[0]!.body)).toBe('hi!');
		await client.close();
	});

	it('refuses a handle for an element the row does not carry', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({ id: 'k1', notes: [ { id: 'n1' } ] });
		await expect(client.db.keyedTextEntity.text('k1', 'notes.nope.body'))
			.rejects.toThrow(/row not found/);
		await expect(client.db.keyedTextEntity.text('k1', 'notes.n1.label'))
			.rejects.toThrow(/not a text field/);
		await client.close();
	});
});

describe('string writes into element documents', () => {
	it('resolves a string in a put init under the element instance path', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'n1', label: 'a', body: 'hello' } ],
			sections: { intro: { body: 'welcome' } },
		});
		const row = await client.db.keyedTextEntity.get('k1');
		expect(plainTextOf(row!.notes[0]!.body)).toBe('hello');
		expect(plainTextOf(row!.sections['intro']!.body)).toBe('welcome');
		await client.close();
	});

	it('merges a string patch with the element document it lands on', async () => {
		const client = newClient();
		await client.db.keyedTextEntity.mutate.put({
			id: 'k1',
			notes: [ { id: 'n1', body: 'one two' } ],
		});
		await client.db.keyedTextEntity.mutate.patch('k1', {
			notes: [ { id: 'n1', body: 'one TWO three' } ],
		});
		const row = await client.db.keyedTextEntity.get('k1');
		expect(plainTextOf(row!.notes[0]!.body)).toBe('one TWO three');
		await client.close();
	});
});
