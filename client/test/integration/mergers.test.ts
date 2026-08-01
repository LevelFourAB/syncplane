// Cross-language merge of the Widget.body text field: the server auto-
// installs the Go text merger for (syncplane.v1.field_behavior).text at entity
// registration, and the client mirrors it from the schema — no MergerSpec
// wiring on either side. These tests drive real pushes/pulls through the
// spawned Go testserver, so they prove both implementations converge on the
// same bytes.

import { describe, expect, it } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	ExpandNone,
	plainTextOf,
	TextDoc,
} from '../../src/text/index.js';
import { freshWorkspace, newClient } from './helpers.js';

/** Builds an encoded document owned by peer with the given text. */
function bodyDoc(peer: string, s: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	return encodeTextDoc(d);
}

/** Decodes state as peer, applies an edit, re-encodes. */
function editBody(
	state: Uint8Array | undefined,
	peer: string,
	edit: (d: TextDoc) => void,
): Uint8Array {
	const d = decodeTextDoc(state, peer);
	edit(d);
	return encodeTextDoc(d);
}

describe('text field merge (auto-installed merger)', () => {
	it('converges concurrent typing at the same position without interleaving', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', body: bodyDoc('seed', '[]') });
		await a.syncNow();
		await b.syncNow();

		// Both clients type into the same position of the same base state and
		// push concurrently; the server's Go text merger linearizes them.
		const baseA = (await a.get(WidgetSchema, 'w1'))?.body;
		const baseB = (await b.get(WidgetSchema, 'w1'))?.body;
		await a.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseA, 'alice', (d) => d.insert(1, 'alice')),
		});
		await b.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseB, 'bob', (d) => d.insert(1, 'bob')),
		});
		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const bodyA = (await a.get(WidgetSchema, 'w1'))?.body;
		const bodyB = (await b.get(WidgetSchema, 'w1'))?.body;
		const gotA = plainTextOf(bodyA);
		// Sequences stay contiguous whichever push won the race.
		expect([ '[alicebob]', '[bobalice]' ]).toContain(gotA);
		expect(plainTextOf(bodyB)).toBe(gotA);
		// Convergence is byte-level, not just textual.
		expect(bodyB).toEqual(bodyA);

		await a.close();
		await b.close();
	});

	it('converges an offline/offline split on reconnect', async () => {
		const ws = freshWorkspace();
		const seedClient = newClient(ws);
		await seedClient.start();
		await seedClient.mutate.put(WidgetSchema, {
			id: 'w1',
			body: bodyDoc('seed', 'hello world'),
		});
		await seedClient.syncNow();

		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();
		await a.syncNow();
		await b.syncNow();

		// Both edit locally with no sync between the edits — an offline split.
		const baseA = (await a.get(WidgetSchema, 'w1'))?.body;
		const baseB = (await b.get(WidgetSchema, 'w1'))?.body;
		await a.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseA, 'alice', (d) => d.insert(0, '>> ')),
		});
		await b.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseB, 'bob', (d) => d.insert(11, ' <<')),
		});

		// Reconnect: push both, pull both.
		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('>> hello world <<');
		expect(plainTextOf((await b.get(WidgetSchema, 'w1'))?.body)).toBe('>> hello world <<');

		await seedClient.close();
		await a.close();
		await b.close();
	});

	it('keeps marks attached across a concurrent edit to the marked range', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		await a.mutate.put(WidgetSchema, { id: 'w1', body: bodyDoc('seed', 'hello world') });
		await a.syncNow();
		await b.syncNow();

		const baseA = (await a.get(WidgetSchema, 'w1'))?.body;
		const baseB = (await b.get(WidgetSchema, 'w1'))?.body;
		await a.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseA, 'alice', (d) => {
				d.mark(6, 11, 'comment:c1', true, ExpandNone);
			}),
		});
		await b.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(baseB, 'bob', (d) => d.insert(6, 'big ')),
		});
		await a.syncNow();
		await b.syncNow();
		await a.syncNow();

		const doc = decodeTextDoc((await a.get(WidgetSchema, 'w1'))?.body, '');
		expect(doc.plainText()).toBe('hello big world');
		expect(doc.resolveMarks()).toEqual([
			{ start: 10, end: 15, key: 'comment:c1', value: true },
		]);

		await a.close();
		await b.close();
	});

	it('applies the text merger optimistically before any sync', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);

		// Pure local optimism — no start(), no sync. The overlay's PATCH
		// merge runs the schema-derived text merger.
		const base = bodyDoc('alice', 'base');
		await a.mutate.put(WidgetSchema, { id: 'w1', body: base });
		await a.mutate.patch(WidgetSchema, 'w1', {
			body: editBody(base, 'alice', (d) => d.insert(4, '+A')),
		});
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('base+A');

		// The server agrees once synced.
		await a.start();
		await a.syncNow();
		expect(plainTextOf((await a.get(WidgetSchema, 'w1'))?.body)).toBe('base+A');

		await a.close();
	});
});
