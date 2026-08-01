/**
 * What a keystroke by SOMEBODY ELSE costs, which no benchmark had asked.
 *
 * text-keystroke.bench.ts times the local typist's loop. This is the other half
 * of a collaborative session, and it runs on a different, more expensive path: a
 * pull page carrying delta state is folded into the confirmed row by
 * `applyServerChanges`, inside the KV write transaction — which is held
 * exclusively across tabs, so it bounds every other writer while it runs, the
 * way the server's write closure bounds a workspace.
 *
 * Two arms, because a row a remote edit lands on is usually a row somebody is
 * looking at:
 *
 *   - apply alone — what the write transaction costs.
 *   - apply, then render the field through `textOf` — the whole visible cost of
 *     one remote keystroke, and the shape both the canvas and the issue-detail
 *     app produce whenever two people have the same row open.
 *
 * Every iteration moves the document, so no arm can be served a projection it
 * saw before, and the fragment is minted the way a real writer mints one: from
 * the ids the edit touched (`encodeFragmentFrom`), so the peer's own bookkeeping
 * stays O(edit) and does not show up in the measurement.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench
 */

import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { bench, describe } from 'vitest';

import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { type RowChange, RowChangeSchema } from '../../src/gen/syncplane/v1/patch_pb.js';
import { confirmedKey, type ConfirmedRecord } from '../../src/keys.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { EntityRegistry } from '../../src/mutations.js';
import { applyServerChanges } from '../../src/optimistic/rebase.js';
import { encodeFragmentFrom, encodeTextDoc, TextDoc, textOf } from '../../src/text/index.js';
import { TouchSet } from '../../src/touch.js';

const WS = 'ws';
const SIZES = [ 5000, 50000 ];
const registry = new EntityRegistry([ WidgetSchema ]);

function docOf(n: number): TextDoc {
	const doc = new TextDoc('remote');
	const chunk = 500;
	for (let written = 0; written < n; written += chunk) {
		doc.insert(written, 'a'.repeat(Math.min(chunk, n - written)));
	}
	return doc;
}

function widgetRow(body: Uint8Array | undefined): Uint8Array {
	return toBinary(WidgetSchema, create(WidgetSchema, { id: 'w1', body }));
}

function deltaChange(fragment: Uint8Array, version: bigint): RowChange {
	return create(RowChangeSchema, {
		entity: 'widget',
		key: 'w1',
		version,
		body: { case: 'value', value: widgetRow(undefined) },
		textDeltas: [ { field: 'body', fragment } ],
	});
}

/**
 * A session with one remote peer typing into a row this client holds confirmed.
 * `keystroke()` is that peer's next edit, arriving as the server would ship it.
 */
async function session(n: number) {
	const kv = new MemoryKVStore();
	// The peer's own copy, which never leaves this closure: it is here to mint
	// each keystroke's fragment, not to stand in for anything the client holds.
	const theirs = docOf(n);
	const tx = await kv.write();
	await tx.put(
		confirmedKey('widget', WS, 'w1'),
		{
			v: widgetRow(encodeTextDoc(theirs)),
			ver: '1',
		} satisfies ConfirmedRecord,
	);
	await tx.commit();

	let version = 1n;
	return {
		async keystroke(): Promise<void> {
			const range = theirs.insert(1, 'x');
			const fragment = encodeFragmentFrom(theirs, [ { peer: range.peer, counter: range.start } ], []);
			version += 1n;
			const wtx = await kv.write();
			await applyServerChanges(wtx, registry, WS, [ deltaChange(fragment, version) ], new TouchSet());
			await wtx.commit();
		},
		async body(): Promise<Uint8Array> {
			const rtx = await kv.read();
			const record = (await rtx.get(confirmedKey('widget', WS, 'w1'))) as ConfirmedRecord;
			rtx.release();
			// fromBinary is what the read pipeline does, and it mints a fresh view
			// every time, which is why textOf's byte-instance memo cannot serve a
			// row that just changed.
			return fromBinary(WidgetSchema, record.v).body!;
		},
	};
}

for (const n of SIZES) {
	describe(`${n}cp`, () => {
		let applying: Awaited<ReturnType<typeof session>> | undefined;
		bench('remote keystroke, applied to the confirmed row', async () => {
			applying ??= await session(n);
			await applying.keystroke();
		});

		let rendering: Awaited<ReturnType<typeof session>> | undefined;
		bench('remote keystroke, applied and rendered', async () => {
			rendering ??= await session(n);
			await rendering.keystroke();
			textOf(await rendering.body());
		});
	});
}
