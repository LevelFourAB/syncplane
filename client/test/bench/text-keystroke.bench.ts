/**
 * What one keystroke costs through the REAL client, end to end.
 *
 * The other text benches measure stages against fakes: text.bench.ts times the
 * codec, overlay-text-log.bench.ts times one KV path, text-handle.bench.ts
 * drives a handle over a store that only pretends. None of them can say what a
 * keystroke actually costs, because that is the whole loop — edit the document,
 * encode the fragment, enqueue it, commit, notify, fold the row back, hand it to
 * whoever is watching — and the loop is where the surprises are. This drives a
 * SyncClient over MemoryKVStore with no server attached, so what is timed is the
 * client's own work and nothing else.
 *
 * The arms exist to separate three readers that pay very different amounts:
 *
 *   - nobody watching — the floor, and still not free: the handle keeps its own
 *     row watch, so a keystroke folds the log back whether or not an app is
 *     looking.
 *   - a watcher that does not touch the text — what re-reading the row costs
 *     once textfold.ts has the fold cached.
 *   - a watcher that RENDERS the text via textOf — the same, plus decoding the
 *     document the fold just encoded. The `memoized` arm feeds textOf a byte
 *     instance it has already seen, so the difference between the two is exactly
 *     that decode and nothing else.
 *
 * And `handle.value`, which is what the person TYPING is looking at: the handle
 * maintains its text from deltas, so it should not read the document at all, and
 * the arm exists to keep it that way.
 *
 * Nothing acks, since there is no server, so the pending queue and the `ot/` log
 * grow for as long as the benchmark runs — an offline typing burst, which is the
 * case 109 cares about. A fold is O(1) in log depth by construction (it decodes
 * once however many fragments it applies); what is NOT constant is the
 * comparison validating the cached fold, one memcmp per logged fragment, which
 * at keystroke-sized fragments and the depths reached here is tens of
 * microseconds against the milliseconds being measured. Read a drift over the
 * run as that, and check it against overlay-text-log.bench.ts, where log depth
 * is a controlled variable.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench
 */

import { bench, describe } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { encodeTextDoc, TextDoc } from '../../src/text/index.js';
import { textOf } from '../../src/text/project.js';

const SIZES = [ 5000, 50000 ];

/** A document of n code points, typed linearly by one peer. */
function docOf(n: number): Uint8Array {
	const doc = new TextDoc('p0');
	const chunk = 500;
	for (let written = 0; written < n; written += chunk) {
		doc.insert(written, 'a'.repeat(Math.min(chunk, n - written)));
	}
	return encodeTextDoc(doc);
}

/** How a watcher, if there is one, reads the row it was handed. */
type Render = 'none' | 'text' | 'memoized';

async function typist(n: number, opts: { watch: boolean; render: Render }) {
	const client = new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
	await client.db.note.mutate.put({ id: 'n1', body: docOf(n) });
	if (opts.watch) {
		// Held across the whole run, so textOf's memo — which is keyed by byte
		// instance — hits every time this arm asks for it.
		const pinned = docOf(n);
		client.db.note.watch((rows) => {
			if (opts.render === 'memoized') textOf(pinned);
			else if (opts.render === 'text') { for (const row of rows) textOf(row.value.body); }
		});
	}
	const handle = await client.text(NoteSchema, 'n1', 'body');
	// One edit before the timed region, so every arm starts with the log
	// non-empty and the fold already cached.
	await handle.insert(0, 'x');
	return handle;
}

for (const n of SIZES) {
	describe(`${n}cp`, () => {
		let alone: Awaited<ReturnType<typeof typist>> | undefined;
		bench('keystroke, nobody watching', async () => {
			alone ??= await typist(n, { watch: false, render: 'none' });
			await alone.insert(1, 'x');
		});

		let quiet: Awaited<ReturnType<typeof typist>> | undefined;
		bench('keystroke, a watcher that ignores the text', async () => {
			quiet ??= await typist(n, { watch: true, render: 'none' });
			await quiet.insert(1, 'x');
		});

		let memo: Awaited<ReturnType<typeof typist>> | undefined;
		bench('keystroke, a watcher rendering memoized text', async () => {
			memo ??= await typist(n, { watch: true, render: 'memoized' });
			await memo.insert(1, 'x');
		});

		let rendering: Awaited<ReturnType<typeof typist>> | undefined;
		bench('keystroke, a watcher rendering the row text', async () => {
			rendering ??= await typist(n, { watch: true, render: 'text' });
			await rendering.insert(1, 'x');
		});

		let mine: Awaited<ReturnType<typeof typist>> | undefined;
		bench('keystroke, then read handle.value', async () => {
			mine ??= await typist(n, { watch: false, render: 'none' });
			await mine.insert(1, 'x');
			void mine.value;
		});
	});
}
