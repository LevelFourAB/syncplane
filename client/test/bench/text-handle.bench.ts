/**
 * Cost of the two things a live TextHandle does: apply a local keystroke, and
 * absorb somebody else's change off the row watch.
 *
 * Separate from text.bench.ts on purpose. These cases keep a handle — and so a
 * whole document — alive across iterations, which is what makes them realistic
 * and also what makes their garbage pressure swamp the pure-function timings if
 * they share a worker. Vitest isolates by file, so the split keeps both honest.
 *
 * Run with: client/node_modules/.bin/vitest bench --project bench
 */

import { bench, describe } from 'vitest';
import { decodeTextDoc, encodeTextDoc } from '../../src/text/codec.js';
import { TextDoc } from '../../src/text/doc.js';
import { applyFragmentBytes, isFragment } from '../../src/text/fragment.js';
import { openTextHandle, type TextHandleDeps } from '../../src/text/handle.js';
import { mergeText } from '../../src/text/merge.js';

const SIZES = [ 5000, 20000, 50000 ];

/**
 * A live handle over an in-memory row, wired the way the client wires one: the
 * PATCH lands in a store whose watcher fires straight back at the handle. That
 * echo is the point — the row watch fires on every change to any field of the
 * row, so a keystroke's real cost includes the handle re-reading its own write.
 *
 * The store holds FULL state, as the real one does. A queued text edit ships as
 * a fragment of the items and marks that edit touched, so the store folds it
 * into the row rather than storing it: what a watcher ever sees is a whole
 * document. Echoing the PATCH value back verbatim would hand the handle a
 * fragment, which it refuses — correctly, since stored rows never hold delta
 * state.
 */
async function benchHandle(initial: Uint8Array) {
	let watcher: ((state: Uint8Array | undefined) => void) | undefined;
	let row = initial;
	const deps: TextHandleDeps = {
		acquirePeer: () => Promise.resolve({ peer: 'p0', release: () => {} }),
		load: () => Promise.resolve(row),
		patch: (state) => {
			row = isFragment(state) ? applyFragmentBytes(row, state) : mergeText(row, state);
			watcher?.(row);
			return Promise.resolve();
		},
		watch: (cb) => {
			watcher = cb;
			return () => {
				watcher = undefined;
			};
		},
	};
	const handle = await openTextHandle(deps);
	return {
		handle,
		deliver: (s: Uint8Array) => {
			row = mergeText(row, s);
			watcher?.(row);
		},
	};
}

/** A document of n code points, typed linearly by one peer. */
function benchState(n: number): Uint8Array {
	const doc = new TextDoc('w');
	const chunk = 500;
	for (let written = 0; written < n; written += chunk) {
		doc.insert(written, 'a'.repeat(Math.min(chunk, n - written)));
	}
	return encodeTextDoc(doc);
}

for (const n of SIZES) {
	describe(`${n}cp`, () => {
		const state = benchState(n);

		// One keystroke end to end through the public API, including the PATCH's
		// encode and the row-watch echo it provokes. This is the number a typist
		// feels, minus storage.
		let typing: Awaited<ReturnType<typeof benchHandle>>['handle'] | undefined;
		bench('keystroke (edit + push + watch echo)', async () => {
			typing ??= (await benchHandle(state)).handle;
			await typing.insert(n / 2, 'x');
		});

		// Somebody else's edit arriving on the row watch: snapshot, merge, diff.
		// The states are built up front and cycled, so the timed body is only the
		// handle's work — and since no single one of them ever equals the merged
		// document, the folded-bytes fast path never hides it either.
		const remoteStates = Array.from({ length: 8 }, (_, i) => {
			const other = decodeTextDoc(state, `r${i}`);
			other.insert(0, 'z');
			return encodeTextDoc(other);
		});
		let deliver: ((s: Uint8Array) => void) | undefined;
		let round = 0;
		bench('remote change (snapshot + merge + diff)', async () => {
			deliver ??= (await benchHandle(state)).deliver;
			deliver(remoteStates[round++ % remoteStates.length]!);
		});
	});
}
