import { describe, expect, it } from 'vitest';

import { openTextHandle, type TextHandleDeps } from '../../src/text/handle.js';
import {
	applyFragmentBytes,
	compactDoc,
	decodeTextDoc,
	encodeTextDoc,
	purgedByRemote,
	type TextDelta,
	TextDoc,
} from '../../src/text/index.js';

/**
 * A live handle over a field the server compacts under it. Driving the handle
 * through its deps rather than through SyncClient is what lets the test hand it
 * the exact state a pull would deliver.
 */
function harness(initial: Uint8Array) {
	let watcher: ((state: Uint8Array | undefined) => void) | undefined;
	const patches: Uint8Array[] = [];
	const deps: TextHandleDeps = {
		acquirePeer: () => Promise.resolve({ peer: 'p0', release: () => {} }),
		load: () => Promise.resolve(initial),
		patch: (state) => {
			patches.push(state);
			return Promise.resolve();
		},
		watch: (cb) => {
			watcher = cb;
			return () => {
				watcher = undefined;
			};
		},
	};
	return {
		deps,
		patches,
		deliver: (state: Uint8Array) => watcher!(state),
	};
}

/** Lets the handle's write chain drain. */
function flush(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

/** "hello world" with " world" deleted, as one writer would leave it. */
function trimmed(): TextDoc {
	const d = new TextDoc('seed');
	d.insert(0, 'hello world');
	d.delete(5, 6);
	return d;
}

describe('a live handle whose field the server compacts', () => {
	it('rebuilds onto the compacted state instead of merging it', async () => {
		const server = trimmed();
		const h = harness(encodeTextDoc(server));
		const handle = await openTextHandle(h.deps);
		const deltas: TextDelta[] = [];
		handle.subscribe((d) => deltas.push(d));

		// A local edit the server has not seen, then the compacted state arrives.
		await handle.insert(2, '!');
		expect(handle.value).toBe('he!llo');
		h.patches.length = 0;

		const compacted = decodeTextDoc(encodeTextDoc(server), '');
		expect(compactDoc(compacted).items).toBe(6);
		const seen = deltas.length;
		h.deliver(encodeTextDoc(compacted));
		// The rebuild's own PATCH rides the handle's write chain, so let it run.
		await flush();

		// The visible text is untouched — the rebuild only changes which items
		// carry it — so an unmarked document reports nothing at all.
		expect(handle.value).toBe('he!llo');
		expect(deltas).toHaveLength(seen);

		// And the document no longer carries what the server dropped, so the
		// state it pushes is one the server will accept.
		expect(h.patches).toHaveLength(1);
		const pushed = decodeTextDoc(h.patches[0]!, '');
		expect(pushed.plainText()).toBe('he!llo');
		expect(compactDoc(pushed).items).toBe(0);
		// The property the server measures against the floor: nothing in the
		// pushed state is an item the compacted document dropped.
		expect(purgedByRemote(pushed, compacted)).toBe(false);
		handle.close();
	});

	it('allocates counters past the edits it has already queued', async () => {
		const server = trimmed();
		const stored = encodeTextDoc(server);
		const h = harness(stored);
		const handle = await openTextHandle(h.deps);

		await handle.insert(0, 'a');
		await handle.insert(handle.value.length, 'a');
		await flush();
		const spoken = handle.doc.versionVector().get('p0')!;
		h.patches.length = 0;

		const compacted = decodeTextDoc(stored, '');
		compactDoc(compacted);
		h.deliver(encodeTextDoc(compacted));
		await flush();

		// The compacted state's version vector knows nothing of the two edits, so
		// rebuilding from it and allocating afresh would hand the re-inserted
		// characters ids the queue is still carrying — one id, two items, two
		// positions.
		const pushed = decodeTextDoc(h.patches[0]!, '');
		for (const n of pushed.byId.values()) {
			if (n.id.peer === 'p0') expect(n.id.counter).toBeGreaterThan(spoken);
		}
	});

	it('keeps merging when a PATCH it queued before the rebuild comes back', async () => {
		const server = trimmed();
		const stored = encodeTextDoc(server);
		const h = harness(stored);
		const handle = await openTextHandle(h.deps);

		// The same character in two places: what turns a reused id into a
		// disagreement about placement rather than about content.
		await handle.insert(0, 'a');
		await handle.insert(handle.value.length, 'a');
		await flush();
		const queued = [ ...h.patches ];

		const compacted = decodeTextDoc(stored, '');
		compactDoc(compacted);
		h.deliver(encodeTextDoc(compacted));
		await flush();

		// Compaction refuses those PATCHes, but not before the overlay has folded
		// them into the row this handle is watching.
		h.deliver(applyFragmentBytes(encodeTextDoc(compacted), queued[0]!));

		expect(handle.status().state).toBe('open');
		handle.close();
	});

	it('pushes nothing when it held no unsynced edits', async () => {
		const server = trimmed();
		const h = harness(encodeTextDoc(server));
		const handle = await openTextHandle(h.deps);

		const compacted = decodeTextDoc(encodeTextDoc(server), '');
		compactDoc(compacted);
		h.deliver(encodeTextDoc(compacted));
		await flush();

		expect(handle.value).toBe('hello');
		expect(h.patches).toHaveLength(0);
		handle.close();
	});

	it('keeps editing normally afterwards', async () => {
		const server = trimmed();
		const h = harness(encodeTextDoc(server));
		const handle = await openTextHandle(h.deps);
		const compacted = decodeTextDoc(encodeTextDoc(server), '');
		compactDoc(compacted);
		h.deliver(encodeTextDoc(compacted));
		h.patches.length = 0;

		// Appending used to attach to the first character of the deleted run —
		// the item the purge removed. After the rebuild there is nothing there to
		// attach to and the edit is expressed against surviving state.
		await handle.insert(handle.value.length, '!');
		expect(handle.value).toBe('hello!');
		// The edit ships as a fragment, so what the server will hold is the
		// compacted state it already has with that fragment applied.
		const pushed = decodeTextDoc(
			applyFragmentBytes(encodeTextDoc(compacted), h.patches.at(-1)!),
			'',
		);
		expect(pushed.plainText()).toBe('hello!');
		expect(compactDoc(pushed).items).toBe(0);
		handle.close();
	});

	it('still merges ordinary remote state', async () => {
		const server = trimmed();
		const h = harness(encodeTextDoc(server));
		const handle = await openTextHandle(h.deps);
		await handle.insert(0, 'X');

		const remote = decodeTextDoc(encodeTextDoc(server), 'bob');
		remote.insert(5, '?');
		h.deliver(encodeTextDoc(remote));

		// A union, not a rebuild: both sides' edits survive.
		expect(handle.value).toBe('Xhello?');
		handle.close();
	});
});
