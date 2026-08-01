import { afterEach, describe, expect, it, vi } from 'vitest';

import {
	openTextHandle,
	type TextHandle,
	type TextHandleDeps,
	type TextHandleError,
} from '../../src/text/handle.js';
import { decodeTextDoc, encodeTextDoc, TextDoc } from '../../src/text/index.js';

/**
 * A handle driven through its deps, so a test can hand the row watch the exact
 * bytes a pull would deliver — including bytes no healthy server sends.
 * `deliver` calls the watcher the way the store's notify loop does: a throw
 * escaping it is the failure this file is about.
 */
function harness(initial: Uint8Array) {
	let watcher: ((state: Uint8Array | undefined) => void) | undefined;
	// Kept past unwatch, so a test can fire a notification the handle already
	// unsubscribed from — what a notify loop mid-iteration does.
	let raw: ((state: Uint8Array | undefined) => void) | undefined;
	let released = 0;
	const patches: Uint8Array[] = [];
	const pending: (() => void)[] = [];
	let hold = false;
	const deps: TextHandleDeps = {
		acquirePeer: () => Promise.resolve({ peer: 'p0', release: () => void released++ }),
		load: () => Promise.resolve(initial),
		patch: (state) => {
			patches.push(state);
			if (!hold) return Promise.resolve();
			return new Promise<void>((resolve) => pending.push(resolve));
		},
		watch: (cb) => {
			watcher = cb;
			raw = cb;
			return () => {
				watcher = undefined;
			};
		},
	};
	return {
		deps,
		patches,
		deliver: (state: Uint8Array) => raw!(state),
		/** Whether the handle is still watching the row. */
		get watching() {
			return watcher !== undefined;
		},
		get released() {
			return released;
		},
		/** Makes subsequent patches hang until releasePatches() is called. */
		holdPatches: () => {
			hold = true;
		},
		releasePatches: () => {
			hold = false;
			for (const resolve of pending.splice(0)) resolve();
		},
	};
}

function docBytes(peer: string, text: string): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, text);
	return encodeTextDoc(d);
}

/** Collects the failure a handle reports, if it reports one. */
function watchErrors(handle: TextHandle): TextHandleError[] {
	const errors: TextHandleError[] = [];
	handle.onError((err) => errors.push(err));
	return errors;
}

const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});

afterEach(() => {
	errorLog.mockClear();
});

describe('a handle handed state it cannot merge', () => {
	it('fails itself instead of throwing into the watcher loop', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		const errors = watchErrors(handle);

		// Bytes no decoder accepts: the row was written by something that did not
		// speak this format.
		expect(() => h.deliver(new Uint8Array([ 0xff, 0xff, 0xff ]))).not.toThrow();

		expect(handle.status().state).toBe('failed');
		expect(handle.status().error?.code).toBe('text_merge_failed');
		expect(errors).toHaveLength(1);
		expect(errors[0]!.message).toMatch(/stopped merging while merging a remote change/);
		expect(errors[0]!.cause).toBeInstanceOf(Error);
		// Nobody may be listening, so it is on the console too.
		expect(errorLog).toHaveBeenCalled();

		await handle.close();
	});

	it('fails on a merge conflict as well as on undecodable bytes', async () => {
		const seed = docBytes('seed', 'hello');
		const h = harness(seed);
		const handle = await openTextHandle(h.deps);
		const errors = watchErrors(handle);

		// A second document built on the same row under the same writer identity
		// — what a shared peer produces. Both mint the same id for their own
		// character, so the merge finds one id carrying two contents.
		const twin = decodeTextDoc(seed, 'p0');
		twin.insert(5, 'Y');
		await handle.insert(5, 'X');
		expect(() => h.deliver(encodeTextDoc(twin))).not.toThrow();

		expect(handle.status().error?.code).toBe('text_merge_failed');
		expect(errors[0]!.message).toMatch(/merge conflict/);

		await handle.close();
	});

	it('stops merging and stops watching once it has failed', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		h.deliver(new Uint8Array([ 0xff, 0xff, 0xff ]));
		expect(h.watching).toBe(false);

		const before = handle.value;
		const remote = new TextDoc('bob');
		remote.insert(0, 'hello!');
		// A watcher that fires anyway (a queued notification) changes nothing.
		expect(() => h.deliver(encodeTextDoc(remote))).not.toThrow();
		expect(handle.value).toBe(before);

		await handle.close();
	});

	it('refuses edits, keeps serving reads, and releases its peer on close', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		h.deliver(new Uint8Array([ 0xff, 0xff, 0xff ]));

		expect(() => handle.insert(0, 'x')).toThrow(/close this handle and open a new one/);
		expect(() => handle.setString('x')).toThrow(/stopped merging/);
		await expect(handle.mark(0, 1, 'b', true)).rejects.toThrow(/stopped merging/);
		// What the user was looking at is still readable — that is how an app
		// salvages it.
		expect(handle.value).toBe('hello');
		expect(handle.marks()).toEqual([]);

		await handle.close();
		expect(h.released).toBe(1);
		// Closing does not erase why: state moves on, the error stays.
		expect(handle.status()).toEqual({
			state: 'closed',
			error: expect.objectContaining({ code: 'text_merge_failed' }),
		});
	});

	it('reports the failure to a listener that arrives after it', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		h.deliver(new Uint8Array([ 0xff, 0xff, 0xff ]));

		const late: TextHandleError[] = [];
		const unsub = handle.onError((err) => late.push(err));
		expect(late).toHaveLength(1);
		unsub();

		await handle.close();
	});

	it('isolates a throwing onError listener from the others', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		handle.onError(() => {
			throw new Error('broken error listener');
		});
		const seen: TextHandleError[] = [];
		handle.onError((err) => seen.push(err));

		expect(() => h.deliver(new Uint8Array([ 0xff, 0xff, 0xff ]))).not.toThrow();
		expect(seen).toHaveLength(1);

		await handle.close();
	});
});

describe('closing a handle', () => {
	it('resolves once the writes queued before it have been enqueued', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		h.holdPatches();

		void handle.insert(5, '!');
		let drained = false;
		const closing = handle.close().then(() => void (drained = true));
		await Promise.resolve();
		expect(drained).toBe(false);

		h.releasePatches();
		await closing;
		expect(drained).toBe(true);
		// Drained, not cut: the edit the user made before closing still went out.
		expect(h.patches).toHaveLength(1);
	});

	it('never rejects, even when the write it drains failed', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const deps: TextHandleDeps = {
			...h.deps,
			patch: () => Promise.reject(new Error('store is gone')),
		};
		const handle = await openTextHandle(deps);

		// The edit's own caller is who hears about the failure.
		await expect(handle.insert(5, '!')).rejects.toThrow(/store is gone/);
		await expect(handle.close()).resolves.toBeUndefined();
	});

	it('is idempotent and still drains on a second call', async () => {
		const h = harness(docBytes('seed', 'hello'));
		const handle = await openTextHandle(h.deps);
		await handle.insert(5, '!');
		await handle.close();
		await handle.close();
		expect(h.released).toBe(1);
		expect(h.watching).toBe(false);
	});
});
