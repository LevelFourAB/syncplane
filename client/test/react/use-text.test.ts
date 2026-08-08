// useText: the React binding over a live TextHandle. Covers the loading gap,
// re-render on local and remote edits, mark-span memoization, setText, the
// missing-row and stopped-merging errors, and handle lifecycle across unmount
// and identity changes.

import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { CodeTextMergeFailed } from '../../src/codes.js';
import { NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';
import type { TextHandle } from '../../src/text/handle.js';
import { encodeTextDoc, ExpandNone, TextDoc } from '../../src/text/index.js';

const { db, SyncProvider } = createReactSync([ NoteSchema ]);

function newClient(): SyncClient<readonly [typeof NoteSchema]> {
	return new SyncClient({
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ NoteSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
	});
}

function providerWrapper(client: SyncClient) {
	return function Wrapper({ children }: { children: ReactNode }): ReactNode {
		return createElement(SyncProvider, { client }, children);
	};
}

/** Encoded document under a foreign peer — stands in for another writer. */
function bodyDoc(peer: string, s: string, mark?: { start: number; end: number }): Uint8Array {
	const d = new TextDoc(peer);
	d.insert(0, s);
	if (mark) d.mark(mark.start, mark.end, 'bold', true, ExpandNone);
	return encodeTextDoc(d);
}

describe('useText', () => {
	it('reports loading, then the value once the handle opens', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });

		const { result } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		expect(result.current.value).toBeUndefined();
		expect(result.current.handle).toBeUndefined();

		await waitFor(() => expect(result.current.value).toBe('hello'));
		expect(result.current.marks).toEqual([]);
		expect(result.current.error).toBeUndefined();
		await client.close();
	});

	it('re-renders on a local edit and on setText', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });
		const { result } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('hello'));
		const handle = result.current.handle;

		await act(async () => {
			await result.current.handle!.insert(5, ' world');
		});
		expect(result.current.value).toBe('hello world');

		await act(async () => {
			await result.current.setText('hello there world');
		});
		expect(result.current.value).toBe('hello there world');
		// setText is stable and the handle survives every edit — an editor
		// binding holding either keeps working.
		expect(result.current.handle).toBe(handle);
		await client.close();
	});

	it('merges a remote change without replacing the handle', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });
		const { result } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('hello'));
		const handle = result.current.handle;

		// Another writer's state arrives on the row: a PUT unions text fields.
		await act(async () => {
			await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('other', '!') });
		});

		await waitFor(() => expect(result.current.value).toContain('!'));
		expect(result.current.value).toContain('hello');
		expect(result.current.handle).toBe(handle);
		await client.close();
	});

	it('keeps the marks array stable until a delta says spans may have moved', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });
		const { result } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('hello'));

		// Unmarked document: typing cannot move a span, so the empty array is
		// handed back unchanged.
		const before = result.current.marks;
		await act(async () => {
			await result.current.handle!.insert(5, '!');
		});
		expect(result.current.value).toBe('hello!');
		expect(result.current.marks).toBe(before);

		await act(async () => {
			await result.current.handle!.mark(0, 5, 'bold', true);
		});
		expect(result.current.marks).toEqual([ { start: 0, end: 5, key: 'bold', value: true } ]);

		// Now that a mark exists, an insert inside it re-resolves the spans.
		const marked = result.current.marks;
		await act(async () => {
			await result.current.handle!.insert(2, 'X');
		});
		expect(result.current.marks).not.toBe(marked);
		expect(result.current.marks).toEqual([ { start: 0, end: 6, key: 'bold', value: true } ]);
		await client.close();
	});

	it('surfaces a missing row as an error and rejects setText while unopened', async () => {
		const client = newClient();
		const { result } = renderHook(() => db.note.useText('nope', 'body'), {
			wrapper: providerWrapper(client),
		});
		await expect(result.current.setText('x')).rejects.toThrow(/not open yet/);

		await waitFor(() => expect(result.current.error).toBeDefined());
		expect(result.current.error?.message).toMatch(/row not found/);
		expect(result.current.value).toBeUndefined();
		await client.close();
	});

	it('surfaces a handle that stopped merging, keeping its last value', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });
		// A handle only fails on state it cannot reconcile, which no healthy
		// client/server pair produces; the hook's own contract is what is under
		// test, so the failure is delivered directly.
		const real = await client.db.note.text('n1', 'body');
		let failNow = (): void => {};
		const failing: TextHandle = {
			...real,
			get value() {
				return real.value;
			},
			marks: () => real.marks(),
			subscribe: (cb) => real.subscribe(cb),
			onError: (cb) => {
				failNow = () =>
					cb({ message: 'text: handle stopped merging', code: CodeTextMergeFailed, cause: undefined });
				return () => {};
			},
		};
		vi.spyOn(client.ws('ws1'), 'text').mockResolvedValue(failing);

		const { result } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('hello'));

		act(() => failNow());
		expect(result.current.error?.message).toMatch(/stopped merging/);
		// The text stays on screen — stale, but it is what the user was reading.
		expect(result.current.value).toBe('hello');

		vi.restoreAllMocks();
		await real.close();
		await client.close();
	});

	it('closes the handle on unmount and reopens the field cleanly', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'hello') });
		const { result, unmount } = renderHook(() => db.note.useText('n1', 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('hello'));
		unmount();
		expect(() => result.current.handle!.insert(0, 'x')).toThrow(/handle is closed/);

		// A handle opened afterwards holds a writer identity of its own and edits
		// with it. Whether that identity is the recycled slot depends on there
		// being a lock manager, which jsdom has none of — see test/unit/text-peer.
		const next = await client.db.note.text('n1', 'body');
		await next.insert(0, 'X');
		expect(next.value).toBe('Xhello');
		next.close();
		await client.close();
	});

	it('reopens and clears the previous value when the row key changes', async () => {
		const client = newClient();
		await client.mutate.put(NoteSchema, { id: 'n1', body: bodyDoc('seed', 'first') });
		await client.mutate.put(NoteSchema, { id: 'n2', body: bodyDoc('seed', 'second') });

		let key = 'n1';
		const { result, rerender } = renderHook(() => db.note.useText(key, 'body'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(result.current.value).toBe('first'));

		key = 'n2';
		rerender();
		// The old row's text is never shown under the new key.
		expect(result.current.value).toBeUndefined();
		await waitFor(() => expect(result.current.value).toBe('second'));
		await client.close();
	});
});
