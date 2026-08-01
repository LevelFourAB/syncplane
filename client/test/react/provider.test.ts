import { act, render, renderHook } from '@testing-library/react';
import { Component, createElement, type ReactNode, StrictMode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { SyncClient, type SyncClientOptions } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync, useSyncClient } from '../../src/react/index.js';

const { db, SyncProvider } = createReactSync([ TodoSchema ]);

type Opts = SyncClientOptions<readonly [typeof TodoSchema]>;

/** Options for a provider-owned client; `transport`/`url` left out on purpose. */
function options(extra?: Partial<Opts>): Opts {
	return {
		workspaceId: 'ws1',
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema ],
		streamEnabled: false,
		pollIntervalMs: 0,
		// A transport that is never reached: these tests only exercise local
		// reads/writes, which need no server.
		transport: {} as Opts['transport'],
		...extra,
	};
}

/** Runs the macrotask the deferred close is scheduled on. */
function flush(): Promise<void> {
	return act(async () => {
		await new Promise((r) => setTimeout(r, 0));
	});
}

/** Renders `fallback` instead of its children once anything below it throws. */
class Boundary extends Component<{ children?: ReactNode; fallback: string }, { failed: boolean }> {
	state = { failed: false };
	static getDerivedStateFromError(): { failed: boolean } {
		return { failed: true };
	}
	override render(): ReactNode {
		return this.state.failed ? this.props.fallback : this.props.children;
	}
}

afterEach(async () => {
	vi.restoreAllMocks();
	// RTL's cleanup unmounts the tree, which SCHEDULES the deferred close. Run it
	// here so a leftover timer cannot fire inside the next test.
	await new Promise((r) => setTimeout(r, 0));
});

describe('SyncProvider lifecycle', () => {
	it('constructs, starts and provides a client built from options', async () => {
		const start = vi.spyOn(SyncClient.prototype, 'start');
		const { result } = renderHook(() => useSyncClient(), {
			wrapper: ({ children }) => createElement(SyncProvider, { options: options() }, children),
		});
		await flush();

		expect(result.current).toBeInstanceOf(SyncClient);
		expect(start).toHaveBeenCalledTimes(1);
		// The provided client is a working one, not just an object in context.
		await act(async () => {
			await result.current.mutate.put(TodoSchema, { id: 't1', title: 'owned' });
		});
		expect(await result.current.get(TodoSchema, 't1')).toBeDefined();
	});

	it('closes the owned client when the tree unmounts', async () => {
		const { result, unmount } = renderHook(() => useSyncClient(), {
			wrapper: ({ children }) => createElement(SyncProvider, { options: options() }, children),
		});
		await flush();
		// Spied per instance, not on the prototype: another test's deferred close
		// runs on this test's timers and would otherwise be counted here.
		const close = vi.spyOn(result.current, 'close');
		expect(close).not.toHaveBeenCalled();

		unmount();
		expect(close).not.toHaveBeenCalled(); // deferred, not immediate
		await flush();
		expect(close).toHaveBeenCalledTimes(1);
	});

	it("survives StrictMode's mount/unmount/remount without closing the client", async () => {
		const start = vi.spyOn(SyncClient.prototype, 'start');
		let captured: SyncClient | undefined;
		function Capture(): ReactNode {
			captured = useSyncClient();
			return 'ready';
		}
		// render(), not renderHook(): only the former actually double-invokes
		// effects under StrictMode here, and the double-invoke IS the test.
		render(
			createElement(
				StrictMode,
				null,
				createElement(SyncProvider, { options: options() }, createElement(Capture)),
			),
		);
		// close() is one-shot: had the remount not cancelled the close the first
		// unmount scheduled, this client would be a corpse by the flush below.
		const close = vi.spyOn(captured!, 'close');
		await flush();

		expect(start).toHaveBeenCalledTimes(2); // once per effect run; memoized inside
		expect(close).not.toHaveBeenCalled();
		await act(async () => {
			await captured!.mutate.put(TodoSchema, { id: 't1', title: 'still alive' });
		});
		expect(await captured!.get(TodoSchema, 't1')).toBeDefined();
	});

	it('leaves a caller-owned client alone', async () => {
		const client = new SyncClient(options());
		const close = vi.spyOn(client, 'close');
		const { unmount } = renderHook(() => useSyncClient(), {
			wrapper: ({ children }) => createElement(SyncProvider, { client }, children),
		});
		await flush();
		unmount();
		await flush();
		expect(close).not.toHaveBeenCalled();
		await client.close();
	});

	it('refuses both a client and options, and neither', () => {
		const client = new SyncClient(options());
		expect(() => render(createElement(SyncProvider, { client, options: options() }))).toThrow(
			/exactly one of/,
		);
		expect(() => render(createElement(SyncProvider, {}))).toThrow(/exactly one of/);
		void client.close();
	});

	it('reports a failed start to onStartError', async () => {
		const onStartError = vi.fn();
		// No url and no transport: start() rejects on configuration alone.
		const opts = { ...options(), transport: undefined };
		renderHook(() => useSyncClient(), {
			wrapper: ({ children }) =>
				createElement(SyncProvider, { options: opts, onStartError }, children),
		});
		await flush();
		expect(onStartError).toHaveBeenCalledTimes(1);
		expect(String(onStartError.mock.calls[0]?.[0])).toMatch(/url|transport/i);
	});

	it('throws an unhandled start failure to the nearest error boundary', async () => {
		vi.spyOn(console, 'error').mockImplementation(() => {});
		const opts = { ...options(), transport: undefined };
		const { container } = render(
			createElement(
				Boundary,
				{ fallback: 'start failed' },
				createElement(SyncProvider, { options: opts }, createElement(Child)),
			),
		);
		await flush();
		expect(container.textContent).toBe('start failed');
	});
});

/** Reads the client so the subtree genuinely depends on the provider. */
function Child(): ReactNode {
	useSyncClient();
	return 'ready';
}

describe('provider-owned client and the hooks', () => {
	it('serves the entity hooks from the owned client', async () => {
		const { result } = renderHook(() => db.todo.useQuery({}), {
			wrapper: ({ children }) => createElement(SyncProvider, { options: options() }, children),
		});
		const mutate = renderHook(() => db.todo.useMutate(), {
			wrapper: ({ children }) => createElement(SyncProvider, { options: options() }, children),
		});
		await flush();
		expect(result.current).toEqual([]);
		await act(async () => {
			await mutate.result.current.put({ id: 't1', title: 'from a hook' });
		});
		await flush();
		// Two providers, two owned clients, two stores: the query must NOT see it.
		expect(result.current).toEqual([]);
	});
});
