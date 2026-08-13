// useRoom / usePresence: the React binding over a live Room. Covers the
// joining gap, re-render on roster changes, the identity guard, message
// delivery, and the room lifecycle across unmount and identity changes.
//
// The transport is a scripted PresenceService, so the specs drive the roster
// the way a server would rather than asserting against a real one.

import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';

import { SyncClient } from '../../src/client.js';
import { NoteSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import {
	PresenceService,
	type PublishItem,
	type WatchResponse,
} from '../../src/gen/syncplane/v1/presence_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { createReactSync } from '../../src/react/index.js';

const { SyncProvider, useRoom, usePresence } = createReactSync([ NoteSchema ]);

interface Server {
	transport: Transport;
	published: PublishItem[];
	watchedTopics(): string[][];
	push(frame: WatchResponse): void;
}

function scriptedServer(): Server {
	const published: PublishItem[] = [];
	const topics: string[][] = [];
	const queue: WatchResponse[] = [];
	let wake: (() => void) | undefined;

	const transport = createRouterTransport(({ service }) => {
		service(PresenceService, {
			// eslint-disable-next-line @typescript-eslint/require-await
			publish: async (req) => {
				published.push(...req.items);
				return { $typeName: 'syncplane.v1.PublishResponse' };
			},
			watch: async function*(req, ctx) {
				topics.push(req.topics.map((t) => t.name));
				for (;;) {
					while (queue.length > 0) yield queue.shift()!;
					if (ctx.signal.aborted) return;
					await new Promise<void>((resolve) => {
						wake = resolve;
						ctx.signal.addEventListener('abort', () => resolve(), { once: true });
					});
				}
			},
		});
	});

	return {
		transport,
		published,
		watchedTopics: () => topics,
		push(frame) {
			queue.push(frame);
			wake?.();
			wake = undefined;
		},
	};
}

function newClient(transport: Transport): SyncClient<readonly [typeof NoteSchema]> {
	return new SyncClient({
		transport,
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

function frame(name: string, body: WatchResponse['body']): WatchResponse {
	return {
		$typeName: 'syncplane.v1.WatchResponse',
		topic: {
			$typeName: 'syncplane.v1.Topic',
			workspaceId: 'ws1',
			name,
			entity: '',
			key: '',
		},
		body,
	};
}

function participant(sessionId: string, state: string) {
	return {
		$typeName: 'syncplane.v1.Participant' as const,
		sessionId,
		subject: `u:${sessionId}`,
		presence: {
			$typeName: 'syncplane.v1.PresenceUpdate' as const,
			state: new TextEncoder().encode(JSON.stringify(state)),
			revision: 1n,
			seq: 0n,
		},
	};
}

function snapshot(name: string, ...people: ReturnType<typeof participant>[]): WatchResponse {
	return frame(name, {
		case: 'snapshot',
		value: {
			$typeName: 'syncplane.v1.RoomSnapshot',
			participants: people,
			truncated: false,
		},
	});
}

describe('useRoom', () => {
	it('reports loading, then the roster once the room joins', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		expect(result.current.loading).toBe(true);
		expect(result.current.peers).toEqual([]);

		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors', participant('s1', 'a'))));

		await waitFor(() => expect(result.current.loading).toBe(false));
		expect(result.current.peers).toHaveLength(1);
		expect(result.current.peers[0]!.state).toBe('a');
		expect(result.current.room).toBeDefined();
		await client.close();
	});

	it('re-renders as people arrive and leave', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors')));
		await waitFor(() => expect(result.current.loading).toBe(false));

		act(() => server.push(frame('cursors', { case: 'updated', value: participant('s1', 'a') })));
		await waitFor(() => expect(result.current.peers).toHaveLength(1));

		act(() => server.push(frame('cursors', { case: 'left', value: 's1' })));
		await waitFor(() => expect(result.current.peers).toHaveLength(0));
		await client.close();
	});

	// useSyncExternalStore tears if a snapshot's reference moves without the
	// value moving, so the hook must hand back the same array between changes.
	it('keeps the peers reference stable across re-renders', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result, rerender } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors', participant('s1', 'a'))));
		await waitFor(() => expect(result.current.peers).toHaveLength(1));

		const before = result.current.peers;
		rerender();
		expect(result.current.peers).toBe(before);
		await client.close();
	});

	it('delivers broadcast messages to the callback', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const got: string[] = [];
		renderHook(() => useRoom<string>('chat', { onMessage: (m) => got.push(m.message) }), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));

		act(() =>
			server.push(frame('chat', {
				case: 'message',
				value: {
					$typeName: 'syncplane.v1.RoomMessage',
					senderSessionId: 's1',
					senderSubject: 'u:s1',
					payload: new TextEncoder().encode(JSON.stringify('ping')),
				},
			}))
		);
		await waitFor(() => expect(got).toEqual([ 'ping' ]));
		await client.close();
	});

	it('reports a dropped topic as an error', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));

		act(() =>
			server.push(frame('cursors', {
				case: 'dropped',
				value: {
					$typeName: 'syncplane.v1.TopicDropped',
					reason: 1,
					message: 'not a member',
				},
			}))
		);
		await waitFor(() => expect(result.current.error).toBeDefined());
		expect(result.current.error?.message).toBe('not a member');
		await client.close();
	});

	// A changed name must never render the previous room's peers, however
	// briefly — an avatar strip showing the wrong document's readers is worse
	// than one showing none.
	it('reports loading again when the room name changes', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result, rerender } = renderHook(
			({ name }: { name: string }) => useRoom<string>(name),
			{ wrapper: providerWrapper(client), initialProps: { name: 'a' } },
		);
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('a', participant('s1', 'in-a'))));
		await waitFor(() => expect(result.current.peers).toHaveLength(1));

		rerender({ name: 'b' });
		expect(result.current.loading).toBe(true);
		expect(result.current.peers).toEqual([]);
		await client.close();
	});

	it('publishes a departure when the component unmounts', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result, unmount } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors')));
		await waitFor(() => expect(result.current.room).toBeDefined());

		unmount();
		await waitFor(() => expect(server.published.some((i) => i.body.case === 'leave')).toBe(true));
		await client.close();
	});

	it('publishes through the handle it hands back', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result } = renderHook(() => useRoom<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors')));
		await waitFor(() => expect(result.current.room).toBeDefined());

		act(() => result.current.room!.setPresence('hello'));
		await waitFor(() => expect(server.published.some((i) => i.body.case === 'presence')).toBe(true));
		await client.close();
	});

	it('anchors a room to a row', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		renderHook(() => useRoom<string>(NoteSchema, 'n1', 'cursors'), {
			wrapper: providerWrapper(client),
		});
		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		await client.close();
	});
});

describe('usePresence', () => {
	it('returns just the roster', async () => {
		const server = scriptedServer();
		const client = newClient(server.transport);
		const { result } = renderHook(() => usePresence<string>('cursors'), {
			wrapper: providerWrapper(client),
		});
		expect(result.current).toEqual([]);

		await waitFor(() => expect(server.watchedTopics()).toHaveLength(1));
		act(() => server.push(snapshot('cursors', participant('s1', 'a'))));
		await waitFor(() => expect(result.current).toHaveLength(1));
		expect(result.current[0]!.subject).toBe('u:s1');
		await client.close();
	});
});
