import { createRouterTransport, type Transport } from '@connectrpc/connect';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { CodeRoomClosed, CodeRoomDropped } from '../../src/codes.js';
import {
	PresenceService,
	type PublishItem,
	type WatchRequest,
	type WatchResponse,
} from '../../src/gen/syncplane/v1/presence_pb.js';
import { createRoom, type Peer, type Room, RoomSendError } from '../../src/room.js';
import { PresenceCoordinator } from '../../src/sync/presence.js';

/**
 * A scripted PresenceService. `frames` is pushed onto the open Watch stream, so
 * a spec drives the client the way a server would; `published` records the
 * exact PublishItems it received, which is the whole assertion surface for
 * batching and throttling.
 */
function scriptedServer(announceMs = 0): {
	transport: Transport;
	published: PublishItem[];
	publishedSessions: string[];
	watchRequests: WatchRequest[];
	push(frame: WatchResponse): void;
	endStream(): void;
	failStream(err: Error): void;
	openStreams(): number;
} {
	const published: PublishItem[] = [];
	const publishedSessions: string[] = [];
	const watchRequests: WatchRequest[] = [];
	let queue: WatchResponse[] = [];
	let wake: (() => void) | undefined;
	let ended = false;
	let failure: Error | undefined;
	let open = 0;

	const transport = createRouterTransport(({ service }) => {
		service(PresenceService, {
			// eslint-disable-next-line @typescript-eslint/require-await
			publish: async (req) => {
				publishedSessions.push(req.sessionId);
				published.push(...req.items);
				return {
					$typeName: 'syncplane.v1.PublishResponse',
					// Left unset unless a spec asks for it, so the keepalive
					// stays off for every spec that is not about it.
					announceInterval: announceMs === 0 ? undefined : {
						$typeName: 'google.protobuf.Duration' as const,
						seconds: BigInt(Math.floor(announceMs / 1000)),
						nanos: (announceMs % 1000) * 1e6,
					},
				};
			},
			watch: async function*(req, ctx) {
				watchRequests.push(req);
				open++;
				try {
					for (;;) {
						while (queue.length > 0) {
							const frame = queue.shift()!;
							yield frame;
						}
						if (failure) throw failure;
						if (ended || ctx.signal.aborted) return;
						// The client reopens by aborting, so the wait has to end
						// on the signal — otherwise the generator sits here and
						// the spec sees a stream that never closed.
						await new Promise<void>((resolve) => {
							wake = resolve;
							ctx.signal.addEventListener('abort', () => resolve(), { once: true });
						});
					}
				} finally {
					open--;
				}
			},
		});
	});

	return {
		transport,
		published,
		publishedSessions,
		watchRequests,
		push(frame) {
			queue.push(frame);
			wake?.();
			wake = undefined;
		},
		endStream() {
			ended = true;
			wake?.();
			wake = undefined;
		},
		failStream(err) {
			failure = err;
			wake?.();
			wake = undefined;
		},
		openStreams: () => open,
	};
}

const TOPIC = { workspaceId: 'w1', name: 'cursors' };

function topicFrame(body: WatchResponse['body']): WatchResponse {
	return {
		$typeName: 'syncplane.v1.WatchResponse',
		topic: {
			$typeName: 'syncplane.v1.Topic',
			workspaceId: TOPIC.workspaceId,
			name: TOPIC.name,
			entity: '',
			key: '',
		},
		body,
	};
}

function participant(sessionId: string, state: string, revision = 1n, seq = 0n) {
	return {
		$typeName: 'syncplane.v1.Participant' as const,
		sessionId,
		subject: `u:${sessionId}`,
		presence: {
			$typeName: 'syncplane.v1.PresenceUpdate' as const,
			state: new TextEncoder().encode(JSON.stringify(state)),
			revision,
			seq,
		},
	};
}

interface Harness {
	server: ReturnType<typeof scriptedServer>;
	coordinator: PresenceCoordinator;
	openRoom<S = string>(name?: string): Room<S>;
}

const harnesses: PresenceCoordinator[] = [];

function harness(
	sessionId = 's-self',
	opts: { announceMs?: number; watchEnabled?: boolean } = {},
): Harness {
	const server = scriptedServer(opts.announceMs ?? 0);
	const coordinator = new PresenceCoordinator({
		transport: () => Promise.resolve(server.transport),
		sessionId,
		presenceLock: 'test:presencelock',
		presenceChannel: 'test:presence',
		watchEnabled: opts.watchEnabled,
		minBackoffMs: 1,
		maxBackoffMs: 5,
		reopenDebounceMs: 1,
	});
	harnesses.push(coordinator);
	return {
		server,
		coordinator,
		openRoom<S = string>(name = TOPIC.name) {
			// createRoom is reached through the client in real use; here the
			// coordinator is driven directly so the spec owns the session id.
			return createRoom<S, S>(
				name,
				sessionId,
				undefined,
				(sink) => coordinator.attach({ ...TOPIC, name }, sink),
			);
		},
	};
}

afterEach(async () => {
	for (const c of harnesses.splice(0)) await c.stop();
	vi.useRealTimers();
});

describe('Room', () => {
	it('opens a Watch stream on the first room and names the session', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
		expect(h.server.watchRequests[0]!.sessionId).toBe('s-self');
		expect(h.server.watchRequests[0]!.topics[0]!.name).toBe('cursors');
		expect(room.status().state).toBe('joining');
	});

	it('applies a snapshot wholesale and reports open', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

		h.server.push(topicFrame({
			case: 'snapshot',
			value: {
				$typeName: 'syncplane.v1.RoomSnapshot',
				participants: [ participant('s1', 'a'), participant('s2', 'b') ],
				truncated: false,
			},
		}));

		await vi.waitFor(() => expect(room.peers()).toHaveLength(2));
		expect(room.status().state).toBe('open');
		expect(room.peers().map((p) => p.sessionId)).toEqual([ 's1', 's2' ]);
		expect(room.peers()[0]!.state).toBe('a');
		expect(room.peers()[0]!.subject).toBe('u:s1');
	});

	it('replaces the roster on a second snapshot', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

		h.server.push(topicFrame({
			case: 'snapshot',
			value: {
				$typeName: 'syncplane.v1.RoomSnapshot',
				participants: [ participant('s1', 'a') ],
				truncated: false,
			},
		}));
		await vi.waitFor(() => expect(room.peers()).toHaveLength(1));

		h.server.push(topicFrame({
			case: 'snapshot',
			value: {
				$typeName: 'syncplane.v1.RoomSnapshot',
				participants: [ participant('s2', 'b') ],
				truncated: false,
			},
		}));
		await vi.waitFor(() => expect(room.peers()[0]!.sessionId).toBe('s2'));
	});

	it('applies an update and a departure', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

		h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'a') }));
		await vi.waitFor(() => expect(room.peers()).toHaveLength(1));
		h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'b', 2n) }));
		await vi.waitFor(() => expect(room.peers()[0]!.state).toBe('b'));

		h.server.push(topicFrame({ case: 'left', value: 's1' }));
		await vi.waitFor(() => expect(room.peers()).toHaveLength(0));
	});

	it('carries the authoring sequence to the app', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
		h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'a', 1n, 42n) }));
		await vi.waitFor(() => expect(room.peers()[0]!.seq).toBe(42));
	});

	it('ignores a heartbeat', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
		const seen: unknown[] = [];
		room.subscribe((p) => seen.push(p));

		h.server.push({
			$typeName: 'syncplane.v1.WatchResponse',
			body: { case: 'heartbeat', value: true },
		});
		h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'a') }));
		await vi.waitFor(() => expect(seen).toHaveLength(1));
	});

	it('delivers a broadcast, flagging the sender', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
		const got: { message: string; self: boolean }[] = [];
		room.onMessage((m) => got.push({ message: m.message, self: m.self }));

		h.server.push(topicFrame({
			case: 'message',
			value: {
				$typeName: 'syncplane.v1.RoomMessage',
				senderSessionId: 's1',
				senderSubject: 'u:s1',
				payload: new TextEncoder().encode(JSON.stringify('ping')),
			},
		}));
		h.server.push(topicFrame({
			case: 'message',
			value: {
				$typeName: 'syncplane.v1.RoomMessage',
				senderSessionId: 's-self',
				senderSubject: 'u:me',
				payload: new TextEncoder().encode(JSON.stringify('mine')),
			},
		}));

		await vi.waitFor(() => expect(got).toHaveLength(2));
		expect(got).toEqual([
			{ message: 'ping', self: false },
			{ message: 'mine', self: true },
		]);
	});

	it('reports a dropped topic as a terminal error', async () => {
		const h = harness();
		const room = h.openRoom();
		await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
		const errors: string[] = [];
		room.onError((e) => errors.push(e.code));

		h.server.push(topicFrame({
			case: 'dropped',
			value: {
				$typeName: 'syncplane.v1.TopicDropped',
				reason: 1,
				message: 'not a member',
			},
		}));

		await vi.waitFor(() => expect(room.status().state).toBe('dropped'));
		expect(errors).toEqual([ 'room_dropped' ]);
		expect(room.status().error?.message).toBe('not a member');
	});

	describe('peers() snapshot identity', () => {
		// useSyncExternalStore tears if a snapshot's reference changes without
		// the value changing, and re-renders every memoized child if unchanged
		// peers get new objects.
		it('keeps the same reference across a no-op read', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
			h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'a') }));
			await vi.waitFor(() => expect(room.peers()).toHaveLength(1));

			expect(room.peers()).toBe(room.peers());
		});

		it('replaces the array but keeps unchanged peers by identity', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
			h.server.push(topicFrame({ case: 'updated', value: participant('s1', 'a') }));
			h.server.push(topicFrame({ case: 'updated', value: participant('s2', 'b') }));
			await vi.waitFor(() => expect(room.peers()).toHaveLength(2));

			const before = room.peers();
			const s1Before = before.find((p) => p.sessionId === 's1')!;
			h.server.push(topicFrame({ case: 'updated', value: participant('s2', 'c', 2n) }));
			await vi.waitFor(() => expect(room.peers().find((p) => p.sessionId === 's2')!.state).toBe('c'));

			const after = room.peers();
			expect(after).not.toBe(before);
			expect(after.find((p) => p.sessionId === 's1')).toBe(s1Before);
		});

		it('freezes the snapshot', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
			expect(Object.isFrozen(room.peers())).toBe(true);
		});
	});

	describe('publishing', () => {
		it('sends the first state immediately', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('hello');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));

			const item = h.server.published[0]!;
			expect(item.body.case).toBe('presence');
			expect(h.server.publishedSessions[0]).toBe('s-self');
			if (item.body.case !== 'presence') throw new Error('wrong body');
			expect(JSON.parse(new TextDecoder().decode(item.body.value.state))).toBe('hello');
		});

		// A cursor room calls setPresence per mouse-move; the intermediate
		// states are worthless and the last one must not be lost.
		it('throttles a burst down to the first and the last state', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('a');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));

			room.setPresence('b');
			room.setPresence('c');
			room.setPresence('d');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(2), { timeout: 2000 });

			const last = h.server.published[1]!;
			if (last.body.case !== 'presence') throw new Error('wrong body');
			expect(JSON.parse(new TextDecoder().decode(last.body.value.state))).toBe('d');
		});

		it('increments the revision per publish', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('a');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));
			room.setPresence('b');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(2), { timeout: 2000 });

			const revs = h.server.published.map((i) =>
				i.body.case === 'presence' ? i.body.value.revision : 0n
			);
			expect(revs[1]!).toBeGreaterThan(revs[0]!);
		});

		it('carries the authoring sequence', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('a', { seq: 77 });
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));
			const item = h.server.published[0]!;
			if (item.body.case !== 'presence') throw new Error('wrong body');
			expect(item.body.value.seq).toBe(77n);
		});

		it('sends a broadcast unthrottled', async () => {
			const h = harness();
			const room = h.openRoom();
			await room.send('one');
			await room.send('two');
			expect(h.server.published).toHaveLength(2);
			expect(h.server.published.every((i) => i.body.case === 'message')).toBe(true);
		});

		it('publishes a leave on close', async () => {
			const h = harness();
			const room = h.openRoom();
			await room.close();
			expect(h.server.published).toHaveLength(1);
			expect(h.server.published[0]!.body.case).toBe('leave');
		});

		it('refuses a send after close', async () => {
			const h = harness();
			const room = h.openRoom();
			await room.close();
			await expect(room.send('x')).rejects.toThrow(/closed/);
			await expect(room.send('x')).rejects.toMatchObject({ code: CodeRoomClosed });
			await expect(room.send('x')).rejects.toBeInstanceOf(RoomSendError);
		});

		it('refuses a send after the topic was dropped', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
			h.server.push(topicFrame({
				case: 'dropped',
				value: { $typeName: 'syncplane.v1.TopicDropped', reason: 1, message: 'no' },
			}));
			await vi.waitFor(() => expect(room.status().state).toBe('dropped'));
			await expect(room.send('x')).rejects.toThrow(/dropped/);
			await expect(room.send('x')).rejects.toMatchObject({ code: CodeRoomDropped });
		});

		it('ignores setPresence after close', async () => {
			const h = harness();
			const room = h.openRoom();
			await room.close();
			const sent = h.server.published.length;
			room.setPresence('late');
			await new Promise((r) => setTimeout(r, 50));
			expect(h.server.published).toHaveLength(sent);
		});
	});

	// The server holds a record only while it is watching the session's stream,
	// so a publish that raced the stream — or one made before a reconnect — left
	// nothing behind, and nothing durable exists to recover it from.
	describe('re-publishing across a stream open', () => {
		it('re-sends its own state when the snapshot does not contain it', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('mine');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));

			h.server.push(topicFrame({
				case: 'snapshot',
				value: {
					$typeName: 'syncplane.v1.RoomSnapshot',
					participants: [ participant('someone-else', 'x') ],
					truncated: false,
				},
			}));

			await vi.waitFor(() => expect(h.server.published).toHaveLength(2), { timeout: 2000 });
			const resent = h.server.published[1]!;
			if (resent.body.case !== 'presence') throw new Error('wrong body');
			expect(JSON.parse(new TextDecoder().decode(resent.body.value.state))).toBe('mine');
			// A fresh revision, so a peer holding the earlier one takes the
			// re-send rather than discarding it as stale.
			const first = h.server.published[0]!;
			if (first.body.case !== 'presence') throw new Error('wrong body');
			expect(resent.body.value.revision).toBeGreaterThan(first.body.value.revision);
		});

		it('does not re-send when the snapshot already holds its record', async () => {
			const h = harness();
			const room = h.openRoom();
			room.setPresence('mine');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));

			h.server.push(topicFrame({
				case: 'snapshot',
				value: {
					$typeName: 'syncplane.v1.RoomSnapshot',
					participants: [ participant('s-self', 'mine') ],
					truncated: false,
				},
			}));

			await vi.waitFor(() => expect(room.peers()).toHaveLength(1));
			await new Promise((r) => setTimeout(r, 100));
			expect(h.server.published).toHaveLength(1);
		});

		// With no Watch stream anywhere, nothing re-announces this session's
		// record, so the room keeps it alive itself on the interval the server
		// reports on every publish response.
		it('keeps its record alive itself when it holds no stream', async () => {
			const h = harness('s-self', { announceMs: 20, watchEnabled: false });
			const room = h.openRoom();
			room.setPresence('mine');
			// No intermediate wait on a count: the keepalive fires faster than
			// waitFor polls, so any exact length would race it.
			await vi.waitFor(() => expect(h.server.published.length).toBeGreaterThan(2), {
				timeout: 2000,
			});
			expect(h.server.watchRequests).toHaveLength(0);

			// The same revision as the publish it is refreshing: the server
			// takes it as liveness and fans nothing out to the watchers.
			const first = h.server.published[0]!;
			const refresh = h.server.published[1]!;
			if (first.body.case !== 'presence' || refresh.body.case !== 'presence') {
				throw new Error('wrong body');
			}
			expect(refresh.body.value.revision).toBe(first.body.value.revision);
			expect(refresh.body.value.state).toEqual(first.body.value.state);
		});

		it('stops the keepalive on close', async () => {
			const h = harness('s-self', { announceMs: 20, watchEnabled: false });
			const room = h.openRoom();
			room.setPresence('mine');
			await vi.waitFor(() => expect(h.server.published.length).toBeGreaterThan(1));

			await room.close();
			const afterClose = h.server.published.length;
			await new Promise((r) => setTimeout(r, 100));
			expect(h.server.published).toHaveLength(afterClose);
		});

		it('does not keep alive when a stream is holding the record', async () => {
			const h = harness('s-self', { announceMs: 20 });
			const room = h.openRoom();
			room.setPresence('mine');
			await vi.waitFor(() => expect(h.server.published).toHaveLength(1));
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

			await new Promise((r) => setTimeout(r, 120));
			expect(h.server.published).toHaveLength(1);
		});

		it('does not re-send for a session that never published', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

			h.server.push(topicFrame({
				case: 'snapshot',
				value: {
					$typeName: 'syncplane.v1.RoomSnapshot',
					participants: [ participant('someone-else', 'x') ],
					truncated: false,
				},
			}));
			await vi.waitFor(() => expect(room.peers()).toHaveLength(1));
			await new Promise((r) => setTimeout(r, 100));
			expect(h.server.published).toHaveLength(0);
		});
	});

	describe('the stream', () => {
		it('reopens after a transport failure', async () => {
			const h = harness();
			h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));
			h.server.failStream(new Error('connection lost'));
			await vi.waitFor(() => expect(h.server.watchRequests.length).toBeGreaterThan(1), {
				timeout: 3000,
			});
		});

		it('closes once the last room closes', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.openStreams()).toBe(1));
			await room.close();
			await vi.waitFor(() => expect(h.server.openStreams()).toBe(0), { timeout: 3000 });
		});

		it('carries every open room on one stream', async () => {
			const h = harness();
			h.openRoom('a');
			h.openRoom('b');
			await vi.waitFor(() => {
				const last = h.server.watchRequests.at(-1)!;
				expect(last.topics.map((t) => t.name).sort()).toEqual([ 'a', 'b' ]);
			}, { timeout: 3000 });
			await vi.waitFor(() => expect(h.server.openStreams()).toBe(1), { timeout: 3000 });
		});
	});

	describe('the codec', () => {
		it('round-trips through a custom codec', async () => {
			const server = scriptedServer();
			const coordinator = new PresenceCoordinator({
				transport: () => Promise.resolve(server.transport),
				sessionId: 's-self',
				presenceLock: 'test:presencelock',
				presenceChannel: 'test:presence',
				reopenDebounceMs: 1,
			});
			harnesses.push(coordinator);
			// A raw-bytes codec: what a cursor room packing encodeAnchor() uses.
			const room = createRoom<Uint8Array, Uint8Array>('raw', 's-self', {
				codec: {
					encodeState: (s) => s,
					decodeState: (b) => b,
					encodeMessage: (m) => m,
					decodeMessage: (b) => b,
				},
			}, (sink) => coordinator.attach({ workspaceId: 'w1', name: 'raw' }, sink));

			room.setPresence(new Uint8Array([ 1, 2, 3 ]));
			await vi.waitFor(() => expect(server.published).toHaveLength(1));
			const item = server.published[0]!;
			if (item.body.case !== 'presence') throw new Error('wrong body');
			expect([ ...item.body.value.state ]).toEqual([ 1, 2, 3 ]);
		});

		it('drops undecodable state from one peer without failing the room', async () => {
			const h = harness();
			const room = h.openRoom();
			await vi.waitFor(() => expect(h.server.watchRequests).toHaveLength(1));

			h.server.push(topicFrame({
				case: 'updated',
				value: {
					$typeName: 'syncplane.v1.Participant',
					sessionId: 's1',
					subject: 'u:s1',
					presence: {
						$typeName: 'syncplane.v1.PresenceUpdate',
						state: new Uint8Array([ 0xff, 0xfe ]), // not JSON
						revision: 1n,
						seq: 0n,
					},
				},
			}));
			await vi.waitFor(() => expect(room.peers()).toHaveLength(1));

			const peers: readonly Peer<string>[] = room.peers();
			expect(peers[0]!.state).toBeUndefined();
			expect(peers[0]!.sessionId).toBe('s1');
			expect(room.status().state).not.toBe('dropped');
		});
	});
});
