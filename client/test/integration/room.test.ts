import { describe, expect, it } from 'vitest';

import { CodeRoomDropped } from '../../src/codes.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import type { Peer, Room } from '../../src/room.js';
import { freshWorkspace, newClient } from './helpers.js';

/**
 * Rooms against the real Go server: the wire contract, the join gate and the
 * expiry rule, none of which a fake transport can answer for.
 *
 * Node has no Web Locks, so leadership declines and each client runs its own
 * Watch stream — which is exactly the shape these specs want, since each
 * SyncClient here stands in for a separate browser.
 */
describe('rooms', () => {
	/** Waits until `check` holds against the room's roster, or fails. */
	async function untilPeers<S>(
		room: Room<S>,
		check: (peers: readonly Peer<S>[]) => boolean,
		what: string,
	): Promise<readonly Peer<S>[]> {
		const deadline = Date.now() + 10_000;
		for (;;) {
			const peers = room.peers();
			if (check(peers)) return peers;
			if (Date.now() > deadline) {
				throw new Error(`timed out waiting for ${what}; saw ${JSON.stringify(peers)}`);
			}
			await new Promise((r) => setTimeout(r, 25));
		}
	}

	it("shows one client another client's presence", async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		const roomA = a.ws(ws).room<{ x: number }>('cursors');
		const roomB = b.ws(ws).room<{ x: number }>('cursors');
		roomA.setPresence({ x: 1 });
		roomB.setPresence({ x: 2 });

		const seenByA = await untilPeers(roomA, (p) => p.length === 2, 'both peers on A');
		const mine = seenByA.find((p) => p.self)!;
		const theirs = seenByA.find((p) => !p.self)!;
		expect(mine.sessionId).toBe(a.sessionId);
		expect(mine.state).toEqual({ x: 1 });
		expect(theirs.sessionId).toBe(b.sessionId);
		expect(theirs.state).toEqual({ x: 2 });

		await roomA.close();
		await roomB.close();
		await a.close();
		await b.close();
	});

	it('gives a later joiner the roster it missed', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		const roomA = a.ws(ws).room<string>('cursors');
		roomA.setPresence('here');
		await untilPeers(roomA, (p) => p.length === 1, 'A to see itself');

		const b = newClient(ws);
		await b.start();
		const roomB = b.ws(ws).room<string>('cursors');
		const seen = await untilPeers(roomB, (p) => p.length === 1, 'the snapshot on B');
		expect(seen[0]!.state).toBe('here');
		expect(seen[0]!.sessionId).toBe(a.sessionId);

		await roomA.close();
		await roomB.close();
		await a.close();
		await b.close();
	});

	it('reports a departure as soon as a peer closes its room', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		const roomA = a.ws(ws).room<string>('cursors');
		const roomB = b.ws(ws).room<string>('cursors');
		roomA.setPresence('a');
		roomB.setPresence('b');
		await untilPeers(roomA, (p) => p.length === 2, 'both peers');

		await roomB.close();
		await untilPeers(roomA, (p) => p.length === 1, 'B to leave');
		expect(roomA.peers()[0]!.self).toBe(true);

		await roomA.close();
		await a.close();
		await b.close();
	});

	// The liveness mechanism: nothing tells the server a client died, so the
	// record is dropped because nobody re-announced it. testserver runs a 500ms
	// announce interval, so the window is ~1.5s.
	it('expires a peer that disconnects without leaving', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		const roomA = a.ws(ws).room<string>('cursors');
		const roomB = b.ws(ws).room<string>('cursors');
		roomA.setPresence('a');
		roomB.setPresence('b');
		await untilPeers(roomA, (p) => p.length === 2, 'both peers');

		// close() the CLIENT without closing the room: the stream drops with no
		// departure published, which is what a killed tab looks like.
		await b.close();
		await untilPeers(roomA, (p) => p.length === 1, 'B to expire');

		await roomA.close();
		await a.close();
	});

	it('delivers a broadcast to another client', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		const b = newClient(ws);
		await a.start();
		await b.start();

		const roomA = a.ws(ws).room<string>('chat');
		const roomB = b.ws(ws).room<string>('chat');
		const received: string[] = [];
		roomB.onMessage((m) => received.push(m.message));
		// Both streams must be up before a broadcast is worth sending: it is
		// at-most-once, and reaches whoever is watching at that moment.
		roomA.setPresence('a');
		roomB.setPresence('b');
		await untilPeers(roomB, (p) => p.length === 2, 'both peers on B');

		await roomA.send('ping');
		const deadline = Date.now() + 10_000;
		while (received.length === 0 && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 25));
		}
		expect(received).toEqual([ 'ping' ]);

		await roomA.close();
		await roomB.close();
		await a.close();
		await b.close();
	});

	it('keeps two rooms of one workspace apart', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws);
		await a.start();
		const here = a.ws(ws).room<string>('here');
		const there = a.ws(ws).room<string>('there');
		here.setPresence('x');
		await untilPeers(here, (p) => p.length === 1, 'the record in "here"');
		expect(there.peers()).toHaveLength(0);

		await here.close();
		await there.close();
		await a.close();
	});

	describe('anchored rooms', () => {
		it('admits a caller who can read the anchor row', async () => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			await a.start();
			await a.mutate.put(TodoSchema, { id: 't1', title: 'plan' });
			await a.syncNow();

			const room = a.ws(ws).room<string>(TodoSchema, 't1', 'cursors');
			room.setPresence('typing');
			const peers = await untilPeers(room, (p) => p.length === 1, 'the anchored room to admit');
			expect(peers[0]!.self).toBe(true);
			expect(room.status().state).toBe('open');

			await room.close();
			await a.close();
		});

		// The example policy hides any todo whose title contains "secret", so
		// the room inherits the refusal without a policy of its own.
		it('refuses a room anchored to a row the read filter hides', async () => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			await a.start();
			await a.mutate.put(TodoSchema, { id: 's1', title: 'secret plan' });
			await a.syncNow();

			const room = a.ws(ws).room<string>(TodoSchema, 's1', 'cursors');
			const codes: string[] = [];
			room.onError((e) => codes.push(e.code));

			const deadline = Date.now() + 10_000;
			while (room.status().state !== 'dropped' && Date.now() < deadline) {
				await new Promise((r) => setTimeout(r, 25));
			}
			expect(room.status().state).toBe('dropped');
			expect(codes).toEqual([ CodeRoomDropped ]);

			await room.close();
			await a.close();
		});

		it('refuses a room anchored to a row that does not exist', async () => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			await a.start();

			const room = a.ws(ws).room<string>(TodoSchema, 'missing', 'cursors');
			const deadline = Date.now() + 10_000;
			while (room.status().state !== 'dropped' && Date.now() < deadline) {
				await new Promise((r) => setTimeout(r, 25));
			}
			expect(room.status().state).toBe('dropped');

			await room.close();
			await a.close();
		});

		it('keeps two anchors of one room name apart', async () => {
			const ws = freshWorkspace();
			const a = newClient(ws);
			await a.start();
			await a.mutate.put(TodoSchema, { id: 't1', title: 'one' });
			await a.mutate.put(TodoSchema, { id: 't2', title: 'two' });
			await a.syncNow();

			const one = a.ws(ws).room<string>(TodoSchema, 't1', 'cursors');
			const two = a.ws(ws).room<string>(TodoSchema, 't2', 'cursors');
			one.setPresence('in one');
			await untilPeers(one, (p) => p.length === 1, 'the record in t1');
			expect(two.peers()).toHaveLength(0);

			await one.close();
			await two.close();
			await a.close();
		});
	});

	// The subject is what an app renders as "who is here", so it must come from
	// the server's view of the caller and never from the payload.
	it('stamps the subject from the authenticated principal', async () => {
		const ws = freshWorkspace();
		const { authenticatedTransport } = await import('./helpers.js');
		const a = newClient(ws, { transport: authenticatedTransport('alice') });
		await a.start();

		const room = a.ws(ws).room<string>('cursors');
		room.setPresence('hi');
		const peers = await untilPeers(room, (p) => p.length === 1, 'the record');
		expect(peers[0]!.subject).toBe('alice');

		await room.close();
		await a.close();
	});
});
