// Live handle over one room — the client API boundary for the ephemeral plane.
//
// A room carries two things that do not belong in the mutation log: soft state,
// one record per session, expired by timeout rather than deleted; and broadcast
// messages, delivered to whoever is watching at the moment they arrive. Neither
// is stored, replayed or acknowledged. The dividing test is "would a reader
// arriving later want this?" — if yes it is a row, and it goes through
// `mutate`.
//
// App state travels through a codec so the library never has to guess a format:
// the default is JSON, and a cursor room hands `encodeAnchor()` bytes straight
// through instead.

import { CodeRoomClosed, CodeRoomDropped, CodeRoomOffline } from './codes.js';
import { validateRoomName } from './naming.js';
import { safeInvoke, safeRun } from './report.js';

/**
 * Encodes and decodes the app's own presence state and broadcast payloads.
 *
 * `decode` runs on bytes another client encoded, possibly an older build of the
 * app, so it must tolerate anything: throwing from it drops that one
 * participant's state rather than failing the room.
 */
export interface RoomCodec<S, M = S> {
	encodeState(state: S): Uint8Array;
	decodeState(bytes: Uint8Array): S;
	encodeMessage(message: M): Uint8Array;
	decodeMessage(bytes: Uint8Array): M;
}

/**
 * One peer in a room, as this client sees it.
 *
 * `Peer` objects are immutable and kept by identity across updates: a peer
 * whose state did not change in a given frame is the same object it was in the
 * previous `peers()` snapshot, so `React.memo` and `key`-based reconciliation
 * behave.
 */
export interface Peer<S> {
	/** The publishing session. Stable while that tab is open. */
	readonly sessionId: string;
	/**
	 * Who is publishing, stamped by the server from the request principal.
	 * Empty when the publisher was unauthenticated. Never client-supplied, so it
	 * is safe to render as an identity.
	 */
	readonly subject: string;
	/** The decoded state, or undefined when the codec could not decode it. */
	readonly state: S | undefined;
	/**
	 * The workspace sequence the state was authored against, as the publisher
	 * last saw it; 0 when it did not say.
	 *
	 * Rooms are not synchronized with the data stream, so a peer's cursor can
	 * name a row — or a position inside a collaborative-text field — this client
	 * has not pulled yet. Compare it against what you have synced to tell "I am
	 * behind" from "this state is nonsense".
	 */
	readonly seq: number;
	/** True for this client's own record. */
	readonly self: boolean;
}

/** A broadcast message received in a room. */
export interface RoomMessage<M> {
	senderSessionId: string;
	senderSubject: string;
	message: M;
	/** True when this client sent it — the sender is not excluded. */
	self: boolean;
}

/** Why a room stopped. */
export interface RoomError {
	message: string;
	code: typeof CodeRoomDropped | typeof CodeRoomOffline;
	/** The exception that caused it, for logs. */
	cause?: unknown;
}

/**
 * Thrown by `Room.send` when the room itself refuses the call rather than the
 * transport failing it: `CodeRoomClosed` once the app has closed the room,
 * `CodeRoomDropped` once the server has withdrawn it.
 *
 * Both are terminal, so a caller that catches one should stop sending. A send
 * the room accepted and the transport then failed rejects with the transport's
 * own error instead.
 */
export class RoomSendError extends Error {
	readonly code: typeof CodeRoomClosed | typeof CodeRoomDropped;

	constructor(message: string, code: typeof CodeRoomClosed | typeof CodeRoomDropped) {
		super(message);
		this.name = 'RoomSendError';
		this.code = code;
	}
}

/** Lifecycle of a room (see Room.status). */
export interface RoomStatus {
	/**
	 * `joining` — opened, no roster yet. `open` — live. `waiting` — the caller
	 * holds no grant in the workspace yet, so the server refused the room; the
	 * roster is empty, and the client asks again with backoff, 1 to 30 seconds
	 * apart, and at once when the caller's access in the workspace changes.
	 * The room goes to `open` when a grant admits the caller. `dropped` — the
	 * server refused or withdrew the room for another reason, see `error`;
	 * this is terminal and reopening will not change it. `closed` — the app
	 * closed it.
	 */
	state: 'joining' | 'open' | 'waiting' | 'dropped' | 'closed';
	/** Set exactly when the room was dropped, whether or not it was closed after. */
	error?: RoomError;
}

/**
 * Live handle over one room.
 *
 * Nothing here is durable, and nothing is queued while offline. `setPresence`
 * on a client with no connection is remembered and published on reconnect, so a
 * cursor is never stale-but-shown; `send` fails, because silently swallowing a
 * broadcast is worse than losing one the caller was told about.
 */
export interface Room<S, M = S> {
	/** The room's name within its workspace. */
	readonly name: string;
	/** This client's session id, which its own records are keyed on. */
	readonly sessionId: string;

	/**
	 * Publishes this session's state, replacing whatever it published before.
	 * There is no merge — soft state is re-sent in full on every announcement
	 * anyway.
	 *
	 * Publishing is throttled, so calling it per mouse-move is the intended
	 * usage: intermediate states are dropped and the newest is sent.
	 *
	 * Pass `seq` when the state names something in the synced data — a row key,
	 * a text anchor — so a peer that has not caught up can tell.
	 */
	setPresence(state: S, opts?: { seq?: number }): void;

	/**
	 * Broadcasts a message to whoever is watching now. At most once, to nobody
	 * if nobody is there.
	 *
	 * Rejects with a `RoomSendError` when the room is closed or dropped, and
	 * with the transport's own error when the client could not reach the
	 * server. It does not queue: a message that arrives after the moment it was
	 * about is worse than one that did not arrive.
	 */
	send(message: M): Promise<void>;

	/**
	 * The current roster, this client's own record included.
	 *
	 * The returned array is frozen and its reference changes only when the
	 * roster actually changed, which is what `useSyncExternalStore` requires of
	 * a snapshot.
	 */
	peers(): readonly Peer<S>[];

	/** Subscribes to roster changes. Returns an unsubscribe function. */
	subscribe(cb: (peers: readonly Peer<S>[]) => void): () => void;

	/** Subscribes to broadcast messages. Returns an unsubscribe function. */
	onMessage(cb: (msg: RoomMessage<M>) => void): () => void;

	/** Subscribes to the room being dropped. Returns an unsubscribe function. */
	onError(cb: (err: RoomError) => void): () => void;

	status(): RoomStatus;

	/**
	 * Leaves the room: withdraws this session's record so the other participants
	 * see it go immediately rather than waiting out the expiry window, and stops
	 * delivering. Idempotent.
	 */
	close(): Promise<void>;
}

/** The default codec: JSON, UTF-8 encoded. */
export function jsonCodec<S, M = S>(): RoomCodec<S, M> {
	const enc = new TextEncoder();
	const dec = new TextDecoder();
	const encode = (v: unknown): Uint8Array => enc.encode(JSON.stringify(v));
	return {
		encodeState: encode,
		decodeState: (b) => JSON.parse(dec.decode(b)) as S,
		encodeMessage: encode,
		decodeMessage: (b) => JSON.parse(dec.decode(b)) as M,
	};
}

/** Options accepted when opening a room. */
export interface RoomOptions<S, M = S> {
	/** How the app's state and messages are encoded. Default: JSON. */
	codec?: RoomCodec<S, M>;
	/**
	 * Smallest gap between two published states, in milliseconds. The first
	 * call in a quiet period goes out immediately and the last of a burst goes
	 * out at the end of it, so a cursor never lags behind where it stopped.
	 * Default 60.
	 */
	throttleMs?: number;
}

/** The raw frames the sync layer feeds a room, before decoding. */
export interface RawParticipant {
	sessionId: string;
	subject: string;
	state: Uint8Array;
	revision: bigint;
	seq: bigint;
}

/**
 * What a Room needs from the sync layer. Implemented by sync/presence.ts, which
 * owns the Watch stream and the cross-tab sharing; the room itself is pure
 * state and knows nothing about transports.
 *
 * @internal
 */
export interface RoomLink {
	publishState(state: Uint8Array, seq: number, revision: bigint): Promise<void>;
	publishMessage(payload: Uint8Array): Promise<void>;
	publishLeave(): Promise<void>;
	/** Stops delivering frames to this room. */
	detach(): void;
}

/**
 * The callbacks the sync layer drives a room through.
 *
 * @internal
 */
export interface RoomSink {
	onSnapshot(participants: RawParticipant[], truncated: boolean): void;
	onUpdated(participant: RawParticipant): void;
	onLeft(sessionId: string): void;
	onMessage(senderSessionId: string, senderSubject: string, payload: Uint8Array): void;
	onDropped(message: string): void;
	/** The server refused the room until the caller holds a grant in the workspace. */
	onWaiting(): void;
	/**
	 * Asks the room to keep its own record alive every `intervalMs`, because no
	 * Watch stream is doing it. Called with the interval the server reported;
	 * calling it again with a different one re-arms at the new period.
	 */
	onKeepalive(intervalMs: number): void;
	onOpen(): void;
}

/**
 * Builds a room over a link the sync layer supplies.
 *
 * @internal
 */
export function createRoom<S, M>(
	name: string,
	sessionId: string,
	opts: RoomOptions<S, M> | undefined,
	attach: (sink: RoomSink) => RoomLink,
): Room<S, M> {
	validateRoomName(name);
	const codec = opts?.codec ?? jsonCodec<S, M>();
	const throttleMs = opts?.throttleMs ?? 60;

	// Records by session id. The frozen snapshot below is derived from it and
	// rebuilt only when something actually changed, so a subscriber holding the
	// previous array can compare by reference.
	const records = new Map<string, Peer<S>>();
	let snapshot: readonly Peer<S>[] = Object.freeze([]);
	let snapshotStale = true;

	const peerSubs = new Set<(peers: readonly Peer<S>[]) => void>();
	const messageSubs = new Set<(msg: RoomMessage<M>) => void>();
	const errorSubs = new Set<(err: RoomError) => void>();

	let status: RoomStatus = { state: 'joining' };
	// Monotonic per (session, room). Seeded from the wall clock so that a
	// session id reclaimed after a reload does not restart below the revision
	// its predecessor published, which peers would discard as stale.
	let revision = BigInt(Date.now());

	const rebuild = (): void => {
		const out = [ ...records.values() ];
		out.sort((a, b) => (a.sessionId < b.sessionId ? -1 : a.sessionId > b.sessionId ? 1 : 0));
		snapshot = Object.freeze(out);
		snapshotStale = false;
	};

	const notifyPeers = (): void => {
		snapshotStale = true;
		if (peerSubs.size === 0) return;
		rebuild();
		for (const cb of [ ...peerSubs ]) {
			safeRun(() => cb(snapshot), 'room peers callback');
		}
	};

	const toPeer = (p: RawParticipant): Peer<S> => {
		let state: S | undefined;
		try {
			state = p.state.length === 0 ? undefined : codec.decodeState(p.state);
		} catch {
			// Another client's bytes, possibly from an older build. One peer's
			// state failing to decode must not take the room down with it.
			state = undefined;
		}
		return Object.freeze({
			sessionId: p.sessionId,
			subject: p.subject,
			state,
			seq: Number(p.seq),
			self: p.sessionId === sessionId,
		});
	};

	const fail = (code: RoomError['code'], message: string, cause?: unknown): void => {
		if (status.state === 'dropped' || status.state === 'closed') return;
		const err: RoomError = { message, code, cause };
		status = { state: 'dropped', error: err };
		for (const cb of [ ...errorSubs ]) {
			safeInvoke(cb, err, 'room onError listener');
		}
	};

	const link = attach({
		onOpen() {
			if (status.state === 'joining') status = { state: 'open' };
		},
		onSnapshot(participants, _truncated) {
			// A snapshot replaces the roster wholesale: it is the server's view
			// of the room, and anything held that is not in it is gone.
			records.clear();
			for (const p of participants) records.set(p.sessionId, toPeer(p));
			const wasWaiting = status.state === 'waiting';
			if (status.state === 'joining' || wasWaiting) status = { state: 'open' };
			notifyPeers();
			// A state set while waiting was held back, as the server refuses
			// a publish from a caller it does not admit.
			if (wasWaiting && pending !== undefined && timer === undefined) {
				flush();
				return;
			}
			// The server holds a record only while it is watching the session's
			// stream, so a publish that beat the stream to the server — or one
			// made across a reconnect — left nothing behind. A snapshot without
			// this session in it is the proof, and re-sending is what makes
			// presence survive a reopen without any of it being durable.
			if (published !== undefined && !records.has(sessionId)) republish();
		},
		onUpdated(p) {
			records.set(p.sessionId, toPeer(p));
			notifyPeers();
		},
		onLeft(id) {
			if (records.delete(id)) notifyPeers();
		},
		onMessage(senderSessionId, senderSubject, payload) {
			if (messageSubs.size === 0) return;
			let message: M;
			try {
				message = codec.decodeMessage(payload);
			} catch {
				return; // undecodable: the sender is on a build this one cannot read
			}
			const msg: RoomMessage<M> = {
				senderSessionId,
				senderSubject,
				message,
				self: senderSessionId === sessionId,
			};
			for (const cb of [ ...messageSubs ]) {
				safeRun(() => cb(msg), 'room onMessage callback');
			}
		},
		onDropped(message) {
			fail(CodeRoomDropped, message);
		},
		onWaiting() {
			if (status.state === 'dropped' || status.state === 'closed') return;
			status = { state: 'waiting' };
			records.clear();
			notifyPeers();
		},
		onKeepalive(intervalMs) {
			if (intervalMs <= 0 || intervalMs === keepaliveMs) return;
			keepaliveMs = intervalMs;
			if (keepalive !== undefined) clearInterval(keepalive);
			keepalive = setInterval(sendKeepalive, intervalMs);
		},
	});

	// Throttle state. `pending` holds the newest state not yet published, so a
	// burst collapses onto its last entry rather than queueing; `published` is
	// the last one that went out, kept so it can be re-sent when the server
	// turns out not to hold it.
	let pending: { state: Uint8Array; seq: number } | undefined;
	let published: { state: Uint8Array; seq: number } | undefined;
	let timer: ReturnType<typeof setTimeout> | undefined;
	let lastSentAt = 0;

	const flush = (): void => {
		timer = undefined;
		if (status.state === 'waiting') return;
		const next = pending;
		pending = undefined;
		if (next === undefined || status.state === 'closed' || status.state === 'dropped') return;
		lastSentAt = Date.now();
		published = next;
		revision += 1n;
		void link.publishState(next.state, next.seq, revision).catch((err: unknown) => {
			// Offline is not a room failure: the record is re-announced by the
			// server for as long as the stream lives, and the next successful
			// publish carries the newest state anyway.
			safeRun(() => {
				for (const cb of [ ...errorSubs ]) {
					safeInvoke(cb, {
						message: 'could not publish presence',
						code: CodeRoomOffline,
						cause: err,
					}, 'room onError listener');
				}
			}, 'room publish error');
		});
	};

	// Keepalive for a client that holds no Watch stream. A record survives
	// because some node re-announces it, and a node does that only for the
	// sessions whose streams it holds — so with no stream anywhere, this
	// session's own publishes are the only thing keeping its record alive.
	//
	// The re-send carries the EXISTING revision rather than a fresh one: the
	// server refreshes liveness on an unchanged revision without fanning the
	// frame out, so a keepalive costs the watchers nothing.
	let keepalive: ReturnType<typeof setInterval> | undefined;
	let keepaliveMs = 0;

	const sendKeepalive = (): void => {
		if (published === undefined) return;
		if (status.state === 'closed' || status.state === 'dropped' || status.state === 'waiting') return;
		void link.publishState(published.state, published.seq, revision).catch(() => {
			// A missed keepalive costs nothing on its own: the expiry window is
			// three intervals wide, so the next one still lands inside it.
		});
	};

	// Re-sends the last state at a fresh revision, bypassing the throttle: this
	// runs once per stream open, not per cursor movement, and the roster is
	// wrong until it lands.
	const republish = (): void => {
		if (published === undefined) return;
		pending = published;
		if (timer !== undefined) {
			clearTimeout(timer);
			timer = undefined;
		}
		flush();
	};

	return {
		name,
		sessionId,

		setPresence(state, o) {
			if (status.state === 'closed' || status.state === 'dropped') return;
			pending = { state: codec.encodeState(state), seq: o?.seq ?? 0 };
			if (timer !== undefined) return;
			const wait = Math.max(0, throttleMs - (Date.now() - lastSentAt));
			if (wait === 0) {
				flush();
				return;
			}
			timer = setTimeout(flush, wait);
		},

		async send(message) {
			if (status.state === 'closed') {
				throw new RoomSendError('room is closed', CodeRoomClosed);
			}
			if (status.state === 'dropped') {
				throw new RoomSendError(
					`room was dropped: ${status.error?.message ?? ''}`,
					CodeRoomDropped,
				);
			}
			await link.publishMessage(codec.encodeMessage(message));
		},

		peers() {
			if (snapshotStale) rebuild();
			return snapshot;
		},

		subscribe(cb) {
			peerSubs.add(cb);
			return () => {
				peerSubs.delete(cb);
			};
		},

		onMessage(cb) {
			messageSubs.add(cb);
			return () => {
				messageSubs.delete(cb);
			};
		},

		onError(cb) {
			errorSubs.add(cb);
			if (status.error) safeInvoke(cb, status.error, 'room onError listener');
			return () => {
				errorSubs.delete(cb);
			};
		},

		status() {
			return status;
		},

		async close() {
			if (status.state === 'closed') return;
			const wasLive = status.state !== 'dropped';
			status = { state: 'closed', error: status.error };
			if (timer !== undefined) {
				clearTimeout(timer);
				timer = undefined;
			}
			if (keepalive !== undefined) {
				clearInterval(keepalive);
				keepalive = undefined;
			}
			pending = undefined;
			peerSubs.clear();
			messageSubs.clear();
			errorSubs.clear();
			records.clear();
			snapshot = Object.freeze([]);
			snapshotStale = false;
			if (wasLive) {
				// Best-effort: an unannounced departure still expires, one
				// announce interval later.
				await link.publishLeave().catch(() => {});
			}
			link.detach();
		},
	};
}
