// The ephemeral plane's client half: one `Watch` stream per account, shared
// across tabs, feeding the Room handles every tab holds.
//
// It sits beside leadership.ts and changeshare.ts rather than inside
// coordinator.ts because it is a second, independent plane — its own stream,
// its own leadership term, its own cross-tab protocol — and shares nothing with
// the data stream but the transport.
//
// # Why its own leadership term
//
// A room is useless without a stream, and the data stream is optional
// (`streamEnabled: false` is what the integration suite runs), so rooms cannot
// ride the data stream's term. The two are elected separately on separate Web
// Locks and need not land on the same tab. Where Web Locks or BroadcastChannel
// are missing — a server runtime, an old browser — leadership declines and
// every tab runs its own `Watch`, which is correct and merely costs a
// connection each.
//
// # The cross-tab protocol
//
// Only RECEIVING needs a leader: `Publish` is unary, so every tab publishes its
// own state directly. Tabs announce their room set when it changes and when a
// new leader asks; the leader unions what it hears, debounces, and reopens
// `Watch` with the new set. Membership changes on navigation, which is why this
// is a live census over a BroadcastChannel rather than the KV-backed
// IntentRegistry the data plane uses — that one's TTLs are minutes.

import type { Duration } from '@bufbuild/protobuf/wkt';
import { type Client, Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';

import { isServerRuntime } from '../env.js';
import {
	PresenceService,
	type PublishItem,
	type Topic,
	type WatchResponse,
} from '../gen/syncplane/v1/presence_pb.js';
import { reportError } from '../report.js';
import type { RawParticipant, RoomLink, RoomSink } from '../room.js';
import { Backoff, sleep, STREAM_STABLE_MS } from './backoff.js';
import { startStreamLeadership, type StreamLeadership } from './leadership.js';

/** How a room is addressed. Mirrors the wire Topic. */
export interface PresenceTopic {
	workspaceId: string;
	name: string;
	entity?: string;
	key?: string;
}

/** Options for the presence coordinator. */
export interface PresenceOptions {
	transport: () => Promise<Transport>;
	sessionId: string;
	presenceLock: string;
	presenceChannel: string;
	/**
	 * False disables the Watch stream entirely: rooms still publish, and no tab
	 * receives. For a client that only writes presence.
	 */
	watchEnabled?: boolean;
	/**
	 * The first and the largest reconnect window, in milliseconds. Each retry
	 * waits a delay drawn uniformly from zero up to the window, and the window
	 * doubles per consecutive failure until it reaches the ceiling.
	 */
	minBackoffMs?: number;
	maxBackoffMs?: number;
	/** How long the leader waits for the census to settle before reopening. */
	reopenDebounceMs?: number;
}

const DEFAULT_MIN_BACKOFF = 250;
const DEFAULT_MAX_BACKOFF = 15_000;
const DEFAULT_REOPEN_DEBOUNCE = 150;

/** Renders a topic as the map key its frames are routed by. */
function topicKey(t: PresenceTopic): string {
	return [ t.workspaceId, t.name, t.entity ?? '', t.key ?? '' ].join('\u001f');
}

function toWire(t: PresenceTopic): Topic {
	return {
		$typeName: 'syncplane.v1.Topic',
		workspaceId: t.workspaceId,
		name: t.name,
		entity: t.entity ?? '',
		key: t.key ?? '',
	};
}

/** One open room on this tab. */
interface Attachment {
	topic: PresenceTopic;
	sink: RoomSink;
}

/**
 * Messages on the presence BroadcastChannel.
 *
 * `rooms` is a tab announcing its set; `census` is a new leader asking for one;
 * `frame` is the leader forwarding what arrived on the stream. Structured-clone
 * safe throughout — plain objects, strings and Uint8Arrays.
 */
type PresenceMessage =
	| { t: 'rooms'; sessionId: string; topics: PresenceTopic[] }
	| { t: 'census' }
	| { t: 'frame'; key: string; frame: SharedFrame };

/** What a peer tab last told the leader about itself. */
interface PeerTab {
	sessionId: string;
	topics: PresenceTopic[];
	/** When its last announcement arrived; see PEER_TTL_MS. */
	seenAt: number;
}

/**
 * How often a tab re-announces its room set, and how long the leader keeps a
 * tab it has not heard from.
 *
 * The leader tells the server which sessions its stream is responsible for, so
 * a tab that crashed without saying goodbye would otherwise have its presence
 * kept alive forever by a tab that is still running. This is the same soft-state
 * rule the server applies to sessions, one level down.
 */
const PEER_ANNOUNCE_MS = 2_000;
const PEER_TTL_MS = 3 * PEER_ANNOUNCE_MS;

/**
 * A stream frame in the form followers receive it. Deliberately not the wire
 * message: a protobuf message is not structured-clone safe, and followers only
 * ever need these five shapes.
 */
type SharedFrame =
	| { k: 'snapshot'; participants: RawParticipant[]; truncated: boolean }
	| { k: 'updated'; participant: RawParticipant }
	| { k: 'left'; sessionId: string }
	| { k: 'message'; senderSessionId: string; senderSubject: string; payload: Uint8Array }
	| { k: 'dropped'; message: string };

/**
 * Owns the account's room membership, its Watch stream and its cross-tab
 * sharing. One per SyncClient.
 */
export class PresenceCoordinator {
	private readonly opts: Required<Omit<PresenceOptions, 'transport' | 'sessionId'>> & {
		transport: () => Promise<Transport>;
		sessionId: string;
	};

	/** Rooms open on THIS tab, by topic key. Several rooms may share a topic. */
	private readonly attached = new Map<string, Set<Attachment>>();
	/** What other tabs last announced about themselves, by tab id. */
	private readonly peerRooms = new Map<string, PeerTab>();
	private readonly tabId = randomId();
	/** Re-announces this tab's set, and ages out peers that stopped. */
	private peerTimer?: ReturnType<typeof setInterval>;
	/**
	 * The topics and fronted sessions the running stream was opened with, so a
	 * peer re-announcing an unchanged set does not cost a reopen.
	 */
	private openedWith = '';

	private channel?: BroadcastChannel;
	private leadership?: StreamLeadership;
	private client?: Client<typeof PresenceService>;

	/** Aborts the running Watch stream so the loop reopens with a fresh set. */
	private streamAbort?: AbortController;
	/** Ends the whole loop. */
	private running = false;
	private stopped = false;
	private reopenTimer?: ReturnType<typeof setTimeout>;
	/** True while this tab holds the Watch stream — as leader or on its own. */
	private receiving = false;
	/**
	 * The announce interval the server last reported, in milliseconds; 0 until a
	 * publish has returned one. Only consulted when watchEnabled is false.
	 */
	private announceMs = 0;

	constructor(opts: PresenceOptions) {
		this.opts = {
			transport: opts.transport,
			sessionId: opts.sessionId,
			presenceLock: opts.presenceLock,
			presenceChannel: opts.presenceChannel,
			watchEnabled: opts.watchEnabled ?? true,
			minBackoffMs: opts.minBackoffMs ?? DEFAULT_MIN_BACKOFF,
			maxBackoffMs: opts.maxBackoffMs ?? DEFAULT_MAX_BACKOFF,
			reopenDebounceMs: opts.reopenDebounceMs ?? DEFAULT_REOPEN_DEBOUNCE,
		};
	}

	/**
	 * Opens a room on this tab and returns the link its handle publishes
	 * through. The Watch stream starts on the first room and stops with the
	 * last.
	 */
	attach(topic: PresenceTopic, sink: RoomSink): RoomLink {
		const key = topicKey(topic);
		const entry: Attachment = { topic, sink };
		let set = this.attached.get(key);
		if (set === undefined) {
			set = new Set();
			this.attached.set(key, set);
		}
		set.add(entry);
		// A room opened after the interval was learned arms straight away; one
		// opened before is armed by the first publish that returns it.
		if (this.announceMs > 0 && !this.opts.watchEnabled) sink.onKeepalive(this.announceMs);
		this.membershipChanged();

		let detached = false;
		const detach = (): void => {
			if (detached) return;
			detached = true;
			const live = this.attached.get(key);
			if (live) {
				live.delete(entry);
				if (live.size === 0) this.attached.delete(key);
			}
			this.membershipChanged();
		};

		return {
			publishState: (state, seq, revision) =>
				this.publish({
					$typeName: 'syncplane.v1.PublishItem',
					topic: toWire(topic),
					body: {
						case: 'presence',
						value: {
							$typeName: 'syncplane.v1.PresenceUpdate',
							state,
							revision,
							seq: BigInt(seq),
						},
					},
				}),
			publishMessage: (payload) =>
				this.publish({
					$typeName: 'syncplane.v1.PublishItem',
					topic: toWire(topic),
					body: { case: 'message', value: payload },
				}),
			publishLeave: () =>
				this.publish({
					$typeName: 'syncplane.v1.PublishItem',
					topic: toWire(topic),
					body: { case: 'leave', value: true },
				}),
			detach,
		};
	}

	/** Stops the stream, the leadership term and the channel. Idempotent. */
	async stop(): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;
		// An empty set retires this tab from the leader's union at once; a tab
		// that dies without sending it is aged out instead.
		this.attached.clear();
		this.announce();
		this.clearReopenTimer();
		this.stopPeerTimer();
		this.running = false;
		this.streamAbort?.abort();
		this.leadership?.stop();
		this.leadership = undefined;
		this.channel?.close();
		this.channel = undefined;
		this.attached.clear();
		this.peerRooms.clear();
		await Promise.resolve();
	}

	private async publish(item: PublishItem): Promise<void> {
		const client = await this.rpc();
		const res = await client.publish({
			$typeName: 'syncplane.v1.PublishRequest',
			sessionId: this.opts.sessionId,
			items: [ item ],
		});
		this.noteAnnounce(res.announceInterval);
	}

	/**
	 * Arms the rooms' own keepalive from the interval the server reported.
	 *
	 * Only a tab receiving nothing needs it: with a Watch stream open — this
	 * tab's or the leader's — the node serving it re-announces these records
	 * already, and a second timer would double the traffic for nothing.
	 */
	private noteAnnounce(d: Duration | undefined): void {
		if (d === undefined || this.opts.watchEnabled) return;
		const ms = Number(d.seconds) * 1000 + Math.round(d.nanos / 1e6);
		if (ms <= 0 || ms === this.announceMs) return;
		this.announceMs = ms;
		for (const set of this.attached.values()) {
			for (const { sink } of set) sink.onKeepalive(ms);
		}
	}

	private async rpc(): Promise<Client<typeof PresenceService>> {
		// The transport is the client's own, memoized there — an app that
		// installed authentication on it must not get an unauthenticated stream.
		this.client ??= createClient(PresenceService, await this.opts.transport());
		return this.client;
	}

	/**
	 * Called whenever this tab's room set changes: announces it, and asks the
	 * stream to reopen if this tab is the one holding it.
	 */
	private membershipChanged(): void {
		if (this.stopped || !this.opts.watchEnabled) return;
		// A loop started by THIS change already opened with the new set, so
		// reopening it would throw away the stream it just established.
		const justStarted = this.ensureStarted();
		this.announce();
		if (!justStarted && this.receiving) this.scheduleReopen();
		if (this.attached.size === 0) this.maybeStop();
	}

	/** Tells the other tabs what this one is watching, and as which session. */
	private announce(): void {
		this.post({ t: 'rooms', sessionId: this.opts.sessionId, topics: this.localTopics() });
	}

	private localTopics(): PresenceTopic[] {
		const out: PresenceTopic[] = [];
		for (const set of this.attached.values()) {
			const first = set.values().next().value;
			if (first) out.push(first.topic);
		}
		return out;
	}

	/** The union this tab would open a stream over: its own rooms plus its peers'. */
	private unionTopics(): PresenceTopic[] {
		const byKey = new Map<string, PresenceTopic>();
		for (const t of this.localTopics()) byKey.set(topicKey(t), t);
		for (const peer of this.peerRooms.values()) {
			for (const t of peer.topics) byKey.set(topicKey(t), t);
		}
		return [ ...byKey.values() ];
	}

	/**
	 * The peer sessions this tab's stream would be responsible for. Its own is
	 * named separately on the request, so it is not in here.
	 */
	private frontedSessions(): string[] {
		const out = new Set<string>();
		for (const peer of this.peerRooms.values()) {
			if (peer.sessionId !== this.opts.sessionId) out.add(peer.sessionId);
		}
		return [ ...out ].sort();
	}

	/** A stable rendering of what a stream would be opened with. */
	private streamKey(topics: PresenceTopic[], fronted: string[]): string {
		return [ ...topics.map(topicKey) ].sort().join('\u0000') + '\u0001' + fronted.join('\u0000');
	}

	/** Drops peers that stopped announcing, so a crashed tab's presence expires. */
	private expirePeers(now: number): boolean {
		let dropped = false;
		for (const [ id, peer ] of this.peerRooms) {
			if (now - peer.seenAt > PEER_TTL_MS) {
				this.peerRooms.delete(id);
				dropped = true;
			}
		}
		return dropped;
	}

	/**
	 * Starts the channel and the leadership term on the first room, reporting
	 * whether this call is what started them.
	 */
	private ensureStarted(): boolean {
		if (this.running || this.attached.size === 0) return false;
		this.running = true;
		this.openChannel();
		this.peerTimer ??= setInterval(() => {
			this.announce();
			if (this.expirePeers(Date.now()) && this.receiving) this.reopenIfChanged();
		}, PEER_ANNOUNCE_MS);

		this.leadership = startStreamLeadership(this.opts.presenceLock, {
			runLeaderLoop: async (signal) => {
				// A new leader has no idea what the other tabs are watching, so it
				// asks before opening anything.
				this.post({ t: 'census' });
				await this.streamLoop(signal, true);
			},
			onUnavailable: () => {
				// Web Locks let this tab down. Run an unshared stream rather than
				// leave the app with no presence at all.
				void this.streamLoop(undefined, false);
			},
		});
		if (this.leadership === undefined) {
			// No Web Locks, no BroadcastChannel, or a server runtime: every tab
			// runs its own stream. Correct, and costs one connection per tab.
			void this.streamLoop(undefined, false);
		}
		return true;
	}

	/** Stops the stream and the term once the last room on every tab closed. */
	private maybeStop(): void {
		if (!this.running || this.attached.size > 0 || this.peerRooms.size > 0) return;
		this.running = false;
		this.clearReopenTimer();
		this.stopPeerTimer();
		this.streamAbort?.abort();
		this.leadership?.stop();
		this.leadership = undefined;
	}

	private stopPeerTimer(): void {
		if (this.peerTimer !== undefined) {
			clearInterval(this.peerTimer);
			this.peerTimer = undefined;
		}
	}

	private openChannel(): void {
		if (this.channel !== undefined) return;
		if (isServerRuntime() || typeof BroadcastChannel === 'undefined') return;
		const channel = new BroadcastChannel(this.opts.presenceChannel);
		channel.onmessage = (e: MessageEvent) => {
			this.onMessage(e.data as PresenceMessage | undefined);
		};
		this.channel = channel;
	}

	private post(msg: PresenceMessage): void {
		try {
			this.channel?.postMessage({ ...msg, from: this.tabId });
		} catch {
			// closed channel — racing stop(); harmless
		}
	}

	private onMessage(msg: (PresenceMessage & { from?: string }) | undefined): void {
		if (msg === undefined || msg.from === this.tabId) return;
		switch (msg.t) {
			case 'rooms': {
				const from = msg.from ?? '';
				if (msg.topics.length === 0) this.peerRooms.delete(from);
				else {
					this.peerRooms.set(from, {
						sessionId: msg.sessionId,
						topics: msg.topics,
						seenAt: Date.now(),
					});
				}
				// A re-announcement of a set this stream already covers is the
				// common case — it arrives every couple of seconds from every
				// tab — and must not cost a reopen.
				if (this.receiving) this.reopenIfChanged();
				else this.maybeStop();
				return;
			}
			case 'census': {
				// A new leader is asking. Answer even with an empty set, so it
				// learns this tab exists and stops waiting on it.
				this.announce();
				return;
			}
			case 'frame': {
				this.deliver(msg.key, msg.frame);
				return;
			}
		}
	}

	/** Reopens only when the union this stream is serving actually moved. */
	private reopenIfChanged(): void {
		if (this.streamKey(this.unionTopics(), this.frontedSessions()) !== this.openedWith) {
			this.scheduleReopen();
		}
	}

	private scheduleReopen(): void {
		this.clearReopenTimer();
		this.reopenTimer = setTimeout(() => {
			this.reopenTimer = undefined;
			// Aborting the stream is how it reopens: streamLoop catches the abort
			// and starts again with the current union.
			this.streamAbort?.abort();
		}, this.opts.reopenDebounceMs);
	}

	private clearReopenTimer(): void {
		if (this.reopenTimer !== undefined) {
			clearTimeout(this.reopenTimer);
			this.reopenTimer = undefined;
		}
	}

	/**
	 * Holds a Watch stream for as long as this tab should, reopening at once on a
	 * membership change and with backoff on any other end of the stream.
	 *
	 * It must never reject: as a leader loop, a rejection would make
	 * startStreamLeadership give up on this tab for the rest of the session —
	 * and take the DATA stream's leadership with it if the two ever shared a
	 * term.
	 */
	private async streamLoop(term: AbortSignal | undefined, shared: boolean): Promise<void> {
		this.receiving = true;
		const backoff = new Backoff(this.opts.minBackoffMs, this.opts.maxBackoffMs);
		// Read through a function: `term.aborted` is a readonly property, so a
		// direct test in the loop condition narrows it for the whole body and the
		// checks after each await would be compiled away.
		const ended = (): boolean => this.stopped || !this.running || term?.aborted === true;
		try {
			while (!ended()) {
				const topics = this.unionTopics();
				const fronted = this.frontedSessions();
				if (topics.length === 0) {
					// Nothing to watch. Wait for a membership change rather than
					// opening an empty stream, which the server refuses.
					await this.waitForChange(term);
					continue;
				}
				const abort = new AbortController();
				this.streamAbort = abort;
				const onTermEnd = (): void => abort.abort();
				term?.addEventListener('abort', onTermEnd, { once: true });
				this.openedWith = this.streamKey(topics, fronted);
				const openedAt = Date.now();
				let deliberate = false;
				try {
					await this.runStream(topics, fronted, abort.signal, shared);
					// runStream returns rather than throws when our own abort lands
					// between frames, so a clean return is not proof the server
					// hung up.
					deliberate = abort.signal.aborted;
				} catch (err) {
					if (ended()) return;
					if (abort.signal.aborted) {
						deliberate = true;
					} else if (isTerminal(err)) {
						// The server will answer the same way next time. Tell every
						// room so the app stops showing them, and stop retrying.
						this.dropAll(errorMessage(err));
						return;
					} else {
						reportError('presence stream failed; retrying', err);
					}
				} finally {
					term?.removeEventListener('abort', onTermEnd);
					if (this.streamAbort === abort) this.streamAbort = undefined;
				}
				if (ended()) return;
				if (deliberate) {
					// A reopen we asked for (a membership change): productive, so it
					// neither waits nor counts against the stream's history.
					backoff.reset();
					continue;
				}
				// A close the server chose is paced exactly like a failure: a node
				// that ends every stream the moment it opens would otherwise get an
				// unthrottled reopen loop from every client it has. Only a stream
				// that lasted clears the escalation — see STREAM_STABLE_MS.
				if (Date.now() - openedAt >= STREAM_STABLE_MS) backoff.reset();
				await sleep(backoff.next(), term);
			}
		} finally {
			this.receiving = false;
		}
	}

	/** Runs one Watch stream to completion. */
	private async runStream(
		topics: PresenceTopic[],
		fronted: string[],
		signal: AbortSignal,
		shared: boolean,
	): Promise<void> {
		const client = await this.rpc();
		const stream = client.watch(
			{
				$typeName: 'syncplane.v1.WatchRequest',
				sessionId: this.opts.sessionId,
				topics: topics.map(toWire),
				limit: 0,
				// The other tabs publish their own presence but hold no stream,
				// so this one keeps their records alive on their behalf.
				frontedSessionIds: fronted,
			},
			{ signal },
		);
		for await (const frame of stream) {
			if (signal.aborted) return;
			this.onFrame(frame, shared);
		}
	}

	private onFrame(frame: WatchResponse, shared: boolean): void {
		if (frame.body.case === 'heartbeat' || frame.topic === undefined) return;
		const key = topicKey({
			workspaceId: frame.topic.workspaceId,
			name: frame.topic.name,
			entity: frame.topic.entity,
			key: frame.topic.key,
		});
		const shareable = toSharedFrame(frame);
		if (shareable === undefined) return;
		this.deliver(key, shareable);
		if (shared) this.post({ t: 'frame', key, frame: shareable });
	}

	/** Hands one frame to every room on this tab holding its topic. */
	private deliver(key: string, frame: SharedFrame): void {
		const set = this.attached.get(key);
		if (set === undefined) return;
		for (const { sink } of [ ...set ]) {
			switch (frame.k) {
				case 'snapshot':
					sink.onSnapshot(frame.participants, frame.truncated);
					break;
				case 'updated':
					sink.onUpdated(frame.participant);
					break;
				case 'left':
					sink.onLeft(frame.sessionId);
					break;
				case 'message':
					sink.onMessage(frame.senderSessionId, frame.senderSubject, frame.payload);
					break;
				case 'dropped':
					sink.onDropped(frame.message);
					break;
			}
		}
	}

	/** Tells every open room the stream cannot come back. */
	private dropAll(message: string): void {
		for (const set of [ ...this.attached.values() ]) {
			for (const { sink } of [ ...set ]) sink.onDropped(message);
		}
	}

	/**
	 * Waits until the room set changes or the term ends. Polled rather than
	 * event-driven because the only thing that can change it — attach, detach, a
	 * peer's announcement — already schedules a reopen, and this path only runs
	 * while there is nothing at all to watch.
	 */
	private async waitForChange(term: AbortSignal | undefined): Promise<void> {
		await sleep(this.opts.reopenDebounceMs * 4, term);
	}
}

/** Translates a wire frame into the structured-clone-safe form tabs share. */
function toSharedFrame(frame: WatchResponse): SharedFrame | undefined {
	switch (frame.body.case) {
		case 'snapshot':
			return {
				k: 'snapshot',
				participants: frame.body.value.participants.map(toRaw),
				truncated: frame.body.value.truncated,
			};
		case 'updated':
			return { k: 'updated', participant: toRaw(frame.body.value) };
		case 'left':
			return { k: 'left', sessionId: frame.body.value };
		case 'message':
			return {
				k: 'message',
				senderSessionId: frame.body.value.senderSessionId,
				senderSubject: frame.body.value.senderSubject,
				payload: frame.body.value.payload,
			};
		case 'dropped':
			return { k: 'dropped', message: frame.body.value.message };
		default:
			return undefined;
	}
}

function toRaw(p: {
	sessionId: string;
	subject: string;
	presence?: { state: Uint8Array; revision: bigint; seq: bigint };
}): RawParticipant {
	return {
		sessionId: p.sessionId,
		subject: p.subject,
		state: p.presence?.state ?? new Uint8Array(0),
		revision: p.presence?.revision ?? 0n,
		seq: p.presence?.seq ?? 0n,
	};
}

/**
 * Whether reopening would meet the same answer. A refusal of the REQUEST — a
 * malformed topic, a caller who may not be here, more rooms than the server
 * serves — is settled; everything else is worth a retry.
 */
function isTerminal(err: unknown): boolean {
	if (!(err instanceof ConnectError)) return false;
	return err.code === Code.PermissionDenied
		|| err.code === Code.InvalidArgument
		|| err.code === Code.ResourceExhausted
		|| err.code === Code.Unauthenticated;
}

function errorMessage(err: unknown): string {
	if (err instanceof ConnectError) return err.rawMessage;
	return err instanceof Error ? err.message : String(err);
}

function randomId(): string {
	return crypto.randomUUID();
}
