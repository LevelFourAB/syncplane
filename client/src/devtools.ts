// Console devtools over a SyncClient — the state a "why doesn't this row show
// up" investigation needs, gathered into one report instead of scattered
// across callbacks the app would have to have wired up in advance: the pending
// queue, the optimistic overlay, per-entity coverage, hydration, stream
// leadership, rejections, text writer leases and room membership.
//
// Attach once, next to the client's construction, and the handle is published
// on `globalThis` for the browser console:
//
//   attachDevtools(client);
//   // …later, in the console:
//   await __syncplane.snapshot()
//   await __syncplane.explain('todo', 't42')
//
// The buffers start at attach — a rejection from before it is not retained —
// which is why attaching belongs next to `new SyncClient(...)` rather than
// behind a debug flag flipped after the fact.

import type { DescMessage } from '@bufbuild/protobuf';

import type { ClientInternals, SyncClient } from './client.js';
import type { Coverage } from './coverage.js';
import {
	confirmedKey,
	overlayKey,
	overlayPrefix,
	type OverlayRecord,
	overlayTextPrefix,
	splitOverlayTextSuffix,
	splitWsKey,
} from './keys.js';
import type { KVRead } from './kv/store.js';
import { readPending, readPushedThrough } from './optimistic/queue.js';
import { rowPresent } from './read.js';
import type { Peer, RoomStatus } from './room.js';
import {
	describeMutation,
	type RejectionInfo,
	type RejectionTarget,
} from './sync/coordinator.js';
import type { SyncEvent } from './sync/events.js';
import type { SyncStatus, WorkspaceStatus } from './sync/status.js';

/** Options accepted by attachDevtools. */
export interface DevtoolsOptions {
	/**
	 * How many events the `events()` ring retains, oldest dropped first.
	 * @defaultValue 200
	 */
	eventBufferSize?: number;
	/**
	 * How many rejections the `rejections()` ring retains, oldest dropped
	 * first.
	 * @defaultValue 100
	 */
	rejectionBufferSize?: number;
	/**
	 * Name the handle is published under on `globalThis`, so it is reachable
	 * from the browser console without an import. `false` publishes nothing. A
	 * later attach under the same name replaces an earlier one.
	 * @defaultValue '__syncplane'
	 */
	expose?: string | false;
}

/** One mutation sitting in a workspace's pending queue. */
export interface QueuedMutationInfo {
	mutationId: bigint;
	/** Rows it would write; empty for a custom mutator (see `mutator`). */
	targets: readonly RejectionTarget[];
	/** Name of the custom mutator, when the mutation is one. */
	mutator?: string;
	/**
	 * The mutation has been handed to the server at least once (it is at or
	 * below the pushed-through watermark) and is awaiting its verdict; false
	 * means no push has carried it yet.
	 */
	pushed: boolean;
}

/** Sync-relevant state of one workspace, queue contents included. */
export interface WorkspaceDebugInfo {
	workspaceId: string;
	/** The same entry `client.ws(id).status()` reports. */
	status: WorkspaceStatus;
	/** The queue, oldest first. */
	pending: QueuedMutationInfo[];
	/** Highest mutation id any tab has attempted to push for this workspace. */
	pushedThrough: bigint;
	/**
	 * Coverage of every syncable entity in this workspace — the answer to "can
	 * an empty result here mean the rows exist but did not sync".
	 */
	coverage: Record<string, Coverage>;
}

/** One row holding optimistic state: an overlay entry, text edits, or both. */
export interface OverlayEntryInfo {
	entity: string;
	workspaceId: string;
	key: string;
	/** The overlay is a local tombstone: a pending delete shadows the row. */
	deleted: boolean;
	/** Optimistic text-log fragments queued for the row's text fields. */
	textEdits: number;
}

/** Where the account's one Subscribe stream is held. */
export interface StreamLeadershipInfo {
	/** This tab holds it. */
	thisTab: boolean;
	/**
	 * Some tab on this device holds it (this one included). Undefined where the
	 * platform cannot say — no Web Locks, or a runtime without elections, where
	 * every client streams for itself and there is no leadership to hold.
	 * `false` with tabs open means every tab is hidden and all of them are on
	 * the poll floor.
	 */
	heldByAnyTab?: boolean;
}

/** One open room, as this client sees it. */
export interface RoomDebugInfo {
	name: string;
	state: RoomStatus['state'];
	/** Why the room was dropped, when it was. */
	error?: string;
	peers: Pick<Peer<unknown>, 'sessionId' | 'subject' | 'self' | 'seq'>[];
}

/** A rejection as retained by the buffer: the info plus when it arrived. */
export interface RetainedRejection extends RejectionInfo {
	/** `Date.now()` when the rejection was reported. */
	at: number;
}

/**
 * Point-in-time report over everything attachDevtools observes.
 *
 * Not plain JSON: mutation ids are `bigint`. `JSON.stringify` needs a replacer
 * for them; the console renders everything as-is.
 */
export interface DevtoolsSnapshot {
	/** `Date.now()` when the snapshot was taken. */
	at: number;
	/** This tab's session id (`SyncClient.sessionId`). */
	sessionId: string;
	/** The per-device idempotency handle server watermarks are keyed by. */
	clientId: string;
	status: SyncStatus;
	leadership: StreamLeadershipInfo;
	workspaces: WorkspaceDebugInfo[];
	/** Every row holding optimistic state, across workspaces and entities. */
	overlay: OverlayEntryInfo[];
	/** The rejection buffer, oldest first. */
	rejections: RetainedRejection[];
	/** The event buffer, oldest first. */
	events: SyncEvent[];
	/** Text writer identities held by this tab's open text handles. */
	textPeers: string[];
	/**
	 * Text writer identities held anywhere on this device, read off the Web
	 * Locks each lease is. Undefined where Web Locks are unavailable.
	 */
	textPeersOnDevice?: string[];
	rooms: RoomDebugInfo[];
}

/** Why one row is, or is not, in the merged view. See SyncDevtools.explain. */
export interface RowExplanation {
	entity: string;
	workspaceId: string;
	key: string;
	/** Whether a read of this row would answer it — the merged view's verdict. */
	visible: boolean;
	/** A server-confirmed copy is in the local store. */
	confirmed: boolean;
	/** What the optimistic overlay holds for the row. */
	overlay: 'none' | 'edit' | 'tombstone';
	/** Queued mutations that would write this row, oldest first. */
	queued: QueuedMutationInfo[];
	/** Retained rejections that targeted this row, oldest first. */
	rejections: RetainedRejection[];
	/** Coverage of the row's entity in its workspace. */
	coverage: Coverage;
	/** The reading, in one sentence. */
	verdict: string;
}

/**
 * The handle attachDevtools returns (and publishes on `globalThis`).
 *
 * Everything answers for the moment it is called; nothing is a live view, and
 * nothing here is needed for correctness — detaching changes only what is
 * observable.
 */
export interface SyncDevtools {
	/**
	 * One report over everything observed: status, leadership, per-workspace
	 * queues and coverage, the overlay, both buffers, text leases and rooms.
	 * Reads the store under the shared read lock, so a concurrent commit waits
	 * or is waited for — never straddled. Rejects once the client is closed.
	 */
	snapshot(): Promise<DevtoolsSnapshot>;
	/**
	 * Answers why one row is, or is not, in the merged view: local presence,
	 * overlay state, queued mutations and retained rejections targeting it, and
	 * the entity's coverage, condensed into a one-sentence verdict.
	 *
	 * Rejects for an entity that is not registered, and for an omitted
	 * `workspaceId` on a client with no default workspace.
	 */
	explain(
		entity: string | DescMessage,
		key: string,
		workspaceId?: string,
	): Promise<RowExplanation>;
	/** The rejection buffer, oldest first. A copy, safe to hold. */
	rejections(): RetainedRejection[];
	/** The event buffer, oldest first. A copy, safe to hold. */
	events(): SyncEvent[];
	/**
	 * Stops observing: unsubscribes from the client, empties the buffers and
	 * withdraws the `globalThis` handle (unless another attach replaced it).
	 * The client is unaffected. Idempotent.
	 */
	detach(): void;
}

const DEFAULT_EVENT_BUFFER = 200;
const DEFAULT_REJECTION_BUFFER = 100;
const DEFAULT_EXPOSE = '__syncplane';

/**
 * Attaches a devtools handle to a client and, by default, publishes it as
 * `globalThis.__syncplane` for the browser console.
 *
 * Purely observational: it subscribes to the client's event and rejection
 * streams and reads the shared store when asked, but writes nothing and
 * changes no sync behavior — shipping it attached costs two listeners and the
 * buffers. Attach before `start()` so the buffers see the session from its
 * first cycle; a rejection reported before the attach is not retained. Call
 * `detach()` when done with it, or leave it for the client's lifetime.
 */
export function attachDevtools<E extends readonly DescMessage[]>(
	client: SyncClient<E>,
	opts?: DevtoolsOptions,
): SyncDevtools {
	const internals = client.inspect();
	const eventCap = opts?.eventBufferSize ?? DEFAULT_EVENT_BUFFER;
	const rejectionCap = opts?.rejectionBufferSize ?? DEFAULT_REJECTION_BUFFER;
	const exposeAs = opts?.expose ?? DEFAULT_EXPOSE;

	const events: SyncEvent[] = [];
	const rejections: RetainedRejection[] = [];
	const unsubEvents = client.onSyncEvent((event) => {
		events.push(event);
		if (events.length > eventCap) events.splice(0, events.length - eventCap);
	});
	const unsubRejections = client.onMutationRejected((info) => {
		rejections.push({ ...info, at: Date.now() });
		if (rejections.length > rejectionCap) {
			rejections.splice(0, rejections.length - rejectionCap);
		}
	});

	const queueOf = async (tx: KVRead, workspaceId: string): Promise<{
		pending: QueuedMutationInfo[];
		pushedThrough: bigint;
	}> => {
		const pushedThrough = await readPushedThrough(tx, workspaceId);
		const pending = (await readPending(tx, workspaceId)).map((m) => ({
			mutationId: m.mutationId,
			...describeMutation(m),
			pushed: m.mutationId <= pushedThrough,
		}));
		return { pending, pushedThrough };
	};

	const overlayOf = async (tx: KVRead): Promise<OverlayEntryInfo[]> => {
		// Keyed by the raw `<wsSegment>/<key>` suffix both keyspaces share, so a
		// text log lines up with its overlay row — and a row that has ONLY text
		// edits (a text patch appends fragments without rewriting the overlay
		// row) still gets an entry.
		const out = new Map<string, OverlayEntryInfo>();
		for (const entity of client.entityRegistry.syncableNames()) {
			const oPrefix = overlayPrefix(entity);
			for await (const [ k, v ] of tx.scan(oPrefix)) {
				const suffix = k.slice(oPrefix.length);
				const record = v as OverlayRecord;
				out.set(`${entity}/${suffix}`, {
					entity,
					...splitWsKey(suffix),
					deleted: record.del === true,
					textEdits: 0,
				});
			}
			if (client.entityRegistry.textPathsOf(entity).length === 0) continue;
			const tPrefix = overlayTextPrefix(entity);
			for await (const [ k ] of tx.scan(tPrefix)) {
				const suffix = k.slice(tPrefix.length);
				const parts = splitOverlayTextSuffix(suffix);
				if (parts === undefined) continue;
				const rowId = `${entity}/${suffix.slice(0, suffix.indexOf('/'))}/${parts.key}`;
				let entry = out.get(rowId);
				if (entry === undefined) {
					entry = {
						entity,
						workspaceId: parts.workspaceId,
						key: parts.key,
						deleted: false,
						textEdits: 0,
					};
					out.set(rowId, entry);
				}
				entry.textEdits += 1;
			}
		}
		return [ ...out.values() ];
	};

	const heldLocks = async (): Promise<Set<string> | undefined> => {
		if (typeof navigator === 'undefined' || navigator.locks?.query === undefined) {
			return undefined;
		}
		try {
			const state = await navigator.locks.query();
			return new Set((state.held ?? []).flatMap((l) => (l.name ? [ l.name ] : [])));
		} catch {
			return undefined;
		}
	};

	const leadershipOf = (held: Set<string> | undefined): StreamLeadershipInfo => {
		const thisTab = internals.isStreamLeader();
		return held === undefined
			? { thisTab }
			: { thisTab, heldByAnyTab: held.has(client.storageNames.streamLock) };
	};

	const roomsOf = (): RoomDebugInfo[] =>
		internals.rooms().map((room) => {
			const status = room.status();
			return {
				name: room.name,
				state: status.state,
				error: status.error?.message,
				peers: room.peers().map((p) => ({
					sessionId: p.sessionId,
					subject: p.subject,
					self: p.self,
					seq: p.seq,
				})),
			};
		});

	const resolveEntity = (entity: string | DescMessage): string => {
		if (typeof entity !== 'string') return client.entityRegistry.bySchema(entity).name;
		if (client.entityRegistry.get(entity) === undefined) {
			throw new Error(`"${entity}" is not a registered entity`);
		}
		return entity;
	};

	const devtools: SyncDevtools = {
		async snapshot(): Promise<DevtoolsSnapshot> {
			const clientId = await client.clientId();
			const held = await heldLocks();
			const workspaceIds = client.workspaces;
			const entities = client.entityRegistry.syncableNames();
			const { queues, overlay } = await internals.read(async (tx) => {
				const queues = new Map<string, Awaited<ReturnType<typeof queueOf>>>();
				for (const ws of workspaceIds) queues.set(ws, await queueOf(tx, ws));
				return { queues, overlay: await overlayOf(tx) };
			});
			const status = client.status();
			const textPeerPrefix = `${client.storageNames.textPeerLocks}:`;
			return {
				at: Date.now(),
				sessionId: client.sessionId,
				clientId,
				status,
				leadership: leadershipOf(held),
				workspaces: workspaceIds.map((workspaceId) => {
					const queue = queues.get(workspaceId)!;
					const coverage: Record<string, Coverage> = {};
					for (const entity of entities) {
						coverage[entity] = internals.coverageOf(entity, workspaceId);
					}
					return {
						workspaceId,
						status: status.workspaces[workspaceId]
							?? { workspaceId, pendingMutations: queue.pending.length },
						pending: queue.pending,
						pushedThrough: queue.pushedThrough,
						coverage,
					};
				}),
				overlay,
				rejections: devtools.rejections(),
				events: devtools.events(),
				textPeers: internals.textPeers(),
				textPeersOnDevice: held === undefined ? undefined : [ ...held ]
					.filter((name) => name.startsWith(textPeerPrefix))
					.map((name) => name.slice(textPeerPrefix.length)),
				rooms: roomsOf(),
			};
		},

		async explain(
			entity: string | DescMessage,
			key: string,
			workspaceId?: string,
		): Promise<RowExplanation> {
			const name = resolveEntity(entity);
			const ws = workspaceId ?? client.defaultWorkspace;
			if (ws === undefined) {
				throw new Error('explain() needs a workspaceId on a client with no default workspace');
			}
			const state = await internals.read(async (tx) => {
				const overlay = (await tx.get(overlayKey(name, ws, key))) as OverlayRecord | undefined;
				return {
					visible: await rowPresent(tx, name, ws, key),
					confirmed: await tx.has(confirmedKey(name, ws, key)),
					overlay: overlay === undefined
						? ('none' as const)
						: overlay.del
						? ('tombstone' as const)
						: ('edit' as const),
					queue: await queueOf(tx, ws),
				};
			});
			const targets = (m: QueuedMutationInfo | RetainedRejection): boolean =>
				m.targets.some((t) => t.entity === name && t.key === key);
			const queued = state.queue.pending.filter(targets);
			const rejected = rejections.filter((r) => r.workspaceId === ws && targets(r));
			const coverage = internals.coverageOf(name, ws);
			return {
				entity: name,
				workspaceId: ws,
				key,
				visible: state.visible,
				confirmed: state.confirmed,
				overlay: state.overlay,
				queued,
				rejections: rejected,
				coverage,
				verdict: verdictOf(name, state, queued, rejected, coverage),
			};
		},

		rejections(): RetainedRejection[] {
			return [ ...rejections ];
		},

		events(): SyncEvent[] {
			return [ ...events ];
		},

		detach(): void {
			unsubEvents();
			unsubRejections();
			events.length = 0;
			rejections.length = 0;
			if (exposeAs !== false) {
				const globals = globalThis as Record<string, unknown>;
				if (globals[exposeAs] === devtools) delete globals[exposeAs];
			}
		},
	};

	if (exposeAs !== false) {
		(globalThis as Record<string, unknown>)[exposeAs] = devtools;
	}
	return devtools;
}

/** Condenses a row's state into the one sentence explain() leads with. */
function verdictOf(
	entity: string,
	state: { visible: boolean; confirmed: boolean; overlay: 'none' | 'edit' | 'tombstone' },
	queued: QueuedMutationInfo[],
	rejected: RetainedRejection[],
	coverage: Coverage,
): string {
	if (state.overlay === 'tombstone') {
		return 'Deleted locally: a pending delete shadows the row until the server acks it.';
	}
	if (state.visible) {
		if (!state.confirmed) {
			return 'Visible optimistically: created locally and not yet acked; a rejection would roll it back.';
		}
		return state.overlay === 'edit' || queued.length > 0
			? `Visible: server-confirmed copy with ${queued.length} queued local edit(s) on top.`
			: 'Visible: server-confirmed copy with no local edits.';
	}
	const last = rejected.at(-1);
	if (last !== undefined) {
		return `Not visible: a mutation targeting it was rejected (${last.errorCode}) and rolled back — `
			+ `"${last.errorMessage}".`;
	}
	if (!coverage.complete) {
		return `Not synced here: ${
			coverage.bounds.map((b) => boundReason(entity, b, coverage)).join('; ')
		}.`;
	}
	return 'Not present: coverage is complete, so the server holds no such row (as of the last sync).';
}

/** One coverage bound as a reason a row may exist without having synced. */
function boundReason(
	entity: string,
	bound: Coverage['bounds'][number],
	coverage: Coverage,
): string {
	switch (bound) {
		case 'unsubscribed':
			return `this tab's subscriptions do not name "${entity}"`;
		case 'predicate':
			return 'only rows matching a subscription predicate sync';
		case 'window':
			return `only the top ${coverage.limit ?? 'N'} rows by the subscribed order sync`;
		case 'hydrating':
			return 'the first snapshot is still downloading this entity';
		case 'relation':
			return 'a crossed reference is not covered locally';
	}
}
