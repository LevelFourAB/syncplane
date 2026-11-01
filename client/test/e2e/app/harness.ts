/**
 * The page half of the multi-tab e2e tier: one SyncClient per tab, wrapped in a
 * JSON-only façade the specs drive over `page.evaluate`. See ../harness-api.ts
 * for the contract and ../../../playwright.config.ts for how it is served.
 *
 * Nothing here configures storage or locking: a browser SyncClient given only a
 * url picks IDBKVStore, WebLockMutex, changeshare and stream leadership on its
 * own, and it is precisely those defaults this tier exists to exercise. The one
 * exception is `measureWrites`, which hands the client the same IDBKVStore
 * wrapped in a counter (see write-meter.ts) so a spec can ask what a write cost.
 * A tab opened as a user of the access-model schema is the other: it hands
 * the client a transport, because a transport is the only place to set the
 * bearer token the access-model testserver takes as the caller.
 */

import { type DescMessage, fromBinary } from '@bufbuild/protobuf';
import type { Interceptor, StreamResponse } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';

import { TodoSchema } from '../../../src/gen/example/todo/v1/todo_pb.js';
import {
	OrgMemberSchema,
	ProjectMemberSchema,
	ProjectSchema,
	TaskSchema,
} from '../../../src/gen/syncplane/testdata/access/v1/access_pb.js';
import {
	BlobHolderSchema,
	WidgetSchema,
} from '../../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { PullResponse, SubscribeResponse } from '../../../src/gen/syncplane/v1/service_pb.js';
import {
	decodeTextDoc,
	type HoldHandle,
	type Room,
	subscription,
	SyncClient,
	type TextHandle,
	textMarksOf,
	textOf,
	type TextSpan,
	type UndoScope,
} from '../../../src/index.js';
import { confirmedKey, type ConfirmedRecord, pendingPrefix } from '../../../src/keys.js';
import { IDBKVStore } from '../../../src/kv/idb.js';
import { entityDefOf } from '../../../src/mutations.js';
import { compactDoc } from '../../../src/text/index.js';
import type { WatchHandle } from '../../../src/watch.js';
import type {
	AccessEntity,
	AccessSamples,
	AccessView,
	AccessWireView,
	BlobReadView,
	BlobRefView,
	HarnessApi,
	MarkView,
	StatusView,
	TodoView,
} from '../harness-api.js';
import { blobBytes } from '../harness-api.js';
import { WriteMeter } from './write-meter.js';

/** The spawned testserver's address, handed over as ?server=... by Tab.open. */
const serverUrl = new URLSearchParams(location.search).get('server');

const WIDGET_ENTITY = entityDefOf(WidgetSchema).name;

/**
 * What a tab opened as a user registers: the grant rows and the rows they
 * sit on, which is all the access specs write and predict for.
 */
const ACCESS_ENTITIES = [ OrgMemberSchema, ProjectSchema, ProjectMemberSchema, TaskSchema ];

const ACCESS_SCHEMAS: Record<AccessEntity, DescMessage> = {
	project: ProjectSchema,
	task: TaskSchema,
};

let client: SyncClient | undefined;
/** The workspace `open` was called with — needed to address confirmed rows. */
let workspaceId: string | undefined;
let unwatch: WatchHandle | undefined;
let notificationCount = 0;
const watched = new Set<string>();
const rejections: string[] = [];
/** Open text handles, keyed by the number the spec holds (handles are not JSON). */
const textHandles = new Map<number, TextHandle>();
let nextTextHandle = 1;
/** Open undo scopes, keyed the same way. */
const undoScopes = new Map<number, UndoScope>();
let nextUndoScope = 1;
/** Live holds, keyed the same way. */
const holds = new Map<number, HoldHandle>();
let nextHold = 1;
/**
 * Open rooms, keyed the same way, each with the broadcasts it has received —
 * messages are at-most-once and not replayed, so a spec that asked for them
 * after the fact would have nothing to read unless they are collected as they
 * arrive.
 */
const rooms = new Map<number, { room: Room<string>; received: string[] }>();
let nextRoom = 1;
/** Set only for a tab opened with measureWrites; see writeBytes. */
let meter: WriteMeter | undefined;
/** Set only for a tab opened with a user; see accessWire. */
let wire: AccessWireView | undefined;
/**
 * Pull requests the transport sent, and Pull pages that asked for another
 * page (has_more), since open. syncNow compares the two to count repulls.
 */
let pullsSent = 0;
let pullsWithMore = 0;
/** syncNow calls in progress; a pull sent while there are none counts as background. */
let syncing = 0;
/** The running sampler; see startSampling. */
let sampler: { stopped: boolean; samples: AccessSamples; done: Promise<void> } | undefined;

function current(): SyncClient {
	if (client === undefined) throw new Error('harness: no client open');
	return client;
}

function text(handle: number): TextHandle {
	const h = textHandles.get(handle);
	if (h === undefined) throw new Error(`harness: no text handle ${handle}`);
	return h;
}

function undoScopeOf(handle: number): UndoScope {
	const scope = undoScopes.get(handle);
	if (scope === undefined) throw new Error(`harness: no undo scope ${handle}`);
	return scope;
}

function roomOf(handle: number): { room: Room<string>; received: string[] } {
	const r = rooms.get(handle);
	if (r === undefined) throw new Error(`harness: no room ${handle}`);
	return r;
}

/** Decodes widget `id`'s stored body, read through the client's local store. */
async function storedDoc(id: string) {
	const row = await current().get(WidgetSchema, id);
	if (row === undefined) throw new Error(`harness: widget ${id} not found`);
	return decodeTextDoc(row.body, '');
}

/**
 * Total bytes the pending queue occupies in IndexedDB, read the same way as
 * confirmedBody. This is what a keystroke actually writes to disk: one queue
 * entry per mutation, holding the delta for that one edit.
 */
async function pendingQueueBytes(): Promise<number> {
	const c = current();
	if (workspaceId === undefined) throw new Error('harness: no workspace');
	const kv = await IDBKVStore.open(`sync:${c.accountId}`);
	try {
		const read = await kv.read();
		try {
			let total = 0;
			for await (const [ , v ] of read.scan(pendingPrefix(workspaceId))) {
				total += (v as Uint8Array).length;
			}
			return total;
		} finally {
			read.release();
		}
	} finally {
		await kv.close();
	}
}

/**
 * The confirmed row's bytes, read from IndexedDB directly. The client keeps its
 * own store private (and its reads go through the optimistic overlay), so this
 * opens a second connection to the same database — safe because IndexedDB
 * allows concurrent connections and this only ever reads.
 */
async function confirmedBody(id: string): Promise<Uint8Array | undefined | null> {
	const c = current();
	if (workspaceId === undefined) throw new Error('harness: no workspace');
	const kv = await IDBKVStore.open(`sync:${c.accountId}`);
	try {
		const read = await kv.read();
		try {
			const rec = await read.get(confirmedKey(WIDGET_ENTITY, workspaceId, id)) as
				| ConfirmedRecord
				| undefined;
			if (rec === undefined) return null;
			return fromBinary(WidgetSchema, rec.v).body;
		} finally {
			read.release();
		}
	} finally {
		await kv.close();
	}
}

/** Mark spans narrowed to the JSON the boundary carries; see MarkView. */
function markViews(spans: readonly TextSpan[]): MarkView[] {
	return spans.map((s) => ({ start: s.start, end: s.end, key: s.key, value: String(s.value) }));
}

/** A tab's view of what CallerAccess one page carried; see AccessWireView. */
function countAccess(page: PullResponse, via: 'pull' | 'stream'): void {
	if (wire === undefined) return;
	if (page.callerAccess !== undefined) {
		if (via === 'pull') wire.pullWhole++;
		else wire.streamWhole++;
	}
	if (page.callerAccessDelta !== undefined) {
		if (via === 'pull') wire.pullDelta++;
		else wire.streamDelta++;
	}
}

/** Counts the CallerAccess of every Subscribe page as the client reads it. */
async function* countFrames(
	frames: AsyncIterable<SubscribeResponse>,
): AsyncIterable<SubscribeResponse> {
	for await (const frame of frames) {
		if (frame.body.case === 'page') countAccess(frame.body.value, 'stream');
		yield frame;
	}
}

/**
 * Sets the bearer token the access-model testserver takes as the caller, and
 * counts the CallerAccess traffic for accessWire.
 */
function asUser(user: string): Interceptor {
	return (next) => async (req) => {
		req.header.set('Authorization', `Bearer ${user}`);
		if (req.method.name === 'Pull') {
			pullsSent++;
			if (syncing === 0 && wire !== undefined) wire.backgroundPulls++;
		}
		const res = await next(req);
		if (req.method.name === 'Pull' && !res.stream) {
			const page = res.message as PullResponse;
			if (page.hasMore) pullsWithMore++;
			countAccess(page, 'pull');
		}
		if (req.method.name === 'Subscribe' && res.stream) {
			const frames = res.message as AsyncIterable<SubscribeResponse>;
			return { ...res, message: countFrames(frames) } as StreamResponse<DescMessage, DescMessage>;
		}
		return res;
	};
}

/** One prediction as the JSON the boundary carries; see AccessView. */
async function accessView(
	entity: AccessEntity,
	key: string,
	permission: string,
): Promise<AccessView> {
	const detail = await current().hasPermission(ACCESS_SCHEMAS[entity], key, permission, {
		detail: true,
	});
	return detail.answer === 'unknown' ? `unknown:${detail.reason}` : detail.answer;
}

function todoView(
	row: { id: string; title: string; completed: boolean } | undefined,
): TodoView | null {
	return row === undefined ? null : { id: row.id, title: row.title, completed: row.completed };
}

function refOf(ref: BlobRefView) {
	return { digest: ref.digest, size: BigInt(ref.size), mediaType: ref.mediaType };
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
	const hash = new Uint8Array(
		await crypto.subtle.digest('SHA-256', bytes as Uint8Array<ArrayBuffer>),
	);
	return [ ...hash ].map((b) => b.toString(16).padStart(2, '0')).join('');
}

const harness: HarnessApi = {
	async open(opts) {
		if (client !== undefined) throw new Error('harness: client already open');
		if (serverUrl === null) throw new Error('harness: page opened without ?server=');
		notificationCount = 0;
		watched.clear();
		rejections.length = 0;
		pullsSent = 0;
		pullsWithMore = 0;
		syncing = 0;
		const user = opts.user;
		wire = user === undefined ? undefined : {
			pullWhole: 0,
			pullDelta: 0,
			streamWhole: 0,
			streamDelta: 0,
			repulls: 0,
			backgroundPulls: 0,
		};

		// The metered store is the one the client would have opened for itself,
		// under the name it would have used — the meter only counts what goes past,
		// so a measured tab still runs on real IndexedDB like every other tab here.
		meter = opts.measureWrites === true
			? new WriteMeter(await IDBKVStore.open(`sync:${opts.accountId ?? opts.workspaceId}`))
			: undefined;

		const opened = new SyncClient({
			...(user === undefined
				? { url: serverUrl }
				: {
					// The binary format the client picks for itself from a url.
					transport: createConnectTransport({
						baseUrl: serverUrl,
						useBinaryFormat: true,
						interceptors: [ asUser(user) ],
					}),
				}),
			workspaceId: opts.workspaceId,
			accountId: opts.accountId,
			kvStore: meter,
			entities: user === undefined
				? (opts.entities ?? [ 'todo', 'widget' ]).map((name) =>
					name === 'todo' ? TodoSchema : name === 'blob' ? BlobHolderSchema : WidgetSchema
				)
				: ACCESS_ENTITIES,
			subscriptions: opts.subscriptions?.map((name) =>
				subscription(name === 'todo' ? TodoSchema : WidgetSchema)
			),
			holdGraceMs: opts.holdGraceMs ?? 0,
			// Both default off so a spec only pays for the machinery it asserts on;
			// the leadership specs turn the stream on explicitly.
			streamEnabled: opts.streamEnabled ?? false,
			pollIntervalMs: opts.pollIntervalMs ?? 0,
			onMutationRejected: (info) => {
				rejections.push(`${info.errorCode}: ${info.errorMessage}`);
			},
			undo: opts.undo === true ? {} : undefined,
		});
		client = opened;
		workspaceId = opts.workspaceId;
		if (opts.start === true) await opened.start();
		return await opened.clientId();
	},

	async start() {
		await current().start();
	},

	async syncNow() {
		const sent = pullsSent;
		const withMore = pullsWithMore;
		syncing++;
		try {
			await current().syncNow();
		} finally {
			syncing--;
		}
		// One cycle pulls one page, plus one per page that asked for more. A
		// further pull is the one the client sends when a delta did not apply.
		if (wire !== undefined && pullsSent - sent > 1 + (pullsWithMore - withMore)) wire.repulls++;
	},

	async close() {
		if (sampler !== undefined) {
			sampler.stopped = true;
			await sampler.done.catch(() => undefined);
			sampler = undefined;
		}
		unwatch?.close();
		unwatch = undefined;
		for (const hold of holds.values()) hold.release();
		holds.clear();
		// Awaited: closing drains each handle's write chain, so an edit made just
		// before the tab closes is queued before the client goes away.
		for (const handle of textHandles.values()) await handle.close();
		textHandles.clear();
		// Closing a room publishes its departure, so the other tabs see this one
		// go at once rather than waiting out the expiry window.
		for (const entry of rooms.values()) await entry.room.close();
		rooms.clear();
		// The client's close() writes and frees the saved ones.
		undoScopes.clear();
		await client?.close();
		client = undefined;
		workspaceId = undefined;
		meter = undefined;
		wire = undefined;
	},

	clientId() {
		return current().clientId();
	},

	async putTodo(id, title) {
		await current().mutate.put(TodoSchema, { id, title });
	},

	async putTodos(ids) {
		const c = current();
		await Promise.all(ids.map((id) => c.mutate.put(TodoSchema, { id, title: id })));
	},

	async patchTodo(id, fields) {
		await current().mutate.patch(TodoSchema, id, fields);
	},

	async getTodo(id) {
		return todoView(await current().get(TodoSchema, id));
	},

	async todoIds() {
		const rows = await current().scan(TodoSchema);
		return [ ...rows ].map((row) => row.value.id).sort();
	},

	async watchTodos() {
		unwatch?.close();
		unwatch = current().watch(TodoSchema, (rows) => {
			notificationCount++;
			for (const row of rows) watched.add(row.value.id);
		});
	},

	async watched() {
		return [ ...watched ].sort();
	},

	async notifications() {
		return notificationCount;
	},

	async createTextRow(id) {
		await current().mutate.put(WidgetSchema, { id });
	},

	async openText(id) {
		const handle = await current().text(WidgetSchema, id, 'body');
		const key = nextTextHandle++;
		textHandles.set(key, handle);
		return key;
	},

	async holdWidget(id) {
		const hold = current().hold(WidgetSchema, id);
		const key = nextHold++;
		holds.set(key, hold);
		await hold.ready;
		return key;
	},

	async releaseHold(key) {
		const hold = holds.get(key);
		if (hold === undefined) throw new Error(`harness: no hold ${key}`);
		hold.release();
		holds.delete(key);
	},

	async hasWidget(id) {
		return (await current().get(WidgetSchema, id)) !== undefined;
	},

	async textPeer(handle) {
		return text(handle).doc.peer;
	},

	async textValue(handle) {
		return text(handle).value;
	},

	async insertText(handle, index, s) {
		await text(handle).insert(index, s);
	},

	async setTextString(handle, s) {
		await text(handle).setString(s);
	},

	async markText(handle, start, end, key, value) {
		await text(handle).mark(start, end, key, value);
	},

	async textMarks(handle) {
		return markViews(text(handle).marks());
	},

	async closeText(handle) {
		await text(handle).close();
		textHandles.delete(handle);
	},

	async joinRoom(name) {
		const room = current().ws(workspaceId!).room<string>(name);
		const key = nextRoom++;
		const received: string[] = [];
		room.onMessage((m) => received.push(m.message));
		rooms.set(key, { room, received });
		return key;
	},

	async sessionId() {
		return current().sessionId;
	},

	async setPresence(id, state) {
		roomOf(id).room.setPresence(state);
	},

	async sendRoomMessage(id, message) {
		await roomOf(id).room.send(message);
	},

	async roomPeers(id) {
		return roomOf(id).room.peers().map((p) => ({
			sessionId: p.sessionId,
			subject: p.subject,
			state: p.state,
			self: p.self,
		}));
	},

	async roomMessages(id) {
		return [ ...roomOf(id).received ];
	},

	async leaveRoom(id) {
		await roomOf(id).room.close();
		rooms.delete(id);
	},

	async storedText(id) {
		return (await storedDoc(id)).plainText();
	},

	async storedMarks(id) {
		const row = await current().get(WidgetSchema, id);
		if (row === undefined) throw new Error(`harness: widget ${id} not found`);
		return markViews(textMarksOf(row.body));
	},

	async storedPeers(id) {
		return [ ...(await storedDoc(id)).vv.keys() ].sort();
	},

	async storedTombstones(id) {
		// Compacting a throwaway decode counts exactly the tombstones a server-side
		// pass would reclaim, which is the number the compaction specs assert on.
		return compactDoc(await storedDoc(id)).items;
	},

	async putTextString(id, s) {
		await current().mutate.put(WidgetSchema, { id, body: s });
	},

	async patchTextString(id, s) {
		await current().mutate.patch(WidgetSchema, id, { body: s });
	},

	async patchWidgetLabel(id, label) {
		await current().mutate.patch(WidgetSchema, id, { label });
	},

	async confirmedText(id) {
		const body = await confirmedBody(id);
		return body === null ? null : textOf(body);
	},

	async putBlob(size, seed, mediaType) {
		const bytes = blobBytes(size, seed);
		const ref = await current().blobs.put(new Blob([ bytes as Uint8Array<ArrayBuffer> ]), {
			mediaType,
		});
		return { digest: ref.digest, size: Number(ref.size), mediaType: ref.mediaType };
	},

	async putBlobHolder(id, ref) {
		await current().mutate.put(BlobHolderSchema, { id, title: id, photo: refOf(ref) });
	},

	async blobUploaded(ref) {
		await current().blobs.uploaded(refOf(ref));
	},

	async getBlob(ref) {
		const blob = await current().blobs.get(refOf(ref));
		return {
			size: blob.size,
			type: blob.type,
			sha256: await sha256Hex(new Uint8Array(await blob.arrayBuffer())),
			chunks: [],
		};
	},

	async streamBlob(ref) {
		const parts: Uint8Array[] = [];
		const reader = current().blobs.stream(refOf(ref)).getReader();
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			parts.push(value);
		}
		const all = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
		let at = 0;
		for (const p of parts) {
			all.set(p, at);
			at += p.length;
		}
		return {
			size: all.length,
			type: ref.mediaType,
			sha256: await sha256Hex(all),
			chunks: parts.map((p) => p.length),
		};
	},

	async storedBlob(ref) {
		const c = current();
		// Read beside the client, on a connection of the spec's own: what is on
		// disk is the question, not what the client reports.
		const db = await new Promise<IDBDatabase>((resolve, reject) => {
			const req = indexedDB.open(c.storageNames.blobStore);
			req.onsuccess = () => resolve(req.result);
			req.onerror = () => reject(req.error);
		});
		try {
			const get = <T>(store: string, key: IDBValidKey): Promise<T | undefined> =>
				new Promise((resolve, reject) => {
					const req = db.transaction(store, 'readonly').objectStore(store).get(key);
					req.onsuccess = () => resolve(req.result as T | undefined);
					req.onerror = () => reject(req.error);
				});
			const ws = workspaceId ?? '';
			const owner = (await get<{ bytesId: string }>('cache', [ ws, ref.digest ]))
				?? (await get<{ bytesId: string }>('uploads', [ ws, ref.digest ]));
			const cached = (await get('cache', [ ws, ref.digest ])) !== undefined;
			if (owner === undefined) return { cached, chunks: 0, allBlobs: false };
			const count = Math.ceil(ref.size / (8 * 1024 * 1024));
			let allBlobs = true;
			for (let i = 0; i < count; i++) {
				const v = await get<unknown>('chunks', [ owner.bytesId, i ]);
				if (!(v instanceof Blob)) allBlobs = false;
			}
			return { cached, chunks: count, allBlobs };
		} finally {
			db.close();
		}
	},

	async databases() {
		const dbs = await indexedDB.databases();
		return dbs.map((db) => db.name ?? '').filter((name) => name !== '').sort();
	},

	pendingBytes() {
		return pendingQueueBytes();
	},

	pendingCount() {
		return current().pendingCount();
	},

	async writeBytes() {
		if (meter === undefined) throw new Error('harness: open with measureWrites to count writes');
		return meter.tallied();
	},

	async resetWriteBytes() {
		if (meter === undefined) throw new Error('harness: open with measureWrites to count writes');
		meter.reset();
	},

	async status() {
		const status = current().status();
		const view: StatusView = {
			state: status.state,
			online: status.online,
			dirty: status.dirty,
			pendingMutations: status.pendingMutations,
		};
		return view;
	},

	async rejections() {
		return [ ...rejections ];
	},

	hasPermission(entity, key, permission) {
		return accessView(entity, key, permission);
	},

	async startSampling(entity, key, permission) {
		if (sampler !== undefined) throw new Error('harness: a sampler is already running');
		const running: { stopped: boolean; samples: AccessSamples; done: Promise<void> } = {
			stopped: false,
			samples: { samples: 0, answers: {} },
			done: Promise.resolve(),
		};
		running.done = (async () => {
			while (!running.stopped) {
				const view = await accessView(entity, key, permission);
				running.samples.samples++;
				running.samples.answers[view] = (running.samples.answers[view] ?? 0) + 1;
				// Yields to the event loop, so the sync work the sampler watches
				// runs between two predictions.
				await new Promise((resolve) => setTimeout(resolve, 0));
			}
		})();
		sampler = running;
	},

	async stopSampling() {
		const running = sampler;
		if (running === undefined) throw new Error('harness: no sampler running');
		sampler = undefined;
		running.stopped = true;
		await running.done;
		return running.samples;
	},

	async accessWire() {
		if (wire === undefined) throw new Error('harness: open with a user to count access traffic');
		return { ...wire };
	},

	async setVisibility(state) {
		Object.defineProperty(document, 'visibilityState', {
			configurable: true,
			get: () => state,
		});
		document.dispatchEvent(new Event('visibilitychange'));
	},

	async restoreUndoScope(key) {
		const scope = await current().restoreUndoScope(key);
		const handle = nextUndoScope++;
		undoScopes.set(handle, scope);
		return handle;
	},

	async undoScope(handle) {
		const scope = undoScopeOf(handle);
		return {
			key: scope.key ?? null,
			saved: scope.saved,
			closed: scope.closed,
			canUndo: scope.canUndo(),
			canRedo: scope.canRedo(),
		};
	},

	async putTodoIn(handle, id, title) {
		await undoScopeOf(handle).mutate.put(TodoSchema, { id, title });
	},

	async patchTodoIn(handle, id, fields) {
		await undoScopeOf(handle).mutate.patch(TodoSchema, id, fields);
	},

	undoIn(handle) {
		return undoScopeOf(handle).undo();
	},

	redoIn(handle) {
		return undoScopeOf(handle).redo();
	},

	async closeUndoScope(handle) {
		undoScopeOf(handle).close();
	},

	async discardUndoScope(handle) {
		await undoScopeOf(handle).discard();
	},

	discardSavedUndo(key) {
		return current().discardUndoScope(key);
	},

	async customMutation(name, id) {
		await current().mutate.custom(name, TodoSchema, { id });
	},
};

window.harness = harness;
