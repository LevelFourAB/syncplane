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
 */

import { fromBinary } from '@bufbuild/protobuf';

import { TodoSchema } from '../../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import {
	decodeTextDoc,
	SyncClient,
	type TextHandle,
	textMarksOf,
	textOf,
	type TextSpan,
} from '../../../src/index.js';
import { confirmedKey, type ConfirmedRecord, pendingPrefix } from '../../../src/keys.js';
import { IDBKVStore } from '../../../src/kv/idb.js';
import { entityDefOf } from '../../../src/mutations.js';
import { compactDoc } from '../../../src/text/index.js';
import type { WatchHandle } from '../../../src/watch.js';
import type { HarnessApi, MarkView, StatusView, TodoView } from '../harness-api.js';
import { WriteMeter } from './write-meter.js';

/** The spawned testserver's address, handed over as ?server=... by Tab.open. */
const serverUrl = new URLSearchParams(location.search).get('server');

const WIDGET_ENTITY = entityDefOf(WidgetSchema).name;

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
/** Set only for a tab opened with measureWrites; see writeBytes. */
let meter: WriteMeter | undefined;

function current(): SyncClient {
	if (client === undefined) throw new Error('harness: no client open');
	return client;
}

function text(handle: number): TextHandle {
	const h = textHandles.get(handle);
	if (h === undefined) throw new Error(`harness: no text handle ${handle}`);
	return h;
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

function todoView(
	row: { id: string; title: string; completed: boolean } | undefined,
): TodoView | null {
	return row === undefined ? null : { id: row.id, title: row.title, completed: row.completed };
}

const harness: HarnessApi = {
	async open(opts) {
		if (client !== undefined) throw new Error('harness: client already open');
		if (serverUrl === null) throw new Error('harness: page opened without ?server=');
		notificationCount = 0;
		watched.clear();
		rejections.length = 0;

		// The metered store is the one the client would have opened for itself,
		// under the name it would have used — the meter only counts what goes past,
		// so a measured tab still runs on real IndexedDB like every other tab here.
		meter = opts.measureWrites === true
			? new WriteMeter(await IDBKVStore.open(`sync:${opts.accountId ?? opts.workspaceId}`))
			: undefined;

		const opened = new SyncClient({
			url: serverUrl,
			workspaceId: opts.workspaceId,
			accountId: opts.accountId,
			kvStore: meter,
			entities: (opts.entities ?? [ 'todo', 'widget' ]).map((name) =>
				name === 'todo' ? TodoSchema : WidgetSchema
			),
			// Both default off so a spec only pays for the machinery it asserts on;
			// the leadership specs turn the stream on explicitly.
			streamEnabled: opts.streamEnabled ?? false,
			pollIntervalMs: opts.pollIntervalMs ?? 0,
			onMutationRejected: (info) => {
				rejections.push(`${info.errorCode}: ${info.errorMessage}`);
			},
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
		await current().syncNow();
	},

	async close() {
		unwatch?.close();
		unwatch = undefined;
		// Awaited: closing drains each handle's write chain, so an edit made just
		// before the tab closes is queued before the client goes away.
		for (const handle of textHandles.values()) await handle.close();
		textHandles.clear();
		await client?.close();
		client = undefined;
		workspaceId = undefined;
		meter = undefined;
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

	async setVisibility(state) {
		Object.defineProperty(document, 'visibilityState', {
			configurable: true,
			get: () => state,
		});
		document.dispatchEvent(new Event('visibilitychange'));
	},
};

window.harness = harness;
