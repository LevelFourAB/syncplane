import type { BrowserContext, Page } from '@playwright/test';

import { ACCESS_SERVER_URL_ENV, SERVER_URL_ENV } from './global-setup.js';
import type {
	AccessEntity,
	AccessSamples,
	AccessView,
	AccessWireView,
	MarkView,
	OpenOptions,
	PeerView,
	StatusView,
	TodoView,
	WriteBytesView,
} from './harness-api.js';

/** Address of the testserver global setup spawned for this run. */
export function serverUrl(): string {
	const url = process.env[SERVER_URL_ENV];
	if (url === undefined) throw new Error(`${SERVER_URL_ENV} unset — global setup did not run`);
	return url;
}

/** Address of the access-model testserver global setup spawned for this run. */
export function accessServerUrl(): string {
	const url = process.env[ACCESS_SERVER_URL_ENV];
	if (url === undefined) {
		throw new Error(`${ACCESS_SERVER_URL_ENV} unset — global setup did not run`);
	}
	return url;
}

/** Unique workspace per test, so tests cannot observe each other's rows. */
export function freshWorkspace(): string {
	return `ws-${crypto.randomUUID()}`;
}

export interface RpcStats {
	/** Calls in flight — for a server-streaming method, streams currently open. */
	active: Record<string, number>;
	/** Calls ever made, so a spec can assert something did NOT happen. */
	total: Record<string, number>;
	/** Push request bytes, keyed by workspace — the direction a push carries. */
	pushBytesIn: Record<string, number>;
	/** Pull response bytes, keyed by workspace. */
	pullBytesOut: Record<string, number>;
}

/**
 * The server's own view of what the tabs are doing (testserver's /debug/rpc).
 * Multi-tab invariants — one Subscribe stream per account, no re-bootstrap on
 * reload — are invisible from inside a tab, so they are asserted from here.
 *
 * The counters are process-wide, which is why the e2e project runs
 * single-worker; specs still compare against a snapshot taken inside the test
 * rather than against absolute totals. `server` defaults to the default-schema
 * server; the access-model server keeps counters of its own.
 */
export async function rpcStats(server = serverUrl()): Promise<RpcStats> {
	const res = await fetch(new URL('/debug/rpc', server));
	if (!res.ok) throw new Error(`/debug/rpc responded ${res.status}`);
	return await res.json() as RpcStats;
}

export async function activeCalls(method: string, server = serverUrl()): Promise<number> {
	return (await rpcStats(server)).active[method] ?? 0;
}

export async function totalCalls(method: string, server = serverUrl()): Promise<number> {
	return (await rpcStats(server)).total[method] ?? 0;
}

/**
 * Bytes the server has seen for one workspace, in the direction that carries
 * the payload. Keyed by workspace rather than by method because the per-method
 * totals move under every other spec's traffic; a workspace is unique per test.
 */
export async function pushBytes(workspaceId: string): Promise<number> {
	return (await rpcStats()).pushBytesIn[workspaceId] ?? 0;
}

export async function pullBytes(workspaceId: string): Promise<number> {
	return (await rpcStats()).pullBytesOut[workspaceId] ?? 0;
}

/**
 * Runs one garbage-collection pass on demand (testserver's /debug/gc), so a
 * spec decides when collaborative-text compaction happens instead of waiting
 * for a timer. The server's delta-log retention is a millisecond; the pause
 * puts the writes just made outside it, which is what makes their rows count
 * as quiet enough to compact.
 */
export async function collectGarbage(workspaceId: string): Promise<void> {
	await new Promise((resolve) => setTimeout(resolve, 20));
	const res = await fetch(new URL('/debug/gc', serverUrl()), {
		method: 'POST',
		body: JSON.stringify({ workspace: workspaceId }),
	});
	if (!res.ok) throw new Error(`/debug/gc responded ${res.status}: ${await res.text()}`);
}

/** One row write or delete for trustedWrite, in testserver's /debug/trusted form. */
export type TrustedWrite =
	| { entity: string; value: Record<string, unknown> }
	| { entity: string; key: string; delete: true };

/**
 * Writes rows of the access-model schema as a trusted caller, through the
 * access-model testserver's /debug/trusted, in one transaction. Values are
 * the entity message in protojson form. This is how a spec seeds and changes
 * grant rows: a schema that uses the access model admits no client write
 * before a grant exists.
 */
export async function trustedWrite(workspaceId: string, writes: TrustedWrite[]): Promise<void> {
	const res = await fetch(new URL('/debug/trusted', accessServerUrl()), {
		method: 'POST',
		body: JSON.stringify({ workspace: workspaceId, writes }),
	});
	if (!res.ok) throw new Error(`/debug/trusted responded ${res.status}: ${await res.text()}`);
}

/**
 * A browser tab holding one SyncClient — the unit these specs are written in.
 * Two tabs opened on the same BrowserContext share an origin, and therefore
 * share IndexedDB, Web Locks and BroadcastChannel: that is what makes them
 * "two tabs of one app" rather than two unrelated clients.
 *
 * Every method is a typed round-trip into the page harness.
 */
export class Tab {
	private constructor(readonly page: Page) {}

	static async open(context: BrowserContext, opts: OpenOptions): Promise<Tab> {
		const page = await context.newPage();
		const server = opts.user === undefined ? serverUrl() : accessServerUrl();
		await page.goto(`/?server=${encodeURIComponent(server)}`);
		await page.waitForFunction(() => window.harness !== undefined);
		await page.evaluate((o) => window.harness.open(o), opts);
		return new Tab(page);
	}

	start(): Promise<void> {
		return this.page.evaluate(() => window.harness.start());
	}

	syncNow(): Promise<void> {
		return this.page.evaluate(() => window.harness.syncNow());
	}

	clientId(): Promise<string> {
		return this.page.evaluate(() => window.harness.clientId());
	}

	putTodo(id: string, title: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.putTodo(args.id, args.title),
			{ id, title },
		);
	}

	putTodos(ids: string[]): Promise<void> {
		return this.page.evaluate((i) => window.harness.putTodos(i), ids);
	}

	patchTodo(id: string, fields: Partial<Omit<TodoView, 'id'>>): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.patchTodo(args.id, args.fields),
			{ id, fields },
		);
	}

	getTodo(id: string): Promise<TodoView | null> {
		return this.page.evaluate((i) => window.harness.getTodo(i), id);
	}

	todoIds(): Promise<string[]> {
		return this.page.evaluate(() => window.harness.todoIds());
	}

	watchTodos(): Promise<void> {
		return this.page.evaluate(() => window.harness.watchTodos());
	}

	watched(): Promise<string[]> {
		return this.page.evaluate(() => window.harness.watched());
	}

	notifications(): Promise<number> {
		return this.page.evaluate(() => window.harness.notifications());
	}

	createTextRow(id: string): Promise<void> {
		return this.page.evaluate((i) => window.harness.createTextRow(i), id);
	}

	openText(id: string): Promise<number> {
		return this.page.evaluate((i) => window.harness.openText(i), id);
	}

	joinRoom(name: string): Promise<number> {
		return this.page.evaluate((n) => window.harness.joinRoom(n), name);
	}

	sessionId(): Promise<string> {
		return this.page.evaluate(() => window.harness.sessionId());
	}

	setPresence(room: number, state: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.setPresence(args.room, args.state),
			{ room, state },
		);
	}

	sendRoomMessage(room: number, message: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.sendRoomMessage(args.room, args.message),
			{ room, message },
		);
	}

	roomPeers(room: number): Promise<PeerView[]> {
		return this.page.evaluate((r) => window.harness.roomPeers(r), room);
	}

	roomMessages(room: number): Promise<string[]> {
		return this.page.evaluate((r) => window.harness.roomMessages(r), room);
	}

	leaveRoom(room: number): Promise<void> {
		return this.page.evaluate((r) => window.harness.leaveRoom(r), room);
	}

	textPeer(handle: number): Promise<string> {
		return this.page.evaluate((h) => window.harness.textPeer(h), handle);
	}

	textValue(handle: number): Promise<string> {
		return this.page.evaluate((h) => window.harness.textValue(h), handle);
	}

	insertText(handle: number, index: number, s: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.insertText(args.handle, args.index, args.s),
			{ handle, index, s },
		);
	}

	setTextString(handle: number, s: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.setTextString(args.handle, args.s),
			{ handle, s },
		);
	}

	markText(handle: number, start: number, end: number, key: string, value: string): Promise<void> {
		return this.page.evaluate(
			(a) => window.harness.markText(a.handle, a.start, a.end, a.key, a.value),
			{ handle, start, end, key, value },
		);
	}

	textMarks(handle: number): Promise<MarkView[]> {
		return this.page.evaluate((h) => window.harness.textMarks(h), handle);
	}

	closeText(handle: number): Promise<void> {
		return this.page.evaluate((h) => window.harness.closeText(h), handle);
	}

	storedText(id: string): Promise<string> {
		return this.page.evaluate((i) => window.harness.storedText(i), id);
	}

	storedMarks(id: string): Promise<MarkView[]> {
		return this.page.evaluate((i) => window.harness.storedMarks(i), id);
	}

	storedPeers(id: string): Promise<string[]> {
		return this.page.evaluate((i) => window.harness.storedPeers(i), id);
	}

	storedTombstones(id: string): Promise<number> {
		return this.page.evaluate((i) => window.harness.storedTombstones(i), id);
	}

	putTextString(id: string, s: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.putTextString(args.id, args.s),
			{ id, s },
		);
	}

	patchTextString(id: string, s: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.patchTextString(args.id, args.s),
			{ id, s },
		);
	}

	patchWidgetLabel(id: string, label: string): Promise<void> {
		return this.page.evaluate(
			(args) => window.harness.patchWidgetLabel(args.id, args.label),
			{ id, label },
		);
	}

	confirmedText(id: string): Promise<string | null> {
		return this.page.evaluate((i) => window.harness.confirmedText(i), id);
	}

	databases(): Promise<string[]> {
		return this.page.evaluate(() => window.harness.databases());
	}

	pendingCount(): Promise<number> {
		return this.page.evaluate(() => window.harness.pendingCount());
	}

	pendingBytes(): Promise<number> {
		return this.page.evaluate(() => window.harness.pendingBytes());
	}

	writeBytes(): Promise<WriteBytesView> {
		return this.page.evaluate(() => window.harness.writeBytes());
	}

	resetWriteBytes(): Promise<void> {
		return this.page.evaluate(() => window.harness.resetWriteBytes());
	}

	status(): Promise<StatusView> {
		return this.page.evaluate(() => window.harness.status());
	}

	rejections(): Promise<string[]> {
		return this.page.evaluate(() => window.harness.rejections());
	}

	holdWidget(id: string): Promise<number> {
		return this.page.evaluate((i) => window.harness.holdWidget(i), id);
	}

	releaseHold(hold: number): Promise<void> {
		return this.page.evaluate((h) => window.harness.releaseHold(h), hold);
	}

	hasWidget(id: string): Promise<boolean> {
		return this.page.evaluate((i) => window.harness.hasWidget(i), id);
	}

	hasPermission(entity: AccessEntity, key: string, permission: string): Promise<AccessView> {
		return this.page.evaluate(
			(a) => window.harness.hasPermission(a.entity, a.key, a.permission),
			{ entity, key, permission },
		);
	}

	startSampling(entity: AccessEntity, key: string, permission: string): Promise<void> {
		return this.page.evaluate(
			(a) => window.harness.startSampling(a.entity, a.key, a.permission),
			{ entity, key, permission },
		);
	}

	stopSampling(): Promise<AccessSamples> {
		return this.page.evaluate(() => window.harness.stopSampling());
	}

	accessWire(): Promise<AccessWireView> {
		return this.page.evaluate(() => window.harness.accessWire());
	}

	setVisibility(state: 'visible' | 'hidden'): Promise<void> {
		return this.page.evaluate((s) => window.harness.setVisibility(s), state);
	}

	/** Closes the client, then the tab — how a user closes a tab, in order. */
	async closeTab(): Promise<void> {
		await this.page.evaluate(() => window.harness.close());
		await this.page.close();
	}
}

/** `count` ids under `prefix`, zero-padded so sort order is stable. */
export function todoIds(prefix: string, count: number): string[] {
	return Array.from({ length: count }, (_, i) => `${prefix}-${String(i).padStart(3, '0')}`);
}
