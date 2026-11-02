// Forwarded sync: a TS client syncs against a local engine that
// engine/upstream keeps converged with an origin, while other writers use the
// origin directly. The testserver runs both engines in one process
// (-forward) and links them over loopback HTTP in the link's default
// write-behind mode, so a local write is acked by the local engine at once
// and reaches the origin later.

import { createConnectTransport } from '@connectrpc/connect-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { SyncClient, type SyncClientOptions } from '../../src/client.js';
import type { RejectionInfo } from '../../src/client.js';
import { CodePermissionDenied } from '../../src/codes.js';
import { CompleteAllArgsSchema, TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { MemoryKVStore } from '../../src/kv/memory.js';
import { plainTextOf } from '../../src/text/index.js';
import { startTestServer, type TestServer } from '../support/testserver.js';
import { eventually, freshWorkspace } from './helpers.js';

let server: TestServer;
let localUrl: string;
let originUrl: string;
beforeAll(async () => {
	server = await startTestServer([ '-forward', '-heartbeat=250ms', '-chunk-size=8' ]);
	const info = await (await fetch(`${server.url}/debug/upstream/info`)).json() as {
		origin: string;
		local: string;
	};
	localUrl = info.local;
	originUrl = info.origin;
});
afterAll(() => server?.stop());
// The partition cuts the one link every spec shares.
afterEach(() => upstream('heal'));

interface LinkStatus {
	linked: boolean;
	state: string;
	pending: number;
	originSeq: number;
	lastError: string;
	rejections: {
		code: string;
		message: string;
		mutator: string;
		rows: { Entity: string; Key: string }[];
	}[];
}

async function upstream(op: string, body: unknown = {}): Promise<void> {
	const res = await fetch(`${server.url}/debug/upstream/${op}`, {
		method: 'POST',
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`upstream ${op} failed: ${res.status} ${await res.text()}`);
}

async function status(ws: string): Promise<LinkStatus> {
	const res = await fetch(`${server.url}/debug/upstream/status?workspace=${encodeURIComponent(ws)}`);
	if (!res.ok) throw new Error(`upstream status failed: ${res.status} ${await res.text()}`);
	return await res.json() as LinkStatus;
}

/** A fresh workspace the link syncs. The local engine refuses writes to
 * any other. */
async function linkedWorkspace(): Promise<string> {
	const ws = freshWorkspace();
	await upstream('link', { workspace: ws });
	return ws;
}

/** Waits until the origin has confirmed every local write of ws and the link
 * is caught up with it. */
function flush(ws: string): Promise<void> {
	return upstream('flush', { workspace: ws });
}

/** Trusted string write to a widget's body at the origin. */
async function originTextString(ws: string, key: string, value: string): Promise<void> {
	const res = await fetch(`${originUrl}/debug/textstring`, {
		method: 'POST',
		body: JSON.stringify({ workspace: ws, key, field: 'body', value }),
	});
	if (!res.ok) throw new Error(`textstring failed: ${res.status} ${await res.text()}`);
}

function clientAt(
	baseUrl: string,
	ws: string,
	overrides: Partial<SyncClientOptions> = {},
): SyncClient {
	return new SyncClient({
		transport: createConnectTransport({ baseUrl, httpVersion: '1.1' }),
		workspaceId: ws,
		kvStore: new MemoryKVStore(),
		entities: [ TodoSchema, WidgetSchema ],
		pollIntervalMs: 0,
		streamEnabled: false,
		...overrides,
	});
}

async function started(c: SyncClient): Promise<SyncClient> {
	await c.start();
	return c;
}

const localClient = (ws: string, o?: Partial<SyncClientOptions>) =>
	started(clientAt(localUrl, ws, o));
const originClient = (ws: string, o?: Partial<SyncClientOptions>) =>
	started(clientAt(originUrl, ws, o));

/** Syncs c until read returns what match accepts, and fails with the last
 * value read when it never does. */
async function settled<T>(
	c: SyncClient,
	read: () => Promise<T>,
	match: (v: T) => boolean,
): Promise<T> {
	let last: T | undefined;
	try {
		return await eventually(async () => {
			await c.syncNow();
			last = await read();
			return match(last) ? { v: last } : undefined;
		}).then((r) => r!.v);
	} catch (err) {
		throw new Error(
			`never settled; last read ${
				JSON.stringify(last, (_, v) => typeof v === 'bigint' ? String(v) : v)
			}`,
			{
				cause: err,
			},
		);
	}
}

describe('forwarded sync', () => {
	it('carries an origin write to the TS client and a TS client write to the origin', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);

		await origin.mutate.put(TodoSchema, { id: 'o1', title: 'from the origin' });
		await origin.syncNow();
		await settled(local, () => local.get(TodoSchema, 'o1'), (t) => t?.title === 'from the origin');

		await local.mutate.put(TodoSchema, { id: 'l1', title: 'from the browser' });
		await local.syncNow();
		expect(await local.pendingCount()).toBe(0);
		await flush(ws);
		await settled(origin, () => origin.get(TodoSchema, 'l1'), (t) => t?.title === 'from the browser');

		await local.close();
		await origin.close();
	});

	it('forwards writes made while partitioned and merges them field by field', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);
		await origin.mutate.put(TodoSchema, { id: 't1', title: 'base', completed: false });
		await origin.syncNow();
		await settled(local, () => local.get(TodoSchema, 't1'), (t) => t?.title === 'base');

		await upstream('partition');
		await local.mutate.patch(TodoSchema, 't1', { title: 'renamed in the browser' });
		await local.syncNow();
		// The local engine acked the write: the browser has nothing pending.
		expect(await local.pendingCount()).toBe(0);
		await origin.mutate.patch(TodoSchema, 't1', { completed: true });
		await origin.syncNow();
		expect((await origin.get(TodoSchema, 't1'))?.title).toBe('base');
		expect((await status(ws)).pending).toBeGreaterThan(0);

		await upstream('heal');
		await flush(ws);

		const merged = (t?: { title: string; completed: boolean }) =>
			t?.title === 'renamed in the browser' && t.completed;
		await settled(local, () => local.get(TodoSchema, 't1'), merged);
		await settled(origin, () => origin.get(TodoSchema, 't1'), merged);

		await local.close();
		await origin.close();
	});

	it('resolves a same-field conflict by arrival at the origin, not by wall clock', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);
		await local.mutate.put(TodoSchema, { id: 't1', title: 'base' });
		await local.syncNow();
		await flush(ws);
		await settled(origin, () => origin.get(TodoSchema, 't1'), (t) => t?.title === 'base');

		await upstream('partition');
		// The browser writes first, but its write reaches the origin last.
		await local.mutate.patch(TodoSchema, 't1', { title: 'browser' });
		await local.syncNow();
		await origin.mutate.patch(TodoSchema, 't1', { title: 'origin' });
		await origin.syncNow();
		// Until the link delivers, the local engine keeps the browser's value
		// on top of what the origin confirmed.
		await settled(local, () => local.get(TodoSchema, 't1'), (t) => t?.title === 'browser');

		await upstream('heal');
		await flush(ws);

		await settled(origin, () => origin.get(TodoSchema, 't1'), (t) => t?.title === 'browser');
		await settled(local, () => local.get(TodoSchema, 't1'), (t) => t?.title === 'browser');

		await local.close();
		await origin.close();
	});

	it('reverts a write the origin rejects after the local engine applied it', async () => {
		const ws = await linkedWorkspace();
		const rejections: RejectionInfo[] = [];
		const local = await localClient(ws, { onMutationRejected: (info) => rejections.push(info) });
		const origin = await originClient(ws);
		await local.mutate.put(TodoSchema, { id: 't1', title: 'ok' });
		await local.syncNow();
		await flush(ws);

		// The origin's policy rejects a todo titled "forbidden"; the local
		// engine has no policy and applies both. The partition holds the
		// writes back, so the browser's pull sees them applied.
		await upstream('partition');
		await local.mutate.put(TodoSchema, { id: 'bad', title: 'forbidden fruit' });
		await local.mutate.patch(TodoSchema, 't1', { title: 'forbidden edit' });
		await local.syncNow();
		expect(await local.pendingCount()).toBe(0);
		expect((await local.get(TodoSchema, 'bad'))?.title).toBe('forbidden fruit');
		expect((await local.get(TodoSchema, 't1'))?.title).toBe('forbidden edit');

		await upstream('heal');
		await flush(ws);
		const st = await status(ws);
		expect(st.rejections.map((r) => r.code)).toEqual([ CodePermissionDenied, CodePermissionDenied ]);

		// The link's revert reaches the browser as an ordinary server change.
		await settled(local, () => local.get(TodoSchema, 'bad'), (t) => t === undefined);
		await settled(local, () => local.get(TodoSchema, 't1'), (t) => t?.title === 'ok');
		// The browser saw an APPLIED ack and then a revert, never a rejection.
		expect(rejections).toEqual([]);

		await origin.syncNow();
		expect(await origin.get(TodoSchema, 'bad')).toBeUndefined();
		expect((await origin.get(TodoSchema, 't1'))?.title).toBe('ok');

		await local.close();
		await origin.close();
	});

	it('converges collaborative text edited through the local engine and at the origin', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);
		await local.mutate.put(WidgetSchema, { id: 'w1', body: 'one two three' });
		await local.syncNow();
		await flush(ws);
		await settled(
			origin,
			() => origin.get(WidgetSchema, 'w1'),
			(w) => plainTextOf(w?.body) === 'one two three',
		);

		await upstream('partition');
		await local.mutate.patch(WidgetSchema, 'w1', { body: 'one TWO three' });
		await local.syncNow();
		await originTextString(ws, 'w1', '* one two three!');
		await settled(
			origin,
			() => origin.get(WidgetSchema, 'w1'),
			(w) => plainTextOf(w?.body) === '* one two three!',
		);
		await origin.mutate.patch(WidgetSchema, 'w1', { body: '* one two three, four!' });
		await origin.syncNow();

		await upstream('heal');
		await flush(ws);

		const want = '* one TWO three, four!';
		const text = (c: SyncClient) => () => c.get(WidgetSchema, 'w1').then((w) => plainTextOf(w?.body));
		await settled(local, text(local), (s) => s === want);
		await settled(origin, text(origin), (s) => s === want);
		// Fresh replicas of each engine: what the local engine stores, and what
		// the origin stores.
		const localEngine = await localClient(ws);
		const originEngine = await originClient(ws);
		await settled(localEngine, text(localEngine), (s) => s === want);
		await settled(originEngine, text(originEngine), (s) => s === want);

		for (const c of [ local, origin, localEngine, originEngine ]) await c.close();
	});

	it('forwards a custom mutation only the origin declares, and its result comes back', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);

		await local.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await local.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await local.syncNow();
		await upstream('partition');
		await local.mutate.custom('complete_all', CompleteAllArgsSchema, {});
		await local.syncNow();
		// The local engine acks the mutation without running anything.
		expect(await local.pendingCount()).toBe(0);
		expect((await local.get(TodoSchema, 't1'))?.completed).toBe(false);

		await upstream('heal');
		await flush(ws);
		expect((await status(ws)).rejections).toEqual([]);
		const allDone = (rows: { value: { completed: boolean } }[]) =>
			rows.length === 2 && rows.every((r) => r.value.completed);
		await settled(local, () => local.scan(TodoSchema), allDone);
		await settled(origin, () => origin.scan(TodoSchema), allDone);

		await local.close();
		await origin.close();
	});

	it('loses no pending write when the link restarts', async () => {
		const ws = await linkedWorkspace();
		const local = await localClient(ws);
		const origin = await originClient(ws);
		await local.mutate.put(TodoSchema, { id: 't1', title: 'one' });
		await local.syncNow();
		await flush(ws);

		await upstream('partition');
		await local.mutate.put(TodoSchema, { id: 't2', title: 'two' });
		await local.mutate.patch(TodoSchema, 't1', { completed: true });
		await local.mutate.del(TodoSchema, 't1');
		await local.mutate.put(TodoSchema, { id: 't3', title: 'three' });
		await local.syncNow();
		expect(await local.pendingCount()).toBe(0);
		const before = await eventually(async () => {
			const st = await status(ws);
			return st.pending > 0 ? st : undefined;
		});

		// Still partitioned: the restarted link starts from what it stored.
		await upstream('restart');
		const after = await status(ws);
		expect(after.linked).toBe(true);
		// Capture notices a write shortly after it commits, so the count read
		// before may still have been growing.
		expect(after.pending).toBeGreaterThanOrEqual(before!.pending);

		// The browser keeps syncing against the new engine instance.
		await local.mutate.put(TodoSchema, { id: 't4', title: 'four' });
		await local.syncNow();

		await upstream('heal');
		await flush(ws);
		const keys = (rows: { key: string }[]) => rows.map((r) => r.key).join(',');
		await settled(origin, () => origin.scan(TodoSchema), (rows) => keys(rows) === 't2,t3,t4');
		await settled(local, () => local.scan(TodoSchema), (rows) => keys(rows) === 't2,t3,t4');
		expect((await status(ws)).rejections).toEqual([]);

		await local.close();
		await origin.close();
	});
});
