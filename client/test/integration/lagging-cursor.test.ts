// A streaming client whose stored cursor is older than the rows it has applied.
//
// The client asks for sparse cursors on its stream, and the server leaves the
// cursor off a frame that weighs less than the cursor would. Such a frame
// applies its rows and moves the store's seq, but keeps the older cursor. The
// next pull starts from that cursor and can answer with pages at a seq below
// the one the store holds: every page of a scope change's resync reports the
// cursor's own seq, and a forward page that ends inside a large transaction
// reports that transaction's seq. The client must take those pages and move
// on, not ask for the same page again.

import type { Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { subscription, type SyncClient } from '../../src/client.js';
import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { PairSchema, WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import type { SubscribeResponse } from '../../src/gen/syncplane/v1/service_pb.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

/** Rows per large transaction: several pull pages each, at PULL_LIMIT. */
const BATCH = 60;
const PULL_LIMIT = 25;

const todoKey = (i: number) => `todo-${String(i).padStart(3, '0')}`;
const todosIn = (ids: string[]) => subscription(TodoSchema, (t) => t.id.in(ids));
const allWidgets = subscription(WidgetSchema);

/**
 * A transport that counts the Pull requests it sends and records, for each
 * stream page, whether it carried a cursor.
 */
function observedTransport() {
	const seen = { pulls: 0, pages: [] as { hasCursor: boolean }[] };
	const observe: Interceptor = (next) => async (req) => {
		if (req.method.name === 'Pull') seen.pulls++;
		const res = await next(req);
		if (req.method.name !== 'Subscribe' || !res.stream) return res;
		const frames = res.message as AsyncIterable<SubscribeResponse>;
		return {
			...res,
			message: (async function*() {
				for await (const frame of frames) {
					if (frame.body.case === 'page') {
						seen.pages.push({ hasCursor: frame.body.value.nextCursor.length > 0 });
					}
					yield frame;
				}
			})(),
		};
	};
	const transport = createConnectTransport({
		baseUrl: inject('serverUrl'),
		httpVersion: '1.1',
		interceptors: [ observe ],
	});
	return { transport, seen };
}

/** Resolves with `promise`, or fails after `ms` naming what never finished. */
async function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeout = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new Error(`${what} did not finish in ${ms}ms`)), ms);
	});
	try {
		return await Promise.race([ promise, timeout ]);
	} finally {
		clearTimeout(timer);
	}
}

const todoKeys = async (client: SyncClient) =>
	(await client.scan(TodoSchema)).map((r) => r.key).sort();

/**
 * A writer, and a streaming reader whose last stream frame came without a
 * cursor, so its stored cursor sits behind the rows it applied. A large
 * transaction of an entity the reader does not sync lies between the two.
 */
async function laggingReader() {
	const ws = freshWorkspace();
	const writer = newClient(ws);
	await writer.start();
	await writer.transact(async (tx) => {
		for (let i = 0; i < BATCH; i++) {
			await tx.mutate.put(TodoSchema, { id: todoKey(i), title: `todo ${i}` });
		}
	});
	await writer.mutate.put(WidgetSchema, { id: 'w', count: 0 });
	await writer.syncNow();

	const open = [ todoKey(0), todoKey(1) ];
	const { transport, seen } = observedTransport();
	const reader = newClient(ws, {
		transport,
		subscriptions: [ todosIn(open), allWidgets ],
		streamEnabled: true,
		pollIntervalMs: 0,
		pullLimit: PULL_LIMIT,
	});
	await reader.start();
	await eventually(async () => (await todoKeys(reader)).join() === open.join());
	await eventually(async () => (await reader.get(WidgetSchema, 'w')) !== undefined);

	// Each round: a large transaction the reader's stream has nothing to send
	// for, then one small write the stream sends in a frame of its own. The
	// rounds stop at the first such frame without a cursor.
	let count = 0;
	for (let round = 0;; round++) {
		if (round === 20) throw new Error('the server sent a cursor on every frame');
		await writer.transact(async (tx) => {
			for (let i = 0; i < BATCH; i++) {
				await tx.mutate.put(PairSchema, { ns: `round-${round}`, id: String(i), label: 'bulk' });
			}
		});
		const frames = seen.pages.length;
		await writer.mutate.patch(WidgetSchema, 'w', { count: ++count });
		await writer.syncNow();
		await eventually(async () => (await reader.get(WidgetSchema, 'w'))?.count === count);
		await eventually(async () => seen.pages.length > frames);
		if (!seen.pages.at(-1)!.hasCursor) break;
	}
	return { writer, reader, seen, count };
}

describe('pull from a cursor behind the applied rows', () => {
	it('drains a forward pull through a large transaction', async () => {
		const { writer, reader, seen, count } = await laggingReader();
		const before = seen.pulls;

		await within(reader.syncNow(), 10_000, 'syncNow');

		// A few pages for the transaction, not one page asked for again and again.
		expect(seen.pulls - before).toBeLessThan(20);
		expect((await reader.get(WidgetSchema, 'w'))?.count).toBe(count);
		await writer.close();
		await reader.close();
	});

	it('finishes each scope change while the subscription set keeps moving', async () => {
		const { writer, reader, seen, count } = await laggingReader();
		const before = seen.pulls;

		// Views open and close: each set adds some todos and drops others.
		const sets = [
			[ todoKey(5), todoKey(6), todoKey(59) ],
			[ todoKey(6), todoKey(30) ],
			[ todoKey(0), todoKey(1), todoKey(2) ],
		];
		for (const ids of sets) {
			await within(reader.setSubscriptions([ todosIn(ids), allWidgets ]), 10_000, 'setSubscriptions');
			expect(await todoKeys(reader)).toEqual([ ...ids ].sort());
		}

		expect(seen.pulls - before).toBeLessThan(60);
		expect((await reader.get(WidgetSchema, 'w'))?.count).toBe(count);
		await writer.close();
		await reader.close();
	});
});
