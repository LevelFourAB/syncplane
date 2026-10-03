// A scope change on a streaming client. The client reopens its stream with the
// cursor it holds, which was issued for the old subscription set, so the
// server must resync that cursor on the stream as a Pull would. The second
// spec makes the stream reopen before the client's own pull can run.

import type { Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { subscription } from '../../src/client.js';
import { WidgetSchema } from '../../src/gen/syncplane/testdata/v1/testdata_pb.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

const highCount = subscription(WidgetSchema, (w) => w.count.gte(10));
const lowCount = subscription(WidgetSchema, (w) => w.count.lt(10));

const keys = async (client: ReturnType<typeof newClient>): Promise<string[]> =>
	(await client.scan(WidgetSchema)).map((r) => r.key).sort();

/** A transport whose Push requests wait `ms` before they leave. A slow push
 *  holds the client's sync chain, so the pull that follows it starts late. */
function slowPushTransport(ms: number) {
	const slowPush: Interceptor = (next) => async (req) => {
		if (req.method.name === 'Push') await new Promise((r) => setTimeout(r, ms));
		return next(req);
	};
	return createConnectTransport({
		baseUrl: inject('serverUrl'),
		httpVersion: '1.1',
		interceptors: [ slowPush ],
	});
}

describe('scope change on a streaming client', () => {
	it('syncs into the new scope after setSubscriptions while the stream is open', async () => {
		// Given w-low (count 5) and w-high (count 50) on the server.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		// And a streaming reader that syncs count >= 10 only.
		const reader = newClient(ws, { subscriptions: [ highCount ], streamEnabled: true });
		await reader.start();
		await eventually(async () => (await keys(reader)).join() === 'w-high');

		// When the reader swaps to count < 10. The client reopens its stream
		// with the cursor it holds and schedules a pull, as on any scope change.
		reader.setSubscriptions([ lowCount ]);

		// Then the reader converges on w-low only.
		let seen: string[] = [];
		try {
			await eventually(async () => {
				seen = await keys(reader);
				return seen.join() === 'w-low';
			}, 5_000);
		} finally {
			await writer.close();
			await reader.close();
		}
		expect(seen).toEqual([ 'w-low' ]);
	});

	it('syncs into the new scope when the stream reopens before the pull runs', async () => {
		// Given w-low (count 5) and w-high (count 50) on the server.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		await writer.mutate.put(WidgetSchema, { id: 'w-low', count: 5 });
		await writer.mutate.put(WidgetSchema, { id: 'w-high', count: 50 });
		await writer.syncNow();

		// And a streaming reader that syncs count >= 10 only, on a network where
		// a push takes 800 ms.
		const reader = newClient(ws, {
			subscriptions: [ highCount ],
			streamEnabled: true,
			transport: slowPushTransport(800),
		});
		await reader.start();
		await eventually(async () => (await keys(reader)).join() === 'w-high');

		// When the reader makes a local write (its push holds the sync chain).
		await reader.mutate.put(WidgetSchema, { id: 'w-mine', count: 70 });
		// And swaps to count < 10 while that push is in flight. The stream
		// reopens at once; the pull waits behind the push.
		reader.setSubscriptions([ lowCount ]);

		// Then the reader still converges on w-low only.
		let seen: string[] = [];
		try {
			await eventually(async () => {
				seen = await keys(reader);
				return seen.join() === 'w-low';
			}, 6_000);
		} catch {
			// Reported by the expectation below with the rows the reader holds.
		} finally {
			await writer.close();
			await reader.close();
		}
		expect(seen, 'the reopened stream stamped the new scope onto the old cursor').toEqual([
			'w-low',
		]);
	});
});
