import type { Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('live change stream', () => {
	it('propagates changes between clients without polling', async () => {
		const ws = freshWorkspace();
		// Polling disabled on both: only the live stream can trigger the pull.
		const a = newClient(ws, { streamEnabled: true, pollIntervalMs: 0 });
		const b = newClient(ws, { streamEnabled: true, pollIntervalMs: 0 });
		await a.start();
		await b.start();

		const start = Date.now();
		await a.mutate.put(TodoSchema, { id: 't1', title: 'streamed' });

		await eventually(async () => (await b.get(TodoSchema, 't1')) !== undefined);
		expect(Date.now() - start).toBeLessThan(1_000);
		expect((await b.get(TodoSchema, 't1'))?.title).toBe('streamed');

		await a.close();
		await b.close();
	});

	it('keeps propagating after multiple rounds (stream stays healthy)', async () => {
		const ws = freshWorkspace();
		const a = newClient(ws, { streamEnabled: true, pollIntervalMs: 0 });
		const b = newClient(ws, { streamEnabled: true, pollIntervalMs: 0 });
		await a.start();
		await b.start();

		for (let round = 0; round < 3; round++) {
			await a.mutate.put(TodoSchema, { id: `t${round}`, title: `round ${round}` });
			await eventually(async () => (await b.get(TodoSchema, `t${round}`)) !== undefined);
			// And the reverse direction.
			await b.mutate.patch(TodoSchema, `t${round}`, { completed: true });
			await eventually(async () => (await a.get(TodoSchema, `t${round}`))?.completed === true);
		}

		await a.close();
		await b.close();
	});

	it('skips the pull when the streamed page shows the client is current', async () => {
		const ws = freshWorkspace();
		let pulls = 0;
		const countPulls: Interceptor = (next) => (req) => {
			if (req.method.name === 'Pull') pulls++;
			return next(req);
		};
		const a = newClient(ws, {
			streamEnabled: true,
			pollIntervalMs: 0,
			transport: createConnectTransport({
				baseUrl: inject('serverUrl'),
				httpVersion: '1.1',
				interceptors: [ countPulls ],
			}),
		});
		await a.start();

		await a.mutate.put(TodoSchema, { id: 't1', title: 'settled' });
		// Wait for the change to round-trip (its stream page may race the cycle's own
		// pull and schedule one extra), then let any in-flight cycle drain.
		await eventually(async () => pulls >= 1);
		await sleep(500);

		// The test server heartbeats every 250ms; each heartbeat carries a
		// seq the client is already at, so none of them may trigger a pull.
		const settled = pulls;
		await sleep(1_000);
		expect(pulls).toBe(settled);

		await a.close();
	});
});
