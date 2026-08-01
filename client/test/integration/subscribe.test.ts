import type { Interceptor } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('subscribe stream', () => {
	it("delivers another client's commit over the live stream without a pull", async () => {
		const ws = freshWorkspace();
		let pulls = 0;
		const countPulls: Interceptor = (next) => (req) => {
			if (req.method.name === 'Pull') pulls++;
			return next(req);
		};
		// A holds the live Subscribe stream (streamEnabled); polling is off so only
		// the stream can deliver B's change.
		const a = newClient(ws, {
			streamEnabled: true,
			pollIntervalMs: 0,
			transport: createConnectTransport({
				baseUrl: inject('serverUrl'),
				httpVersion: '1.1',
				interceptors: [ countPulls ],
			}),
		});
		const b = newClient(ws);
		await a.start();
		await b.start();

		// Let A finish its initial cycle + stream catch-up, then freeze the pull
		// count: anything after this must arrive via the stream, not a pull.
		await eventually(async () => pulls >= 1);
		await sleep(400);
		const baseline = pulls;

		await b.mutate.put(TodoSchema, { id: 't1', title: 'streamed' });

		await eventually(async () => (await a.get(TodoSchema, 't1'))?.title === 'streamed');
		// The row arrived on the stream — A issued no further Pull RPC for it.
		expect(pulls).toBe(baseline);

		await a.close();
		await b.close();
	});
});
