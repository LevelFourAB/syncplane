import type { DescMessage } from '@bufbuild/protobuf';
import { Code, ConnectError, type StreamResponse, type Transport } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-node';
import { describe, expect, inject, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

/** Rows per bootstrap chunk on the spawned testserver (see global-setup). */
const CHUNK = 8;

interface Truncating {
	transport: Transport;
	/** Rows the server delivered, per Bootstrap attempt. */
	delivered: number[];
}

/**
 * A transport that hangs up on the FIRST Bootstrap after one chunk, the way a
 * connection drops mid-snapshot, and lets every later attempt run to End. It
 * also counts the rows each attempt delivered, which is how the specs tell a
 * resumed snapshot from one downloaded again.
 */
function truncatingTransport(): Truncating {
	const base = createConnectTransport({ baseUrl: inject('serverUrl'), httpVersion: '1.1' });
	const delivered: number[] = [];
	async function stream(
		...args: Parameters<Transport['stream']>
	): Promise<StreamResponse<DescMessage, DescMessage>> {
		const res = await base.stream(...args);
		if (args[0].name !== 'Bootstrap') return res;
		const attempt = delivered.push(0);
		const inner = res.message;
		async function* counted(): AsyncIterable<never> {
			let chunks = 0;
			for await (const msg of inner) {
				const body = (msg as unknown as { body: { case?: string; value?: { rows: unknown[] } } })
					.body;
				if (body.case === 'chunk') {
					delivered[attempt - 1]! += body.value?.rows.length ?? 0;
					chunks++;
				}
				yield msg as never;
				if (attempt === 1 && chunks === 1) {
					throw new ConnectError('connection lost', Code.Unavailable);
				}
			}
		}
		return { ...res, message: counted() };
	}
	const transport: Transport = {
		unary: base.unary.bind(base),
		stream: stream as Transport['stream'],
	};
	return { transport, delivered };
}

describe('bootstrap resume (against the real server)', () => {
	it('downloads only what the interrupted attempt missed', async () => {
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		const ids = Array.from({ length: CHUNK * 3 }, (_, i) => `t${String(i).padStart(2, '0')}`);
		for (const id of ids) await writer.mutate.put(TodoSchema, { id, title: id });
		await writer.syncNow();

		const { transport, delivered } = truncatingTransport();
		const reader = newClient(ws, { transport });
		await reader.start();

		// The cycle start() schedules is the interrupted attempt: one chunk, then
		// the connection drops.
		await eventually(async () => delivered.length === 1);

		// The next one resumes: it downloads the rest of the same snapshot rather
		// than the whole thing again.
		await reader.syncNow();
		expect(delivered).toEqual([ CHUNK, ids.length - CHUNK ]);

		const rows = await reader.scan(TodoSchema);
		expect(rows.map((r) => r.key).sort()).toEqual(ids);

		await writer.close();
		await reader.close();
	});

	it('resumes across a filter-epoch bump in another workspace', async () => {
		// The resume carries the epoch its snapshot was taken under, and the
		// server drops one whose epoch has moved. That check is per workspace, so
		// a visibility change somewhere else must not cost this snapshot the rows
		// it already downloaded.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		const ids = Array.from({ length: CHUNK * 3 }, (_, i) => `t${String(i).padStart(2, '0')}`);
		for (const id of ids) await writer.mutate.put(TodoSchema, { id, title: id });
		await writer.syncNow();

		const { transport, delivered } = truncatingTransport();
		const reader = newClient(ws, { transport });
		await reader.start();
		await eventually(async () => delivered[0] === CHUNK);

		const elsewhere = newClient(freshWorkspace());
		await elsewhere.start();
		await elsewhere.mutate.custom('bump_epoch');
		await elsewhere.syncNow();

		await reader.syncNow();
		expect(delivered).toEqual([ CHUNK, ids.length - CHUNK ]);

		await elsewhere.close();
		await writer.close();
		await reader.close();
	});

	it('re-delivers rows that changed between the attempts', async () => {
		// The resumed snapshot keeps its first attempt's cursor, so the pull that
		// follows still covers the whole span — including an edit to a row the
		// interrupted attempt had already delivered, which no later chunk carries.
		const ws = freshWorkspace();
		const writer = newClient(ws);
		await writer.start();
		const ids = Array.from({ length: CHUNK * 2 }, (_, i) => `t${String(i).padStart(2, '0')}`);
		for (const id of ids) await writer.mutate.put(TodoSchema, { id, title: 'before' });
		await writer.syncNow();

		const { transport, delivered } = truncatingTransport();
		const reader = newClient(ws, { transport });
		await reader.start();
		await eventually(async () => delivered.length === 1);

		// Lands between the attempts, on a row the first attempt already shipped.
		await writer.mutate.patch(TodoSchema, ids[0]!, { title: 'after' });
		await writer.syncNow();

		await reader.syncNow();
		expect((await reader.get(TodoSchema, ids[0]!))?.title).toBe('after');
		expect((await reader.scan(TodoSchema)).map((r) => r.key).sort()).toEqual(ids);

		await writer.close();
		await reader.close();
	});
});
