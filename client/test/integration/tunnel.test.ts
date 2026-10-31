import { connect, type Socket } from 'node:net';
import { Duplex } from 'node:stream';

import { Code, ConnectError, createClient } from '@connectrpc/connect';
import { describe, expect, inject, it } from 'vitest';

import { TodoSchema } from '../../src/gen/example/todo/v1/todo_pb.js';
import { SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import { type ByteStream, createTunnelTransport, fromByteStream } from '../../src/tunnel/index.js';
import { eventually, freshWorkspace, newClient } from './helpers.js';

/**
 * A transport through the testserver's tunnel listener: every channel is a
 * TCP connection carrying length-prefixed frames, the same handlers the HTTP
 * listener serves on the other side.
 */
function tunnelTransport(options: { multiplex?: boolean; sockets?: Socket[] } = {}) {
	const [ host, port ] = inject('tunnelAddr').split(':');
	return createTunnelTransport(() => {
		const socket = connect(Number(port), host);
		options.sockets?.push(socket);
		return fromByteStream(Duplex.toWeb(socket) as unknown as ByteStream);
	}, { multiplex: options.multiplex });
}

describe('sync through a tunnel', () => {
	for (const multiplex of [ true, false ]) {
		it(`converges with a client on HTTP (${multiplex ? 'one shared channel' : 'a channel per call'})`, async () => {
			const ws = freshWorkspace();
			const tunneled = newClient(ws, {
				transport: tunnelTransport({ multiplex }),
				streamEnabled: true,
			});
			const direct = newClient(ws, { streamEnabled: true });
			await tunneled.start();
			await direct.start();

			await tunneled.mutate.put(TodoSchema, { id: 't1', title: 'through the tunnel' });
			await eventually(async () =>
				(await direct.get(TodoSchema, 't1'))?.title === 'through the tunnel'
			);

			// The live stream through the tunnel is what brings this back.
			await direct.mutate.patch(TodoSchema, 't1', { completed: true });
			await eventually(async () => (await tunneled.get(TodoSchema, 't1'))?.completed === true);

			await tunneled.close();
			await direct.close();
		});
	}

	it('passes the server error code through unchanged', async () => {
		const rpc = createClient(SyncService, tunnelTransport());
		const err = await rpc.pull({ workspaceId: '' }).catch((e: unknown) => e);
		expect(err).toBeInstanceOf(ConnectError);
		expect((err as ConnectError).code).toBe(Code.InvalidArgument);
	});

	it('recovers when the connection under a live stream drops', async () => {
		const ws = freshWorkspace();
		const sockets: Socket[] = [];
		const tunneled = newClient(ws, {
			transport: tunnelTransport({ multiplex: true, sockets }),
			streamEnabled: true,
		});
		const direct = newClient(ws);
		await tunneled.start();
		await direct.start();

		await direct.mutate.put(TodoSchema, { id: 'before', title: 'before' });
		await direct.syncNow();
		await eventually(async () => (await tunneled.get(TodoSchema, 'before')) !== undefined);

		for (const socket of sockets) socket.destroy();

		await direct.mutate.put(TodoSchema, { id: 'after', title: 'after' });
		await direct.syncNow();
		await eventually(async () => (await tunneled.get(TodoSchema, 'after')) !== undefined, 15_000);
		// The shared channel was opened again, not the old one reused.
		expect(sockets.length).toBeGreaterThan(1);

		await tunneled.close();
		await direct.close();
	});
});
