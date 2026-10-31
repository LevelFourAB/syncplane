import { create, fromBinary, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError, createClient } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';

import { type Frame, FrameSchema } from '../../src/gen/syncplane/tunnel/v1/tunnel_pb.js';
import { PullRequestSchema, PullResponseSchema } from '../../src/gen/syncplane/v1/service_pb.js';
import { SyncService } from '../../src/gen/syncplane/v1/service_pb.js';
import {
	createTunnelTransport,
	type FrameChannel,
	type FrameReceiver,
	fromByteStream,
	fromMessagePort,
	fromWebSocket,
	MAX_FRAME_SIZE,
	tunnelFetch,
	type WebSocketLike,
} from '../../src/tunnel/index.js';

// Compile-time: the platform objects fit the adapters' structural types.
const _ws: (ws: WebSocket) => FrameChannel = fromWebSocket;
const _port: (port: MessagePort) => FrameChannel = fromMessagePort;
void _ws;
void _port;

const tick = () => new Promise((r) => setTimeout(r, 0));

/** The server end of an in-memory channel, driven by hand. */
class FakeChannel implements FrameChannel {
	readonly sent: Frame[] = [];
	closed = false;
	private receiver?: FrameReceiver;

	start(receiver: FrameReceiver): void {
		this.receiver = receiver;
	}

	send(frame: Uint8Array): void {
		this.sent.push(fromBinary(FrameSchema, frame));
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.receiver?.closed();
	}

	deliver(frame: MessageInitShape<typeof FrameSchema>): void {
		this.receiver?.frame(toBinary(FrameSchema, create(FrameSchema, frame)));
	}

	drop(error: Error): void {
		this.closed = true;
		this.receiver?.closed(error);
	}

	respond(call: number, status: number, body: string[], headers: Record<string, string> = {}): void {
		this.deliver({
			call,
			kind: {
				case: 'response',
				value: {
					status,
					headers: Object.entries(headers).map(([ name, value ]) => ({ name, values: [ value ] })),
				},
			},
		});
		for (const chunk of body) {
			this.deliver({ call, kind: { case: 'body', value: new TextEncoder().encode(chunk) } });
		}
		this.deliver({ call, kind: { case: 'end', value: {} } });
	}

	kinds(call: number): string[] {
		return this.sent.filter((f) => f.call === call).map((f) => f.kind.case ?? 'none');
	}
}

function opener() {
	const channels: FakeChannel[] = [];
	return {
		channels,
		open: () => {
			const c = new FakeChannel();
			channels.push(c);
			return c;
		},
	};
}

describe('tunnelFetch', () => {
	it('sends a request as a head, body chunks under 64 KiB and an end', async () => {
		const { channels, open } = opener();
		const fetch = tunnelFetch(open);
		const body = new Uint8Array(100 << 10).fill(1);
		void fetch('http://ignored/syncplane.v1.SyncService/Push?x=1', {
			method: 'POST',
			headers: { 'content-type': 'application/proto', authorization: 'Bearer t' },
			body,
		});
		await tick();

		const c = channels[0]!;
		const [ head, ...rest ] = c.sent;
		expect(head?.kind.case).toBe('request');
		if (head?.kind.case !== 'request') return;
		expect(head.call).not.toBe(0);
		expect(head.kind.value.method).toBe('POST');
		expect(head.kind.value.target).toBe('/syncplane.v1.SyncService/Push?x=1');
		const headers = Object.fromEntries(head.kind.value.headers.map((h) => [ h.name, h.values ]));
		expect(headers).toMatchObject({
			'content-type': [ 'application/proto' ],
			authorization: [ 'Bearer t' ],
			'content-length': [ String(body.length) ],
		});

		const chunks = rest.filter((f) => f.kind.case === 'body').map((f) => f.kind.value as Uint8Array);
		expect(chunks.every((chunk) => chunk.length <= 64 << 10)).toBe(true);
		expect(chunks.reduce((n, chunk) => n + chunk.length, 0)).toBe(body.length);
		expect(rest.at(-1)?.kind.case).toBe('end');
	});

	it('resolves with the response and streams its body', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open)('http://ignored/x', { method: 'POST', body: 'hi' });
		await tick();
		const c = channels[0]!;
		const call = c.sent[0]!.call;
		c.respond(call, 201, [ 'a', 'b' ], { 'x-test': 'yes' });

		const response = await pending;
		expect(response.status).toBe(201);
		expect(response.headers.get('x-test')).toBe('yes');
		expect(await response.text()).toBe('ab');
	});

	it('opens a channel per call and closes it when the call ends', async () => {
		const { channels, open } = opener();
		const fetch = tunnelFetch(open);
		const one = fetch('http://ignored/1', { method: 'POST' });
		const two = fetch('http://ignored/2', { method: 'POST' });
		await tick();
		expect(channels).toHaveLength(2);
		for (const c of channels) c.respond(c.sent[0]!.call, 200, [ 'ok' ]);
		await Promise.all([ one, two ]);
		expect(channels.every((c) => c.closed)).toBe(true);
	});

	it('multiplexes calls over one channel and routes each response to its call', async () => {
		const { channels, open } = opener();
		const fetch = tunnelFetch(open, { multiplex: true });
		const one = fetch('http://ignored/1', { method: 'POST' });
		const two = fetch('http://ignored/2', { method: 'POST' });
		await tick();
		expect(channels).toHaveLength(1);
		const c = channels[0]!;
		const [ id1, id2 ] = c.sent.filter((f) => f.kind.case === 'request').map((f) => f.call);
		expect(id1).not.toBe(id2);

		c.respond(id2!, 200, [ 'second' ]);
		c.respond(id1!, 200, [ 'first' ]);
		expect(await (await one).text()).toBe('first');
		expect(await (await two).text()).toBe('second');
		expect(c.closed).toBe(false);
	});

	it('opens a new shared channel after the old one ends', async () => {
		const { channels, open } = opener();
		const fetch = tunnelFetch(open, { multiplex: true });
		const first = fetch('http://ignored/', { method: 'POST' });
		await tick();
		channels[0]!.drop(new Error('gone'));
		await expect(first).rejects.toMatchObject({ code: Code.Unavailable });

		const second = fetch('http://ignored/', { method: 'POST' });
		await tick();
		expect(channels).toHaveLength(2);
		channels[1]!.respond(channels[1]!.sent[0]!.call, 200, [ 'again' ]);
		expect(await (await second).text()).toBe('again');
	});

	it('rejects with the abort reason and cancels the call when the signal aborts first', async () => {
		const { channels, open } = opener();
		const abort = new AbortController();
		const pending = tunnelFetch(open, { multiplex: true })('http://ignored/', {
			method: 'POST',
			signal: abort.signal,
		});
		await tick();
		abort.abort();
		await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
		const call = channels[0]!.sent[0]!.call;
		expect(channels[0]!.kinds(call)).toEqual([ 'request', 'end', 'cancel' ]);
	});

	it('rejects at once, sending nothing, for a signal that has already aborted', async () => {
		const { channels, open } = opener();
		await expect(tunnelFetch(open)('http://ignored/', { signal: AbortSignal.abort() }))
			.rejects.toMatchObject({ name: 'AbortError' });
		expect(channels).toHaveLength(0);
	});

	it('fails the body read and cancels the call when the signal aborts mid-body', async () => {
		const { channels, open } = opener();
		const abort = new AbortController();
		const pending = tunnelFetch(open, { multiplex: true })('http://ignored/', {
			method: 'POST',
			signal: abort.signal,
		});
		await tick();
		const c = channels[0]!;
		const call = c.sent[0]!.call;
		c.deliver({ call, kind: { case: 'response', value: { status: 200 } } });
		const response = await pending;
		abort.abort();
		await expect(response.text()).rejects.toMatchObject({ name: 'AbortError' });
		expect(c.kinds(call).at(-1)).toBe('cancel');
	});

	it('cancels the call when the reader cancels the body', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open, { multiplex: true })('http://ignored/', { method: 'POST' });
		await tick();
		const c = channels[0]!;
		const call = c.sent[0]!.call;
		c.deliver({ call, kind: { case: 'response', value: { status: 200 } } });
		await (await pending).body!.cancel();
		expect(c.kinds(call).at(-1)).toBe('cancel');
	});

	it('fails with Unavailable when the channel ends before the response or inside the body', async () => {
		const { channels, open } = opener();
		const fetch = tunnelFetch(open);
		const before = fetch('http://ignored/', { method: 'POST' });
		const inside = fetch('http://ignored/', { method: 'POST' });
		await tick();
		channels[0]!.drop(new Error('pipe broke'));
		const err = await before.catch((e: unknown) => e);
		expect(err).toBeInstanceOf(ConnectError);
		expect(err).toMatchObject({ code: Code.Unavailable });
		expect((err as Error).message).toContain('pipe broke');

		const c = channels[1]!;
		c.deliver({ call: c.sent[0]!.call, kind: { case: 'response', value: { status: 200 } } });
		const response = await inside;
		c.drop(new Error('pipe broke'));
		await expect(response.text()).rejects.toMatchObject({ code: Code.Unavailable });
	});

	it('fails with Unavailable for a call the server ends with an error', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open)('http://ignored/', { method: 'POST' });
		await tick();
		const c = channels[0]!;
		c.deliver({ call: c.sent[0]!.call, kind: { case: 'end', value: { error: 'handler aborted' } } });
		const err = await pending.catch((e: unknown) => e);
		expect(err).toMatchObject({ code: Code.Unavailable });
		expect((err as Error).message).toContain('handler aborted');
	});

	it('closes the channel and fails its calls on a frame out of order', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open, { multiplex: true })('http://ignored/', { method: 'POST' });
		await tick();
		const c = channels[0]!;
		c.deliver({ call: c.sent[0]!.call, kind: { case: 'body', value: new Uint8Array([ 1 ]) } });
		await expect(pending).rejects.toMatchObject({ code: Code.Unavailable });
		expect(c.closed).toBe(true);
	});

	it('ignores frames of a kind it does not know and frames for calls it does not know', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open, { multiplex: true })('http://ignored/', { method: 'POST' });
		await tick();
		const c = channels[0]!;
		c.deliver({ call: 77 });
		c.deliver({ call: 99, kind: { case: 'end', value: {} } });
		c.respond(c.sent[0]!.call, 200, [ 'fine' ]);
		expect(await (await pending).text()).toBe('fine');
		expect(c.closed).toBe(false);
	});

	it('gives a 204 response no body', async () => {
		const { channels, open } = opener();
		const pending = tunnelFetch(open)('http://ignored/', { method: 'POST' });
		await tick();
		channels[0]!.respond(channels[0]!.sent[0]!.call, 204, []);
		const response = await pending;
		expect(response.status).toBe(204);
		expect(response.body).toBeNull();
	});
});

describe('createTunnelTransport', () => {
	it('makes Connect calls in binary through the channel', async () => {
		const { channels, open } = opener();
		const rpc = createClient(SyncService, createTunnelTransport(open));
		const pending = rpc.pull({ workspaceId: 'ws' });
		await tick();
		const c = channels[0]!;
		const head = c.sent[0]!;
		if (head.kind.case !== 'request') throw new Error('expected a request head');
		expect(head.kind.value.target).toBe('/syncplane.v1.SyncService/Pull');
		const body = c.sent.filter((f) => f.kind.case === 'body').map((f) => f.kind.value as Uint8Array);
		expect(fromBinary(PullRequestSchema, body[0]!).workspaceId).toBe('ws');

		c.deliver({
			call: head.call,
			kind: {
				case: 'response',
				value: { status: 200, headers: [ { name: 'content-type', values: [ 'application/proto' ] } ] },
			},
		});
		c.deliver({
			call: head.call,
			kind: {
				case: 'body',
				value: toBinary(PullResponseSchema, create(PullResponseSchema, { hasMore: true })),
			},
		});
		c.deliver({ call: head.call, kind: { case: 'end', value: {} } });
		expect((await pending).hasMore).toBe(true);
	});

	it('maps a closed channel to Unavailable', async () => {
		const { channels, open } = opener();
		const pending = createClient(SyncService, createTunnelTransport(open)).pull({
			workspaceId: 'ws',
		});
		await tick();
		channels[0]!.drop(new Error('gone'));
		await expect(pending).rejects.toMatchObject({ code: Code.Unavailable });
	});
});

/** Collects what an adapter delivers. */
function collect(channel: FrameChannel) {
	const frames: Uint8Array[] = [];
	const ends: (Error | undefined)[] = [];
	channel.start({ frame: (f) => frames.push(f), closed: (e) => ends.push(e) });
	return { frames, ends };
}

describe('fromByteStream', () => {
	function pipes() {
		const toAdapter = new TransformStream<Uint8Array, Uint8Array>();
		const fromAdapter = new TransformStream<Uint8Array, Uint8Array>();
		const channel = fromByteStream({ readable: toAdapter.readable, writable: fromAdapter.writable });
		return {
			channel,
			peerWriter: toAdapter.writable.getWriter(),
			peerReader: fromAdapter.readable.getReader(),
		};
	}
	const framed = (...frames: number[][]) => {
		const out: number[] = [];
		for (const f of frames) out.push(0, 0, (f.length >> 8) & 0xff, f.length & 0xff, ...f);
		return new Uint8Array(out);
	};

	it('writes each frame with a 4-byte big-endian length', async () => {
		const { channel, peerReader } = pipes();
		collect(channel);
		channel.send(new Uint8Array([ 1, 2, 3 ]));
		const { value } = await peerReader.read();
		expect([ ...value! ]).toEqual([ 0, 0, 0, 3, 1, 2, 3 ]);
	});

	it('reads frames split across chunks and several frames in one chunk', async () => {
		const { channel, peerWriter } = pipes();
		const { frames } = collect(channel);
		const bytes = framed([ 1, 2, 3 ], [], [ 4 ]);
		for (const b of bytes.subarray(0, 6)) await peerWriter.write(new Uint8Array([ b ]));
		await peerWriter.write(bytes.subarray(6));
		await tick();
		expect(frames.map((f) => [ ...f ])).toEqual([ [ 1, 2, 3 ], [], [ 4 ] ]);
	});

	it('ends with an error when the stream ends inside a frame', async () => {
		const { channel, peerWriter } = pipes();
		const { ends } = collect(channel);
		await peerWriter.write(new Uint8Array([ 0, 0, 0, 5, 1 ]));
		await peerWriter.close();
		await tick();
		expect(ends).toHaveLength(1);
		expect(ends[0]?.message).toContain('inside a frame');
	});

	it('ends with an error on a frame above MAX_FRAME_SIZE', async () => {
		const { channel, peerWriter } = pipes();
		const { ends } = collect(channel);
		const size = MAX_FRAME_SIZE + 1;
		void peerWriter.write(
			new Uint8Array([ size >>> 24, (size >>> 16) & 0xff, (size >>> 8) & 0xff, size & 0xff ]),
		);
		await tick();
		expect(ends[0]?.message).toContain('MAX_FRAME_SIZE');
	});

	it('reports no error after close()', async () => {
		const { channel } = pipes();
		const { ends } = collect(channel);
		channel.close();
		channel.close();
		expect(ends).toEqual([ undefined ]);
	});
});

/** A WebSocket stand-in the spec drives by hand. */
class FakeSocket extends EventTarget implements WebSocketLike {
	readyState = 0;
	binaryType = 'blob';
	readonly sent: Uint8Array[] = [];
	closeCalls = 0;

	send(data: Uint8Array): void {
		if (this.readyState !== 1) throw new Error('InvalidStateError');
		this.sent.push(data);
	}

	close(): void {
		this.closeCalls++;
	}

	emit(type: string, init: Record<string, unknown> = {}): void {
		if (type === 'open') this.readyState = 1;
		if (type === 'close') this.readyState = 3;
		this.dispatchEvent(Object.assign(new Event(type), init));
	}
}

describe('fromWebSocket', () => {
	it('holds frames until the socket opens and receives binary messages', () => {
		const ws = new FakeSocket();
		const channel = fromWebSocket(ws);
		const { frames } = collect(channel);
		expect(ws.binaryType).toBe('arraybuffer');

		channel.send(new Uint8Array([ 1 ]));
		expect(ws.sent).toHaveLength(0);
		ws.emit('open');
		channel.send(new Uint8Array([ 2 ]));
		expect(ws.sent.map((f) => [ ...f ])).toEqual([ [ 1 ], [ 2 ] ]);

		ws.emit('message', { data: new Uint8Array([ 9 ]).buffer });
		expect(frames.map((f) => [ ...f ])).toEqual([ [ 9 ] ]);
	});

	it('ends with an error on a text message and closes the socket', () => {
		const ws = new FakeSocket();
		const { ends } = collect(fromWebSocket(ws));
		ws.emit('open');
		ws.emit('message', { data: 'text' });
		expect(ends[0]?.message).toContain('not binary');
		expect(ws.closeCalls).toBe(1);
	});

	it('ends with an error when the socket closes under it, and without one after close()', () => {
		const dropped = new FakeSocket();
		const a = collect(fromWebSocket(dropped));
		dropped.emit('close', { code: 1006 });
		expect(a.ends[0]?.message).toContain('1006');

		const closed = new FakeSocket();
		const channel = fromWebSocket(closed);
		const b = collect(channel);
		channel.close();
		closed.emit('close', { code: 1000 });
		expect(b.ends).toEqual([ undefined ]);
	});
});

describe('fromMessagePort', () => {
	it('carries frames both ways over a MessageChannel', async () => {
		const { port1, port2 } = new MessageChannel();
		const a = fromMessagePort(port1);
		const b = fromMessagePort(port2);
		const atA = collect(a);
		const atB = collect(b);
		a.send(new Uint8Array([ 1, 2 ]));
		b.send(new Uint8Array([ 3 ]));
		await new Promise((r) => setTimeout(r, 20));
		expect(atB.frames.map((f) => [ ...f ])).toEqual([ [ 1, 2 ] ]);
		expect(atA.frames.map((f) => [ ...f ])).toEqual([ [ 3 ] ]);
		a.close();
		b.close();
	});
});
