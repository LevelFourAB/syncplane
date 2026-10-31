import type { FrameChannel, FrameReceiver } from './channel.js';

/**
 * The part of the WebSocket API {@link fromWebSocket} uses. A browser or Node
 * `WebSocket` fits, and so does any host stream object that mirrors it.
 */
export interface WebSocketLike {
	readonly readyState: number;
	binaryType: string;
	send(data: Uint8Array): void;
	close(): void;
	addEventListener(
		type: 'open' | 'message' | 'close' | 'error',
		listener: (event: any) => void,
	): void;
}

const CONNECTING = 0;
const CLOSING = 2;

/**
 * Adapts a WebSocket, or an object shaped like one, to a {@link FrameChannel}.
 * Pass it a socket that is connecting or open; frames sent while it connects go
 * out once it opens.
 *
 * Sets `binaryType` to `'arraybuffer'`. A text message from the peer ends the
 * channel with an error, since the tunnel's frames are binary. Closing the
 * channel closes the socket.
 *
 * @example One socket per call, to a server that hands each connection to
 * `tunnel.Serve`.
 * ```ts
 * const transport = createTunnelTransport(() => fromWebSocket(new WebSocket('wss://app.example/tunnel')));
 * ```
 */
export function fromWebSocket(ws: WebSocketLike): FrameChannel {
	ws.binaryType = 'arraybuffer';
	let receiver: FrameReceiver | undefined;
	let queue: Uint8Array[] | undefined = ws.readyState === CONNECTING ? [] : undefined;
	let ended = false;
	const end = (error?: Error) => {
		if (ended) return;
		ended = true;
		queue = undefined;
		receiver?.closed(error);
	};

	ws.addEventListener('open', () => {
		const pending = queue;
		queue = undefined;
		for (const frame of pending ?? []) ws.send(frame);
	});
	ws.addEventListener('message', (event: { data: unknown }) => {
		if (ended) return;
		const frame = toBytes(event.data);
		if (frame) {
			receiver?.frame(frame);
			return;
		}
		ws.close();
		end(new Error('WebSocket delivered a message that is not binary'));
	});
	ws.addEventListener('close', (event: { code?: number; reason?: string }) => {
		end(
			new Error(
				`WebSocket closed (code ${event.code ?? 'unknown'}${event.reason ? `: ${event.reason}` : ''})`,
			),
		);
	});
	ws.addEventListener('error', () => end(new Error('WebSocket failed')));

	return {
		start(r) {
			receiver = r;
			if (ws.readyState >= CLOSING) end(new Error('WebSocket is already closed'));
		},
		send(frame) {
			if (ended) return;
			if (queue) queue.push(frame);
			else ws.send(frame);
		},
		close() {
			if (ended) return;
			ws.close();
			end();
		},
	};
}

/**
 * The part of the MessagePort API {@link fromMessagePort} uses. A DOM
 * `MessagePort` fits, as does a worker's or a window's port.
 */
export interface MessagePortLike {
	postMessage(message: Uint8Array): void;
	addEventListener(type: 'message' | 'close', listener: (event: any) => void): void;
	start?(): void;
	close(): void;
}

/**
 * Adapts a MessagePort to a {@link FrameChannel}. A port is a single channel,
 * so use it with `multiplex: true`.
 *
 * The channel ends when the port fires `close`. Platforms that never fire it
 * give no sign that the peer went away, and calls in flight then wait until
 * their signal aborts; the peer should close its end of the port when it shuts
 * down. Closing the channel closes the port.
 *
 * @example
 * ```ts
 * const transport = createTunnelTransport(() => fromMessagePort(port), { multiplex: true });
 * ```
 */
export function fromMessagePort(port: MessagePortLike): FrameChannel {
	let receiver: FrameReceiver | undefined;
	let ended = false;
	const end = (error?: Error) => {
		if (ended) return;
		ended = true;
		receiver?.closed(error);
	};
	return {
		start(r) {
			receiver = r;
			port.addEventListener('message', (event: { data: unknown }) => {
				if (ended) return;
				const frame = toBytes(event.data);
				if (frame) {
					r.frame(frame);
					return;
				}
				port.close();
				end(new Error('MessagePort delivered a message that is not binary'));
			});
			port.addEventListener('close', () => end(new Error('MessagePort closed')));
			port.start?.();
		},
		send(frame) {
			if (!ended) port.postMessage(frame);
		},
		close() {
			if (ended) return;
			port.close();
			end();
		},
	};
}

/** A byte stream in both directions, such as Node's `Duplex.toWeb(socket)`. */
export interface ByteStream {
	readable: ReadableStream<Uint8Array>;
	writable: WritableStream<Uint8Array>;
}

/**
 * The largest frame, in bytes, {@link fromByteStream} reads or writes; the
 * same as the Go `tunnel.MaxFrameSize`.
 */
export const MAX_FRAME_SIZE = 4 << 20;

/**
 * Adapts a byte stream, such as stdio or a socket, to a {@link FrameChannel}.
 * Each frame goes as its length, a 4-byte big-endian unsigned integer, followed
 * by the frame: the framing of the Go `tunnel.NewStreamConn`, which serves the
 * other end.
 *
 * The channel ends when the stream ends or fails, with an error when it ends
 * inside a frame or carries a frame above {@link MAX_FRAME_SIZE}. Closing the
 * channel cancels the readable side and closes the writable side.
 *
 * @example A Node process talking to a sidecar over TCP.
 * ```ts
 * const transport = createTunnelTransport(
 * 	() => fromByteStream(Duplex.toWeb(net.connect(port))),
 * 	{ multiplex: true },
 * );
 * ```
 */
export function fromByteStream(stream: ByteStream): FrameChannel {
	const reader = stream.readable.getReader();
	const writer = stream.writable.getWriter();
	let receiver: FrameReceiver | undefined;
	let ended = false;
	const end = (error?: Error) => {
		if (ended) return;
		ended = true;
		reader.cancel().catch(() => {});
		writer.close().catch(() => {});
		receiver?.closed(error);
	};

	const read = async (r: FrameReceiver) => {
		const prefix = new Uint8Array(4);
		let prefixFill = 0;
		let frame: Uint8Array | undefined;
		let frameFill = 0;
		for (;;) {
			let chunk: Uint8Array;
			try {
				const next = await reader.read();
				if (next.done) break;
				chunk = next.value;
			} catch (err) {
				end(err instanceof Error ? err : new Error(String(err)));
				return;
			}
			let at = 0;
			while (at < chunk.length && !ended) {
				if (!frame) {
					const take = Math.min(4 - prefixFill, chunk.length - at);
					prefix.set(chunk.subarray(at, at + take), prefixFill);
					prefixFill += take;
					at += take;
					if (prefixFill < 4) break;
					const size = new DataView(prefix.buffer).getUint32(0);
					if (size > MAX_FRAME_SIZE) {
						end(new Error(`frame of ${size} bytes exceeds MAX_FRAME_SIZE`));
						return;
					}
					frame = new Uint8Array(size);
					frameFill = 0;
				}
				const take = Math.min(frame.length - frameFill, chunk.length - at);
				frame.set(chunk.subarray(at, at + take), frameFill);
				frameFill += take;
				at += take;
				if (frameFill === frame.length) {
					const whole = frame;
					frame = undefined;
					prefixFill = 0;
					r.frame(whole);
				}
			}
			if (ended) return;
		}
		end(
			frame || prefixFill > 0
				? new Error('byte stream ended inside a frame')
				: new Error('byte stream ended'),
		);
	};

	return {
		start(r) {
			receiver = r;
			void read(r);
		},
		send(frame) {
			if (ended) return;
			if (frame.length > MAX_FRAME_SIZE) {
				end(new Error(`frame of ${frame.length} bytes exceeds MAX_FRAME_SIZE`));
				return;
			}
			const out = new Uint8Array(4 + frame.length);
			new DataView(out.buffer).setUint32(0, frame.length);
			out.set(frame, 4);
			writer.write(out).catch((err: unknown) =>
				end(err instanceof Error ? err : new Error(String(err)))
			);
		},
		close() {
			end();
		},
	};
}

function toBytes(data: unknown): Uint8Array | undefined {
	if (data instanceof ArrayBuffer) return new Uint8Array(data);
	if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
	return undefined;
}
