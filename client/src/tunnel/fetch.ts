import { create, fromBinary, type MessageInitShape, toBinary } from '@bufbuild/protobuf';
import { Code, ConnectError } from '@connectrpc/connect';

import {
	type Frame,
	FrameSchema,
	type HeaderSchema,
	type ResponseHead,
} from '../gen/syncplane/tunnel/v1/tunnel_pb.js';
import type { FrameChannel } from './channel.js';

/** Options for {@link tunnelFetch}. */
export interface TunnelFetchOptions {
	/**
	 * Carries every call over one channel instead of one channel per call. The
	 * shared channel opens on the first call and, after it ends, again on the
	 * next call. Use it for channels that cost much to open or that a host
	 * hands over only once, such as a MessagePort. On a shared channel, a call
	 * whose reader stops reading stalls the others.
	 *
	 * @defaultValue false
	 */
	multiplex?: boolean;
}

/**
 * The bytes of request or response body one frame carries at most. The
 * protocol asks senders to stay at or below 64 KiB.
 */
const CHUNK_SIZE = 32 << 10;

/**
 * Returns a `fetch` that sends each request through a tunnel to a server
 * running the Go `tunnel.Serve`, instead of over the network. `open` makes a
 * new channel; it is called once per call, or once per shared channel with
 * `multiplex`.
 *
 * Only the path and query of the request URL reach the server. The request
 * body is read whole before the call starts; the response body streams as
 * frames arrive. Cookies, redirects, CORS and caching do not apply: the
 * returned `Response` is what the server's handler wrote.
 *
 * The promise rejects with the signal's reason when the request's signal
 * aborts, and the server stops the handler. When the channel ends before the
 * call finishes, the promise, or a read of the body after it resolved, fails
 * with a `ConnectError` with code `Unavailable`; so does a call the server
 * fails below HTTP.
 */
export function tunnelFetch(
	open: () => FrameChannel,
	options: TunnelFetchOptions = {},
): typeof globalThis.fetch {
	let shared: Session | undefined;
	const session = (): Session => {
		if (!options.multiplex) return new Session(open(), true);
		if (!shared || shared.ended) shared = new Session(open(), false);
		return shared;
	};
	return async (input, init) => {
		const request = new Request(input, init);
		const signal = request.signal;
		signal.throwIfAborted();
		const body = new Uint8Array(await request.arrayBuffer());
		signal.throwIfAborted();
		return session().call(request, body);
	};
}

/** The wire error for a channel that ended, or a call failed below HTTP. */
function unavailable(message: string): ConnectError {
	return new ConnectError(message, Code.Unavailable);
}

/** One channel and the calls in flight on it. */
class Session {
	private readonly calls = new Map<number, Call>();
	private next = 0;
	ended = false;

	constructor(private readonly channel: FrameChannel, private readonly single: boolean) {
		channel.start({
			frame: (frame) => this.receive(frame),
			closed: (error) => this.end(error),
		});
	}

	call(request: Request, body: Uint8Array): Promise<Response> {
		if (this.ended) return Promise.reject(unavailable('tunnel channel closed'));
		do this.next = (this.next + 1) >>> 0;
		while (this.next === 0 || this.calls.has(this.next));
		const id = this.next;

		const url = new URL(request.url);
		const headers: MessageInitShape<typeof HeaderSchema>[] = [];
		request.headers.forEach((value, name) => headers.push({ name, values: [ value ] }));
		headers.push({ name: 'content-length', values: [ String(body.length) ] });

		const promise = new Promise<Response>((resolve, reject) => {
			this.calls.set(id, new Call(id, this, request.signal, resolve, reject));
		});
		this.send({
			call: id,
			kind: {
				case: 'request',
				value: { method: request.method, target: url.pathname + url.search, headers },
			},
		});
		for (let at = 0; at < body.length; at += CHUNK_SIZE) {
			this.send({ call: id, kind: { case: 'body', value: body.subarray(at, at + CHUNK_SIZE) } });
		}
		this.send({ call: id, kind: { case: 'end', value: {} } });
		return promise;
	}

	send(frame: MessageInitShape<typeof FrameSchema>): void {
		if (this.ended) return;
		this.channel.send(toBinary(FrameSchema, create(FrameSchema, frame)));
	}

	/** Takes a call out of flight; a single-call session closes with it. */
	finish(id: number): void {
		if (!this.calls.delete(id)) return;
		if (this.single) this.channel.close();
	}

	private receive(bytes: Uint8Array): void {
		if (this.ended) return;
		let frame: Frame;
		try {
			frame = fromBinary(FrameSchema, bytes);
		} catch (err) {
			this.violation(`malformed frame: ${String(err)}`);
			return;
		}
		const call = this.calls.get(frame.call);
		switch (frame.kind.case) {
			case 'response':
				if (call && !call.head(frame.kind.value)) {
					this.violation(`second response head for call ${frame.call}`);
				}
				return;
			case 'body':
				if (call && !call.body(frame.kind.value)) {
					this.violation(`body before the response head for call ${frame.call}`);
				}
				return;
			case 'end':
				if (call) {
					this.finish(call.id);
					call.end(frame.kind.value.error);
				}
				return;
			case 'request':
			case 'cancel':
				this.violation('client-only frame from the server');
				return;
			default:
				// A kind this version does not know. Ignoring it is what lets the
				// protocol add kinds without breaking older clients.
				return;
		}
	}

	private violation(message: string): void {
		this.end(new Error(`tunnel protocol violation: ${message}`));
		this.channel.close();
	}

	private end(error?: Error): void {
		if (this.ended) return;
		this.ended = true;
		const failure = unavailable(
			error ? `tunnel channel closed: ${error.message}` : 'tunnel channel closed',
		);
		const calls = [ ...this.calls.values() ];
		this.calls.clear();
		for (const call of calls) call.fail(failure);
	}
}

/** One call: settles fetch's promise, then feeds the response body. */
class Call {
	private settled = false;
	private done = false;
	private controller?: ReadableStreamDefaultController<Uint8Array>;
	private readonly onAbort = () => this.abandon(this.signal.reason);

	constructor(
		readonly id: number,
		private readonly session: Session,
		private readonly signal: AbortSignal,
		private readonly resolve: (response: Response) => void,
		private readonly reject: (reason: unknown) => void,
	) {
		signal.addEventListener('abort', this.onAbort, { once: true });
	}

	/** Starts the response. Reports false for a second head. */
	head(head: ResponseHead): boolean {
		if (this.settled) return false;
		this.settled = true;
		let response: Response;
		try {
			const headers = new Headers();
			for (const header of head.headers) {
				for (const value of header.values) headers.append(header.name, value);
			}
			// The Response constructor refuses a body for these statuses.
			const empty = head.status === 204 || head.status === 205 || head.status === 304;
			const body = empty ? null : new ReadableStream<Uint8Array>({
				start: (controller) => {
					this.controller = controller;
				},
				cancel: () => this.abandon(undefined),
			});
			response = new Response(body, { status: head.status, headers });
		} catch (err) {
			// Unsettled again, so the failure rejects fetch's promise instead of
			// going to a body nobody holds.
			this.settled = false;
			this.abandon(
				new ConnectError(`tunnel response is not valid HTTP: ${String(err)}`, Code.Internal),
			);
			return true;
		}
		this.resolve(response);
		return true;
	}

	/** Appends to the response body. Reports false before the head. */
	body(chunk: Uint8Array): boolean {
		if (!this.settled) return false;
		if (!this.done) this.controller?.enqueue(chunk);
		return true;
	}

	/** The server ended its half; a non-empty `error` fails the call. */
	end(error: string): void {
		if (error) {
			this.fail(unavailable(`tunnel call failed: ${error}`));
		} else if (!this.settled) {
			this.fail(unavailable('tunnel call ended without a response'));
		} else {
			this.close(() => this.controller?.close());
		}
	}

	/** Fails the call wherever it is, without telling the server. */
	fail(error: unknown): void {
		this.close(() => {
			if (this.settled) this.controller?.error(error);
			else this.reject(error);
		});
	}

	/**
	 * The caller gave up: the signal aborted, or the body was cancelled
	 * (`reason` undefined, and the stream is already closed to the reader).
	 */
	private abandon(reason: unknown): void {
		if (this.done) return;
		this.session.send({ call: this.id, kind: { case: 'cancel', value: {} } });
		this.session.finish(this.id);
		// The reason goes through unchanged: an AbortError is what tells
		// connect-web, and any other fetch caller, that the call was cancelled.
		if (reason === undefined) this.close(() => {});
		else this.fail(reason);
	}

	private close(settle: () => void): void {
		if (this.done) return;
		this.done = true;
		this.signal.removeEventListener('abort', this.onAbort);
		settle();
	}
}
