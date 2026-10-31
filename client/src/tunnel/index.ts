/**
 * The client half of the tunnel, importable as `syncplane/tunnel`: Connect
 * calls carried over a message channel instead of the network, to a server
 * running the Go `tunnel.Serve`. It serves a webview inside a desktop shell, a
 * page with a sidecar process, or anywhere else the sync server is reached by
 * passing messages rather than by HTTP.
 *
 * {@link createTunnelTransport} returns a transport for `SyncClient`;
 * {@link tunnelFetch} is the `fetch` beneath it, for building a transport of
 * your own. Both take a function that opens a {@link FrameChannel}; the
 * adapters here cover WebSockets, MessagePorts and byte streams.
 *
 * @example A Wails app, whose `Stream` objects mirror the WebSocket API.
 * ```ts
 * import { Stream } from '@wailsio/runtime';
 * import { SyncClient } from 'syncplane';
 * import { createTunnelTransport, fromWebSocket } from 'syncplane/tunnel';
 *
 * const client = new SyncClient({
 * 	transport: createTunnelTransport(() => fromWebSocket(Stream('syncplane'))),
 * 	workspaceId,
 * 	entities: [ TodoSchema ],
 * });
 * ```
 *
 * @packageDocumentation
 */

import type { Transport } from '@connectrpc/connect';
import { type ConnectTransportOptions, createConnectTransport } from '@connectrpc/connect-web';

import type { FrameChannel } from './channel.js';
import { tunnelFetch, type TunnelFetchOptions } from './fetch.js';

export {
	type ByteStream,
	fromByteStream,
	fromMessagePort,
	fromWebSocket,
	MAX_FRAME_SIZE,
	type MessagePortLike,
	type WebSocketLike,
} from './adapters.js';
export type { FrameChannel, FrameReceiver } from './channel.js';
export { tunnelFetch, type TunnelFetchOptions } from './fetch.js';

/** Options for {@link createTunnelTransport}. */
export interface TunnelTransportOptions
	extends Omit<ConnectTransportOptions, 'baseUrl' | 'fetch' | 'useBinaryFormat'>, TunnelFetchOptions
{
	/**
	 * The URL calls are made against. Only its path reaches the server, as a
	 * prefix of every call's path; set it when the server's handler mounts the
	 * services below a prefix.
	 *
	 * @defaultValue `'http://tunnel'`, no prefix
	 */
	baseUrl?: string;
	/**
	 * Encoding of the messages: binary protobuf, or `'json'` to make frames
	 * readable while debugging. The server accepts either.
	 *
	 * @defaultValue `'binary'`
	 */
	wireFormat?: 'binary' | 'json';
}

/**
 * Returns a Connect transport that makes every call through a tunnel, for
 * `SyncClient`'s `transport` option. `open` makes a new channel: once per call,
 * or once per shared channel with `multiplex`.
 *
 * Calls fail with code `Unavailable` when the channel ends under them, which
 * `SyncClient` treats as being offline and retries. See {@link tunnelFetch} for
 * the rest of the behaviour.
 */
export function createTunnelTransport(
	open: () => FrameChannel,
	options: TunnelTransportOptions = {},
): Transport {
	const { multiplex, baseUrl = 'http://tunnel', wireFormat = 'binary', ...rest } = options;
	return createConnectTransport({
		...rest,
		baseUrl,
		useBinaryFormat: wireFormat !== 'json',
		fetch: tunnelFetch(open, { multiplex }),
	});
}
