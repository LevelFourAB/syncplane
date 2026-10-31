# Tunnel reference

The tunnel carries HTTP exchanges over a message channel. The Go half is the package `github.com/levelfourab/syncplane/engine/tunnel`. The TypeScript half is the entry point `syncplane/tunnel`. Both speak the frame protocol `syncplane.tunnel.v1`.

For a walkthrough, see [Serve sync through a tunnel](../how-to/serve-sync-through-a-tunnel.md). For the design, see [Tunnels](../explanation/tunnels.md).

## Frame protocol

Each message on a channel is one `syncplane.tunnel.v1.Frame` in the binary protobuf encoding. `proto/contract/syncplane/tunnel/v1/tunnel.proto` is the authoritative definition.

| Kind       | Direction        | Content                                                          |
| ---------- | ---------------- | ---------------------------------------------------------------- |
| `request`  | client to server | Method, target (path and query), headers. Opens the call.        |
| `body`     | both             | The next chunk of the request or response body.                  |
| `response` | server to client | Status (200 to 599) and headers. Comes before any response body. |
| `end`      | both             | Ends the sender's half. A non-empty `error` fails the call.      |
| `cancel`   | client to server | Abandons the call.                                               |

Rules:

- `call` is non-zero and chosen by the client. It must not match a call still in flight on the same channel.
- The client sends `request`, `body`…, `end`. The server sends `response`, `body`…, `end`. The server can start before the client finishes.
- After `cancel`, the server sends nothing more for the call. Frames already in flight can still arrive.
- A side ignores frames for an unknown or finished call, and frames of a kind it does not know.
- A frame that does not decode, a `request` with call id 0 or with an id in flight, and a frame sent in the wrong direction are protocol violations. The receiving side closes the channel.
- Senders keep `body` chunks at or below 64 KiB. Both implementations send at most 32 KiB.

### Byte-stream framing

A byte stream (stdio, a pipe, a socket) carries each frame as its length, a 4-byte big-endian unsigned integer, followed by the frame. The largest frame is 4 MiB (`tunnel.MaxFrameSize`, `MAX_FRAME_SIZE`).

## Go: `engine/tunnel`

### `Conn`

```go
type Conn interface {
	Send(frame []byte) error
	Receive() ([]byte, error)
	Close() error
}
```

One message channel. The tunnel never calls `Send` concurrently with `Send`, nor `Receive` with `Receive`, but calls all three concurrently with each other. `Close` must unblock a pending `Send` and `Receive`, and may be called more than once. `Receive` returns `io.EOF` or another error after the channel closes.

A Wails `*application.StreamConn` satisfies `Conn` as it is.

### `NewStreamConn(rwc io.ReadWriteCloser) Conn`

Carries frames over a byte stream with the length prefix above. The returned `Conn` owns `rwc`. `Send` is safe for concurrent use.

| Condition                      | `Receive` returns     |
| ------------------------------ | --------------------- |
| The stream ends between frames | `io.EOF`              |
| The stream ends inside a frame | `io.ErrUnexpectedEOF` |
| A length above `MaxFrameSize`  | `ErrFrameTooLarge`    |

`Send` also returns `ErrFrameTooLarge` for a frame above `MaxFrameSize`.

### `Serve(ctx, conn, h, opts...) error`

Answers the calls on `conn` with `h` until `conn` closes or `ctx` ends. Each call runs `h` on its own goroutine.

The request that `h` gets:

| Field               | Value                                                                    |
| ------------------- | ------------------------------------------------------------------------ |
| `Context()`         | Ends when the client cancels the call, the channel closes or `ctx` ends. |
| `URL`, `RequestURI` | From the frame's target, read as a request line: `//host/x` is a path.   |
| `Host`              | From a `Host` header, removed from `Header`. Empty without one.          |
| `ContentLength`     | From `Content-Length`; `-1` without it. `Body` is `http.NoBody` for `0`. |
| `RemoteAddr`, `TLS` | Empty.                                                                   |

The `ResponseWriter` implements `http.Flusher` and `FlushError`. A flush sends the bytes written so far. Informational `1xx` heads are dropped. A handler that panics fails only its call. The panic is logged unless it is `http.ErrAbortHandler`.

`Serve` cancels every call, waits for the handlers and closes `conn` before it returns. It returns `nil` for `io.EOF` or an ended `ctx`. Otherwise it returns the `Send` or `Receive` error, or an error that wraps `ErrProtocol`.

| Option                 | Effect                                                                                               | Default                         |
| ---------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------- |
| `WithRequestBuffer(n)` | Bounds the unread request-body bytes per call. A call above it fails and its handler's context ends. | `DefaultRequestBuffer` (32 MiB) |
| `WithLogger(l)`        | Where handler panics are logged.                                                                     | `slog.Default()`                |

### `Client`

`NewClient(conn)` returns an `http.RoundTripper` that multiplexes every request over `conn` and owns it. Use it as an `http.Client`'s `Transport`, for example under a connect-go client:

```go
c := tunnel.NewClient(tunnel.NewStreamConn(sock))
defer c.Close()
sync := syncplanev1connect.NewSyncServiceClient(&http.Client{Transport: c}, "http://tunnel")
```

- The URL's scheme and host are ignored. A non-empty `req.Host` goes as a `Host` header.
- The response body streams and must be closed. Closing it early, or ending the request context, cancels the call on the server.
- Response bytes are held in memory until read.
- After the channel closes, every call in flight and every later call fails. Make a new `Client` over a new `Conn` to reconnect. A connect-go client reports this as `CodeUnavailable`.

### Errors

| Error              | Meaning                                                                       |
| ------------------ | ----------------------------------------------------------------------------- |
| `ErrProtocol`      | The peer broke the frame protocol. The channel is closed.                     |
| `ErrCallFailed`    | The peer ended the call with an error. The wrapping error carries its reason. |
| `ErrClosed`        | The `Client`'s channel closed before the call finished, or before it started. |
| `ErrFrameTooLarge` | A frame above `MaxFrameSize` on a `NewStreamConn` channel.                    |

## TypeScript: `syncplane/tunnel`

### `createTunnelTransport(open, options?)`

Returns a Connect `Transport` for `SyncClient`'s `transport` option. `open` returns a new `FrameChannel`.

| Option       | Effect                                                                                           | Default           |
| ------------ | ------------------------------------------------------------------------------------------------ | ----------------- |
| `multiplex`  | One shared channel for every call, opened again after it ends. Without it, one channel per call. | `false`           |
| `baseUrl`    | Its path is a prefix on every call's target.                                                     | `'http://tunnel'` |
| `wireFormat` | `'binary'` or `'json'`.                                                                          | `'binary'`        |

Every other connect-web `ConnectTransportOptions` field, such as `interceptors`, passes through.

### `tunnelFetch(open, options?)`

The `fetch` under `createTunnelTransport`, for building a transport of your own. It takes `multiplex`.

- Only the URL's path and query reach the server. The request body is read whole before the call starts; the response body streams.
- Cookies, redirects, CORS and caching do not apply.
- Each request carries a `content-length` header.

| Condition                                      | Result                                                                     |
| ---------------------------------------------- | -------------------------------------------------------------------------- |
| The request's signal aborts                    | Rejects, or fails the body read, with the signal's reason; sends `cancel`. |
| The reader cancels the response body           | Sends `cancel`.                                                            |
| The channel ends before the call finishes      | `ConnectError` with `Code.Unavailable`.                                    |
| The server ends the call with an error         | `ConnectError` with `Code.Unavailable`.                                    |
| The server's head is not a valid HTTP response | `ConnectError` with `Code.Internal`.                                       |

### `FrameChannel` and `FrameReceiver`

```ts
interface FrameChannel {
	start(receiver: FrameReceiver): void;
	send(frame: Uint8Array): void;
	close(): void;
}

interface FrameReceiver {
	frame(frame: Uint8Array): void;
	closed(error?: Error): void;
}
```

The tunnel calls `start` once, before `send`. `send` must not block or throw; a frame sent before the connection opens goes out when it does, and one sent after the channel ended is dropped. The channel calls `closed` once, after which no frame arrives. `error` is absent after `close()`. The tunnel does not use a frame again after `send`, so an adapter may transfer its buffer.

### Adapters

| Adapter                  | Takes                                                           | Ends when                                                            |
| ------------------------ | --------------------------------------------------------------- | -------------------------------------------------------------------- |
| `fromWebSocket(ws)`      | A `WebSocket`, or an object with the same API (`WebSocketLike`) | The socket closes or fails, or delivers a text message.              |
| `fromMessagePort(port)`  | A `MessagePort` (`MessagePortLike`)                             | The port fires `close`, or delivers a message that is not binary.    |
| `fromByteStream(stream)` | `{ readable, writable }` web streams (`ByteStream`)             | The stream ends or fails, or carries a frame above `MAX_FRAME_SIZE`. |

`fromWebSocket` sets `binaryType` to `'arraybuffer'` and holds frames until the socket opens. Closing an adapted channel closes what it wraps.
