# Serve sync through a tunnel

This guide shows you how to connect a client to the sync engine through a message channel instead of over HTTP. Use it when the client has no network path to the server, or when a host process must own the connection. Examples are a webview inside a desktop shell such as Wails or Electron, a page that talks to a sidecar process, and a WebSocket endpoint.

The engine can run in the host process, or on a remote server that the host forwards to. The client code is the same for both.

For why the tunnel carries HTTP and what that costs, see [Tunnels](../explanation/tunnels.md). For every type and option, see the [tunnel reference](../reference/tunnel.md).

## Prerequisites

Before you start, make sure that you have:

- A Go host process that can give you one end of a channel. The channel must carry whole binary messages in order: a host stream API, a WebSocket, stdio or a socket.
- A `SyncClient` that you can give a `transport`.

## Steps

1. **Make the handler that answers the tunnel.**

   To run the engine in the host process, mount every service on one mux:

   ```go
   mux := http.NewServeMux()
   for path, h := range srv.Handlers() {
   	mux.Handle(path, h)
   }
   ```

   To forward to a remote server, use a reverse proxy. Mount only the client-facing services. The admin service must not be reachable from the client. Put the credential on the proxy, so that it never enters the client:

   ```go
   remote, _ := url.Parse("https://sync.example.com")
   proxy := &httputil.ReverseProxy{
   	Rewrite: func(r *httputil.ProxyRequest) {
   		r.SetURL(remote)
   		// Set, not Add: a header from the client never reaches the remote.
   		r.Out.Header.Set("Authorization", "Bearer "+token)
   	},
   	FlushInterval: -1, // send each stream message on as it arrives
   }
   mux := http.NewServeMux()
   mux.Handle("/syncplane.v1.SyncService/", proxy)
   mux.Handle("/syncplane.v1.PresenceService/", proxy)
   ```

2. **Give the handler an identity, if the engine runs locally.**

   The engine's `Authenticator` reads the request headers. Anything that holds the client end of the channel can set them. To make the signed-in desktop user the principal whatever the client sends, set the header in the host:

   ```go
   handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
   	r.Header.Set("Authorization", "Bearer "+session.Token())
   	mux.ServeHTTP(w, r)
   })
   ```

3. **Serve each channel with `tunnel.Serve`.**

   `Serve` takes a `tunnel.Conn`: `Send`, `Receive` and `Close`. It blocks until the channel closes, and it closes the channel before it returns.

   **Wails.** A `*application.StreamConn` is a `tunnel.Conn` as it is:

   ```go
   app.HandleStream("syncplane", func(c *application.StreamConn) {
   	_ = tunnel.Serve(c.Context(), c, handler)
   })
   ```

   **A byte stream** (stdio, a pipe, a socket). Wrap it with `tunnel.NewStreamConn`, which adds a length prefix to each frame:

   ```go
   ln, _ := net.Listen("unix", socketPath)
   for {
   	conn, err := ln.Accept()
   	if err != nil {
   		return err
   	}
   	go tunnel.Serve(ctx, tunnel.NewStreamConn(conn), handler)
   }
   ```

   **A WebSocket.** Wrap the connection in a type with the three methods. With `github.com/coder/websocket`:

   ```go
   type wsConn struct {
   	ctx context.Context
   	c   *websocket.Conn
   }

   func (w wsConn) Send(b []byte) error      { return w.c.Write(w.ctx, websocket.MessageBinary, b) }
   func (w wsConn) Receive() ([]byte, error) { _, b, err := w.c.Read(w.ctx); return b, err }
   func (w wsConn) Close() error             { return w.c.CloseNow() }

   http.HandleFunc("/tunnel", func(w http.ResponseWriter, r *http.Request) {
   	c, err := websocket.Accept(w, r, nil)
   	if err != nil {
   		return
   	}
   	// The default limit, 32 KiB, is below the size of one body frame.
   	c.SetReadLimit(tunnel.MaxFrameSize)
   	_ = tunnel.Serve(r.Context(), wsConn{r.Context(), c}, handler)
   })
   ```

4. **Give the client a tunnel transport.**

   Import `createTunnelTransport` and an adapter from `syncplane/tunnel`. The function you pass opens a new channel:

   ```ts
   import { SyncClient } from 'syncplane';
   import { createTunnelTransport, fromWebSocket } from 'syncplane/tunnel';

   const client = new SyncClient({
   	transport: createTunnelTransport(() => fromWebSocket(Stream('syncplane'))),
   	workspaceId,
   	entities: [ TodoSchema ],
   });
   ```

   Pick the adapter that matches the channel:

   | Channel                                          | Adapter                                                                           | `multiplex` |
   | ------------------------------------------------ | --------------------------------------------------------------------------------- | ----------- |
   | Wails `Stream(name)`                             | `fromWebSocket(Stream(name))`                                                     | off         |
   | WebSocket                                        | `fromWebSocket(new WebSocket(url))`                                               | off         |
   | `MessagePort` (worker, iframe, Electron preload) | `fromMessagePort(port)`                                                           | on          |
   | Byte stream in Node (child process, socket)      | `fromByteStream({ readable: Readable.toWeb(out), writable: Writable.toWeb(in) })` | on          |

   Without `multiplex`, every call opens its own channel and closes it when the call ends. Keep it off when a channel is cheap to open. With `multiplex: true`, all calls share one channel, and the transport opens a new one on the next call after the old one ends. Turn it on when there is only one channel, or when a channel is expensive to open.

   When the server mounts the services below a path prefix, such as a `server.Manager` app, set `baseUrl` to that prefix:

   ```ts
   createTunnelTransport(open, { baseUrl: 'http://tunnel/app-a' });
   ```

## Verify

1. Start the client and write a row.
2. Read the row from a second client, or from the server.
3. Close the channel from the host side. The client status goes to `offline`, then back to `online` after the client opens a new channel.

## Troubleshooting

- **Calls fail with `Unavailable` and "tunnel channel closed".** The channel ended under the call. Look at the host side: a `Serve` that returned, or a stream that the host closed.
- **A WebSocket channel closes on the first large write.** The server-side read limit is below the frame size. Set it to at least `tunnel.MaxFrameSize`.
- **Calls stall while a stream is open on a shared channel.** One reader that stopped reading blocks the shared channel. Turn `multiplex` off if the host can open a channel for each call.
- **Calls through a `MessagePort` wait forever after the peer goes away.** Some platforms never fire the port's `close` event. Make the peer close its end when it shuts down.
