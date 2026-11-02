# Embed sync in a Go desktop app

This guide shows you how to run a sync engine inside a Go desktop app and keep it converged with a remote server. The Go side of the app holds the data in a local engine. The webview runs the TypeScript client against that engine through a tunnel. The app works offline, and its data survives a restart.

The examples use Wails. Other shells work the same way: only the step that serves the tunnel changes.

For the model behind the link, see [Chained sync](../explanation/chained-sync.md). For every option, see the [upstream reference](../reference/upstream.md).

## Prerequisites

Before you start, make sure that you have:

- A remote syncplane server (the **origin**) and a credential for it, for example a bearer token of the signed-in user.
- The generated Go messages of the entities to sync, for example `&todov1.Todo{}`.
- A desktop shell that gives the Go side a message channel to the webview. In Wails, this is a stream (`app.HandleStream`).

## Steps

1. **Pick a directory for the local data.**

   ```go
   base, err := os.UserConfigDir()
   if err != nil {
   	return err
   }
   dataDir := filepath.Join(base, "MyApp")
   if err := os.MkdirAll(dataDir, 0o700); err != nil {
   	return err
   }
   ```

2. **Give the link the user's credential.**

   The link sends every call to the origin through `Config.HTTPClient`. A transport that sets the `Authorization` header covers unary calls and streams:

   ```go
   type bearer struct {
   	token func() string
   	base  http.RoundTripper
   }

   func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
   	r = r.Clone(r.Context())
   	r.Header.Set("Authorization", "Bearer "+b.token())
   	return b.base.RoundTrip(r)
   }
   ```

   Do not set `http.Client.Timeout`. It also ends the live stream from the origin.

3. **Build the link.**

   ```go
   link, err := upstream.New(upstream.Config{
   	OriginURL:  "https://sync.example.com",
   	HTTPClient: &http.Client{Transport: bearer{token: session.Token, base: http.DefaultTransport}},
   	Bindings:   []upstream.Binding{{Prototype: &todov1.Todo{}}},
   	OnRejected: func(r upstream.Rejection) {
   		slog.Warn("the server refused a change",
   			"workspace", r.Workspace, "code", r.Code, "rows", r.Rows)
   	},
   	OnStatus: func(s upstream.Status) {
   		slog.Info("sync status", "workspace", s.Workspace, "state", s.State, "pending", s.Pending)
   	},
   })
   if err != nil {
   	return err
   }
   ```

   `Bindings` names the entities that sync with the origin. Register other entities on the local engine and leave them out of `Bindings` to keep them on this device only, for example window layout or drafts.

   The callbacks run on the link's goroutines. To show a refused change or the sync state in the UI, send it to the webview with your shell's event API.

4. **Build the local engine with the link's options.**

   ```go
   store, err := sqlitestore.Open(filepath.Join(dataDir, "sync.db"))
   if err != nil {
   	return err
   }
   srv, err := server.New(store, append([]server.Option{
   	server.WithEntities(&todov1.Todo{}),
   }, link.ServerOptions()...)...)
   if err != nil {
   	return err
   }
   ```

   `link.ServerOptions()` installs the write capture that records local writes. Pass it to this engine only.

5. **Start the link and link the user's workspace.**

   ```go
   if err := link.Start(ctx, srv); err != nil {
   	return err
   }
   if err := link.AddWorkspace(ctx, workspaceID); err != nil {
   	return err
   }
   ```

   `Start` resumes every workspace that was linked before. `AddWorkspace` stores the workspace, so call it once after sign-in. Calling it again for a linked workspace only sets its subscriptions.

6. **Serve the local engine to the webview.**

   Mount the engine's services on a mux and answer each stream with `tunnel.Serve`:

   ```go
   mux := http.NewServeMux()
   for path, h := range srv.Handlers() {
   	mux.Handle(path, h)
   }
   app.HandleStream("syncplane", func(c *application.StreamConn) {
   	_ = tunnel.Serve(c.Context(), c, mux)
   })
   ```

   Only the webview reaches the local engine, so the engine can run without an `Authenticator`. The origin applies its permissions to the link's credential.

7. **Point the TypeScript client at the tunnel.**

   ```ts
   import { SyncClient } from 'syncplane';
   import { createTunnelTransport, fromWebSocket } from 'syncplane/tunnel';

   const client = new SyncClient({
   	transport: createTunnelTransport(() => fromWebSocket(Stream('syncplane'))),
   	workspaceId,
   	entities: [ TodoSchema ],
   });
   await client.start();
   ```

   The client syncs with the local engine as with any server. Its writes reach the origin through the link.

8. **Close in order when the app quits.**

   ```go
   link.Close()  // ends the origin stream and waits for state writes
   srv.Close()
   store.Close()
   ```

   Pending writes stay in the store and go to the origin after the next `Start`.

## Wipe the data on sign-out

Push what is pending, then remove the workspace and its rows:

```go
flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()
if err := link.Flush(flushCtx, workspaceID); err != nil {
	slog.Warn("sign-out before every change reached the server", "error", err)
}
err := link.RemoveWorkspace(ctx, workspaceID, upstream.RemoveOptions{DeleteRows: true})
```

`RemoveWorkspace` deletes the writes the origin has not received. Rows of local-only entities stay. Also wipe the TypeScript client's local store; see [Wipe local data on sign-out](sign-out.md).

## Verify

1. Start the app, sign in and write a row in the UI.
2. Read the row on the origin, for example from a second client.
3. Disconnect the network and write another row. The UI shows it at once, and `OnStatus` reports `offline` with `pending` above 0.
4. Reconnect. `pending` goes to 0 and the row reaches the origin.
5. Quit and start the app offline. The rows are still there.

## Troubleshooting

- **`Start` fails with "bound entity ... is not registered on the server".** Pass every entity in `Bindings` to `server.WithEntities` of the local engine.
- **A write fails with `ErrNotStarted`.** Something wrote to a bound entity before `link.Start` returned. Start the link before you serve the tunnel.
- **A write fails with `permission_denied` and "is not linked to the origin".** The workspace was not added. Call `AddWorkspace` after sign-in.
- **The status stays `no_access`.** The origin grants the link's credential no permission in the workspace yet. Check the token and the origin's permissions.
- **Rows that the app wrote before it had a link are gone after the first sync.** The origin's first snapshot replaces the local rows of bound entities. Link the workspace with `AddWorkspaceWith` and `AddOptions{AdoptLocalRows: true}` to push them instead.
- **Writes stall for a moment while the origin is slow.** A write in write-through mode holds the store's write lock for its round trip. Keep write-through for single calls; see [Read and write the local engine from Go](read-and-write-the-local-engine-from-go.md).

## Related

- [Serve sync through a tunnel](serve-sync-through-a-tunnel.md): other channels and adapters.
- [Forward sync to browsers](forward-sync-to-browsers.md): the same link in a server, with presence and blobs.
- `upstream.Example` in `engine/upstream/example_test.go`: a compiling version of steps 3 to 5.
