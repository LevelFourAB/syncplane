# Forward sync to browsers through a local server

This guide shows you how to run a local syncplane server that browsers sync with, while the local server syncs with an origin server. Use it for a site that must keep working when its connection to the origin is down, or to bring the data close to a group of users.

The local server holds a copy of the linked workspaces. Browsers read and write it with the usual TypeScript client. A link pushes their writes to the origin and pulls the origin's changes. Two bridges carry what the mutation log does not: presence and broadcast (`presencebridge`), and blob bytes (`blobbridge`).

For the model and its limits, see [Chained sync](../explanation/chained-sync.md). For every option, see the [upstream reference](../reference/upstream.md).

## Prerequisites

Before you start, make sure that you have:

- An origin server and a credential for a service principal on it. The origin must let that principal read and write the linked workspaces, watch and publish their rooms, and upload and read their blobs.
- An `Authenticator` and `Permissions` for the browsers on the local server. The origin sees only the service principal, so the local server must decide what each user may do.
- The generated Go messages of the entities to sync.

## Steps

1. **Make one HTTP client for the origin.**

   The link and both bridges use the same credential:

   ```go
   type bearer struct {
   	token string
   	base  http.RoundTripper
   }

   func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
   	r = r.Clone(r.Context())
   	r.Header.Set("Authorization", "Bearer "+b.token)
   	return b.base.RoundTrip(r)
   }

   originHTTP := &http.Client{Transport: bearer{token: serviceToken, base: http.DefaultTransport}}
   ```

   Do not set `http.Client.Timeout`. It also ends the live streams.

2. **Build the link and the origin client for the bridges.**

   ```go
   link, err := upstream.New(upstream.Config{
   	OriginURL:  originURL,
   	HTTPClient: originHTTP,
   	Bindings:   []upstream.Binding{{Prototype: &todov1.Todo{}}},
   	OnRejected: func(r upstream.Rejection) {
   		slog.Warn("origin rejected a write", "workspace", r.Workspace,
   			"code", r.Code, "mutator", r.Mutator, "rows", r.Rows)
   	},
   })
   if err != nil {
   	return err
   }
   origin, err := client.New(originURL, client.WithHTTPClient(originHTTP))
   if err != nil {
   	return err
   }
   ```

3. **Build the presence bridge.**

   ```go
   rooms := presencebridge.New(presence.NewLocal(), origin.PresenceService(),
   	presencebridge.WithWorkspaces("acme"))
   ```

   `WithWorkspaces` limits the bridge to the linked workspaces. Without it, every workspace is bridged. Use `WithWorkspaceFilter` when the set changes at run time. Keep `WithAnnounce` at or below the local server's `server.WithRoomAnnounce`; the defaults match.

4. **Build the blob bridge.**

   ```go
   fs, err := fsstore.Open("/var/lib/site/blobs")
   if err != nil {
   	return err
   }
   blobs := blobbridge.New(fs, origin.BlobService(),
   	blobbridge.WithEntities("todo")) // the bound entities
   ```

   The bridge owns `fs` and closes it. `WithEntities` keeps blobs of local-only entities off the origin. Add `blobbridge.WithPrefetch()` to fetch blobs before a reader asks for them.

5. **Build the local server with the bridges and the link's options.**

   ```go
   store, err := sqlitestore.Open("/var/lib/site/sync.db")
   if err != nil {
   	return err
   }
   srv, err := server.New(store, append([]server.Option{
   	server.WithEntities(&todov1.Todo{}),
   	server.WithAuthenticator(siteAuth),
   	server.WithPermissions(sitePermissions),
   	server.WithPresenceBus(rooms),
   	server.WithBlobStore(blobs),
   }, link.ServerOptions()...)...)
   if err != nil {
   	return err
   }
   ```

   Do not host this engine in a `server.Manager`. Both bridges read the workspace ids the engine uses as the origin's workspace ids.

6. **Start the link and the blob bridge, and link the workspaces.**

   ```go
   if err := link.Start(ctx, srv); err != nil {
   	return err
   }
   go func() {
   	if err := blobs.Run(ctx, srv); err != nil {
   		slog.Error("blob bridge stopped", "error", err)
   	}
   }()
   if err := link.AddWorkspace(ctx, "acme"); err != nil {
   	return err
   }
   ```

7. **Serve the local server to the browsers.**

   ```go
   http.ListenAndServe(":8787", srv.HTTPHandler())
   ```

   Browsers connect with `url` set to the local server. They need no change.

8. **Shut down in order.**

   ```go
   link.Close()  // stop syncing rows
   cancel()      // ends blobs.Run; wait for it to return
   srv.Close()   // no local room outlives the presence bridge
   rooms.Close()
   blobs.Close() // closes fs
   store.Close()
   ```

## Choose the write mode

The default, `upstream.ModeWriteBehind`, commits a browser's write at once and forwards it later. The site keeps working while the origin is down. When the origin rejects a write, the browser sees its row change back, and its `onMutationRejected` runs with `late: true` and the origin's code. `OnRejected` reports the same rejection to the Go app.

Set `Config.WriteMode` to `upstream.ModeWriteThrough` when browsers must get the origin's verdict on each push. A browser's push then fails with the origin's code when the origin rejects it. While the origin cannot be reached, every push fails with `internal`. The browser keeps its writes queued and retries with backoff. Each write holds the local store's write lock for a round trip to the origin, so writes to the site wait for each other.

## Custom mutations

Declare a mutator on the local server to show its result at once. Its rows are a prediction, and the origin's result replaces them. A mutator that only the origin declares also works: the local server forwards it and answers the browser at once, and the rows arrive from the origin a moment later. See [Custom mutations: mirror or forward](../explanation/chained-sync.md#custom-mutations-mirror-or-forward).

## Verify

1. Open a browser against the local server and another against the origin, in the same workspace.
2. Write a row in each. Both browsers show both rows.
3. Join the same room in both. Each browser sees the other's presence.
4. Attach a file in the local browser. The origin browser can open it.
5. Cut the local server off from the origin. The local browser can still write. After you reconnect, the origin browser gets the writes.

The test server runs this setup with the `-forward` flag: an origin and a linked local server in one process, printed as `ORIGIN` and `LISTENING`. See `cmd/testserver/forward.go`.

## Troubleshooting

- **Origin participants have the issuer `syncplane.upstream`.** This is `presencebridge.OriginIssuer`, and it is expected. Your `Authenticator` must never issue it to a local user.
- **Local users appear on the origin under one name.** The origin stamps every forwarded presence record with the bridge's principal. Put the user's display name in the presence state.
- **A room stays local only.** The origin refuses the bridge's principal for that room. The bridge retries with backoff, so a room anchored to a row the origin does not have yet joins when the row arrives.
- **A browser waits for a blob.** The local server fetches it from the origin on the first read. `blobbridge.WithReadWait` sets how long a read waits for the fetch before the blob reads as being uploaded.
- **A browser cannot find rows that exist on the origin.** The link holds only its own scope. Widen it with `SetSubscriptions`, or check what the origin lets the service principal read.

## Related

- [Embed sync in a Go desktop app](embed-sync-in-a-go-desktop-app.md)
- [Add presence to an app](add-presence.md)
- [Attach files to rows](attach-files-to-rows.md)
