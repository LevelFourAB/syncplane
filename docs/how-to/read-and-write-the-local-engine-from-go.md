# Read and write the local engine from Go

This guide shows you how Go code reads and writes a local engine that an `upstream.Link` keeps converged with an origin. Use it in a desktop app whose Go side changes data, in a background job next to a local server, or in a Go app that has no TypeScript client at all.

`upstream.Workspace` is the handle. Its writes go through the local engine's write path, so the link forwards them like any other local write. Its reads see the local rows, pending writes included. Each write returns an `upstream.Receipt` that can wait for the origin's verdict.

This guide assumes a started link with a linked workspace; see [Embed sync in a Go desktop app](embed-sync-in-a-go-desktop-app.md). For the model, see [Chained sync](../explanation/chained-sync.md).

## Get a handle

```go
w := link.Workspace("acme")
```

A handle is cheap and safe for concurrent use. Every call uses the context it gets for authorization, as `server.Apply` and `server.View` do. Mark the context for the caller:

- `server.WithPrincipal(ctx, p)` acts as `p`, and the local engine's permissions apply.
- `server.Trusted(ctx)` or `server.TrustedAs(ctx, "importer")` skips the permission hooks, for system work.
- A context with neither is anonymous.

## Write

```go
receipt, err := w.Put(ctx, &todov1.Todo{Id: "t1", Title: "Buy milk"})
if err != nil {
	return err // the local engine refused the write
}
```

The write commits locally at once. The call returns before the origin sees it.

Patch named fields, with a compare-and-swap against the version you read:

```go
row, ok, err := upstream.Get[*todov1.Todo](ctx, w, "t1")
if err != nil || !ok {
	return err
}
v := row.Version
_, err = w.Patch(ctx, &todov1.Todo{Id: "t1", Completed: true}, []string{"completed"}, &v)
```

`row.Version` is a local version. The link translates the guard for the origin, so the origin applies the patch only when no other writer changed the row there first.

Delete a row:

```go
_, err = w.Delete(ctx, &todov1.Todo{}, "t1")
```

Write several rows as one atomic mutation:

```go
_, err = w.Apply(ctx, func(tx *server.ApplyTx) error {
	if err := tx.Put(&todov1.Todo{Id: "t2", Title: "Send invoice"}); err != nil {
		return err
	}
	return tx.Delete(&todov1.Todo{}, "t3")
})
```

Set a collaborative-text field from a string. Give each Go writer a stable text peer:

```go
_, err = w.Put(ctx, &todov1.Note{Id: "n1"},
	server.WithTextString("body", "Meeting notes"),
	server.WithTextPeer("desktop-sync"))
```

Run a custom mutation:

```go
receipt, err = w.Invoke(ctx, "complete_all", &todov1.CompleteAllArgs{})
```

When the local engine declares `complete_all`, its mutator runs locally as a prediction. When it does not, the link forwards the mutation, nothing changes locally, and `receipt.Seq` is 0.

## Wait for the origin

```go
err = receipt.Wait(ctx)
var rej *upstream.RejectedError
switch {
case errors.As(err, &rej):
	// The origin rejected the write, and the link reverted its rows.
	slog.Warn("rejected", "code", rej.Code, "rows", rej.Rows)
case errors.Is(err, upstream.ErrRevoked):
	// The origin revoked the workspace before it decided the write.
case err != nil:
	return err // ctx ended first
}
```

`Wait` returns `nil` at once for a write that the link did not forward (`receipt.Forwarded` is false), such as a write to a local-only entity.

To wait for everything, call `w.Flush(ctx)`. It returns when the origin confirmed every pending write, every rejection went to `Config.OnRejected`, and the workspace is caught up.

To wait for a write that another path made, such as an effect or a downstream client, pass its local sequence to `link.Await(ctx, "acme", seq)`. It returns `upstream.ErrUnknownWrite` when the link has no record of a forwarded write at that sequence.

## Make one write wait for the origin

Mark the context with `upstream.WriteThrough` to send one write to the origin before it commits locally:

```go
_, err = w.Put(upstream.WriteThrough(ctx), &todov1.Todo{Id: "t9", Title: "Pay invoice"})
if rej, ok := server.AsRejection(err); ok {
	return fmt.Errorf("the server refused: %s", rej.Code)
}
if errors.Is(err, upstream.ErrOriginUnavailable) {
	return errors.New("cannot reach the server; try again")
}
```

The write commits only when the origin applied it. When the origin rejects it, the error is a `server.Rejection` with the origin's code. When the origin does not answer, the error wraps `upstream.ErrOriginUnavailable`, nothing commits locally, and the origin may still have applied the write; the row then arrives back through a pull.

The call to the origin holds the local store's write lock for its round trip. Use write-through for single writes whose result the user must see before going on. `upstream.WriteBehind(ctx)` does the opposite, for a link whose `Config.WriteMode` is write-through.

## Read

Read one row:

```go
row, ok, err := upstream.Get[*todov1.Todo](ctx, w, "t1")
```

`ok` is false for a row that is absent, deleted, or hidden from the caller.

Query rows:

```go
rows, err := upstream.Query[*todov1.Todo](ctx, w, query.Spec{
	Where:   query.Eq("completed", false),
	OrderBy: []*syncplanev1.OrderKey{query.Asc("title")},
	Limit:   50,
})
```

`Query` returns the whole result as a slice. Set `Limit` for a large entity.

## Watch changes

```go
for page, err := range w.Watch(ctx, nil) {
	if err != nil {
		return err
	}
	for _, c := range page.Changes {
		slog.Info("changed", "entity", c.Entity, "key", c.Key, "deleted", c.Deleted)
	}
	saveCursor(page.Cursor)
}
```

`Watch` yields pages of committed changes, from the origin and from local writes, as they commit. A `nil` cursor starts at the current head. To resume after a restart, store the `Cursor` of the last page you handled, and pass it as the cursor; it carries the store lineage, so a cursor from a replaced or restored store is refused rather than misread. The iteration ends without an error when `ctx` ends, and yields `server.ErrCursorTooOld` for a cursor the change feed can no longer serve. For durable processing with retries, use the [effects runtime](react-to-changes.md) instead.

## Handle rejections

Every rejected write reaches `Config.OnRejected`, whichever path made it:

```go
OnRejected: func(r upstream.Rejection) {
	if alreadySeen(r.Workspace, r.MutationID) {
		return
	}
	notifyUser(r.Workspace, r.Code, r.Rows)
},
```

Here `alreadySeen` and `notifyUser` are your own functions. The link calls `OnRejected` after it reverted the rows, one call at a time per workspace, in the order the rejections arrived. Delivery is at least once: after a crash during a call, the same rejection comes again after the next `Start`, with the same `MutationID`.

The workspace's sync waits while a callback runs. A callback can call the link, for example `Flush`, and can write to the local engine, for example a corrected write.

## Related

- [Upstream reference](../reference/upstream.md): every method, error and option.
- `upstream.Example` in `engine/upstream/example_test.go`.
- [Write from server-side code](server-side-writes.md): the in-process write API that `Workspace` builds on.
