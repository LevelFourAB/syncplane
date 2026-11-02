# Use the Go client against a remote server

This guide shows you how Go code reads and writes a remote syncplane server with `engine/client`. Use it in a backend job, a command-line tool or a test that keeps no local copy of the data. Every read goes to the server, and every write returns when the server has applied or rejected it.

To keep a local copy that works offline, use `engine/upstream` instead; see [Chained sync](../explanation/chained-sync.md). For every type and option, see the [Go client reference](../reference/go-client.md).

## Prerequisites

- The URL of a syncplane server and a credential for it.
- The generated Go messages of the server's entities, for example `&todov1.Todo{}`.

## Connect

```go
c, err := client.New("https://sync.example.com",
	client.WithHTTPClient(&http.Client{Transport: bearer{token: token, base: http.DefaultTransport}}))
if err != nil {
	return err
}
ws := c.Workspace("acme")
```

`bearer` is a `http.RoundTripper` that sets the `Authorization` header; see [Embed sync in a Go desktop app](embed-sync-in-a-go-desktop-app.md#steps). `client.WithInterceptors` is the alternative for Connect interceptors. `New` does not connect.

A `Client` sends a random client id. A job that runs again and again can pin one with `client.WithClientID`. The client then reads the server's watermark for that id before its first write. Never let two running clients share an id.

## Write

```go
res, err := ws.Put(ctx, &todov1.Todo{Id: "t1", Title: "Write the report"})
if err != nil {
	return err
}

// A compare-and-swap against the version the put left.
_, err = ws.Patch(ctx, &todov1.Todo{Id: "t1", Completed: true}, []string{"completed"},
	client.WithExpectedVersion(res.Seq))
```

Other writes:

- `ws.Delete(ctx, &todov1.Todo{}, "t1")` deletes a row. Deleting an absent row is not an error.
- `ws.Apply(ctx, client.Put(&todov1.Todo{Id: "t2", Title: "Send the invoice"}), client.Delete(&todov1.Todo{}, "t1"))` applies several operations as one atomic mutation.
- `ws.Invoke(ctx, "complete_all", &todov1.CompleteAllArgs{})` runs a custom mutator.
- `client.WithText("body", "Meeting notes")` on a `Put` or `Patch` sets a collaborative-text field from a string. The client reads the stored document first, so concurrent edits stay.

## Handle write errors

```go
_, err = ws.Put(ctx, &todov1.Todo{Id: "t3", Title: "Call the bank"})
var rej *client.RejectedError
var wsErr *client.WorkspaceError
switch {
case errors.Is(err, client.ErrUnconfirmed):
	// The outcome is unknown. The client sends the write again, under the same
	// mutation id, before the next write to the workspace. Test this case
	// first: the error also wraps the last failure, which can be a
	// *client.WorkspaceError.
	_, err = ws.Confirm(ctx)
case errors.As(err, &rej):
	// Final: the server wrote nothing. rej.Code is, for example, "conflict".
case errors.As(err, &wsErr):
	// The server refused the whole push, for example workspace_access_revoked.
case errors.Is(err, client.ErrClientIDInUse):
	// Another running client uses the same client id.
}
```

`client.CodeOf(err)` returns the code of any of these errors, and of a `*connect.Error` from the server.

Writes to one workspace run one at a time: each waits until the previous one has its outcome. Writes to different workspaces run concurrently.

## Read

```go
row, ok, err := client.Get[*todov1.Todo](ctx, ws, "t1")

page, err := client.Query[*todov1.Todo](ctx, ws, query.Spec{
	Where: query.Eq("completed", false),
	Limit: 100,
})

for row, err := range client.QueryAll[*todov1.Todo](ctx, ws, query.Spec{Where: query.Eq("completed", false)}) {
	if err != nil {
		return err
	}
	fmt.Println(row.Key, row.Value.GetTitle())
}
```

The server applies the caller's read rules. `Query` returns one page; `QueryAll` fetches page after page.

## Follow changes

Take a snapshot, then pull the changes after it:

```go
snap, err := ws.Bootstrap(ctx, client.WithNoDeltaEntities("note"))
if err != nil {
	return err
}
for rc, err := range snap.Rows() {
	if err != nil {
		return err
	}
	apply(rc)
}
cursor := snap.Cursor

for page, err := range ws.Pull(ctx, cursor, client.WithNoDeltaEntities("note")) {
	if errors.Is(err, client.ErrReset) {
		// Bootstrap again.
	}
	if err != nil {
		return err
	}
	for _, rc := range page.GetPatch().GetChanges() {
		apply(rc)
	}
	if len(page.GetNextCursor()) > 0 {
		cursor = page.GetNextCursor()
	}
}
```

Here `apply` is your own function. `client.DecodeRow[*todov1.Todo](rc)` decodes a change of that entity. `WithNoDeltaEntities` makes the server send collaborative-text fields in full, because a client that keeps no rows cannot apply a text fragment.

For live changes, `c.Subscribe(ctx, []client.Scope{{Workspace: "acme", Cursor: cursor}})` yields catch-up pages and then live pages over one stream. The stream does not reconnect: open it again after it ends.

## Move blobs

```go
f, _ := os.Open("report.pdf")
st, _ := f.Stat()
ref, err := ws.UploadBlob(ctx, f, st.Size(), "application/pdf")
// Store ref in a BlobRef field of a row.

n, err := ws.DownloadBlob(ctx, ref.GetDigest(), dst)
```

An upload that the server already holds sends no chunks, and an interrupted upload resumes. A download fails with `NotFound` when no row the caller can read names the blob.

## Join a room

```go
room := ws.Room("lobby")
if _, err := room.SetPresence(ctx, []byte(`{"name":"report-bot"}`), 0); err != nil {
	return err
}
for frame, err := range room.Watch(ctx) {
	if err != nil {
		return err
	}
	handle(frame)
}
```

While `Watch` runs, the server keeps the session's presence alive. Without it, call `SetPresence` again at the interval the response names. `room.Leave(ctx)` removes the session at once.

## Related

- `client.Example` in `engine/client/example_test.go`: a compiling version of the write and read steps.
- [Go client reference](../reference/go-client.md)
- [Write from server-side code](server-side-writes.md): writes from code in the server process.
