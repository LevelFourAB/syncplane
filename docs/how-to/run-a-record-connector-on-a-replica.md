# Run a record connector on a replica

Use this guide when a local engine is linked to an origin server with `engine/upstream`, and both the origin and the replica sync their own folder of files with a record-mode connector. For example, a server syncs a shared folder, and each desktop app syncs a folder on the user's computer.

This guide continues [Sync a folder of files as records](sync-a-folder-of-files-as-records.md), and uses its `Page` and `Line` entities. `pageType` is the `RecordType` from that guide. For the link, see [Embed sync in a Go desktop app](embed-sync-in-a-go-desktop-app.md). For the reasons behind each step, see [Refused writes on a replica](../explanation/connectors.md#refused-writes-on-a-replica) and [TextPeer and WriterID](../explanation/connectors.md#textpeer-and-writerid).

## Prerequisites

- An origin server with a record-mode connector.
- A replica: a local engine built with `link.ServerOptions()`, and a link that binds the entities of the records.

## Steps

### 1. Give every connector the same WriterID and its own TextPeer

```go
// On the origin.
originCfg := connect.Config{Server: originSrv, WriterID: "pages", TextPeer: "pages@origin", Records: originRecords}

// On each replica.
replicaCfg := connect.Config{Server: srv, WriterID: "pages", TextPeer: "pages@" + deviceID, Records: replicaRecords}
```

- `WriterID` labels the writes as `connect:pages` on every server, so one `effects.SkipWriters("connect:pages")` skips the writes of all instances.
- `TextPeer` is the collaborative-text writer identity. The origin connector and a replica connector can edit the same line at the same time, so each needs its own value. Two writers that share a peer make documents that the merge refuses.
- `deviceID` must stay the same across restarts of the replica. Store it with the local engine. A new peer on each start adds a dead entry to each document that the connector writes.

### 2. Make the types that the origin owns EgressOnly

Some records belong to the origin, for example templates that only an administrator changes. On the replica, make their type `EgressOnly`:

```go
templateType := connect.RecordType{
	Root:      &pagesv1.Template{},
	OpenStore: openTemplates,
	Codec:     connect.RowCodec{Encode: encodeTemplate},
	Direction: connect.EgressOnly,
}
```

`openTemplates` and `encodeTemplate` are a store opener and an `Encode` function for template files, written as in the folder guide. An `EgressOnly` type writes the rows to the folder and never writes a row. A user edit of such a file is not sent to the origin: the connector writes the engine's content over it, after it gives the edited content to `Records.Overwrite` (see step 5). A deleted file is written again. A `RowCodec` for an `EgressOnly` type needs no `Decode`.

### 3. Report refused writes to the connector

The origin can refuse a write that the replica connector made from a file edit. The link then reverts the rows, and without more work the connector writes the reverted content over the user's file. Call `Connector.Refused` from `upstream.Config.OnRejected` so that the connector knows which records the refusal reverted:

```go
var conn *connect.Connector // the replica's record-mode connector

link, err := upstream.New(upstream.Config{
	OriginURL:  originURL,
	HTTPClient: httpClient,
	Bindings: []upstream.Binding{
		{Prototype: &pagesv1.Page{}},
		{Prototype: &pagesv1.Line{}},
		{Prototype: &pagesv1.Template{}},
	},
	OnRejected: func(r upstream.Rejection) {
		rows := make([]connect.RowRef, len(r.Rows))
		for i, row := range r.Rows {
			rows[i] = connect.RowRef(row)
		}
		if err := conn.Refused(context.Background(), r.Workspace, rows); err != nil {
			slog.Error("mark refused records", "workspace", r.Workspace, "err", err)
		}
	},
})
if err != nil {
	return err
}
```

`OnRejected` can report a rejection from before a restart as soon as the link starts. Build the connector before you call `link.Start`, so that `conn` is set:

```go
srv, err := server.New(store, append([]server.Option{
	server.WithEntities(&pagesv1.Page{}, &pagesv1.Line{}, &pagesv1.Template{}),
}, link.ServerOptions()...)...)
if err != nil {
	return err
}
conn, err = connect.New(connect.Config{
	Server:   srv,
	WriterID: "pages",
	TextPeer: "pages@" + deviceID,
	Records: &connect.Records{
		Types:        []connect.RecordType{pageType, templateType},
		KeepReplaced: time.Minute,
		Overwrite:    backup,
	},
})
if err != nil {
	return err
}
if err := link.Start(ctx, srv); err != nil {
	return err
}
if err := conn.Start(ctx); err != nil {
	return err
}
```

`OnRejected` delivers at least once. A second `Refused` call for the same rows is safe.

### 4. Keep the content that egress replaced

The link reverts the rows before it calls `OnRejected`, so the connector can write the reverted content to the file before `Refused` runs. `Records.KeepReplaced` keeps the file content that an egress write replaced, when the file had an ingest within that time before. `Refused` can then still give that content to `Overwrite`.

Set it to more than the usual time from a file edit to the origin's answer. One minute covers a connected replica. The kept content is stored in the engine's store, at most one copy for each record.

### 5. Keep the store content that a write replaces

`Records.Overwrite` gets the content before egress replaces or deletes it, in three cases:

| `Reason`              | When                                                                 |
| --------------------- | -------------------------------------------------------------------- |
| `OverwriteRefused`    | The origin refused the write of an edit of this file.                |
| `OverwriteEgressOnly` | A user edited a file of an `EgressOnly` type.                        |
| `OverwriteFirstPass`  | A first pass with `EngineWins` replaces the file with other content. |

Write a backup copy, outside the synced folder:

```go
backup := func(ctx context.Context, o connect.Overwrite) error {
	dir := filepath.Join(backups, o.WorkspaceID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("%s.%d.bak", o.Locator, time.Now().UnixNano())
	return os.WriteFile(filepath.Join(dir, name), o.Old, 0o644)
}
```

`o.Old` is the content that the file holds, and `o.New` the content that replaces it (`nil` for a delete). Copy them if you keep them after the call. An error from `Overwrite` stops the write and fails the pass, and the next pass tries again. Use `o.Reason` to tell the user why the file changed.

## Confirming the setup

- Edit a file on the replica that the origin refuses, for example one without permission. The file returns to the engine's content, and a backup of the edit appears.
- Edit the same line in the origin's folder and in the replica's folder before they sync. Both edits stay in the line, in both folders.
- Edit a template file on the replica. The file returns to the origin's content, and a backup appears with `OverwriteEgressOnly`.

## Related

- `ExampleConnector_Refused` in `engine/connect/refused_example_test.go`
- [Upstream reference](../reference/upstream.md): `OnRejected` and `Rejection`.
- [Connectors reference](../reference/connectors.md#refused-writes)
- [Chained sync](../explanation/chained-sync.md): how a rejection becomes a revert.
