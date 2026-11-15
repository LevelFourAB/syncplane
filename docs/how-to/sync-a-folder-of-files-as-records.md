# Sync a folder of files as records

Use record mode of the `connect` package to sync a folder of text files with the engine, where one file holds many rows. In this guide each Markdown file is one page: its heading is a `Page` row, and each further line is a `Line` row. A user can edit the files in any editor while clients edit the rows, and both sides keep each other's edits.

For row mode, where one external item is one row, see [Sync an external store](sync-an-external-store.md). For the model behind record mode, see [Record mode](../explanation/connectors.md#record-mode). For every type and option, see the [connectors reference](../reference/connectors.md#record-mode).

## Prerequisites

- A running engine server (`srv`) with the entities below registered.
- A folder for each workspace, under one root directory.

## Steps

### 1. Declare the entities

The member entity names its page in a string field. The connector finds the lines of a page with a query on that field, so index it.

```proto
message Page {
	option (syncplane.v1.entity) = {
		name: "page"
		key_fields: [ "id" ]
	};

	string id = 1;
	string title = 2;
}

message Line {
	option (syncplane.v1.entity) = {
		name: "line"
		key_fields: [ "id" ]
	};

	string id = 1;
	string page_id = 2 [(syncplane.v1.field_behavior) = { indexed: true }];
	bytes body = 3 [(syncplane.v1.field_behavior) = { text: true }];
	int32 position = 4;
	// Set by clients. The file does not hold it.
	bool done = 5;
}
```

`body` is a collaborative-text field, so a store edit and a client edit of the same line both stay.

### 2. Write the store

A `connect.RecordStore` reads and writes whole files. The locator of a record is its file name. The fingerprint is a hash of the content: equal content must give an equal fingerprint, and a content hash also lets the connector find a renamed file by its content.

```go
// folder is a RecordStore over the .md files of one directory.
type folder struct {
	dir     string
	watcher *fsnotify.Watcher
}

func fingerprint(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// path returns the file of a locator. A locator that is not an .md file in
// this folder is not a record.
func (f *folder) path(locator string) (string, error) {
	if filepath.Base(locator) != locator || filepath.Ext(locator) != ".md" {
		return "", fmt.Errorf("%q: %w", locator, connect.ErrRecordNotFound)
	}
	return filepath.Join(f.dir, locator), nil
}

func (f *folder) Read(_ context.Context, _ string, locator string) (connect.Record, error) {
	p, err := f.path(locator)
	if err != nil {
		return connect.Record{}, err
	}
	content, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return connect.Record{}, fmt.Errorf("%q: %w", locator, connect.ErrRecordNotFound)
	}
	if err != nil {
		return connect.Record{}, err
	}
	ref := connect.RecordRef{Locator: locator, Fingerprint: fingerprint(content)}
	return connect.Record{RecordRef: ref, Content: content}, nil
}

func (f *folder) List(ctx context.Context, ws string, fn func(connect.RecordRef) error) error {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".md" {
			continue
		}
		rec, err := f.Read(ctx, ws, e.Name())
		if errors.Is(err, connect.ErrRecordNotFound) {
			continue // removed after ReadDir
		}
		if err != nil {
			return err
		}
		if err := fn(rec.RecordRef); err != nil {
			return err
		}
	}
	return nil
}
```

This `List` reads each file to hash it. For a large folder, keep the hash of each file with its size and modification time, and read only the files whose size or time changed.

`Write` must replace the file in one step, so that an editor or the next `List` never sees half a file. Write a temporary file and move it over the target. When the record moved, `from` is its old locator: remove that file.

```go
func (f *folder) Write(ctx context.Context, ws string, rec connect.Record, from string) (string, error) {
	p, err := f.path(rec.Locator)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(f.dir, ".write-*.tmp")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name()) // does nothing after the rename
	if _, err := tmp.Write(rec.Content); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return "", err
	}
	if from != "" && from != rec.Locator {
		if err := f.Delete(ctx, ws, from); err != nil {
			return "", err
		}
	}
	return fingerprint(rec.Content), nil
}

func (f *folder) Delete(_ context.Context, _ string, locator string) error {
	p, err := f.path(locator)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
```

The connector delivers at least once, so each method is idempotent: a second `Write` of the same content writes the same file, and `Delete` of a missing file is not an error.

### 3. Write the codec

A `connect.RowCodec` turns the content of a file into rows (`Decode`) and rows into content (`Encode`). The connector merges the decoded rows into the engine one row at a time, so `Decode` only has to give each line the key of its row.

A line in a text file has no key. `connect.MatchKeys` gives each line the key of the line it continues in the file as it was last synced (`DecodeInput.Base`), so an edited line keeps its row:

```go
// parse splits a page file into its title and its lines.
func parse(content []byte) (title string, lines []string, err error) {
	head, rest, more := strings.Cut(string(content), "\n")
	title, ok := strings.CutPrefix(head, "# ")
	if !ok {
		return "", nil, errors.New(`a page starts with "# <title>"`)
	}
	if more {
		lines = strings.Split(rest, "\n")
	}
	return title, lines, nil
}

// byPosition returns the lines in file order.
func byPosition(members []proto.Message) []*pagesv1.Line {
	lines := make([]*pagesv1.Line, 0, len(members))
	for _, m := range members {
		lines = append(lines, m.(*pagesv1.Line))
	}
	slices.SortFunc(lines, func(a, b *pagesv1.Line) int {
		return cmp.Or(cmp.Compare(a.GetPosition(), b.GetPosition()), strings.Compare(a.GetId(), b.GetId()))
	})
	return lines
}

// bodies returns the text and the key of each line, in file order.
func bodies(members []proto.Message) (texts, keys []string, err error) {
	for _, l := range byPosition(members) {
		body, err := connect.PlainTextField(l, "body")
		if err != nil {
			return nil, nil, err
		}
		texts, keys = append(texts, body), append(keys, l.GetId())
	}
	return texts, keys, nil
}

func decode(in connect.DecodeInput) (connect.Decoded, error) {
	title, lines, err := parse(in.Record.Content)
	if err != nil {
		return connect.Decoded{}, err
	}
	// With no agreed state, as on a first sync, match against the engine
	// rows, so that equal lines keep their keys.
	prev := in.Base
	if prev.Root == nil {
		prev = in.Rows
	}
	prevTexts, prevKeys, err := bodies(prev.Members)
	if err != nil {
		return connect.Decoded{}, err
	}
	nowTexts, nowKeys, err := bodies(in.Rows.Members)
	if err != nil {
		return connect.Decoded{}, err
	}
	matched := connect.MatchKeys(prevTexts, prevKeys, lines)
	place := make(map[string]int32)
	for _, rows := range [][]proto.Message{in.Rows.Members, prev.Members} {
		for _, l := range byPosition(rows) {
			place[l.GetId()] = l.GetPosition()
		}
	}
	used := make(map[string]bool, len(matched))
	for _, k := range matched {
		used[k] = true
	}
	out := connect.Decoded{Root: &pagesv1.Page{Title: title}}
	last := int32(-1)
	for i, text := range lines {
		key := matched[i]
		if key == "" {
			// A row that an earlier merge of this content wrote: reuse it,
			// so that a repeated merge writes nothing.
			for j, t := range nowTexts {
				if t == text && !used[nowKeys[j]] {
					key = nowKeys[j]
					break
				}
			}
		}
		if key == "" {
			key = id.New()
		}
		used[key] = true
		// A row keeps its position while it still sorts after the line
		// before it, so that the file gives back the positions Encode got.
		pos, ok := place[key]
		if !ok || pos <= last {
			pos = last + 1
		}
		last = pos
		l := &pagesv1.Line{Id: key, Position: pos}
		out.Members = append(out.Members, l)
		out.SetText(l, "body", text)
	}
	return out, nil
}
```

`Decode` leaves the keys of the page and `page_id` empty: the connector fills in the record key. It gives each text value with `SetText` and leaves the `body` bytes unset, so the connector merges the value into the engine's document. An error refuses the file: the connector changes no row, lists the file in `Health.Refused`, and tries it again when the file next changes.

`Decode` and `Encode` run inside the merge transaction, under the workspace write lock. They must do no I/O and have no effect outside the call, because the transaction can roll back and run again. A new key from `id.New()` is allowed. `Encode` must give the same content for the same rows.

`Encode` writes the rows back. When the store does not hold the page yet, `EncodeInput.Locator` is empty and `Encode` must give a file name:

```go
func encode(in connect.EncodeInput) (connect.Rendered, error) {
	page := in.Rows.Root.(*pagesv1.Page)
	texts, _, err := bodies(in.Rows.Members)
	if err != nil {
		return connect.Rendered{}, err
	}
	content := strings.Join(append([]string{"# " + page.GetTitle()}, texts...), "\n")
	out := connect.Rendered{Content: []byte(content)}
	if in.Locator == "" {
		out.Locator = strings.NewReplacer("/", "-", "\\", "-").Replace(page.GetTitle()) + ".md"
	}
	return out, nil
}
```

With an empty `Locator` in the result, the file keeps its name when the title changes. To rename the file with the title, return the new name each time: the connector then writes the new file and passes the old name as `from`. Two pages that want the same file name cannot both have it: the second one waits and is listed in `Health.Refused`. Add the page key to the name when titles can repeat.

`Decode` must read back what `Encode` wrote, in the fields the file owns. The file holds the order of the lines but not their positions, so `Decode` keeps the position of a matched row while it still sorts after the line before it, and gives any other line the position after the line before it. A file that `Encode` wrote then gives back the positions that clients wrote, gaps included.

Name the fields that the file owns. The file holds the text and the order of a line, but not its `done` flag:

```go
codec := connect.RowCodec{
	Decode: decode,
	Encode: encode,
	Fields: map[proto.Message][]string{&pagesv1.Line{}: {"body", "position"}},
}
```

A field that the file does not own keeps its engine value on every merge. Without the entry, the next file edit would clear `done` on each line, because the file cannot hold it. The key fields and `page_id` are always owned. `Page` has no entry, so the file owns all its fields.

### 4. Build the connector

Each workspace gets its own folder, so open the store with `OpenStore`. The connector calls it on the first pass of a workspace and closes the store (`io.Closer`) when it is done with the workspace.

```go
var conn *connect.Connector // OpenStore runs after New returns
conn, err := connect.New(connect.Config{
	Server:   srv,
	WriterID: "pages",
	Records: &connect.Records{
		Types: []connect.RecordType{{
			Root:    &pagesv1.Page{},
			Members: []connect.Member{{Prototype: &pagesv1.Line{}, RecordField: "page_id"}},
			OpenStore: func(ctx context.Context, ws string) (connect.RecordStore, error) {
				return openFolder(filepath.Join(root, ws), func(locator string) {
					conn.Touch(ws, locator)
				})
			},
			Codec: codec,
		}},
	},
})
if err != nil {
	return err
}
```

Record mode keeps its bookkeeping in the engine's store, so it takes no `State`. It requires a `WriterID`: keep it the same across restarts, because it names that bookkeeping.

### 5. Report file changes

`openFolder` watches the folder and reports each changed file with `Touch`. The connector ingests the touched files in a pass soon after. A periodic full list (`Config.ReconcileInterval`) still catches the events that a watcher misses.

```go
// openFolder opens the folder of one workspace and reports each change of a
// file in it.
func openFolder(dir string, touch func(locator string)) (*folder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := w.Add(dir); err != nil {
		w.Close()
		return nil, err
	}
	go func() {
		for {
			select {
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				touch(filepath.Base(ev.Name))
			case _, ok := <-w.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	return &folder{dir: dir, watcher: w}, nil
}

// Close stops the watcher.
func (f *folder) Close() error { return f.watcher.Close() }
```

`Touch` ignores a locator that is not a record, such as a temporary file of `Write`. The connector's own writes come back as events too. Their fingerprint matches what the connector wrote, so it does not read them.

Start the connector:

```go
if err := conn.Start(ctx); err != nil {
	return err
}
defer conn.Close()
```

The connector writes engine edits to a file after the page has had no edit for `Records.Quiet` (1 second by default), and no later than `Records.MaxDelay` after the first unsent edit. `Close` writes the edits that still wait.

A file that an editor saves by delete and create keeps its rows: the connector deletes the rows of a missing file only after `Records.DeleteGrace` (2 seconds by default). A renamed file keeps its page and line keys.

### 6. Choose the first-pass policy

The first sync of a workspace has nothing to compare against, so you choose which side wins:

- `connect.SourceWins` (the default) imports the folder. It deletes every page and line that no file holds.
- `connect.EngineWins` writes every page to the folder and imports the files that no page renders to. It deletes nothing.

With `SourceWins`, an empty folder deletes every page of the workspace. To write the rows to a new folder and import an existing one, choose the policy for each workspace:

```go
ChooseFirstPass: func(ctx context.Context, ws string) (connect.FirstPass, error) {
	entries, err := os.ReadDir(filepath.Join(root, ws))
	if errors.Is(err, fs.ErrNotExist) || (err == nil && len(entries) == 0) {
		return connect.EngineWins, nil
	}
	if err != nil {
		return 0, err
	}
	return connect.SourceWins, nil
},
```

Set it in `connect.Records`. The connector calls it once for each workspace, at its first pass, and again only after a first pass that failed.

To keep a copy of each file that `EngineWins` replaces with other content, set `Records.Overwrite`. See [Keep the store content that a write replaces](run-a-record-connector-on-a-replica.md#5-keep-the-store-content-that-a-write-replaces).

## Confirming the sync

Check the health of a workspace after a pass:

```go
if h, ok := conn.Health(ws); ok {
	for _, f := range h.Refused {
		slog.Warn("file skipped", "workspace", ws, "file", f.Locator, "err", f.Err, "failures", f.Failures)
	}
}
```

`LastSuccess`, `LastError` and the other fields work as in row mode. `h.Refused` lists the files that the connector skips: `Decode` or `Encode` refused them, or their file name belongs to another page. A skipped file does not fail the pass. Fix the file, and the connector imports it at its next change.

To sync a workspace by hand, for example in a test, call `conn.Reconcile(ctx, ws)` to list the folder, and `conn.Flush(ctx, ws)` to write the engine edits now.

## Related

- `ExampleRowCodec` in `engine/connect/example_rowcodec_test.go`: a compiling text-file codec.
- [Run a record connector on a replica](run-a-record-connector-on-a-replica.md)
- [Record mode](../explanation/connectors.md#record-mode): keys and locators, echo detection, the quiet period, rename detection, and the first-pass policy.
- [Connectors reference](../reference/connectors.md#record-mode)
