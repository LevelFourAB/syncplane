package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	connectrpc "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
)

func TestRenderParseRoundTrip(t *testing.T) {
	todos := []*todov1.Todo{
		{Id: "t1", Title: "Buy milk", Completed: true, CreatedAtMs: 1718000000000, SortKey: "a0"},
		{Id: "t2", Title: "Multi\nline\ntitle"},
		{Id: "weird/id", Title: "slash in id"},
		{Id: "t3"}, // empty title
	}
	for _, want := range todos {
		got, err := parseTodo(renderTodo(want))
		if err != nil {
			t.Fatalf("parse(render(%v)): %v", want, err)
		}
		if !proto.Equal(want, got) {
			t.Errorf("round trip mutated todo: want %v, got %v", want, got)
		}
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, raw := range []string{"", "no front matter", "---\nid: x\n(unterminated)"} {
		if _, err := parseTodo([]byte(raw)); err == nil {
			t.Errorf("parseTodo(%q) accepted garbage", raw)
		}
	}
}

// TestEndToEnd runs the real wiring — server, connector, sqlite state and
// Markdown files on disk — through both sync directions.
// pushOf builds a single-workspace push; a PushRequest carries one batch per
// workspace.
func pushOf(ws string, muts []*syncplanev1.Mutation) *syncplanev1.PushRequest {
	return &syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: muts}}}
}

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	const ws = "w1"
	dir := t.TempDir()

	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := server.New(store, server.WithEntities(&todov1.Todo{}))
	if err != nil {
		t.Fatal(err)
	}
	state, err := sqlitestate.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := connect.New(connect.Config{
		Server:   srv,
		Source:   &mdSource{root: dir},
		State:    state,
		Entities: []proto.Message{&todov1.Todo{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Client push → file appears.
	value, err := proto.Marshal(&todov1.Todo{Id: "t1", Title: "from client"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Push(ctx, connectrpc.NewRequest(pushOf(
		ws,
		[]*syncplanev1.Mutation{{
			ClientId:   "c1",
			MutationId: 1,
			Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
				Entity: "todo", Key: "t1", Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
			}},
		}})))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Msg.GetResults()[0].GetResults()[0].GetStatus(); got != syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED {
		t.Fatalf("push not applied: %v", got)
	}
	if err := conn.Drain(ctx, ws); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ws, "t1.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected %s after drain: %v", path, err)
	}
	onDisk, err := parseTodo(raw)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.GetTitle() != "from client" {
		t.Fatalf("file holds %q", onDisk.GetTitle())
	}

	// File edited on disk → engine row updates, file does not churn.
	edited := renderTodo(&todov1.Todo{Id: "t1", Title: "edited in a text editor"})
	if err := os.WriteFile(path, edited, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := conn.Reconcile(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if err := conn.Drain(ctx, ws); err != nil {
		t.Fatal(err)
	}
	var inEngine todov1.Todo
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		_, err := tx.Get(&inEngine, "t1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if inEngine.GetTitle() != "edited in a text editor" {
		t.Fatalf("engine holds %q", inEngine.GetTitle())
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(edited) {
		t.Fatal("ingest echoed back and rewrote the file")
	}

	// New file dropped in → imported.
	if err := os.WriteFile(filepath.Join(dir, ws, "t2.md"),
		renderTodo(&todov1.Todo{Id: "t2", Title: "dropped in"}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := conn.Reconcile(ctx, ws); err != nil {
		t.Fatal(err)
	}
	var t2 todov1.Todo
	var found bool
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var err error
		found, err = tx.Get(&t2, "t2")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !found || t2.GetTitle() != "dropped in" {
		t.Fatalf("dropped-in file not imported (found=%v, title=%q)", found, t2.GetTitle())
	}

	// File deleted → engine row deleted.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := conn.Reconcile(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var todo todov1.Todo
		var err error
		found, err = tx.Get(&todo, "t1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("deleting the file did not delete the engine row")
	}
}

// TestNotesTextRoundTrip proves a collaborative-text field survives the
// external round trip: a file edit and a concurrent client edit to the same
// note MERGE through the text CRDT instead of clobbering each other.
func TestNotesTextRoundTrip(t *testing.T) {
	ctx := context.Background()
	const ws = "w1"
	dir := t.TempDir()

	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := server.New(store, server.WithEntities(&todov1.Todo{}, &todov1.Note{}))
	if err != nil {
		t.Fatal(err)
	}
	state, err := sqlitestate.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := connect.New(connect.Config{
		Server:   srv,
		Source:   &mdSource{root: dir},
		State:    state,
		Entities: []proto.Message{&todov1.Todo{}, &todov1.Note{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A client creates the note.
	doc := text.New("client")
	if err := doc.Insert(0, "hello world"); err != nil {
		t.Fatal(err)
	}
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		return tx.Put(&todov1.Note{Id: "n1", Body: doc.Encode()})
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Drain(ctx, ws); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ws, "notes", "n1.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected %s after drain: %v", path, err)
	}
	if string(raw) != "hello world\n" {
		t.Fatalf("file holds %q", raw)
	}

	// Concurrently: the file is edited externally AND a client edits the
	// engine's document.
	if err := os.WriteFile(path, []byte("hello brave world\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var cur todov1.Note
		if _, err := tx.Get(&cur, "n1"); err != nil {
			return err
		}
		d, err := text.Decode(cur.GetBody(), "client")
		if err != nil {
			return err
		}
		if err := d.Insert(d.Len(), "!"); err != nil {
			return err
		}
		return tx.Patch(&todov1.Note{Id: "n1", Body: d.Encode()}, []string{"body"}, nil)
	}); err != nil {
		t.Fatal(err)
	}

	if err := conn.Reconcile(ctx, ws); err != nil {
		t.Fatal(err)
	}
	if err := conn.Drain(ctx, ws); err != nil {
		t.Fatal(err)
	}

	// Both edits survive on both sides.
	var note todov1.Note
	if err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		_, err := tx.Get(&note, "n1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err := text.PlainText(note.GetBody())
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello brave world!" {
		t.Fatalf("engine text = %q, want merge of both edits", got)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != "hello brave world!\n" {
		t.Fatalf("file = %q, want the merged text", after)
	}
}
