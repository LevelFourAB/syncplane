package connect_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// A text file is a record: its first line is the name of a Project, and each
// further line is a Widget of that project, with the line in the
// collaborative-text field body and its place in count. The file cannot hold
// the other Widget fields, so the codec owns body and count only.
func ExampleRowCodec() {
	codec := engineconnect.RowCodec{
		Decode: func(in engineconnect.DecodeInput) (engineconnect.Decoded, error) {
			lines := strings.Split(string(in.Record.Content), "\n")
			// A line that stays, also when edited, keeps the key of its row.
			var prev, keys []string
			for _, w := range byPlace(in.Base.Members) {
				body, err := engineconnect.PlainTextField(w, "body")
				if err != nil {
					return engineconnect.Decoded{}, err
				}
				prev, keys = append(prev, body), append(keys, w.GetId())
			}
			matched := engineconnect.MatchKeys(prev, keys, lines[1:])
			out := engineconnect.Decoded{Root: &testdatav1.Project{Name: lines[0]}}
			for i, line := range lines[1:] {
				key := matched[i]
				if key == "" {
					key = id.New()
				}
				w := &testdatav1.Widget{Id: key, Count: int32(i)}
				out.Members = append(out.Members, w)
				out.SetText(w, "body", line)
			}
			return out, nil
		},
		Encode: func(in engineconnect.EncodeInput) (engineconnect.Rendered, error) {
			root := in.Rows.Root.(*testdatav1.Project)
			lines := []string{root.GetName()}
			for _, w := range byPlace(in.Rows.Members) {
				body, err := engineconnect.PlainTextField(w, "body")
				if err != nil {
					return engineconnect.Rendered{}, err
				}
				lines = append(lines, body)
			}
			out := engineconnect.Rendered{Content: []byte(strings.Join(lines, "\n"))}
			if in.Locator == "" {
				out.Locator = root.GetName() + ".txt"
			}
			return out, nil
		},
		Fields: map[proto.Message][]string{&testdatav1.Widget{}: {"body", "count"}},
	}

	ctx := context.Background()
	dir, err := os.MkdirTemp("", "rowcodec")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := sqlitestore.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	srv, err := server.New(db, server.WithEntities(&testdatav1.Project{}, &testdatav1.Widget{}))
	if err != nil {
		panic(err)
	}
	// files is a RecordStore over the text files of each workspace.
	files := newMemStore()
	conn, err := engineconnect.New(engineconnect.Config{
		Server:   srv,
		WriterID: "files",
		Records: &engineconnect.Records{
			Quiet: -1,
			Types: []engineconnect.RecordType{{
				Root:    &testdatav1.Project{},
				Members: []engineconnect.Member{{Prototype: &testdatav1.Widget{}, RecordField: "label"}},
				Store:   files,
				Codec:   codec,
			}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	files.put("ws1", "Plan.txt", "Plan\nwrite docs\nship")
	if err := conn.Reconcile(ctx, "ws1"); err != nil {
		panic(err)
	}

	// A client edits a line, and the connector writes the file.
	err = srv.Mutate(server.TrustedAs(ctx, "client"), "ws1", func(tx *server.EntityTx) error {
		var ship *testdatav1.Widget
		if err := tx.Scan(&testdatav1.Widget{}, func(msg proto.Message) error {
			if body, _ := engineconnect.PlainTextField(msg, "body"); body == "ship" {
				ship = msg.(*testdatav1.Widget)
			}
			return nil
		}); err != nil {
			return err
		}
		next := proto.Clone(ship).(*testdatav1.Widget)
		if err := server.ApplyTextStrings(next, nil, ship, map[string]string{"body": "ship it"}, "client-1"); err != nil {
			return err
		}
		return tx.Put(next)
	})
	if err != nil {
		panic(err)
	}
	if err := conn.Flush(ctx, "ws1"); err != nil {
		panic(err)
	}
	content, _ := files.get("ws1", "Plan.txt")
	fmt.Println(content)
	// Output:
	// Plan
	// write docs
	// ship it
}
