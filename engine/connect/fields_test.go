package connect_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// computedTodoSource is a todoSource that cannot hold sort_key, as a file
// cannot hold a field another writer computes: Write drops it, and List never
// reports it.
type computedTodoSource struct{ *todoSource }

func (s computedTodoSource) Write(ctx context.Context, ws string, msg proto.Message) (string, error) {
	todo := proto.Clone(msg).(*todov1.Todo)
	todo.SortKey = ""
	return s.todoSource.Write(ctx, ws, todo)
}

// widgetSource holds widgets that have a label and a plain-text body, the
// only fields a spec gives it.
type widgetSource struct {
	mu     sync.Mutex
	label  map[string]string
	body   map[string]string
	writes int
}

func newWidgetSource() *widgetSource {
	return &widgetSource{label: make(map[string]string), body: make(map[string]string)}
}

func (s *widgetSource) set(id, label, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.label[id], s.body[id] = label, body
}

func (s *widgetSource) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func widgetFP(label, body string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(label+"\u0000"+body)))
}

func (s *widgetSource) List(_ context.Context, _ string, fn func(engineconnect.Item) error) error {
	s.mu.Lock()
	var items []engineconnect.Item
	for id, label := range s.label {
		items = append(items, engineconnect.Item{
			Msg:         &testdatav1.Widget{Id: id, Label: label},
			Fingerprint: widgetFP(label, s.body[id]),
			TextFields:  map[string]string{"body": s.body[id]},
		})
	}
	s.mu.Unlock()
	for _, item := range items {
		if err := fn(item); err != nil {
			return err
		}
	}
	return nil
}

func (s *widgetSource) Write(_ context.Context, _ string, msg proto.Message) (string, error) {
	w := msg.(*testdatav1.Widget)
	body, err := text.PlainText(w.GetBody())
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	s.label[w.GetId()], s.body[w.GetId()] = w.GetLabel(), body
	return widgetFP(w.GetLabel(), body), nil
}

func (s *widgetSource) Delete(_ context.Context, _ string, _ proto.Message, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.label, key)
	delete(s.body, key)
	return nil
}

var _ = Describe("Connector owned fields", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	ownTitle := func(cfg *engineconnect.Config) {
		cfg.Entities = nil
		cfg.Bindings = []engineconnect.Binding{{
			Prototype: &todov1.Todo{},
			Fields:    []string{"title", "completed"},
		}}
	}

	// editTodo changes a todo in the engine, as a client or an effect would.
	editTodo := func(f *fixture, id string, edit func(*todov1.Todo)) {
		GinkgoHelper()
		Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			todo := &todov1.Todo{Id: id}
			if _, err := tx.Get(todo, id); err != nil {
				return err
			}
			edit(todo)
			return tx.Put(todo)
		})).To(Succeed())
	}

	It("keeps a field the source does not own when the source edits a synced row", func() {
		src := computedTodoSource{newTodoSource()}
		f := newFixtureOver(nil, src, ownTitle)
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		editTodo(f, "t1", func(t *todov1.Todo) { t.SortKey = "a0" })
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "two"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		got := f.engineTodo(ctx, ws, "t1")
		Expect(got.GetTitle()).To(Equal("two"))
		Expect(got.GetSortKey()).To(Equal("a0"))
	})

	It("keeps a field the source does not own when there is no shadow", func() {
		src := computedTodoSource{newTodoSource()}
		f := newFixtureOver(nil, src, ownTitle)
		editTodo(f, "t1", func(t *todov1.Todo) { t.Title, t.SortKey = "one", "a0" })
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "two"})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		got := f.engineTodo(ctx, ws, "t1")
		Expect(got.GetTitle()).To(Equal("two"))
		Expect(got.GetSortKey()).To(Equal("a0"))
		// The source holds every field it owns, so there is nothing to send
		// back.
		Expect(f.src.writeCount()).To(BeZero())
	})

	It("creates a row with only the fields the source owns", func() {
		f := newFixture(ownTitle)
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one", SortKey: "z9"})

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		got := f.engineTodo(ctx, ws, "t1")
		Expect(got.GetTitle()).To(Equal("one"))
		Expect(got.GetSortKey()).To(BeEmpty())
	})

	It("does not write the source for a change to only fields it does not own", func() {
		src := computedTodoSource{newTodoSource()}
		f := newFixtureOver(nil, src, ownTitle)
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		writes := f.src.writeCount()

		editTodo(f, "t1", func(t *todov1.Todo) { t.SortKey = "a0" })
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))

		editTodo(f, "t1", func(t *todov1.Todo) { t.Title = "three" })
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes + 1))
		Expect(f.src.get(ws, "t1").GetTitle()).To(Equal("three"))

		// The next pass finds the source in step and changes nothing.
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		got := f.engineTodo(ctx, ws, "t1")
		Expect(got.GetTitle()).To(Equal("three"))
		Expect(got.GetSortKey()).To(Equal("a0"))
	})

	It("does not write the source for such a change when it heals from the tombstone horizon", func() {
		src := computedTodoSource{newTodoSource()}
		f := newFixtureOver([]server.Option{server.WithGC(-time.Second, 0)}, src, ownTitle)
		f.src.set(ws, &todov1.Todo{Id: "t1", Title: "one"})
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		writes := f.src.writeCount()

		// While the connector does not run, t1 changes in a field the source
		// does not own, and GC collects a tombstone, so the next drain cannot
		// read the feed and compares the rows with the shadows.
		editTodo(f, "t1", func(t *todov1.Todo) { t.SortKey = "a0" })
		editTodo(f, "gone", func(*todov1.Todo) {})
		Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Delete(&todov1.Todo{}, "gone")
		})).To(Succeed())
		Expect(f.srv.CollectGarbage(ctx, ws)).To(Succeed())

		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.writeCount()).To(Equal(writes))
		Expect(f.engineTodo(ctx, ws, "t1").GetSortKey()).To(Equal("a0"))
	})

	It("ignores a text field the source does not own", func() {
		wsrc := newWidgetSource()
		f := newFixtureOver(nil, newTodoSource(), func(cfg *engineconnect.Config) {
			cfg.Source = wsrc
			cfg.Entities = nil
			cfg.Bindings = []engineconnect.Binding{{
				Prototype: &testdatav1.Widget{},
				Fields:    []string{"label"},
			}}
		})
		Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			doc, err := text.Decode(nil, "client-1")
			if err != nil {
				return err
			}
			if err := doc.Insert(0, "engine"); err != nil {
				return err
			}
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a", Body: doc.Encode()})
		})).To(Succeed())
		engineWidget := func() (label, body string) {
			GinkgoHelper()
			var w testdatav1.Widget
			Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				_, err := tx.Get(&w, "w1")
				return err
			})).To(Succeed())
			body, err := text.PlainText(w.GetBody())
			Expect(err).NotTo(HaveOccurred())
			return w.GetLabel(), body
		}

		wsrc.set("w1", "b", "source")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		label, body := engineWidget()
		Expect(label).To(Equal("b"))
		Expect(body).To(Equal("engine"))
		Expect(wsrc.writeCount()).To(BeZero())

		wsrc.set("w1", "c", "source two")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		label, body = engineWidget()
		Expect(label).To(Equal("c"))
		Expect(body).To(Equal("engine"))
		Expect(wsrc.writeCount()).To(BeZero())
	})

	Context("New", func() {
		build := func(fields ...string) error {
			GinkgoHelper()
			f := newFixture()
			st, err := sqlitestate.Open(filepath.Join(GinkgoT().TempDir(), "fields.db"))
			Expect(err).NotTo(HaveOccurred())
			conn, err := engineconnect.New(engineconnect.Config{
				Server:   f.srv,
				Source:   newTodoSource(),
				State:    st,
				Bindings: []engineconnect.Binding{{Prototype: &testdatav1.Widget{}, Fields: fields}},
			})
			if err != nil {
				Expect(st.Close()).To(Succeed())
				return err
			}
			return conn.Close()
		}

		It("accepts top-level fields, text fields and oneof groups", func() {
			Expect(build("label", "body", "choice", "maybe")).To(Succeed())
		})

		It("refuses a field the entity does not declare", func() {
			Expect(build("label", "nope")).To(MatchError(ContainSubstring(`"nope"`)))
		})

		It("refuses a member of a oneof", func() {
			Expect(build("choice_text")).To(MatchError(ContainSubstring(`"choice"`)))
		})
	})
})
