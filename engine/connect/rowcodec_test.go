package connect_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/server"
)

// rowLines is the record-mode test adapter of lineCodec rebuilt on
// RowCodec: a text file is a record, its first line the Project's name, and
// each further line a Widget whose body holds the line and whose count is
// its place. Widget.flag is a field the file cannot hold, so the codec owns
// only body and count of a Widget (and label, its record field).
type rowLines struct {
	mu sync.Mutex
	// refuse makes Decode refuse content that holds it.
	refuse string
	// pin gives the line with this text this row key, as a format that
	// holds row ids would.
	pin map[string]string
	// foreign makes Decode put a line with this text in record "elsewhere".
	foreign string
}

func (l *rowLines) codec() engineconnect.RowCodec {
	return engineconnect.RowCodec{
		Decode: l.decode,
		Encode: l.encode,
		Fields: map[proto.Message][]string{&testdatav1.Widget{}: {"body", "count"}},
	}
}

func (l *rowLines) set(fn func(l *rowLines)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(l)
}

// byPlace sorts widgets by count, then by key.
func byPlace(members []proto.Message) []*testdatav1.Widget {
	out := make([]*testdatav1.Widget, 0, len(members))
	for _, m := range members {
		out = append(out, m.(*testdatav1.Widget))
	}
	slices.SortFunc(out, func(a, b *testdatav1.Widget) int {
		if a.GetCount() != b.GetCount() {
			return int(a.GetCount() - b.GetCount())
		}
		return strings.Compare(a.GetId(), b.GetId())
	})
	return out
}

func (l *rowLines) decode(in engineconnect.DecodeInput) (engineconnect.Decoded, error) {
	l.mu.Lock()
	refuse, pin, foreign := l.refuse, maps.Clone(l.pin), l.foreign
	l.mu.Unlock()
	if refuse != "" && strings.Contains(string(in.Record.Content), refuse) {
		return engineconnect.Decoded{}, errors.New("refuses " + refuse)
	}
	name, lines := parseLines(in.Record.Content)
	prev := in.Base
	if prev.Root == nil {
		prev = in.Rows
	}
	var texts, keys []string
	for _, w := range byPlace(prev.Members) {
		texts, keys = append(texts, bodyOf(w)), append(keys, w.GetId())
	}
	matched := engineconnect.MatchKeys(texts, keys, lines)
	used := make(map[string]bool, len(matched))
	for _, k := range matched {
		used[k] = true
	}
	out := engineconnect.Decoded{Root: &testdatav1.Project{Name: name}}
	for i, line := range lines {
		k := matched[i]
		if p, ok := pin[line]; ok {
			k = p
		}
		if k == "" {
			// A row an earlier merge of this content wrote.
			for _, w := range byPlace(in.Rows.Members) {
				if !used[w.GetId()] && bodyOf(w) == line {
					k = w.GetId()
					break
				}
			}
		}
		if k == "" {
			k = id.New()
		}
		used[k] = true
		w := &testdatav1.Widget{Id: k, Count: int32(i)}
		if line == foreign {
			w.Label = "elsewhere"
		}
		out.Members = append(out.Members, w)
		out.SetText(w, "body", line)
	}
	return out, nil
}

func (l *rowLines) encode(in engineconnect.EncodeInput) (engineconnect.Rendered, error) {
	root := in.Rows.Root.(*testdatav1.Project)
	parts := []string{root.GetName()}
	for _, w := range byPlace(in.Rows.Members) {
		s, err := engineconnect.PlainTextField(w, "body")
		if err != nil {
			return engineconnect.Rendered{}, err
		}
		parts = append(parts, s)
	}
	out := engineconnect.Rendered{Content: []byte(strings.Join(parts, "\n"))}
	if in.Locator == "" {
		out.Locator = root.GetName() + ".txt"
	}
	return out, nil
}

// keyedStore is a memStore whose records name their key, as a store that
// keeps an id in each record does.
type keyedStore struct {
	*memStore
	keys map[string]string // locator → key
}

func (s keyedStore) List(ctx context.Context, ws string, fn func(engineconnect.RecordRef) error) error {
	return s.memStore.List(ctx, ws, func(ref engineconnect.RecordRef) error {
		ref.Key = s.keys[ref.Locator]
		return fn(ref)
	})
}

func (s keyedStore) Read(ctx context.Context, ws, loc string) (engineconnect.Record, error) {
	rec, err := s.memStore.Read(ctx, ws, loc)
	rec.Key = s.keys[loc]
	return rec, err
}

func newRowFixture(opts ...func(*engineconnect.Config)) (*recFixture, *rowLines) {
	GinkgoHelper()
	l := &rowLines{}
	use := func(cfg *engineconnect.Config) { cfg.Records.Types[0].Codec = l.codec() }
	return newRecFixture(append([]func(*engineconnect.Config){use}, opts...)...), l
}

var _ = Describe("RowCodec", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	setFlag := func(f *recFixture, widget string) {
		GinkgoHelper()
		f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
			var w testdatav1.Widget
			if _, err := tx.Get(&w, widget); err != nil {
				return err
			}
			w.Flag = true
			return tx.Put(&w)
		})
	}
	widget := func(f *recFixture, key string) *testdatav1.Widget {
		GinkgoHelper()
		var w testdatav1.Widget
		var found bool
		Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&w, key)
			return err
		})).To(Succeed())
		if !found {
			return nil
		}
		return &w
	}

	Describe("New", func() {
		build := func(c engineconnect.RowCodec) error {
			f := newRecFixture()
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{
					Root:    &testdatav1.Project{},
					Members: []engineconnect.Member{{Prototype: &testdatav1.Widget{}, RecordField: "label"}},
					Store:   f.store,
					Codec:   c,
				}},
			}})
			return err
		}

		It("refuses a RowCodec with no Decode or no Encode", func() {
			l := &rowLines{}
			Expect(build(engineconnect.RowCodec{Encode: l.encode})).To(MatchError(ContainSubstring("Decode")))
			Expect(build(engineconnect.RowCodec{Decode: l.decode})).To(MatchError(ContainSubstring("Encode")))
		})

		It("refuses Fields of an entity outside the type, or of a field the entity does not declare", func() {
			l := &rowLines{}
			c := l.codec()
			f := newRecFixture()
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{{Root: &testdatav1.Project{}, Store: f.store, Codec: c}},
			}})
			Expect(err).To(MatchError(ContainSubstring(`"widget" is not in the record type`)))
			c.Fields = map[proto.Message][]string{&testdatav1.Widget{}: {"nope"}}
			Expect(build(c)).To(MatchError(ContainSubstring("nope")))
		})

		It("refuses a call outside a record type", func() {
			_, err := engineconnect.RowCodec{}.Merge(nil, engineconnect.MergeInput{})
			Expect(err).To(HaveOccurred())
		})
	})

	It("imports a record, and writes nothing for its echo", func() {
		f, _ := newRowFixture()
		f.store.put(ws, "a.txt", "T\none\ntwo")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		Expect(f.lines(ctx, ws, key)).To(Equal("T\none\ntwo"))
		seq := f.seq(ctx, ws)

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.seq(ctx, ws)).To(Equal(seq))
		Expect(f.store.writeCount()).To(BeZero())

		f.editLine(ctx, ws, f.widgetWith(ctx, ws, key, "one"), "uno")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "a.txt")).To(Equal("T\nuno\ntwo"))
		seq = f.seq(ctx, ws)

		// The same content with another fingerprint is read and merged, and
		// still writes nothing.
		f.store.put(ws, "a.txt", "T\nuno\ntwo")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.seq(ctx, ws)).To(Equal(seq))
		Expect(f.store.writeCount()).To(Equal(1))
	})

	It("writes the rows of a new engine record to the store", func() {
		f, _ := newRowFixture()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		f.addRecord(ctx, ws, "p1", "Plan", "one", "two")
		Expect(f.conn.Flush(ctx, ws)).To(Succeed())
		Expect(f.content(ws, "Plan.txt")).To(Equal("Plan\none\ntwo"))

		seq := f.seq(ctx, ws)
		f.store.put(ws, "Plan.txt", "Plan\none\ntwo!")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.widgetKeys(ctx, ws, "p1")).To(Equal([]string{"p1-w0", "p1-w1"}), "the lines kept their rows")
		Expect(f.lines(ctx, ws, "p1")).To(Equal("Plan\none\ntwo!"))
		Expect(f.seq(ctx, ws)).To(Equal(seq + 1))
	})

	Describe("fields the record does not own", func() {
		It("keep their engine value when the record changes", func() {
			f, _ := newRowFixture()
			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			one := f.widgetWith(ctx, ws, key, "one")
			setFlag(f, one)

			f.store.put(ws, "a.txt", "T\none!\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(bodyOf(widget(f, one))).To(Equal("one!"))
			Expect(widget(f, one).GetFlag()).To(BeTrue())
		})

		keyedFixture := func(keys map[string]string) *recFixture {
			GinkgoHelper()
			keyed := keyedStore{memStore: newMemStore(), keys: keys}
			f, _ := newRowFixture(func(cfg *engineconnect.Config) { cfg.Records.Types[0].Store = keyed })
			f.store = keyed.memStore
			return f
		}

		It("keep their engine value on a first import over existing rows", func() {
			f := keyedFixture(map[string]string{"a.txt": "p1"})
			f.addRecord(ctx, ws, "p1", "T", "one", "two")
			setFlag(f, "p1-w0")

			f.store.put(ws, "a.txt", "T\none!")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.widgetKeys(ctx, ws, "p1")).To(Equal([]string{"p1-w0"}), "the record wins over the rows it does not hold")
			Expect(bodyOf(widget(f, "p1-w0"))).To(Equal("one!"))
			Expect(widget(f, "p1-w0").GetFlag()).To(BeTrue())
		})

		It("are not written on a first import over equal rows", func() {
			f := keyedFixture(map[string]string{"a.txt": "p1"})
			f.addRecord(ctx, ws, "p1", "T", "one", "two")
			setFlag(f, "p1-w0")
			seq := f.seq(ctx, ws)

			f.store.put(ws, "a.txt", "T\none\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.seq(ctx, ws)).To(Equal(seq))
			Expect(f.store.writeCount()).To(BeZero())
			Expect(widget(f, "p1-w0").GetFlag()).To(BeTrue())
		})
	})

	Describe("collaborative text", func() {
		It("keeps a store edit and an engine edit of the same line", func() {
			f, _ := newRowFixture()
			f.store.put(ws, "a.txt", "T\nhello world")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			w := f.widgetWith(ctx, ws, key, "hello world")

			f.editLine(ctx, ws, w, "hello brave world")
			f.store.put(ws, "a.txt", "T\nhello world!")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(bodyOf(widget(f, w))).To(Equal("hello brave world!"))
			Expect(f.content(ws, "a.txt")).To(Equal("T\nhello brave world!"), "the merge was written back")

			writes, seq := f.store.writeCount(), f.seq(ctx, ws)
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.store.writeCount()).To(Equal(writes))
			Expect(f.seq(ctx, ws)).To(Equal(seq))
		})

		It("keeps an engine edit whose write-back failed when the store changes again", func() {
			f, _ := newRowFixture()
			f.store.put(ws, "a.txt", "T\nhello world")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			w := f.widgetWith(ctx, ws, key, "hello world")

			f.editLine(ctx, ws, w, "hello brave world")
			f.store.put(ws, "a.txt", "T\nhello world!")
			f.store.fail(nil, errors.New("disk full"))
			Expect(f.conn.Reconcile(ctx, ws)).NotTo(Succeed())
			Expect(bodyOf(widget(f, w))).To(Equal("hello brave world!"))

			f.store.fail(nil, nil)
			f.store.put(ws, "a.txt", "T\nhello world!?")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(bodyOf(widget(f, w))).To(Equal("hello brave world!?"))
			Expect(f.content(ws, "a.txt")).To(Equal("T\nhello brave world!?"))
		})

		It("keeps an engine edit an IngressOnly store never receives when the store changes again", func() {
			f, _ := newRowFixture(withRecords(func(r *engineconnect.Records) { r.Types[0].Direction = engineconnect.IngressOnly }))
			f.store.put(ws, "a.txt", "T\nhello world")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			key := f.key(ctx, ws, "a.txt")
			w := f.widgetWith(ctx, ws, key, "hello world")

			f.editLine(ctx, ws, w, "hello brave world")
			f.store.put(ws, "a.txt", "T\nhello world!")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(bodyOf(widget(f, w))).To(Equal("hello brave world!"))

			// The agreed text is the store's, so the store's text missing
			// "brave" is no deletion of it.
			f.store.put(ws, "a.txt", "T\nhello world!?")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(bodyOf(widget(f, w))).To(Equal("hello brave world!?"))
			Expect(f.content(ws, "a.txt")).To(Equal("T\nhello world!?"))
			Expect(f.store.writeCount()).To(BeZero())
		})
	})

	It("deletes the row of a line the store deleted", func() {
		f, _ := newRowFixture()
		f.store.put(ws, "a.txt", "T\none\ntwo\nthree")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		two := f.widgetWith(ctx, ws, key, "two")
		setFlag(f, two)

		f.store.put(ws, "a.txt", "T\none\nthree")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(widget(f, two)).To(BeNil())
		Expect(f.lines(ctx, ws, key)).To(Equal("T\none\nthree"))
		Expect(f.content(ws, "a.txt")).To(Equal("T\none\nthree"))
	})

	It("keeps a line a client added and writes it to the store", func() {
		f, _ := newRowFixture()
		f.store.put(ws, "a.txt", "T\none\ntwo")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")

		f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
			w := &testdatav1.Widget{Id: "c1", Label: key, Count: 5}
			if err := server.ApplyTextStrings(w, nil, nil, map[string]string{"body": "new"}, "client-1"); err != nil {
				return err
			}
			return tx.Put(w)
		})
		f.store.put(ws, "a.txt", "T\none\ntwo!")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.lines(ctx, ws, key)).To(Equal("T\none\ntwo!\nnew"))
		Expect(f.content(ws, "a.txt")).To(Equal("T\none\ntwo!\nnew"))

		f.store.put(ws, "a.txt", "T\none\ntwo!\nnew!")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.widgetWith(ctx, ws, key, "new!")).To(Equal("c1"), "the written line keeps its row")
	})

	It("writes a line a client deleted out of the store, unless the store changed it", func() {
		f, _ := newRowFixture()
		f.store.put(ws, "a.txt", "T\none\ntwo\nthree")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		two, three := f.widgetWith(ctx, ws, key, "two"), f.widgetWith(ctx, ws, key, "three")
		f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
			if err := tx.Delete(&testdatav1.Widget{}, two); err != nil {
				return err
			}
			return tx.Delete(&testdatav1.Widget{}, three)
		})

		f.store.put(ws, "a.txt", "T\none!\ntwo\nthree!")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(widget(f, two)).To(BeNil())
		Expect(f.lines(ctx, ws, key)).To(Equal("T\none!\nthree!"))
		Expect(f.content(ws, "a.txt")).To(Equal("T\none!\nthree!"))
	})

	Describe("rows that move between records", func() {
		It("writes both records when a client moves a row", func() {
			f, _ := newRowFixture()
			f.store.put(ws, "a.txt", "A\none\ntwo")
			f.store.put(ws, "b.txt", "B\nthree")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a, b := f.key(ctx, ws, "a.txt"), f.key(ctx, ws, "b.txt")
			two := f.widgetWith(ctx, ws, a, "two")

			f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
				var w testdatav1.Widget
				if _, err := tx.Get(&w, two); err != nil {
					return err
				}
				w.Label = b
				return tx.Put(&w)
			})
			// The store changes the record the row left, which still holds
			// the line: the move wins, as the store did not change the line.
			f.store.put(ws, "a.txt", "A\none!\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.content(ws, "a.txt")).To(Equal("A\none!"))
			Expect(f.content(ws, "b.txt")).To(Equal("B\nthree\ntwo"))
			Expect(widget(f, two).GetLabel()).To(Equal(b))
		})

		It("moves a row whose key the store names to the record that holds it", func() {
			f, l := newRowFixture()
			f.store.put(ws, "a.txt", "A\none\ntwo")
			f.store.put(ws, "b.txt", "B\nthree")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			a, b := f.key(ctx, ws, "a.txt"), f.key(ctx, ws, "b.txt")
			one := f.widgetWith(ctx, ws, a, "one")
			setFlag(f, one)

			l.set(func(l *rowLines) { l.pin = map[string]string{"one": one} })
			f.store.put(ws, "b.txt", "B\nthree\none")
			f.store.put(ws, "a.txt", "A\ntwo")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
			Expect(f.conn.Flush(ctx, ws)).To(Succeed())
			Expect(f.lines(ctx, ws, a)).To(Equal("A\ntwo"))
			Expect(f.lines(ctx, ws, b)).To(Equal("B\nthree\none"))
			Expect(widget(f, one).GetLabel()).To(Equal(b))
			Expect(f.content(ws, "a.txt")).To(Equal("A\ntwo"))
			Expect(f.content(ws, "b.txt")).To(Equal("B\nthree\none"))
		})
	})

	It("refuses a record whose row names another record", func() {
		f, l := newRowFixture()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		l.set(func(l *rowLines) { l.foreign = "stray" })
		f.store.put(ws, "a.txt", "A\none\nstray")
		f.store.put(ws, "b.txt", "B\nfine")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.key(ctx, ws, "a.txt")).To(BeEmpty())
		Expect(f.key(ctx, ws, "b.txt")).NotTo(BeEmpty())
		h, _ := f.conn.Health(ws)
		Expect(h.Refused).To(ContainElement(HaveField("Err", MatchError(ContainSubstring(`record "elsewhere"`)))))
	})

	It("skips a record whose Decode refuses it, and imports it once it decodes", func() {
		f, l := newRowFixture()
		l.set(func(l *rowLines) { l.refuse = "boom" })
		f.store.put(ws, "a.txt", "A\none")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		seq := f.seq(ctx, ws)

		f.store.put(ws, "a.txt", "A\none\nboom")
		f.store.put(ws, "b.txt", "B\nfine")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.lines(ctx, ws, key)).To(Equal("A\none"))
		Expect(f.lines(ctx, ws, f.key(ctx, ws, "b.txt"))).To(Equal("B\nfine"))
		Expect(f.seq(ctx, ws)).To(Equal(seq + 1))

		l.set(func(l *rowLines) { l.refuse = "" })
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.lines(ctx, ws, key)).To(Equal("A\none\nboom"))
	})

	It("writes nothing when it replays a merge whose shadow was lost in a crash", func() {
		f, _ := newRowFixture()
		f.store.put(ws, "a.txt", "T\none\ntwo")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		key := f.key(ctx, ws, "a.txt")
		before := snapshotShadows(ctx, f, ws)

		f.store.put(ws, "a.txt", "T\nONE\ntwo\nthree")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		rows := f.widgetKeys(ctx, ws, key)
		restoreShadows(ctx, f, ws, before)

		seq, writes := f.seq(ctx, ws), f.store.writeCount()
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.seq(ctx, ws)).To(Equal(seq))
		Expect(f.store.writeCount()).To(Equal(writes))
		Expect(f.widgetKeys(ctx, ws, key)).To(Equal(rows))
	})
})

var _ = DescribeTable("MatchKeys",
	func(prev, next []string, want []string) {
		keys := make([]string, len(prev))
		for i := range prev {
			keys[i] = fmt.Sprintf("k%d", i)
		}
		Expect(engineconnect.MatchKeys(prev, keys, next)).To(Equal(want))
	},
	Entry("unchanged", []string{"a", "b"}, []string{"a", "b"}, []string{"k0", "k1"}),
	Entry("an insert", []string{"a", "b"}, []string{"a", "x", "b"}, []string{"k0", "", "k1"}),
	Entry("a delete", []string{"a", "b", "c"}, []string{"a", "c"}, []string{"k0", "k2"}),
	Entry("an edit keeps the key", []string{"a", "b", "c"}, []string{"a", "B", "c"}, []string{"k0", "k1", "k2"}),
	Entry("an edit and an insert in one run", []string{"a", "b", "c"}, []string{"a", "B", "x", "c"}, []string{"k0", "k1", "", "k2"}),
	Entry("a move takes a new key", []string{"a", "b", "c"}, []string{"b", "c", "a"}, []string{"k1", "k2", ""}),
	Entry("repeated parts", []string{"a", "a"}, []string{"a", "a", "a"}, []string{"k0", "k1", ""}),
	Entry("an edit in the middle of a long run", []string{"a", "b", "c", "d", "e"}, []string{"a", "b", "C", "d", "e"}, []string{"k0", "k1", "k2", "k3", "k4"}),
	Entry("from nothing", []string{}, []string{"a"}, []string{""}),
	Entry("to nothing", []string{"a"}, []string{}, []string{}),
)

var _ = It("MatchKeys panics when the parts and the keys differ in length", func() {
	Expect(func() { engineconnect.MatchKeys([]string{"a"}, nil, []string{"a"}) }).To(Panic())
})
