package connect_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/connect/sqlitestate"
	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
)

// noteSource is an in-memory Source over Note entities, whose only interesting
// field is collaborative text. A real source (cmd/mdtodo) holds the plain
// string, never the document bytes, which is exactly what Item.TextFields
// carries — so the fake stores strings too.
type noteSource struct {
	mu   sync.Mutex
	body map[string]map[string]string // ws → id → plain text
	fps  map[string]map[string]string
}

func newNoteSource() *noteSource {
	return &noteSource{
		body: make(map[string]map[string]string),
		fps:  make(map[string]map[string]string),
	}
}

// edit simulates an external editor saving a new file content.
func (s *noteSource) edit(ws, id, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.body[ws] == nil {
		s.body[ws] = make(map[string]string)
		s.fps[ws] = make(map[string]string)
	}
	s.body[ws][id] = body
	s.fps[ws][id] = fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
}

// touch rewrites the stored fingerprint without changing the body — an mtime
// bump, which is how a spec reaches the "source content is unchanged" path
// without the fingerprint short-circuit hiding it.
func (s *noteSource) touch(ws, id, fp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fps[ws][id] = fp
}

func (s *noteSource) at(ws, id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body[ws][id]
}

func (s *noteSource) List(_ context.Context, ws string, fn func(engineconnect.Item) error) error {
	s.mu.Lock()
	type entry struct{ id, body, fp string }
	var entries []entry
	for id, b := range s.body[ws] {
		entries = append(entries, entry{id, b, s.fps[ws][id]})
	}
	s.mu.Unlock()
	for _, e := range entries {
		if err := fn(engineconnect.Item{
			Msg:         &todov1.Note{Id: e.id},
			Fingerprint: e.fp,
			TextFields:  map[string]string{"body": e.body},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *noteSource) Write(_ context.Context, ws string, msg proto.Message) (string, error) {
	note, ok := msg.(*todov1.Note)
	if !ok {
		return "", fmt.Errorf("noteSource: unexpected type %T", msg)
	}
	body, err := text.PlainText(note.GetBody())
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.body[ws] == nil {
		s.body[ws] = make(map[string]string)
		s.fps[ws] = make(map[string]string)
	}
	s.body[ws][note.GetId()] = body
	fp := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	s.fps[ws][note.GetId()] = fp
	return fp, nil
}

func (s *noteSource) Delete(_ context.Context, ws string, _ proto.Message, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.body[ws], key)
	delete(s.fps[ws], key)
	return nil
}

type noteFixture struct {
	srv  *server.Server
	src  *noteSource
	st   engineconnect.State
	conn *engineconnect.Connector
}

func newNoteFixture() *noteFixture { return newNoteFixtureWriter("") }

// newNoteFixtureWriter builds a fixture whose connector runs under writerID
// (empty for a per-instance identity).
func newNoteFixtureWriter(writerID string) *noteFixture {
	GinkgoHelper()
	return newNoteFixtureOpts(func(cfg *engineconnect.Config) { cfg.WriterID = writerID })
}

// newNoteFixtureOpts builds a note fixture whose config the caller adjusts.
func newNoteFixtureOpts(opts ...func(*engineconnect.Config)) *noteFixture {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "engine.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

	srv, err := server.New(store, server.WithEntities(&todov1.Note{}))
	Expect(err).NotTo(HaveOccurred())

	st, err := sqlitestate.Open(filepath.Join(GinkgoT().TempDir(), "state.db"))
	Expect(err).NotTo(HaveOccurred())

	src := newNoteSource()
	cfg := engineconnect.Config{
		Server:   srv,
		Source:   src,
		State:    st,
		Entities: []proto.Message{&todov1.Note{}},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	conn, err := engineconnect.New(cfg)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })
	return &noteFixture{srv: srv, src: src, st: st, conn: conn}
}

// restart replaces the fixture's connector with a fresh instance over the same
// server, source and (surviving) state, as a process restart would.
func (f *noteFixture) restart(writerID string) {
	GinkgoHelper()
	conn, err := engineconnect.New(engineconnect.Config{
		Server:   f.srv,
		Source:   f.src,
		State:    f.st,
		Entities: []proto.Message{&todov1.Note{}},
		WriterID: writerID,
	})
	Expect(err).NotTo(HaveOccurred())
	f.conn = conn
}

func (f *noteFixture) shadow(ctx context.Context, ws, id string) engineconnect.Shadow {
	GinkgoHelper()
	sh, err := f.st.Shadow(ctx, ws, "note", id)
	Expect(err).NotTo(HaveOccurred())
	Expect(sh).NotTo(BeNil())
	return *sh
}

// engineDoc reads the note's decoded text document; nil when the row is gone.
func (f *noteFixture) engineDoc(ctx context.Context, ws, id string) *text.Doc {
	GinkgoHelper()
	var doc *text.Doc
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var note todov1.Note
		found, err := tx.Get(&note, id)
		if err != nil || !found {
			return err
		}
		doc, err = text.Decode(note.GetBody(), "")
		return err
	})).To(Succeed())
	return doc
}

// engineEdit makes a trusted engine-side edit under its own peer, standing in
// for a sync client typing into the same document.
func (f *noteFixture) engineEdit(ctx context.Context, ws, id, peer string, edit func(*text.Doc)) {
	GinkgoHelper()
	Expect(f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
		var note todov1.Note
		if _, err := tx.Get(&note, id); err != nil {
			return err
		}
		doc, err := text.Decode(note.GetBody(), peer)
		if err != nil {
			return err
		}
		edit(doc)
		note.Id = id
		note.Body = doc.Encode()
		return tx.Put(&note)
	})).To(Succeed())
}

var _ = Describe("Connector text fields", func() {
	const ws = "ws1"
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	It("keeps one writer identity across a run of source edits", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "one")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		for _, body := range []string{"one two", "one two three", "two three", "two three four"} {
			f.src.edit(ws, "n1", body)
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		}

		doc := f.engineDoc(ctx, ws, "n1")
		Expect(doc.PlainText()).To(Equal("two three four"))
		// A peer per edit would leave five entries here. The peer table is per
		// document and never collected, so an unbounded one is a slow leak in
		// every document a connector touches.
		Expect(doc.VersionVector()).To(HaveLen(1))
	})

	It("keeps one writer identity across a restart when WriterID is pinned", func() {
		f := newNoteFixtureWriter("mdtodo")
		f.src.edit(ws, "n1", "one")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.restart("mdtodo")
		f.src.edit(ws, "n1", "one two")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		doc := f.engineDoc(ctx, ws, "n1")
		Expect(doc.PlainText()).To(Equal("one two"))
		Expect(doc.VersionVector()).To(HaveLen(1))
	})

	// The cost of leaving WriterID empty, pinned so it stays a deliberate
	// trade-off: one dead peer per run, in every document the connector wrote.
	It("costs one entry per restart without a pinned WriterID", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "one")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		f.restart("")
		f.src.edit(ws, "n1", "one two")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		Expect(f.engineDoc(ctx, ws, "n1").VersionVector()).To(HaveLen(2))
	})

	It("merges a source edit with a concurrent engine-side edit", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "hello")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		// Both sides move from "hello" without seeing the other.
		f.engineEdit(ctx, ws, "n1", "client-1", func(d *text.Doc) {
			Expect(d.Insert(5, " world")).To(Succeed())
		})
		f.src.edit(ws, "n1", "oh hello")

		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("oh hello world"))

		// And the source is told about the merged result.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.at(ws, "n1")).To(Equal("oh hello world"))
	})

	// The connector keeps one peer for its lifetime, which is only sound
	// because it allocates past the CURRENT document rather than past the
	// ancestor it diffs against. A crash between the engine write and the
	// shadow update leaves exactly that gap: the engine holds writes under this
	// peer that the ancestor does not know about.
	It("survives an ancestor that is behind its own last write", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "one")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		stale := f.shadow(ctx, ws, "n1")

		f.src.edit(ws, "n1", "one two")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("one two"))

		// Rewind the shadow to before that write, as a crash before PutShadow
		// would have left it.
		Expect(f.st.PutShadow(ctx, ws, "note", "n1", stale)).To(Succeed())

		f.src.edit(ws, "n1", "one XYZ")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		// Both survive: the engine's " two" (which the ancestor never saw) and
		// the source's " XYZ".
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("one two XYZ"))
	})

	// Shadows written before Shadow.BaseText existed carry none, and are
	// bidirectional by construction — so the base document's own rendering is
	// what the source last held, and decoding it must keep working.
	It("reads a pre-BaseText shadow by decoding its base document", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "hello")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())

		sh := f.shadow(ctx, ws, "n1")
		Expect(sh.BaseText).To(HaveKeyWithValue("body", "hello"))
		sh.BaseText = nil
		Expect(f.st.PutShadow(ctx, ws, "note", "n1", sh)).To(Succeed())

		// An unchanged body must still read as unchanged...
		f.src.touch(ws, "n1", "restamped")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("hello"))

		// ...and a changed one still merges.
		f.src.edit(ws, "n1", "hello there")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.engineDoc(ctx, ws, "n1").PlainText()).To(Equal("hello there"))
	})

	It("converges when the source and the engine edit repeatedly in turn", func() {
		f := newNoteFixture()
		f.src.edit(ws, "n1", "a")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())

		for i := 0; i < 5; i++ {
			f.engineEdit(ctx, ws, "n1", "client-1", func(d *text.Doc) {
				Expect(d.Insert(d.Len(), "E")).To(Succeed())
			})
			Expect(f.conn.Drain(ctx, ws)).To(Succeed())
			f.src.edit(ws, "n1", f.src.at(ws, "n1")+"S")
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		}

		doc := f.engineDoc(ctx, ws, "n1")
		Expect(doc.PlainText()).To(Equal("aESESESESES"))
		// One connector peer plus the one engine-side peer.
		Expect(doc.VersionVector()).To(HaveLen(2))
	})

	// A file the source cannot decode (latin-1, a truncated read) has no
	// faithful document: storing it would substitute U+FFFD, which never
	// compares equal to the source again, so every later pass would see the row
	// as changed and rewrite it forever. It is skipped instead — and skipped
	// per item, because a whole source must not stop ingressing over one file.
	It("skips a source item whose text is not valid UTF-8, without stalling the pass", func() {
		f := newNoteFixture()
		f.src.edit(ws, "good", "hello")
		f.src.edit(ws, "bad", "caf\xe9 latte")

		for i := 0; i < 3; i++ {
			Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		}

		Expect(f.engineDoc(ctx, ws, "good").PlainText()).To(Equal("hello"))
		Expect(f.engineDoc(ctx, ws, "bad")).To(BeNil(), "no row may be written from undecodable text")
		sh, err := f.st.Shadow(ctx, ws, "note", "bad")
		Expect(err).NotTo(HaveOccurred())
		Expect(sh).To(BeNil(), "no shadow may claim the item converged")

		// Skipping is not deleting: the item is still listed by the source, so
		// the deletion sweep must leave the (absent) row alone and the good row
		// must survive every pass.
		Expect(f.conn.Drain(ctx, ws)).To(Succeed())
		Expect(f.src.at(ws, "good")).To(Equal("hello"))

		// Fixing the encoding converges it on the next pass, with no trace of
		// the passes that skipped it.
		f.src.edit(ws, "bad", "café latte")
		Expect(f.conn.Reconcile(ctx, ws)).To(Succeed())
		doc := f.engineDoc(ctx, ws, "bad")
		Expect(doc.PlainText()).To(Equal("café latte"))
		Expect(doc.VersionVector()).To(HaveLen(1))
	})
})
