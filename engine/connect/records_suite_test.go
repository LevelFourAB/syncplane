package connect_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/id"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
)

// The record-mode test adapter: a text file is a record, and each line is a
// row. The first line is the record's title, held by a Project root row; each
// further line is a Widget member row, whose label holds the project key,
// whose body (a collaborative-text field) holds the line, and whose count is
// the line's position. Widget.flag is a field the file cannot hold. A record
// the engine creates goes to "<title>.txt".

// memStore is an in-memory RecordStore: locator → content, fingerprint =
// content hash. Specs edit it directly to stand in for another program.
type memStore struct {
	mu        sync.Mutex
	files     map[string]map[string]string // ws → locator → content
	writes    int
	deletes   int
	reads     int
	failRead  error
	failWrite error
	// beforeWrite, when set, runs at the start of each Write, outside the
	// lock. A spec uses it to act at the moment between the connector's
	// bookkeeping and the store write.
	beforeWrite func(ctx context.Context, rec engineconnect.Record, from string) error
	// beforeDelete, when set, runs at the start of each Delete, outside the
	// lock, as beforeWrite does for Write.
	beforeDelete func(ctx context.Context, loc string)
	// listTwice makes List yield each record twice, as a paged listing that
	// shifts between pages does.
	listTwice bool
}

func newMemStore() *memStore { return &memStore{files: make(map[string]map[string]string)} }

func hashOf(content string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
}

func (s *memStore) put(ws, loc, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files[ws] == nil {
		s.files[ws] = make(map[string]string)
	}
	s.files[ws][loc] = content
}

func (s *memStore) remove(ws, loc string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files[ws], loc)
}

func (s *memStore) rename(ws, from, to string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[ws][to] = s.files[ws][from]
	delete(s.files[ws], from)
}

func (s *memStore) get(ws, loc string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.files[ws][loc]
	return c, ok
}

func (s *memStore) locators(ws string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for loc := range s.files[ws] {
		out = append(out, loc)
	}
	sort.Strings(out)
	return out
}

func (s *memStore) setBeforeWrite(fn func(ctx context.Context, rec engineconnect.Record, from string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeWrite = fn
}

func (s *memStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *memStore) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *memStore) fail(read, write error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRead, s.failWrite = read, write
}

func (s *memStore) List(_ context.Context, ws string, fn func(engineconnect.RecordRef) error) error {
	s.mu.Lock()
	if s.failRead != nil {
		s.mu.Unlock()
		return s.failRead
	}
	var refs []engineconnect.RecordRef
	for loc, c := range s.files[ws] {
		refs = append(refs, engineconnect.RecordRef{Locator: loc, Fingerprint: hashOf(c)})
	}
	twice := s.listTwice
	s.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].Locator < refs[j].Locator })
	if twice {
		refs = append(refs, refs...)
	}
	for _, ref := range refs {
		if err := fn(ref); err != nil {
			return err
		}
	}
	return nil
}

func (s *memStore) Read(_ context.Context, ws, loc string) (engineconnect.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.failRead != nil {
		return engineconnect.Record{}, s.failRead
	}
	c, ok := s.files[ws][loc]
	if !ok {
		return engineconnect.Record{}, fmt.Errorf("%s: %w", loc, engineconnect.ErrRecordNotFound)
	}
	return engineconnect.Record{
		RecordRef: engineconnect.RecordRef{Locator: loc, Fingerprint: hashOf(c)},
		Content:   []byte(c),
	}, nil
}

func (s *memStore) Write(ctx context.Context, ws string, rec engineconnect.Record, from string) (string, error) {
	s.mu.Lock()
	hook := s.beforeWrite
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, rec, from); err != nil {
			return "", err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWrite != nil {
		return "", s.failWrite
	}
	if s.files[ws] == nil {
		s.files[ws] = make(map[string]string)
	}
	s.writes++
	s.files[ws][rec.Locator] = string(rec.Content)
	if from != "" && from != rec.Locator {
		delete(s.files[ws], from)
	}
	return hashOf(string(rec.Content)), nil
}

func (s *memStore) Delete(ctx context.Context, ws, loc string) error {
	s.mu.Lock()
	hook := s.beforeDelete
	s.mu.Unlock()
	if hook != nil {
		hook(ctx, loc)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	delete(s.files[ws], loc)
	return nil
}

// lineCodec maps a text file to a Project and its Widgets. Lines keep their
// row keys through the payload: a line matches the agreed line it equals
// (longest common subsequence), then the agreed line at the same place in a
// changed run, then a row with the same text that the agreed state does not
// hold yet (the rows of a merge that committed before its shadow).
type lineCodec struct {
	mu sync.Mutex
	// failOn makes Merge fail, after its writes, for content that holds it.
	failOn string
	// failRender makes Render fail for a root whose name holds it.
	failRender string
	// merges and renders count the calls.
	merges, renders int
	// nameFiles names every record after its title, not only a new one, so
	// that an engine edit of the title moves the record.
	nameFiles bool
}

type linePayload struct {
	Name  string     `json:"name"`
	Lines []lineItem `json:"lines"`
}

type lineItem struct {
	Key  string `json:"key"`
	Text string `json:"text"`
}

func (c *lineCodec) setFailOn(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failOn = s
}

func (c *lineCodec) setFailRender(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failRender = s
}

// calls returns how many times Merge and Render ran.
func (c *lineCodec) calls() (merges, renders int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.merges, c.renders
}

func parseLines(content []byte) (string, []string) {
	if len(content) == 0 {
		return "", nil
	}
	parts := strings.Split(string(content), "\n")
	return parts[0], parts[1:]
}

func bodyOf(w *testdatav1.Widget) string {
	s, err := text.PlainText(w.GetBody())
	if err != nil {
		panic(err)
	}
	return s
}

// matchLines gives each new line a row key.
func matchLines(base []lineItem, lines []string, cur map[string]*testdatav1.Widget) []string {
	keys := make([]string, len(lines))
	// Longest common subsequence of the texts.
	n, m := len(base), len(lines)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if base[i].Text == lines[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	used := make(map[string]bool)
	pair := func(bs, ls []int) {
		for k := 0; k < len(bs) && k < len(ls); k++ {
			keys[ls[k]] = base[bs[k]].Key
			used[base[bs[k]].Key] = true
		}
	}
	var gapB, gapL []int
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case base[i].Text == lines[j]:
			pair(gapB, gapL)
			gapB, gapL = nil, nil
			keys[j] = base[i].Key
			used[base[i].Key] = true
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			gapB = append(gapB, i)
			i++
		default:
			gapL = append(gapL, j)
			j++
		}
	}
	for ; i < n; i++ {
		gapB = append(gapB, i)
	}
	for ; j < m; j++ {
		gapL = append(gapL, j)
	}
	pair(gapB, gapL)
	inBase := make(map[string]bool, len(base))
	for _, b := range base {
		inBase[b.Key] = true
	}
	rowKeys := make([]string, 0, len(cur))
	for k := range cur {
		rowKeys = append(rowKeys, k)
	}
	sort.Strings(rowKeys)
	for li, text := range lines {
		if keys[li] != "" {
			continue
		}
		for _, k := range rowKeys {
			if !inBase[k] && !used[k] && bodyOf(cur[k]) == text {
				keys[li] = k
				used[k] = true
				break
			}
		}
		if keys[li] == "" {
			keys[li] = id.New()
		}
	}
	return keys
}

func (c *lineCodec) Merge(tx *server.EntityTx, in engineconnect.MergeInput) (engineconnect.Merged, error) {
	c.mu.Lock()
	c.merges++
	c.mu.Unlock()
	name, lines := parseLines(in.Record.Content)
	var base linePayload
	if in.Base != nil {
		if err := json.Unmarshal(in.Base.Payload, &base); err != nil {
			return engineconnect.Merged{}, err
		}
	}
	root := &testdatav1.Project{Id: in.Key}
	if in.Root != nil {
		root = proto.Clone(in.Root).(*testdatav1.Project)
	}
	if in.Base == nil || name != base.Name {
		root.Name = name
	}
	if in.Root == nil || !proto.Equal(root, in.Root) {
		if err := tx.Put(root); err != nil {
			return engineconnect.Merged{}, err
		}
	}
	cur := make(map[string]*testdatav1.Widget, len(in.Members))
	for _, m := range in.Members {
		w := m.(*testdatav1.Widget)
		cur[w.GetId()] = w
	}
	baseText := make(map[string]string, len(base.Lines))
	for _, b := range base.Lines {
		baseText[b.Key] = b.Text
	}
	keys := matchLines(base.Lines, lines, cur)
	kept := make(map[string]bool, len(keys))
	payload := linePayload{Name: name}
	for i, line := range lines {
		k := keys[i]
		kept[k] = true
		payload.Lines = append(payload.Lines, lineItem{Key: k, Text: line})
		w, ok := cur[k]
		want := &testdatav1.Widget{Id: k, Label: in.Key}
		if ok {
			want = proto.Clone(w).(*testdatav1.Widget)
		}
		want.Count = int32(i)
		// A line the file did not change keeps the engine's text.
		if was, inBase := baseText[k]; !ok || !inBase || was != line {
			var curRow *testdatav1.Widget
			if ok {
				curRow = w
			}
			if !ok || bodyOf(w) != line {
				if err := server.ApplyTextStrings(want, nil, curRow, map[string]string{"body": line}, in.TextPeer); err != nil {
					return engineconnect.Merged{}, err
				}
			}
		}
		if !ok || !proto.Equal(want, w) {
			if err := tx.Put(want); err != nil {
				return engineconnect.Merged{}, err
			}
		}
	}
	for k, w := range cur {
		if kept[k] {
			continue
		}
		// With a base, a row the base does not hold is an engine addition
		// and stays; with none, the file wins.
		if _, inBase := baseText[k]; in.Base != nil && !inBase {
			continue
		}
		if err := tx.Delete(w, k); err != nil {
			return engineconnect.Merged{}, err
		}
	}
	c.mu.Lock()
	failOn := c.failOn
	c.mu.Unlock()
	if failOn != "" && strings.Contains(string(in.Record.Content), failOn) {
		return engineconnect.Merged{}, errors.New("codec refuses " + failOn)
	}
	raw, err := json.Marshal(payload)
	return engineconnect.Merged{Payload: raw}, err
}

func (c *lineCodec) Render(_ *server.EntityTx, in engineconnect.RenderInput) (engineconnect.Rendered, bool, error) {
	root := in.Root.(*testdatav1.Project)
	c.mu.Lock()
	c.renders++
	failRender, nameFiles := c.failRender, c.nameFiles
	c.mu.Unlock()
	if failRender != "" && strings.Contains(root.GetName(), failRender) {
		return engineconnect.Rendered{}, false, errors.New("codec cannot render " + root.GetName())
	}
	ws := make([]*testdatav1.Widget, 0, len(in.Members))
	for _, m := range in.Members {
		ws = append(ws, m.(*testdatav1.Widget))
	}
	slices.SortFunc(ws, func(a, b *testdatav1.Widget) int {
		if a.GetCount() != b.GetCount() {
			return int(a.GetCount() - b.GetCount())
		}
		return strings.Compare(a.GetId(), b.GetId())
	})
	payload := linePayload{Name: root.GetName()}
	parts := []string{root.GetName()}
	for _, w := range ws {
		parts = append(parts, bodyOf(w))
		payload.Lines = append(payload.Lines, lineItem{Key: w.GetId(), Text: bodyOf(w)})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return engineconnect.Rendered{}, false, err
	}
	out := engineconnect.Rendered{Content: []byte(strings.Join(parts, "\n")), Payload: raw}
	if in.Base == nil || nameFiles {
		// A new record is named after its title, so a rebuild finds the
		// file a title already has.
		name := root.GetName()
		if name == "" {
			name = in.Key
		}
		out.Locator = name + ".txt"
	}
	return out, true, nil
}

// fakeClock is a settable clock for quiet periods and delete grace.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const recWriter = "files"

// recFixture is a record-mode connector over a fresh engine, a memStore and a
// lineCodec.
type recFixture struct {
	srv   *server.Server
	store *memStore
	codec *lineCodec
	clock *fakeClock
	conn  *engineconnect.Connector
	opts  []func(*engineconnect.Config)
}

func newRecFixture(opts ...func(*engineconnect.Config)) *recFixture {
	GinkgoHelper()
	return newRecFixtureFull(nil, opts...)
}

func newRecFixtureFull(srvOpts []server.Option, opts ...func(*engineconnect.Config)) *recFixture {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "engine.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	srv, err := server.New(store, append(srvOpts,
		server.WithEntities(&testdatav1.Project{}, &testdatav1.Widget{}))...)
	Expect(err).NotTo(HaveOccurred())
	f := &recFixture{
		srv:   srv,
		store: newMemStore(),
		codec: &lineCodec{},
		clock: &fakeClock{t: time.Unix(1_700_000_000, 0)},
		opts:  opts,
	}
	f.conn = f.connector()
	return f
}

// connector builds a connector over the fixture's engine and store with the
// fixture's options and then extra.
func (f *recFixture) connector(extra ...func(*engineconnect.Config)) *engineconnect.Connector {
	GinkgoHelper()
	cfg := engineconnect.Config{
		Server:   f.srv,
		WriterID: recWriter,
		Records: &engineconnect.Records{
			Types: []engineconnect.RecordType{{
				Root:    &testdatav1.Project{},
				Members: []engineconnect.Member{{Prototype: &testdatav1.Widget{}, RecordField: "label"}},
				Store:   f.store,
				Codec:   f.codec,
			}},
		},
	}
	for _, opt := range append(slices.Clone(f.opts), extra...) {
		opt(&cfg)
	}
	conn, err := engineconnect.New(cfg)
	Expect(err).NotTo(HaveOccurred())
	if f.clock != nil {
		engineconnect.SetClock(conn, f.clock.now)
	}
	DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })
	return conn
}

// restart closes the connector and starts a new one over the same engine and
// store, as a process restart does.
func (f *recFixture) restart(extra ...func(*engineconnect.Config)) {
	GinkgoHelper()
	Expect(f.conn.Close()).To(Succeed())
	f.conn = f.connector(extra...)
}

// key returns the record key of the record at loc.
func (f *recFixture) key(ctx context.Context, ws, loc string) string {
	GinkgoHelper()
	raw, err := f.srv.ConsumerShadow(ctx, ws, "connect:"+recWriter, "loc:project", loc)
	Expect(err).NotTo(HaveOccurred())
	return string(raw)
}

// rows returns the record's root row and its widgets in line order.
func (f *recFixture) rows(ctx context.Context, ws, key string) (*testdatav1.Project, []*testdatav1.Widget) {
	GinkgoHelper()
	var (
		root *testdatav1.Project
		ws2  []*testdatav1.Widget
	)
	Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		var p testdatav1.Project
		found, _, err := tx.Get(&p, key)
		if err != nil {
			return err
		}
		if found {
			root = &p
		}
		_, _, err = tx.ScanPage(&testdatav1.Widget{}, "", 1000, func(msg proto.Message, _ uint64) error {
			if w := msg.(*testdatav1.Widget); w.GetLabel() == key {
				ws2 = append(ws2, w)
			}
			return nil
		})
		return err
	})).To(Succeed())
	slices.SortFunc(ws2, func(a, b *testdatav1.Widget) int {
		if a.GetCount() != b.GetCount() {
			return int(a.GetCount() - b.GetCount())
		}
		return strings.Compare(a.GetId(), b.GetId())
	})
	return root, ws2
}

// lines renders the record from the engine as the file would read.
func (f *recFixture) lines(ctx context.Context, ws, key string) string {
	GinkgoHelper()
	root, widgets := f.rows(ctx, ws, key)
	if root == nil {
		return ""
	}
	parts := []string{root.GetName()}
	for _, w := range widgets {
		parts = append(parts, bodyOf(w))
	}
	return strings.Join(parts, "\n")
}

// widgetKeys returns the row keys of the record's widgets in line order.
func (f *recFixture) widgetKeys(ctx context.Context, ws, key string) []string {
	GinkgoHelper()
	_, widgets := f.rows(ctx, ws, key)
	out := make([]string, len(widgets))
	for i, w := range widgets {
		out[i] = w.GetId()
	}
	return out
}

// widgetWith returns the key of the record's widget whose text is s.
func (f *recFixture) widgetWith(ctx context.Context, ws, key, s string) string {
	GinkgoHelper()
	_, widgets := f.rows(ctx, ws, key)
	for _, w := range widgets {
		if bodyOf(w) == s {
			return w.GetId()
		}
	}
	Fail(fmt.Sprintf("record %q has no line %q", key, s))
	return ""
}

// editLine changes a widget's text as a client would, under its own peer.
func (f *recFixture) editLine(ctx context.Context, ws, widget, s string) {
	GinkgoHelper()
	f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
		var w testdatav1.Widget
		if _, err := tx.Get(&w, widget); err != nil {
			return err
		}
		next := proto.Clone(&w).(*testdatav1.Widget)
		if err := server.ApplyTextStrings(next, nil, &w, map[string]string{"body": s}, "client-1"); err != nil {
			return err
		}
		return tx.Put(next)
	})
}

func (f *recFixture) clientWrite(ctx context.Context, ws string, fn func(tx *server.EntityTx) error) {
	GinkgoHelper()
	Expect(f.srv.Mutate(server.TrustedAs(ctx, "client"), ws, fn)).To(Succeed())
}

// addRecord writes a project and its lines to the engine as a client would.
func (f *recFixture) addRecord(ctx context.Context, ws, key, name string, lines ...string) {
	GinkgoHelper()
	f.clientWrite(ctx, ws, func(tx *server.EntityTx) error {
		if err := tx.Put(&testdatav1.Project{Id: key, Name: name}); err != nil {
			return err
		}
		for i, l := range lines {
			w := &testdatav1.Widget{Id: fmt.Sprintf("%s-w%d", key, i), Label: key, Count: int32(i)}
			if err := server.ApplyTextStrings(w, nil, nil, map[string]string{"body": l}, "client-1"); err != nil {
				return err
			}
			if err := tx.Put(w); err != nil {
				return err
			}
		}
		return nil
	})
}

func (f *recFixture) seq(ctx context.Context, ws string) uint64 {
	GinkgoHelper()
	seq, err := f.srv.CurrentSeq(ctx, ws)
	Expect(err).NotTo(HaveOccurred())
	return seq
}

func (f *recFixture) content(ws, loc string) string {
	GinkgoHelper()
	c, ok := f.store.get(ws, loc)
	Expect(ok).To(BeTrue(), "store has no record at %q", loc)
	return c
}

// spyCodec is a lineCodec that hands each MergeInput to seen first.
type spyCodec struct {
	*lineCodec
	seen func(in engineconnect.MergeInput)
}

func (c spyCodec) Merge(tx *server.EntityTx, in engineconnect.MergeInput) (engineconnect.Merged, error) {
	c.seen(in)
	return c.lineCodec.Merge(tx, in)
}

// peerSpy is a lineCodec that records the TextPeer of each Merge and Render.
type peerSpy struct {
	*lineCodec
	peers *[]string
}

func (c peerSpy) Merge(tx *server.EntityTx, in engineconnect.MergeInput) (engineconnect.Merged, error) {
	c.mu.Lock()
	*c.peers = append(*c.peers, "merge "+in.TextPeer)
	c.mu.Unlock()
	return c.lineCodec.Merge(tx, in)
}

func (c peerSpy) Render(tx *server.EntityTx, in engineconnect.RenderInput) (engineconnect.Rendered, bool, error) {
	c.mu.Lock()
	*c.peers = append(*c.peers, "render "+in.TextPeer)
	c.mu.Unlock()
	return c.lineCodec.Render(tx, in)
}

// nameMatchCodec is a lineCodec that matches a store record new to the
// connector to the root row with the same name, as a codec that rebuilds
// lost bookkeeping from the content would.
type nameMatchCodec struct{ *lineCodec }

func (m nameMatchCodec) Merge(tx *server.EntityTx, in engineconnect.MergeInput) (engineconnect.Merged, error) {
	var hit string
	if in.Base == nil && in.Root == nil {
		name, _ := parseLines(in.Record.Content)
		if err := tx.Query(&testdatav1.Project{}, query.Spec{Where: query.Eq("name", name)}, func(msg proto.Message) error {
			hit = msg.(*testdatav1.Project).GetId()
			return nil
		}); err != nil {
			return engineconnect.Merged{}, err
		}
	}
	if hit != "" {
		var p testdatav1.Project
		if _, err := tx.Get(&p, hit); err != nil {
			return engineconnect.Merged{}, err
		}
		in.Key, in.Root, in.Members = hit, &p, nil
		if err := tx.Query(&testdatav1.Widget{}, query.Spec{Where: query.Eq("label", hit)}, func(msg proto.Message) error {
			in.Members = append(in.Members, msg)
			return nil
		}); err != nil {
			return engineconnect.Merged{}, err
		}
	}
	out, err := m.lineCodec.Merge(tx, in)
	if hit != "" {
		out.Key = hit
	}
	return out, err
}

// projects returns the name of every project in the workspace, by key.
func (f *recFixture) projects(ctx context.Context, ws string) map[string]string {
	GinkgoHelper()
	out := map[string]string{}
	Expect(f.srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
		_, _, err := tx.ScanPage(&testdatav1.Project{}, "", 1000, func(msg proto.Message, _ uint64) error {
			p := msg.(*testdatav1.Project)
			out[p.GetId()] = p.GetName()
			return nil
		})
		return err
	})).To(Succeed())
	return out
}
