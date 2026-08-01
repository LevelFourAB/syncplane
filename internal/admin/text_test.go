package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
)

var _ = Describe("admin collaborative text", func() {
	const (
		appID = "demo"
		ws    = "w1"
		path  = "/apps/demo/ws/w1/e/note"
	)
	var (
		ctx    context.Context
		mgr    *server.Manager
		ts     *httptest.Server
		client *http.Client
	)

	note := server.DynamicEntity{
		Name: "note", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "body", Number: 2, Kind: protoreflect.BytesKind, Text: true},
		},
	}

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "admin-text.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		mgr, err = server.NewManager(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(mgr.Close()).To(Succeed()) })
		_, err = mgr.Register(ctx, appID, descriptorSetBytes(note))
		Expect(err).NotTo(HaveOccurred())

		ts = httptest.NewServer(Handler(mgr))
		DeferCleanup(ts.Close)
		client = ts.Client()
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	})

	submit := func(method, p string, vals url.Values) *http.Response {
		GinkgoHelper()
		req, err := http.NewRequest(method, ts.URL+p, strings.NewReader(vals.Encode()))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("HX-Request", "true")
		resp, err := client.Do(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		return resp
	}
	create := func(key, body string) {
		GinkgoHelper()
		resp := submit(http.MethodPost, path+"/rows", url.Values{"id": {key}, "body": {body}})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	}
	update := func(key, body string) {
		GinkgoHelper()
		resp := submit(http.MethodPut, path+"/rows/"+key, url.Values{"id": {key}, "body": {body}})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	}
	// storedDoc decodes the stored note's document under a throwaway reader
	// identity (nothing is edited through it).
	storedDoc := func(key string) *text.Doc {
		GinkgoHelper()
		meta, ok := mgr.App(appID).Entity("note")
		Expect(ok).To(BeTrue())
		msg := dynamicpb.NewMessage(meta.Descriptor)
		var found bool
		Expect(mgr.App(appID).View(ctx, ws, func(tx *server.ViewTx) error {
			f, _, err := tx.Get(msg, key)
			found = f
			return err
		})).To(Succeed())
		Expect(found).To(BeTrue())
		fd := meta.Descriptor.Fields().ByName("body")
		doc, err := text.Decode(msg.Get(fd).Bytes(), "")
		Expect(err).NotTo(HaveOccurred())
		return doc
	}
	// engineEdit makes a trusted engine-side edit under its own peer, standing
	// in for a synced client editing the same field.
	engineEdit := func(key, peer string, edit func(*text.Doc)) {
		GinkgoHelper()
		meta, ok := mgr.App(appID).Entity("note")
		Expect(ok).To(BeTrue())
		fd := meta.Descriptor.Fields().ByName("body")
		Expect(mgr.App(appID).Mutate(ctx, ws, func(tx *server.EntityTx) error {
			msg := dynamicpb.NewMessage(meta.Descriptor)
			found, err := tx.Get(msg, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			doc, err := text.Decode(msg.Get(fd).Bytes(), peer)
			Expect(err).NotTo(HaveOccurred())
			edit(doc)
			msg.Set(fd, protoreflect.ValueOfBytes(doc.Encode()))
			return tx.Patch(msg, []string{"body"}, nil)
		})).To(Succeed())
	}

	It("keeps one writer identity across a run of admin edits", func() {
		create("n1", "one")
		for _, body := range []string{"one two", "one two three", "two three", "two three four"} {
			update("n1", body)
		}

		doc := storedDoc("n1")
		Expect(doc.PlainText()).To(Equal("two three four"))
		// A peer per edit would leave five entries here. The peer table is per
		// document and never collected, so an unbounded one is a slow leak in
		// every document the admin touches.
		Expect(doc.VersionVector()).To(HaveLen(1))
	})

	It("edits the stored document when a create lands on a live row", func() {
		create("n1", "hello")
		// Put merges text fields, so a create over a live row must diff against
		// the stored document; a fresh one would re-spend the handler peer's
		// counters and union both texts instead of replacing.
		create("n1", "hello world")

		doc := storedDoc("n1")
		Expect(doc.PlainText()).To(Equal("hello world"))
		Expect(doc.VersionVector()).To(HaveLen(1))
	})

	// The submitted string is diffed against the stored document (SetString)
	// rather than replacing it, so a client's items — and the marks anchored to
	// them — survive an admin edit outside the changed hunk.
	It("edits the client's document in place, keeping its marks anchored", func() {
		create("n1", "hello world")
		engineEdit("n1", "client-1", func(d *text.Doc) {
			_, err := d.Mark(6, 11, "comment:c1", true, text.ExpandNone)
			Expect(err).NotTo(HaveOccurred())
		})

		update("n1", "hello world!")

		doc := storedDoc("n1")
		Expect(doc.PlainText()).To(Equal("hello world!"))
		Expect(doc.Marks()).To(Equal([]text.Span{
			{Start: 6, End: 11, Key: "comment:c1", Value: true},
		}))
		Expect(doc.VersionVector()).To(HaveLen(2))
	})
})
