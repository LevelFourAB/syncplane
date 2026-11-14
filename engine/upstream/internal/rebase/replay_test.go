package rebase_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

var _ = Describe("Replay", func() {
	var model *rebase.Model

	BeforeEach(func() {
		model = mustModel(&testdatav1.Widget{})
	})

	present := func(m proto.Message) rebase.Row {
		return rebase.Row{Present: true, Value: deterministic(m)}
	}
	decodeWidget := func(r rebase.Row) *testdatav1.Widget {
		GinkgoHelper()
		Expect(r.Present).To(BeTrue())
		var w testdatav1.Widget
		Expect(proto.Unmarshal(r.Value, &w)).To(Succeed())
		return &w
	}
	put := func(w *testdatav1.Widget) rebase.Op {
		return opOf(crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", w.GetId(), w))
	}
	patch := func(w *testdatav1.Widget, mask ...string) rebase.Op {
		return opOf(crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", w.GetId(), w, mask...))
	}
	del := rebase.Op{Kind: syncplanev1.CrudOp_CRUD_OP_DELETE}

	It("returns confirmed unchanged when nothing is pending", func() {
		confirmed := rebase.Row{Present: true, Value: []byte{0x0a, 0x02, 'w', '1'}}
		got, err := model.Replay(confirmed, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(confirmed))
	})

	It("keeps an absent row absent under a PATCH", func() {
		op := patch(&testdatav1.Widget{Id: "w1", Label: "x"}, "label")
		_, err := model.Apply(rebase.Row{}, op)
		Expect(err).To(MatchError(rebase.ErrNoTarget))
		Expect(errors.Is(err, rebase.ErrRefused)).To(BeTrue())

		got, err := model.Replay(rebase.Row{}, []rebase.Op{op})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Present).To(BeFalse())
	})

	It("refuses a PATCH without a mask and leaves the row", func() {
		confirmed := present(&testdatav1.Widget{Id: "w1", Label: "a"})
		got, err := model.Apply(confirmed, patch(&testdatav1.Widget{Id: "w1", Label: "b"}))
		Expect(err).To(MatchError(rebase.ErrRefused))
		Expect(got).To(Equal(confirmed))
	})

	It("starts a fresh document after a DELETE and a PUT", func() {
		confirmed := present(&testdatav1.Widget{Id: "w1", Label: "old", Body: doc("a", "old text")})
		got, err := model.Replay(confirmed, []rebase.Op{
			del,
			put(&testdatav1.Widget{Id: "w1", Label: "new", Body: doc("b", "new text")}),
		})
		Expect(err).NotTo(HaveOccurred())
		w := decodeWidget(got)
		Expect(w.GetLabel()).To(Equal("new"))
		Expect(plain(w.GetBody())).To(Equal("new text"), "the deleted row's document is no base")
	})

	It("unions text on PUT with the row it replaces, and keeps a document the PUT leaves empty", func() {
		base := doc("a", "hello")
		confirmed := present(&testdatav1.Widget{Id: "w1", Body: edited(base, "origin", " world")})

		got, err := model.Apply(confirmed, put(&testdatav1.Widget{Id: "w1", Body: edited(base, "local", "!")}))
		Expect(err).NotTo(HaveOccurred())
		Expect(plain(decodeWidget(got).GetBody())).To(Or(Equal("hello world!"), Equal("hello! world")))

		got, err = model.Apply(confirmed, put(&testdatav1.Widget{Id: "w1", Label: "no text"}))
		Expect(err).NotTo(HaveOccurred())
		Expect(plain(decodeWidget(got).GetBody())).To(Equal("hello world"))
	})

	It("is idempotent for text: replaying a write twice gives the row once", func() {
		base := doc("a", "hello")
		confirmed := present(&testdatav1.Widget{Id: "w1", Body: base})
		op := patch(&testdatav1.Widget{Id: "w1", Body: edited(base, "local", " there")}, "body")

		once, err := model.Replay(confirmed, []rebase.Op{op})
		Expect(err).NotTo(HaveOccurred())
		twice, err := model.Replay(confirmed, []rebase.Op{op, op})
		Expect(err).NotTo(HaveOccurred())
		Expect(twice.Value).To(Equal(once.Value))

		putOp := put(decodeWidget(once))
		again, err := model.Apply(once, putOp)
		Expect(err).NotTo(HaveOccurred())
		Expect(again.Value).To(Equal(once.Value))
	})

	It("keeps the version vector of a document a PATCH clears", func() {
		confirmed := present(&testdatav1.Widget{Id: "w1", Body: doc("a", "hello")})
		got, err := model.Apply(confirmed, patch(&testdatav1.Widget{Id: "w1"}, "body"))
		Expect(err).NotTo(HaveOccurred())
		body := decodeWidget(got).GetBody()
		Expect(body).NotTo(BeEmpty())
		d, err := text.Decode(body, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(d.PlainText()).To(BeEmpty())
		Expect(d.VersionVector()).To(HaveKeyWithValue("a", BeNumerically(">", 0)))
	})

	It("refuses to replay text it holds only as delta state", func() {
		base := doc("a", "hello")
		frag := asFragment(edited(base, "local", "!"), base)
		op := patch(&testdatav1.Widget{Id: "w1", Body: frag}, "body")
		_, err := model.Apply(present(&testdatav1.Widget{Id: "w1", Body: base}), op)
		Expect(err).To(MatchError(rebase.ErrUncapturedText))
		_, err = model.Replay(present(&testdatav1.Widget{Id: "w1", Body: base}), []rebase.Op{op})
		Expect(err).To(MatchError(rebase.ErrUncapturedText))
		_, err = model.UpstreamValue(op)
		Expect(err).To(MatchError(rebase.ErrUncapturedText))

		op.Text = map[string][]byte{"body": edited(base, "local", "!")}
		got, err := model.Apply(present(&testdatav1.Widget{Id: "w1", Body: base}), op)
		Expect(err).NotTo(HaveOccurred())
		Expect(plain(decodeWidget(got).GetBody())).To(Equal("hello!"))
	})

	It("strips reference stubs to their key fields", func() {
		taskModel := mustModel(&testdatav1.Task{})
		task := &testdatav1.Task{Id: "t1", Assignee: &testdatav1.UserRef{User: &testdatav1.User{Id: "u1", Name: "Ann"}},
			Watchers: []*testdatav1.UserRef{{WorkspaceId: "other", User: &testdatav1.User{Id: "u2", Name: "Bo"}}}}
		got, err := taskModel.Apply(rebase.Row{}, opOf(crud(syncplanev1.CrudOp_CRUD_OP_PUT, "task", "t1", task)))
		Expect(err).NotTo(HaveOccurred())

		origin := newNode()
		Expect(origin.push("c", crud(syncplanev1.CrudOp_CRUD_OP_PUT, "task", "t1", task))).To(BeTrue())
		Expect(taskModel.Equal(got, origin.row(&testdatav1.Task{}, "t1"))).To(BeTrue())

		var stored testdatav1.Task
		Expect(proto.Unmarshal(got.Value, &stored)).To(Succeed())
		Expect(stored.GetAssignee().GetUser().GetName()).To(BeEmpty())
		Expect(stored.GetWatchers()[0].GetWorkspaceId()).To(Equal("other"))
	})

	It("runs an app merger the way the origin does", func() {
		concat := func(base, incoming []byte) ([]byte, error) {
			return append(append([]byte{}, base...), incoming...), nil
		}
		merged, err := rebase.NewModel((&testdatav1.Widget{}).ProtoReflect().Descriptor(),
			lww.Mergers{"raw": {Fn: concat}})
		Expect(err).NotTo(HaveOccurred())

		origin := newNode(server.WithMerger(&testdatav1.Widget{}, "raw", concat))
		first := crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", "w1", &testdatav1.Widget{Id: "w1", Raw: []byte("ab")})
		second := crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1", &testdatav1.Widget{Raw: []byte("cd")}, "raw")
		Expect(origin.push("c", first)).To(BeTrue())
		Expect(origin.push("c", second)).To(BeTrue())

		got, err := merged.Replay(rebase.Row{}, []rebase.Op{opOf(first), opOf(second)})
		Expect(err).NotTo(HaveOccurred())
		Expect(merged.Equal(got, origin.row(&testdatav1.Widget{}, "w1"))).To(BeTrue())
		Expect(decodeWidget(got).GetRaw()).To(Equal([]byte("abcd")))
	})
})

var _ = Describe("Upstream values", func() {
	It("forwards delta state as the full document the replica captured", func() {
		model := mustModel(&testdatav1.Widget{})
		origin, local := newNode(), newNode()
		base := doc("setup", "hello")
		create := crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", "w1", &testdatav1.Widget{Id: "w1", Body: base})
		Expect(origin.push("setup", create)).To(BeTrue())
		Expect(local.push("setup", create)).To(BeTrue())

		// Text the origin never saw: a write the replica took and the origin
		// confirmed nowhere. A fragment cut against the replica's document
		// builds on it.
		ahead := edited(base, "local", " there")
		Expect(local.push("local", crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1",
			&testdatav1.Widget{Body: ahead}, "body"))).To(BeTrue())
		full := edited(ahead, "local", "!")
		c := crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1",
			&testdatav1.Widget{Body: asFragment(full, ahead)}, "body")
		Expect(local.push("local", c)).To(BeTrue())

		raw := origin.pushResult("raw", c)
		Expect(raw.GetErrorCode()).To(Equal(server.CodeTextFullStateRequired), "the fragment means nothing to the origin")

		op := opOf(c)
		var err error
		op.Text, err = model.CaptureText(op, local.row(&testdatav1.Widget{}, "w1").Value)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.Text).To(HaveKey("body"))
		value, err := model.UpstreamValue(op)
		Expect(err).NotTo(HaveOccurred())

		confirmed := origin.row(&testdatav1.Widget{}, "w1")
		up := proto.Clone(c).(*syncplanev1.CrudMutation)
		up.Value = value
		Expect(origin.push("link", up)).To(BeTrue())
		want := origin.row(&testdatav1.Widget{}, "w1")
		var w testdatav1.Widget
		Expect(proto.Unmarshal(want.Value, &w)).To(Succeed())
		Expect(plain(w.GetBody())).To(Equal("hello there!"))

		got, err := model.Replay(confirmed, []rebase.Op{op})
		Expect(err).NotTo(HaveOccurred())
		Expect(model.Equal(got, want)).To(BeTrue())
	})

	It("forwards a fragment that the origin merges as it merges the full document", func() {
		model := mustModel(&testdatav1.Widget{})
		origin := newNode()
		base := doc("setup", "hello")
		Expect(origin.push("setup", crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", "w1",
			&testdatav1.Widget{Id: "w1", Body: base}))).To(BeTrue())
		confirmed := origin.row(&testdatav1.Widget{}, "w1")
		// The origin moves past the confirmed row before the link pushes.
		Expect(origin.push("other", crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1",
			&testdatav1.Widget{Body: edited(base, "other", " world")}, "body"))).To(BeTrue())

		full := edited(base, "local", "!")
		c := crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1", &testdatav1.Widget{Body: full}, "body")
		op := opOf(c)
		op.Text = map[string][]byte{"body": full}
		value, err := model.UpstreamFragments(op, confirmed.Value)
		Expect(err).NotTo(HaveOccurred())
		var sent testdatav1.Widget
		Expect(proto.Unmarshal(value, &sent)).To(Succeed())
		Expect(text.IsFragment(sent.GetBody())).To(BeTrue())

		up := proto.Clone(c).(*syncplanev1.CrudMutation)
		up.Value = value
		Expect(origin.push("link", up)).To(BeTrue())
		var w testdatav1.Widget
		Expect(proto.Unmarshal(origin.row(&testdatav1.Widget{}, "w1").Value, &w)).To(Succeed())
		Expect(plain(w.GetBody())).To(Or(Equal("hello world!"), Equal("hello! world")))
	})

	It("refuses to cut a fragment for a PUT", func() {
		model := mustModel(&testdatav1.Widget{})
		op := opOf(crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", "w1", &testdatav1.Widget{Id: "w1"}))
		_, err := model.UpstreamFragments(op, nil)
		Expect(err).To(HaveOccurred())
	})

	It("keeps a cleared document cleared", func() {
		model := mustModel(&testdatav1.Widget{})
		op := opOf(crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", "w1", &testdatav1.Widget{Label: "x"}, "label", "body"))
		op.Text = map[string][]byte{"body": doc("a", "kept elsewhere")}
		value, err := model.UpstreamValue(op)
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal(op.Value), "nothing to replace, so the value is passed through")
	})

	It("leaves text outside the mask alone", func() {
		model := mustModel(&testdatav1.KeyedTextEntity{})
		e := &testdatav1.KeyedTextEntity{Id: "k1", Notes: []*testdatav1.KeyedNote{
			{Id: "n1", Body: doc("a", "one")}, {Id: "n2", Body: doc("a", "two")},
		}}
		op := opOf(crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "keyed_text", "k1", e, "notes.n1.body"))
		after := deterministic(&testdatav1.KeyedTextEntity{Id: "k1", Notes: []*testdatav1.KeyedNote{
			{Id: "n1", Body: doc("b", "one, merged")}, {Id: "n2", Body: doc("b", "two, stored")},
		}})
		captured, err := model.CaptureText(op, after)
		Expect(err).NotTo(HaveOccurred())
		Expect(captured).To(HaveLen(1))
		Expect(plain(captured["notes.n1.body"])).To(Equal("one, merged"))
	})

	It("captures nothing for a DELETE or an entity without text", func() {
		model := mustModel(&testdatav1.KeyedTextEntity{})
		captured, err := model.CaptureText(rebase.Op{Kind: syncplanev1.CrudOp_CRUD_OP_DELETE}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(captured).To(BeNil())

		pairs := mustModel(&testdatav1.Pair{})
		value := deterministic(&testdatav1.Pair{Ns: "n", Id: "p"})
		up, err := pairs.UpstreamValue(rebase.Op{Kind: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value})
		Expect(err).NotTo(HaveOccurred())
		Expect(up).To(Equal(value))
	})
})

var _ = Describe("Model", func() {
	It("refuses text fields the engine refuses to register", func() {
		for _, m := range []proto.Message{&testdatav1.OneofTextEntity{}, &testdatav1.OneofNestedTextEntity{}, &testdatav1.ListTextEntity{}} {
			_, err := rebase.NewModel(m.ProtoReflect().Descriptor(), nil)
			Expect(err).To(HaveOccurred(), string(m.ProtoReflect().Descriptor().FullName()))
		}
	})

	It("refuses a merger on a text field", func() {
		_, err := rebase.NewModel((&testdatav1.Widget{}).ProtoReflect().Descriptor(),
			lww.Mergers{"body": {Fn: text.Merge}})
		Expect(err).To(HaveOccurred())
	})

	It("lists text documents by instance path", func() {
		model := mustModel(&testdatav1.KeyedTextEntity{})
		Expect(model.HasText()).To(BeTrue())
		Expect(mustModel(&testdatav1.Pair{}).HasText()).To(BeFalse())
		docs, err := model.TextDocuments(deterministic(&testdatav1.KeyedTextEntity{
			Id: "k1",
			Notes: []*testdatav1.KeyedNote{
				{Id: "a.b", Body: doc("a", "x"), Deep: &testdatav1.TextHolder{Body: doc("a", "y")}},
				{Id: "n2"},
			},
			Sections: map[string]*testdatav1.TextHolder{"s1": {Body: doc("a", "z")}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(docs).To(HaveLen(3))
		Expect(docs).To(HaveKey(`notes.a\.b.body`))
		Expect(docs).To(HaveKey(`notes.a\.b.deep.body`))
		Expect(docs).To(HaveKey("sections.s1.body"))
	})

	It("compares rows by value, not by encoding", func() {
		model := mustModel(&testdatav1.Widget{})
		w := &testdatav1.Widget{Id: "w1", Attrs: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}}
		reordered := []byte{}
		// Map entries in reverse key order: a valid encoding of the same row.
		reordered = append(reordered, deterministic(&testdatav1.Widget{Id: "w1"})...)
		for _, k := range []string{"d", "c", "b", "a"} {
			reordered = append(reordered, deterministic(&testdatav1.Widget{Attrs: map[string]string{k: w.Attrs[k]}})...)
		}
		a := rebase.Row{Present: true, Value: deterministic(w)}
		b := rebase.Row{Present: true, Value: reordered}
		Expect(a.Value).NotTo(Equal(b.Value))
		Expect(model.Equal(a, b)).To(BeTrue())
		Expect(model.Equal(a, rebase.Row{})).To(BeFalse())
		Expect(model.Equal(rebase.Row{}, rebase.Row{Value: []byte("ignored")})).To(BeTrue())
		Expect(model.Equal(a, rebase.Row{Present: true, Value: deterministic(&testdatav1.Widget{Id: "w2"})})).To(BeFalse())
	})

	It("clears top-level fields and oneofs by unit name", func() {
		model := mustModel(&testdatav1.Guarded{})
		value := deterministic(&testdatav1.Guarded{
			Id: "g1", Label: "kept", Owner: "o", Audit: &testdatav1.Settings{},
			State: &testdatav1.Guarded_StateCode{StateCode: 3},
		})
		out, err := model.ClearUnits(value, []string{"owner", "audit", "state", "not_a_field"})
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(deterministic(&testdatav1.Guarded{Id: "g1", Label: "kept"})))

		same, err := model.ClearUnits(value, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(same).To(Equal(value))
		_, err = model.ClearUnits([]byte{0xff}, []string{"owner"})
		Expect(err).To(HaveOccurred())
	})
})
