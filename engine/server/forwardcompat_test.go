package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
)

// Schema evolution is additive, and PATCH is what keeps a field this server's
// schema does not know yet: a newer writer's field travels as unknown bytes,
// and a masked merge must carry them along wherever they sit — top level, in a
// nested message, or in a keyed-list element. A PUT that carries them keeps
// them too. Field numbers 900+ stand in for fields a newer schema added.
var _ = Describe("Fields a newer schema wrote, through an older server", func() {
	const wsID = "ws-unknown"

	var (
		ctx    context.Context
		client syncplanev1connect.SyncServiceClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv := newTestServer()
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	})

	// future appends an unknown string field to m, as a newer schema would
	// have encoded it.
	future := func(m protoreflect.Message, num protowire.Number, v string) {
		b := protowire.AppendTag(append([]byte{}, m.GetUnknown()...), num, protowire.BytesType)
		m.SetUnknown(protowire.AppendString(b, v))
	}
	// futureOf returns the value of unknown field num in m, or "" if absent.
	futureOf := func(m protoreflect.Message, num protowire.Number) string {
		b := m.GetUnknown()
		for len(b) > 0 {
			n, typ, l := protowire.ConsumeTag(b)
			Expect(l).To(BeNumerically(">", 0))
			b = b[l:]
			if n == num && typ == protowire.BytesType {
				s, vl := protowire.ConsumeString(b)
				Expect(vl).To(BeNumerically(">", 0))
				return s
			}
			vl := protowire.ConsumeFieldValue(n, typ, b)
			Expect(vl).To(BeNumerically(">", 0))
			b = b[vl:]
		}
		return ""
	}

	push := func(m *syncplanev1.Mutation) {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(wsID, []*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		results := pushResults(resp.Msg)
		Expect(results).To(HaveLen(1))
		Expect(results[0].GetErrorCode()).To(BeEmpty(), results[0].GetErrorMessage())
	}

	stored := func(key string) *testdatav1.Widget {
		GinkgoHelper()
		resp, err := client.Pull(ctx, connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: wsID}))
		Expect(err).NotTo(HaveOccurred())
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetEntity() == "widget" && rc.GetKey() == key && !rc.GetTombstone() {
				var w testdatav1.Widget
				Expect(proto.Unmarshal(rc.GetValue(), &w)).To(Succeed())
				return &w
			}
		}
		Fail("widget " + key + " not found")
		return nil
	}

	seed := func() {
		GinkgoHelper()
		w := &testdatav1.Widget{
			Id:       "w1",
			Label:    "base",
			Settings: &testdatav1.Settings{Color: "red"},
			Members:  []*testdatav1.Member{{Uid: "m1", Name: "ann"}},
		}
		future(w.ProtoReflect(), 900, "top")
		future(w.GetSettings().ProtoReflect(), 901, "nested")
		future(w.GetMembers()[0].ProtoReflect(), 902, "element")
		push(putMutation("newer", 1, "widget", "w1", w))
	}

	// Given a newer writer PUTs a row with fields this server does not
	// know, When the row is pulled, Then those fields come back untouched.
	It("keeps unknown fields a PUT carried", func() {
		seed()
		w := stored("w1")
		Expect(futureOf(w.ProtoReflect(), 900)).To(Equal("top"))
		Expect(futureOf(w.GetSettings().ProtoReflect(), 901)).To(Equal("nested"))
		Expect(futureOf(w.GetMembers()[0].ProtoReflect(), 902)).To(Equal("element"))
	})

	// Given that row, When an older client PATCHes an unrelated
	// top-level field, Then every unknown field survives.
	It("keeps them through a top-level PATCH", func() {
		seed()
		push(patchMutation("older", 1, "widget", "w1", &testdatav1.Widget{Label: "edited"}, "label"))
		w := stored("w1")
		Expect(w.GetLabel()).To(Equal("edited"))
		Expect(futureOf(w.ProtoReflect(), 900)).To(Equal("top"))
		Expect(futureOf(w.GetSettings().ProtoReflect(), 901)).To(Equal("nested"))
		Expect(futureOf(w.GetMembers()[0].ProtoReflect(), 902)).To(Equal("element"))
	})

	// When an older client PATCHes a sibling inside the nested message
	// that carries an unknown field, Then the nested unknown field survives.
	It("keeps them through a PATCH into the nested message that holds them", func() {
		seed()
		push(patchMutation("older", 1, "widget", "w1",
			&testdatav1.Widget{Settings: &testdatav1.Settings{Size: 3}}, "settings.size"))
		w := stored("w1")
		Expect(w.GetSettings().GetSize()).To(BeEquivalentTo(3))
		Expect(w.GetSettings().GetColor()).To(Equal("red"))
		Expect(futureOf(w.GetSettings().ProtoReflect(), 901)).To(Equal("nested"))
	})

	// When an older client clears the only KNOWN field of that nested
	// message, Then the message stays, because the unknown field is still
	// content. Must NOT: drop the message as empty.
	It("keeps a nested message whose only remaining content is unknown", func() {
		seed()
		push(patchMutation("older", 1, "widget", "w1", &testdatav1.Widget{}, "settings.color"))
		w := stored("w1")
		Expect(w.GetSettings()).NotTo(BeNil())
		Expect(futureOf(w.GetSettings().ProtoReflect(), 901)).To(Equal("nested"))
	})

	// When an older client PATCHes one field of a keyed-list element that
	// carries an unknown field, Then the element's unknown field survives.
	It("keeps them through a PATCH into a keyed-list element", func() {
		seed()
		push(patchMutation("older", 1, "widget", "w1",
			&testdatav1.Widget{Members: []*testdatav1.Member{{Uid: "m1", Name: "bob"}}}, "members.m1.name"))
		w := stored("w1")
		Expect(w.GetMembers()).To(HaveLen(1))
		Expect(w.GetMembers()[0].GetName()).To(Equal("bob"))
		Expect(futureOf(w.GetMembers()[0].ProtoReflect(), 902)).To(Equal("element"))
	})
})
