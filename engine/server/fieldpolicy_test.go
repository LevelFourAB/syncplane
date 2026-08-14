package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// fieldPolicy is a policy that protects further fields per caller, which is the
// half a schema cannot state. protect is consulted per entity.
type fieldPolicy struct {
	server.AllowAll
	protect map[string][]string
	// fail, when set, is returned instead of an answer.
	fail error
	// calls counts ProtectedFields invocations, which is how the specs observe
	// the per-entity memo.
	calls int
}

func (p *fieldPolicy) ProtectedFields(
	_ context.Context, _ server.Principal, _, entity string,
) ([]string, error) {
	p.calls++
	if p.fail != nil {
		return nil, p.fail
	}
	return p.protect[entity], nil
}

var _ = Describe("Protected fields", func() {
	var (
		ctx    context.Context
		policy *fieldPolicy
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
	)

	// The fixture is "guarded" in proto/private/syncplane/testdata/v1: `owner`,
	// `audit`, `summary` (collaborative text) and the `state` oneof are marked
	// TRUSTED_ONLY, while `label` and `settings` stay client-writable.
	build := func(opts ...server.Option) {
		GinkgoHelper()
		srv = newTestServer(append([]server.Option{
			// Registered here rather than in testEntities: the fixture exists for
			// these specs, and every spec that enumerates the catalog would have
			// to account for it.
			server.WithEntities(&testdatav1.Guarded{}),
			server.WithPermissions(policy),
			// The escape hatch: a mutator's rows are covered by the verdict on
			// its invocation, so this one writes a protected field on behalf of
			// an untrusted caller.
			server.WithMutator("reassign", &testdatav1.Guarded{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					g := args.(*testdatav1.Guarded)
					return tx.Patch(g, []string{"owner"}, nil)
				}),
		}, opts...)...)

		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	}

	BeforeEach(func() {
		ctx = context.Background()
		policy = &fieldPolicy{}
		build()
	})

	push := func(mutations ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, mutations)))
		Expect(err).NotTo(HaveOccurred())
		return pushResults(resp.Msg)
	}

	// seed plants a row through the trusted path, which no policy applies to.
	seed := func(g *testdatav1.Guarded) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(g)
		})).To(Succeed())
	}

	read := func(id string) *testdatav1.Guarded {
		GinkgoHelper()
		var (
			out   testdatav1.Guarded
			found bool
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, id)
			return err
		})).To(Succeed())
		Expect(found).To(BeTrue(), "row %q", id)
		return &out
	}

	exists := func(id string) bool {
		GinkgoHelper()
		var (
			out   testdatav1.Guarded
			found bool
		)
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&out, id)
			return err
		})).To(Succeed())
		return found
	}

	denied := func(result *syncplanev1.MutationResult) string {
		GinkgoHelper()
		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(result.GetErrorCode()).To(Equal(server.CodePermissionDenied))
		return result.GetErrorMessage()
	}

	applied := func(result *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(result.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
			result.GetErrorMessage())
	}

	Describe("registration", func() {
		It("refuses a protected key field, which no write could ever honour", func() {
			err := newTestServerErr(server.WithEntities(&testdatav1.ProtectedKeyEntity{}))

			Expect(err).To(MatchError(ContainSubstring("key field \"id\"")))
			Expect(err).To(MatchError(ContainSubstring("write_policy")))
		})
	})

	Describe("PATCH, which names what it writes", func() {
		BeforeEach(func() {
			seed(&testdatav1.Guarded{Id: "g1", Label: "seeded", Owner: "alice"})
		})

		It("refuses a mask naming a protected field and leaves the row", func() {
			msg := denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{Owner: "mallory"}, "owner"))[0])

			Expect(msg).To(ContainSubstring("owner"))
			Expect(read("g1").GetOwner()).To(Equal("alice"))
		})

		It("refuses a mask reaching INTO a protected message", func() {
			Expect(denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{Audit: &testdatav1.Settings{Size: 3}}, "audit.size"))[0])).
				To(ContainSubstring("audit"))
		})

		It("refuses a mask naming the group a protected oneof member belongs to", func() {
			Expect(denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{State: &testdatav1.Guarded_StateCode{StateCode: 7}}, "state"))[0])).
				To(ContainSubstring("state"))
		})

		It("refuses before the app's policy is consulted", func() {
			recorder := &recordingPolicy{}
			srv = newTestServer(
				server.WithEntities(&testdatav1.Guarded{}),
				server.WithPermissions(recorder))
			path, handler := srv.Handler()
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			ts := httptest.NewServer(mux)
			DeferCleanup(ts.Close)
			client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
			seed(&testdatav1.Guarded{Id: "g2", Label: "seeded", Owner: "alice"})

			push(patchMutation("c1", 1, "guarded", "g2", &testdatav1.Guarded{Owner: "mallory"}, "owner"))

			Expect(recorder.writes).To(BeEmpty())
		})

		It("applies a mask that names only writable fields", func() {
			applied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{Label: "edited"}, "label"))[0])

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("edited"))
			Expect(row.GetOwner()).To(Equal("alice"))
		})

		It("rolls back a whole batch that carries one protected mask", func() {
			result := push(batchMutation("c1", 1,
				crudPatch("guarded", "g1", &testdatav1.Guarded{Label: "edited"}, "label"),
				crudPatch("guarded", "g1", &testdatav1.Guarded{Owner: "mallory"}, "owner"),
			))[0]

			Expect(denied(result)).To(ContainSubstring("owner"))
			Expect(read("g1").GetLabel()).To(Equal("seeded"))
		})
	})

	Describe("PUT, which names no field", func() {
		It("lands, keeping the stored value of every protected field", func() {
			seed(&testdatav1.Guarded{
				Id:    "g1",
				Label: "seeded",
				Owner: "alice",
				Audit: &testdatav1.Settings{Color: "green"},
				State: &testdatav1.Guarded_StateName{StateName: "open"},
			})

			applied(push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{
				Id:    "g1",
				Label: "edited",
				Owner: "mallory",
				Audit: &testdatav1.Settings{Color: "red"},
				State: &testdatav1.Guarded_StateCode{StateCode: 9},
			}))[0])

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("edited"))
			Expect(row.GetOwner()).To(Equal("alice"))
			Expect(row.GetAudit().GetColor()).To(Equal("green"))
			Expect(row.GetStateName()).To(Equal("open"))
		})

		It("clears a protected field a client tries to seed on a new row", func() {
			applied(push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{
				Id: "g1", Label: "made", Owner: "mallory",
			}))[0])

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("made"))
			Expect(row.GetOwner()).To(BeEmpty())
		})

		It("keeps the stored document of a protected collaborative-text field", func() {
			seed(&testdatav1.Guarded{Id: "g1", Label: "seeded", Summary: bodyDoc("srv", "server wrote this")})

			applied(push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{
				Id: "g1", Label: "edited", Summary: bodyDoc("cli", "client wrote this"),
			}))[0])

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("edited"))
			// Not a union of the two documents: the client's bytes are discarded
			// before the text machinery ever decodes them.
			Expect(plainBody(row.GetSummary())).To(Equal("server wrote this"))
		})

		It("leaves a row with no protected field untouched", func() {
			applied(push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{
				Id: "g1", Label: "made", Settings: &testdatav1.Settings{Color: "blue"},
			}))[0])

			Expect(read("g1").GetSettings().GetColor()).To(Equal("blue"))
		})
	})

	Describe("DELETE", func() {
		It("is unaffected: a protected field guards a value, not the row", func() {
			seed(&testdatav1.Guarded{Id: "g1", Owner: "alice"})

			applied(push(deleteMutation("c1", 1, "guarded", "g1"))[0])

			Expect(exists("g1")).To(BeFalse())
		})
	})

	Describe("the writes protection does not reach", func() {
		BeforeEach(func() {
			seed(&testdatav1.Guarded{Id: "g1", Label: "seeded", Owner: "alice"})
		})

		It("admits a Trusted write, which is what TRUSTED_ONLY is defined against", func() {
			Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "bob"}, []string{"owner"}, nil)
			})).To(Succeed())

			Expect(read("g1").GetOwner()).To(Equal("bob"))
		})

		It("admits a mutator body, which the verdict on its invocation covers", func() {
			applied(push(customMutation("c1", 1, "reassign",
				&testdatav1.Guarded{Id: "g1", Owner: "bob"}))[0])

			Expect(read("g1").GetOwner()).To(Equal("bob"))
		})
	})

	Describe("the in-process API, held to the same policy as a client", func() {
		BeforeEach(func() {
			seed(&testdatav1.Guarded{Id: "g1", Label: "seeded", Owner: "alice"})
		})

		It("refuses an Apply that patches a protected field", func() {
			err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "mallory"}, []string{"owner"}, nil)
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))
			Expect(read("g1").GetOwner()).To(Equal("alice"))
		})

		It("carries protected fields over on an Apply that puts", func() {
			Expect(srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
				return tx.Put(&testdatav1.Guarded{Id: "g1", Label: "edited", Owner: "mallory"})
			})).To(Succeed())

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("edited"))
			Expect(row.GetOwner()).To(Equal("alice"))
		})

		It("refuses a Mutate that patches a protected field", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "mallory"}, []string{"owner"}, nil)
			})

			_, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(read("g1").GetOwner()).To(Equal("alice"))
		})

		It("carries protected fields over on a Mutate that puts", func() {
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Guarded{Id: "g1", Label: "edited", Owner: "mallory"})
			})).To(Succeed())

			Expect(read("g1").GetOwner()).To(Equal("alice"))
		})

		It("refuses a WithTextString naming a protected field, which the option names outright", func() {
			err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Guarded{Id: "g1", Label: "edited"},
					server.WithTextString("summary", "client wrote this"),
					server.WithTextPeer("cli"))
			})

			rej, ok := server.AsRejection(err)
			Expect(ok).To(BeTrue(), "got %v", err)
			Expect(rej.Code).To(Equal(server.CodePermissionDenied))
		})
	})

	Describe("per-caller protection", func() {
		BeforeEach(func() {
			policy = &fieldPolicy{protect: map[string][]string{"guarded": {"label"}}}
			build()
			seed(&testdatav1.Guarded{Id: "g1", Label: "seeded", Owner: "alice"})
		})

		It("refuses a PATCH of a field only the hook protects", func() {
			Expect(denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{Label: "edited"}, "label"))[0])).To(ContainSubstring("label"))

			Expect(read("g1").GetLabel()).To(Equal("seeded"))
		})

		It("carries a field only the hook protects over on a PUT", func() {
			applied(push(putMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{
				Id: "g1", Label: "edited", Settings: &testdatav1.Settings{Color: "blue"},
			}))[0])

			row := read("g1")
			Expect(row.GetLabel()).To(Equal("seeded"))
			Expect(row.GetSettings().GetColor()).To(Equal("blue"))
		})

		It("still protects what the schema marks", func() {
			Expect(denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{Owner: "mallory"}, "owner"))[0])).To(ContainSubstring("owner"))
		})

		It("resolves the hook once per entity per request", func() {
			policy.calls = 0

			push(
				patchMutation("c1", 1, "guarded", "g1", &testdatav1.Guarded{Settings: &testdatav1.Settings{}}, "settings"),
				patchMutation("c1", 2, "guarded", "g1", &testdatav1.Guarded{Settings: &testdatav1.Settings{}}, "settings"),
			)

			Expect(policy.calls).To(Equal(1))
		})

		It("fails the batch when the hook cannot decide, rather than admitting the write", func() {
			policy.fail = context.DeadlineExceeded

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws,
				[]*syncplanev1.Mutation{patchMutation("c1", 1, "guarded", "g1",
					&testdatav1.Guarded{Label: "edited"}, "label")})))

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(read("g1").GetLabel()).To(Equal("seeded"))
		})

		It("fails the batch when the hook names a field that does not exist", func() {
			policy.protect = map[string][]string{"guarded": {"nosuchfield"}}

			resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws,
				[]*syncplanev1.Mutation{patchMutation("c1", 1, "guarded", "g1",
					&testdatav1.Guarded{Label: "edited"}, "label")})))

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeInternal))
			Expect(read("g1").GetLabel()).To(Equal("seeded"))
		})

		It("resolves a oneof member the hook names to its group", func() {
			policy.protect = map[string][]string{"guarded": {"state_code"}}

			Expect(denied(push(patchMutation("c1", 1, "guarded", "g1",
				&testdatav1.Guarded{State: &testdatav1.Guarded_StateCode{StateCode: 7}}, "state"))[0])).
				To(ContainSubstring("state"))
		})
	})
})
