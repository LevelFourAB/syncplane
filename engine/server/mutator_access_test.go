package server_test

import (
	"context"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Mutators in a schema that uses the access model", func() {
	noop := func(context.Context, *server.EntityTx, proto.Message) error { return nil }

	// warningsNaming returns the text of every warning in logs whose message
	// or attributes name s.
	warningsNaming := func(logs *recordingHandler, s string) []string {
		logs.mu.Lock()
		defer logs.mu.Unlock()
		var out []string
		for _, r := range logs.records {
			if r.Level < slog.LevelWarn {
				continue
			}
			text := r.Message
			r.Attrs(func(a slog.Attr) bool {
				text += " " + a.Value.String()
				return true
			})
			if strings.Contains(text, s) {
				out = append(out, text)
			}
		}
		return out
	}

	It("warns at registration about a mutator that no client can invoke because it declares no MutatorAccess", func() {
		// Given an application whose mutator "archive_all" predates the access
		// model and declares no MutatorAccess
		logs := &recordingHandler{}

		// When it registers a schema in which entities declare access
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), accessEntities(),
			server.WithMutator("archive_all", nil, noop))

		// Then registration tells the operator that only trusted callers can
		// invoke the mutator now, before the first client invocation is refused
		Expect(warningsNaming(logs, "archive_all")).NotTo(BeEmpty(),
			"nothing at registration says that every client invocation of archive_all is refused")
	})

	It("does not warn about a mutator that declares TrustedOnly", func() {
		// Given a mutator meant for server code alone, which says so
		logs := &recordingHandler{}

		// When it registers in a schema in which entities declare access
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), accessEntities(),
			server.WithMutator("rebuild", nil, noop, server.MutatorAccess{TrustedOnly: true}))

		// Then registration logs nothing about it
		Expect(warningsNaming(logs, "rebuild")).To(BeEmpty())
	})

	It("refuses a TrustedOnly mutator to every caller but a trusted one", func() {
		// Given a TrustedOnly mutator in the org schema
		f := newAccessFixture(server.WithMutator("rebuild", nil, noop, server.MutatorAccess{TrustedOnly: true}))
		f.seedOrg()

		// When the org admin invokes it, and then a trusted caller
		admin := server.WithPrincipal(f.ctx, server.Principal{Subject: "admin"})
		err := f.srv.Invoke(admin, ws, "rebuild", nil)

		// Then the admin is refused with access_denied, and the trusted caller
		// is not
		r, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "%v", err)
		Expect(r.Code).To(Equal(server.CodeAccessDenied))
		Expect(f.srv.Invoke(server.Trusted(f.ctx), ws, "rebuild", nil)).To(Succeed())
	})

	It("refuses TrustedOnly together with another field", func() {
		_, err := server.New(newTestStore(), accessEntities(),
			server.WithMutator("rebuild", nil, noop, server.MutatorAccess{TrustedOnly: true, Permission: "edit_tasks"}))
		Expect(err).To(MatchError(ContainSubstring("sets TrustedOnly together with")))
	})

	It("answers Can and checks AsCaller writes with a grant row the body wrote earlier", func() {
		// Given a mutator whose body makes the invoking caller an admin of
		// project p2 with an unchecked write, as a "create project" mutator
		// makes its creator the first admin
		var canManage bool
		promote := func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
			if err := tx.Put(&accessv1.ProjectMember{Id: "pm-guest-p2", Project: project("p2"), UserId: "guest", Role: "project_admin"}); err != nil {
				return err
			}
			var err error
			if canManage, err = tx.Can("manage_members", &accessv1.Project{}, "p2"); err != nil {
				return err
			}
			return tx.AsCaller().Put(&accessv1.ProjectMember{Id: "pm-member-p2", Project: project("p2"), UserId: "member", Role: "project_reader"})
		}
		f := newAccessFixture(server.WithMutator("promote", nil, promote, server.MutatorAccess{Admitted: true}))
		f.seedOrg()

		// When guest, who reads p1 only, invokes it
		req := connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{{
			ClientId: "c-guest", MutationId: 1,
			Payload: &syncplanev1.Mutation_Custom{Custom: &syncplanev1.CustomMutation{Name: "promote"}},
		}}))
		as("guest", req.Header())
		resp, err := f.sync.Push(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())

		// Then the body sees the role it gave, and the caller's own write of a
		// grant at p2 passes the subset rule
		Expect(pushResults(resp.Msg)[0].GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED),
			"%s", pushResults(resp.Msg)[0].GetErrorMessage())
		Expect(canManage).To(BeTrue())
		Expect(f.pull("guest")).To(ContainElements("project_member/pm-guest-p2", "project_member/pm-member-p2"))
	})
})
