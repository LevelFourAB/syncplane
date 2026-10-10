package server_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	gosync "sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// hideTask hides one task from every caller, so a spec can show that the
// application's ReadFilter narrows what the model allows.
type hideTask struct {
	server.AllowAll
	key string
}

func (h hideTask) ReadFilter(_ context.Context, _ server.Principal, _, entity string) (server.RowFilter, error) {
	if entity != "task" {
		return nil, nil
	}
	return func(key string, _ proto.Message) bool { return key != h.key }, nil
}

var _ = Describe("Access model reads", func() {
	var f *accessFixture

	bootstrap := func(user string) []string {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user})
		as(user, req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var rows []string
		for stream.Receive() {
			if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
				for _, rc := range chunk.Chunk.GetRows() {
					rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		sort.Strings(rows)
		return rows
	}

	queryKeys := func(user, entity string) []string {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: entity})
		as(user, req.Header())
		resp, err := f.sync.Query(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var keys []string
		for _, rc := range resp.Msg.GetRows() {
			keys = append(keys, rc.GetKey())
		}
		sort.Strings(keys)
		return keys
	}

	everything := []string{
		"label/l1", "note/n1",
		"org_member/om-admin", "org_member/om-member",
		"project/p1", "project/p2",
		"project_member/pm-badrole", "project_member/pm-ghost", "project_member/pm-guest",
		"project_member/pm-owner", "project_member/pm-padmin", "project_member/pm-writer",
		"task/t1", "task/t2", "task/t3",
	}
	guestSees := []string{
		"note/n1", "project/p1",
		"project_member/pm-badrole", "project_member/pm-guest", "project_member/pm-owner", "project_member/pm-padmin",
		"task/t1",
	}

	Context("with the subject index built and without it", func() {
		for _, indexed := range []bool{false, true} {
			Context(map[bool]string{false: "scanning grant rows", true: "probing the subject index"}[indexed], func() {
				BeforeEach(func() {
					f = newAccessFixture()
					f.seedOrg()
					if indexed {
						Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
					}
				})

				It("shows a root role every row", func() {
					Expect(f.pull("admin")).To(Equal(everything))
				})

				It("shows a root role only the entities its permissions name", func() {
					Expect(f.pull("member")).To(Equal([]string{
						"label/l1", "note/n1", "project/p1", "project/p2", "task/t1", "task/t2", "task/t3",
					}))
				})

				// The guest holds project_reader at p1 and nothing at the root,
				// which is what the access model exists for.
				It("shows a project role the project and the rows below it", func() {
					Expect(f.pull("guest")).To(Equal(guestSees))
					Expect(f.pull("writer")).To(Equal([]string{
						"note/n1", "project/p2", "project_member/pm-writer", "task/t2",
					}))
				})
			})
		}
	})

	Context("admission", func() {
		BeforeEach(func() {
			f = newAccessFixture()
			f.seedOrg()
		})

		DescribeTable("refuses a caller who holds no permission",
			func(user string) {
				_, err := f.pullErr(user)
				Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
				var cerr *connect.Error
				Expect(err).To(BeAssignableToTypeOf(cerr))
				Expect(err.(*connect.Error).Meta().Get(server.ErrorCodeMetadataKey)).To(Equal(server.CodeWorkspaceNoAccess))
			},
			Entry("an anonymous caller", ""),
			Entry("a caller with no grant rows", "stranger"),
			Entry("a caller whose grant is anchored at a missing row", "ghost"),
			Entry("a caller whose grant names an undeclared role", "badrole"),
		)

		It("never matches a grant row with an empty subject", func() {
			f.plant("org_member", "om-empty", &accessv1.OrgMember{Id: "om-empty", UserId: "", Role: "org_admin"})
			_, err := f.pullErr("")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("admits a caller as soon as a grant row names them", func() {
			_, err := f.pullErr("late")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
			f.seed(&accessv1.ProjectMember{Id: "pm-late", Project: project("p2"), UserId: "late", Role: "project_reader"})
			Expect(f.pull("late")).To(ContainElement("task/t2"))
		})

		It("tells a caller with no grant on Push and Subscribe that a grant can still admit it", func() {
			req := connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
				putMutation("c-late", 1, "task", "t9", &accessv1.Task{Id: "t9", Project: project("p2")}),
			}))
			as("late", req.Header())
			resp, err := f.sync.Push(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeWorkspaceNoAccess))

			sub := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-late",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
			})
			as("late", sub.Header())
			streamCtx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			stream, err := f.sync.Subscribe(streamCtx, sub)
			Expect(err).NotTo(HaveOccurred())
			var dropped *syncplanev1.WorkspaceDropped
			for dropped == nil && stream.Receive() {
				dropped = stream.Msg().GetDropped()
			}
			Expect(dropped.GetReason()).To(Equal(syncplanev1.WorkspaceDropped_REASON_NO_ACCESS))
		})
	})

	Context("caller access", func() {
		It("grants a root role at the root of the workspace", func() {
			f = newAccessFixture(rootRolesBy(func(p server.Principal) []string {
				if staff, _ := p.StringClaim("staff"); staff == "admin" {
					return []string{"org_admin"}
				}
				return nil
			}))
			f.seedOrg()
			req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws})
			req.Header().Set(userHeader, "staffer")
			req.Header().Set(staffHeader, "admin")
			resp, err := f.sync.Pull(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Msg.GetPatch().GetChanges()).To(HaveLen(len(everything)))
		})

		It("ignores a root role the schema does not declare, and warns once", func() {
			logs := &recordingHandler{}
			f = newAccessFixture(server.WithLogger(slog.New(logs)), rootRolesBy(func(server.Principal) []string {
				return []string{"superuser", "org_admin"}
			}))
			f.seedOrg()
			Expect(f.pull("nobody")).To(Equal(everything))
			Expect(f.pull("nobody")).To(Equal(everything))
			var unknown []string
			for _, w := range logs.attrs("access model warning", "finding") {
				if strings.Contains(w, "root role") {
					unknown = append(unknown, w)
				}
			}
			Expect(unknown).To(ConsistOf(
				ContainSubstring(`root role "superuser", which no grant entity of the schema declares`)))
		})

		It("refuses a caller whose only root role the schema does not declare", func() {
			f = newAccessFixture(rootRolesBy(func(server.Principal) []string {
				return []string{"superuser"}
			}))
			f.seedOrg()
			_, err := f.pullErr("nobody")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("hands the hook the workspace of the request", func() {
			var seen gosync.Map
			f = newAccessFixture(server.WithCallerAccess(
				func(_ context.Context, p server.Principal, workspaceID string) (server.CallerAccess, error) {
					seen.Store(workspaceID, true)
					return server.CallerAccess{Subjects: []string{p.Subject}}, nil
				}))
			f.seedOrg()
			Expect(f.pull("admin")).To(Equal(everything))
			_, ok := seen.Load(ws)
			Expect(ok).To(BeTrue())
		})

		It("gives a caller no subject when the hook returns none", func() {
			f = newAccessFixture(server.WithCallerAccess(
				func(context.Context, server.Principal, string) (server.CallerAccess, error) {
					return server.CallerAccess{}, nil
				}))
			f.seedOrg()
			_, err := f.pullErr("admin")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})

		It("fails the request when the hook fails, and keeps the code of a connect error", func() {
			fail := errors.New("directory is down")
			f = newAccessFixture(server.WithCallerAccess(
				func(context.Context, server.Principal, string) (server.CallerAccess, error) {
					return server.CallerAccess{}, fail
				}))
			f.seedOrg()
			_, err := f.pullErr("admin")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeInternal))
			Expect(err).To(MatchError(ContainSubstring("directory is down")))

			fail = connect.NewError(connect.CodeUnavailable, errors.New("directory is down"))
			_, err = f.pullErr("admin")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodeUnavailable))
		})

		It("matches grant rows against every subject the hook returns", func() {
			f = newAccessFixture(subjectsBy(func(p server.Principal) []string {
				if strings.HasPrefix(p.Subject, "eng") {
					return []string{p.Subject, "team:eng", ""}
				}
				return []string{p.Subject}
			}))
			f.seedOrg()
			f.seed(&accessv1.ProjectMember{Id: "pm-team", Project: project("p2"), UserId: "team:eng", Role: "project_reader"})
			Expect(f.pull("eng1")).To(ContainElements("project/p2", "task/t2"))
			_, err := f.pullErr("ops1")
			Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied))
		})
	})

	Context("read paths", func() {
		BeforeEach(func() {
			f = newAccessFixture()
			f.seedOrg()
		})

		It("filters a bootstrap snapshot", func() {
			Expect(bootstrap("guest")).To(Equal(guestSees))
			Expect(bootstrap("admin")).To(Equal(everything))
		})

		It("filters the Query RPC", func() {
			Expect(queryKeys("guest", "task")).To(Equal([]string{"t1"}))
			Expect(queryKeys("member", "task")).To(Equal([]string{"t1", "t2", "t3"}))
		})

		// A nil filter lets a grouped count read index entries without rows,
		// so a caller the model narrows must not get one.
		It("counts only the rows a grouped Query may show", func() {
			count := func(user string) uint64 {
				GinkgoHelper()
				req := connect.NewRequest(&syncplanev1.QueryRequest{
					WorkspaceId: ws, Entity: "task",
					GroupBy:   []string{"project", "id"},
					Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
				})
				as(user, req.Header())
				resp, err := f.sync.Query(f.ctx, req)
				Expect(err).NotTo(HaveOccurred())
				var total uint64
				for _, g := range resp.Msg.GetGroups() {
					total += g.GetCount()
				}
				return total
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			Expect(count("guest")).To(Equal(uint64(1)))
			Expect(count("admin")).To(Equal(uint64(3)))
		})

		It("filters in-process reads by the ctx principal", func() {
			guest := server.WithPrincipal(f.ctx, server.Principal{Subject: "guest"})
			Expect(f.srv.View(guest, ws, func(tx *server.ViewTx) error {
				found, _, err := tx.Get(&accessv1.Task{}, "t1")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				found, _, err = tx.Get(&accessv1.Task{}, "t2")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeFalse())
				return nil
			})).To(Succeed())

			stranger := server.WithPrincipal(f.ctx, server.Principal{Subject: "stranger"})
			err := f.srv.View(stranger, ws, func(*server.ViewTx) error { return nil })
			_, denied := server.AsWorkspaceDenial(err)
			Expect(denied).To(BeTrue())
			Expect(err).To(MatchError(server.ErrNoAccess))
		})

		It("filters a Subscribe catch-up", func() {
			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-guest",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: ws}},
			})
			as("guest", req.Header())
			streamCtx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			stream, err := f.sync.Subscribe(streamCtx, req)
			Expect(err).NotTo(HaveOccurred())
			var rows []string
			for stream.Receive() {
				msg := stream.Msg()
				if msg.GetCaughtUp() {
					break
				}
				for _, rc := range msg.GetPage().GetPatch().GetChanges() {
					if !rc.GetTombstone() {
						rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
					}
				}
			}
			sort.Strings(rows)
			Expect(rows).To(Equal(guestSees))
		})

		It("admits a room anchored on a readable row only", func() {
			watch := func(key string) *syncplanev1.WatchResponse {
				GinkgoHelper()
				streamCtx, cancel := context.WithCancel(f.ctx)
				DeferCleanup(cancel)
				req := connect.NewRequest(&syncplanev1.WatchRequest{
					SessionId: "s-" + key,
					Topics:    []*syncplanev1.Topic{{WorkspaceId: ws, Name: "cursors", Entity: "task", Key: key}},
				})
				as("guest", req.Header())
				stream, err := f.presence.Watch(streamCtx, req)
				Expect(err).NotTo(HaveOccurred())
				for stream.Receive() {
					if !stream.Msg().GetHeartbeat() {
						return stream.Msg()
					}
				}
				Fail("stream closed early: " + stream.Err().Error())
				return nil
			}
			Expect(watch("t1").GetSnapshot()).NotTo(BeNil())
			Expect(watch("t2").GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_ACCESS_REVOKED))
		})
	})

	// A guest's sync should cost what the guest receives, not the size of the
	// workspace: every entity the model narrows is read by key.
	It("bootstraps a caller below the root without scanning the entities it narrows", func() {
		counting := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
		f = newAccessFixtureOn(counting)
		f.seedOrg()
		for i := range 50 {
			f.seed(&accessv1.Task{Id: fmt.Sprintf("x%02d", i), Project: project("p2")})
		}
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		counting.reset()
		Expect(bootstrap("guest")).To(Equal(guestSees))
		for _, entity := range []string{"task", "project", "project_member", "org_member", "label"} {
			Expect(counting.count(entity)).To(BeZero(), entity)
		}
	})

	// The same holds for the Query RPC, whichever way a query is answered: an
	// index walk skips rows the guest cannot read, and a query no index serves
	// reads the guest's rows by key instead of scanning.
	Context("a Query by a caller below the root", func() {
		var counting *readCountingAdapter

		BeforeEach(func() {
			counting = &readCountingAdapter{Adapter: newTestStore(), counts: map[string]int{}}
			f = newAccessFixtureOn(counting)
			f.seedOrg()
			for i := range 50 {
				f.seed(&accessv1.Task{Id: fmt.Sprintf("x%02d", i), Project: project("p2"), Title: "in two"})
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			counting.reset()
		})

		ask := func(req *syncplanev1.QueryRequest) *syncplanev1.QueryResponse {
			GinkgoHelper()
			req.WorkspaceId = ws
			r := connect.NewRequest(req)
			as("guest", r.Header())
			resp, err := f.sync.Query(f.ctx, r)
			Expect(err).NotTo(HaveOccurred())
			return resp.Msg
		}
		keysOf := func(rows []*syncplanev1.RowChange) []string {
			keys := []string{}
			for _, rc := range rows {
				keys = append(keys, rc.GetKey())
			}
			sort.Strings(keys)
			return keys
		}

		It("reads the guest's rows by key when no index serves the predicate", func() {
			resp := ask(&syncplanev1.QueryRequest{Entity: "task", Predicate: query.Neq("title", "x")})
			Expect(keysOf(resp.GetRows())).To(Equal([]string{"t1"}))
			Expect(counting.count("scan:task")).To(BeZero())
			Expect(counting.count("batchkeys:task")).To(Equal(1))
		})

		It("skips the rows an index names that the guest cannot read", func() {
			resp := ask(&syncplanev1.QueryRequest{Entity: "task", Predicate: query.In("project.id", "p1", "p2")})
			Expect(keysOf(resp.GetRows())).To(Equal([]string{"t1"}))
			Expect(counting.count("scan:task")).To(BeZero())
			Expect(counting.count("batchkeys:task")).To(Equal(1))
		})

		It("counts groups from the guest's rows only", func() {
			resp := ask(&syncplanev1.QueryRequest{
				Entity:    "task",
				GroupBy:   []string{"project", "id"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			})
			Expect(resp.GetGroups()).To(HaveLen(1))
			Expect(resp.GetGroups()[0].GetCount()).To(Equal(uint64(1)))
			Expect(counting.count("scan:task")).To(BeZero())
			Expect(counting.count("batchkeys:task")).To(Equal(1))
		})

		It("returns grouped rows from the guest's rows only", func() {
			resp := ask(&syncplanev1.QueryRequest{
				Entity:    "task",
				GroupBy:   []string{"title"},
				Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_ROWS,
				PerGroup:  5,
			})
			Expect(resp.GetRowGroups()).To(HaveLen(1))
			Expect(keysOf(resp.GetRowGroups()[0].GetRows())).To(Equal([]string{"t1"}))
			Expect(counting.count("scan:task")).To(BeZero())
			Expect(counting.count("batchkeys:task")).To(Equal(1))
		})

		It("matches a reference against the target rows the guest can read", func() {
			// Resolving the guest's grants reads the project their grant
			// anchors at; a query that does not cross into projects reads the
			// same.
			ask(&syncplanev1.QueryRequest{Entity: "task"})
			resolving := counting.count("batchkeys:project")
			counting.reset()

			resp := ask(&syncplanev1.QueryRequest{
				Entity:    "task",
				Predicate: query.Rel("project", query.Neq("name", "x")),
			})
			Expect(keysOf(resp.GetRows())).To(Equal([]string{"t1"}))
			Expect(counting.count("scan:project")).To(BeZero())
			Expect(counting.count("batchkeys:project")).To(Equal(resolving + 1))
		})
	})

	It("lets the application's ReadFilter narrow what the model allows", func() {
		f = newAccessFixture(server.WithPermissions(hideTask{key: "t1"}))
		f.seedOrg()
		Expect(f.pull("guest")).NotTo(ContainElement("task/t1"))
		Expect(f.pull("admin")).NotTo(ContainElement("task/t1"))
		Expect(f.pull("admin")).To(ContainElement("task/t2"))
	})
})
