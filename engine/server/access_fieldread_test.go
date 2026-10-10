package server_test

import (
	"fmt"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// The fixture is "staff" in proto/private/syncplane/testdata/access/v1, below
// a project. Its guards let only edit_projects read `salary` and `bio`, only
// the person a record is about read `review`, and no client read `bonus` or
// `pin`; padmin writes `pin` all the same.
//
// In seedOrg, admin holds edit_projects at the root, owner at p1 alone, and
// guest, padmin and member read the tasks of p1 (member of every project)
// without it.

var _ = Describe("Access model registration of field read rules", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	accessOf := func(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.AccessOptions {
		GinkgoHelper()
		m := messageNamed(file, message)
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	}
	hiding := func(fields ...string) *syncplanev1.FieldGuard {
		return &syncplanev1.FieldGuard{Fields: fields, Allow: []*syncplanev1.AccessRule{
			{Read: true, Requires: []string{"edit_projects"}},
		}}
	}

	It("registers a guard without a read rule and a read rule with a source and a check", func() {
		_, err := srv.RegisterDescriptorSet(accessFileSet(nil))
		Expect(err).NotTo(HaveOccurred())
	})

	DescribeTable("refuses to hide a field a client reads to decide access",
		func(change func(file *descriptorpb.FileDescriptorProto), want string) {
			_, err := srv.RegisterDescriptorSet(accessFileSet(change))
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("a field a rule of the entity checks with caller_is", func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "Staff")
			a.Allow = append(a.Allow, &syncplanev1.AccessRule{Read: true, Admitted: &syncplanev1.AdmittedCaller{}, CallerIs: "review"})
		}, `access allow[2] reads "review", which a guard can hide`),
		Entry("a field a rule of the entity checks with when", func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "Staff")
			a.Allow = append(a.Allow, &syncplanev1.AccessRule{
				Update: true, Requires: []string{"edit_tasks"}, When: query.Gt("salary", int64(0)),
			})
		}, `access allow[2] reads "salary", which a guard can hide`),
		Entry("a field a within entry checks", func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "Binder")
			a.Guard = append(a.Guard, hiding("owner_id"))
		}, `reads "owner_id" of entity "binder", which a guard can hide`),
		Entry("the subject of a grant entity", func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "OrgMember")
			a.Guard = append(a.Guard, hiding("user_id"))
		}, `grant field "user_id" is restricted by a guard that can hide it`),
		Entry("the role of a grant entity", func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "OrgMember")
			a.Guard = append(a.Guard, hiding("role"))
		}, `grant field "role" is restricted by a guard that can hide it`),
	)

	It("refuses a change of a guard's read rules unless the registration allows it", func() {
		_, err := srv.RegisterDescriptorSet(accessFileSet(nil))
		Expect(err).NotTo(HaveOccurred())
		change := func(file *descriptorpb.FileDescriptorProto) {
			a := accessOf(file, "Staff")
			a.Guard[0].Allow = append(a.Guard[0].Allow, &syncplanev1.AccessRule{Read: true, Requires: []string{"manage_members"}})
		}

		_, err = srv.RegisterDescriptorSet(accessFileSet(change))
		Expect(err).To(MatchError(ContainSubstring("AllowAccessChange")))
		_, err = srv.RegisterDescriptorSet(accessFileSet(change), server.AllowAccessChange())
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Access model field read rules", func() {
	var (
		f       *accessFixture
		clients int
	)

	bio := bodyDoc("seed", "a biography long enough that a delta of it would be worth sending on its own")

	BeforeEach(func() {
		clients = 0
		f = newAccessFixture(server.WithEntities(&accessv1.Staff{}))
		f.seedOrg()
		f.seed(
			&accessv1.Staff{Id: "s1", Project: project("p1"), Name: "ann", Salary: 5000, Review: "good",
				PersonId: "guest", Bonus: 7, Pin: "1234", Bio: bio},
			&accessv1.Staff{Id: "s2", Project: project("p2"), Name: "bob", Salary: 6000, Review: "fine",
				PersonId: "writer", Bonus: 9, Pin: "5678"},
		)
	})

	pullFrom := func(user string, cursor []byte, subs ...*syncplanev1.Subscription) (*syncplanev1.PullResponse, error) {
		req := connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: ws, ClientId: "c-" + user, Cursor: cursor, Subscriptions: subs,
		})
		as(user, req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	staffIn := func(changes []*syncplanev1.RowChange) map[string]*syncplanev1.RowChange {
		out := map[string]*syncplanev1.RowChange{}
		for _, rc := range changes {
			if rc.GetEntity() == "staff" {
				out[rc.GetKey()] = rc
			}
		}
		return out
	}
	pulled := func(user string) map[string]*syncplanev1.RowChange {
		GinkgoHelper()
		resp, err := pullFrom(user, nil)
		Expect(err).NotTo(HaveOccurred())
		return staffIn(resp.GetPatch().GetChanges())
	}
	decoded := func(rc *syncplanev1.RowChange) *accessv1.Staff {
		GinkgoHelper()
		Expect(rc).NotTo(BeNil())
		var out accessv1.Staff
		Expect(proto.Unmarshal(rc.GetValue(), &out)).To(Succeed())
		return &out
	}
	queryAs := func(user string, req *syncplanev1.QueryRequest) (*syncplanev1.QueryResponse, error) {
		req.WorkspaceId = ws
		r := connect.NewRequest(req)
		as(user, r.Header())
		resp, err := f.sync.Query(f.ctx, r)
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	push := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user string, value *accessv1.Staff) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, "staff", value.GetId(), value) })
	}
	patch := func(user, key string, value *accessv1.Staff, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, "staff", key, value, paths...) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	stored := func(key string) *accessv1.Staff {
		GinkgoHelper()
		var out accessv1.Staff
		Expect(f.srv.View(server.Trusted(f.ctx), ws, func(tx *server.ViewTx) error {
			found, _, err := tx.Get(&out, key)
			Expect(found).To(BeTrue())
			return err
		})).To(Succeed())
		return &out
	}
	permissionDenied := func(err error) {
		GinkgoHelper()
		Expect(err).To(HaveOccurred())
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied), "%v", err)
	}

	Describe("sync", func() {
		It("clears each field a guard hides from the caller and names it", func() {
			rc := pulled("guest")["s1"]
			Expect(rc.GetHidden()).To(Equal([]string{"bio", "bonus", "pin", "salary"}))
			got := decoded(rc)
			Expect(got.GetName()).To(Equal("ann"))
			Expect(got.GetReview()).To(Equal("good"), "the record is about guest")
			Expect(got.GetSalary()).To(BeZero())
			Expect(got.GetBio()).To(BeEmpty())
			Expect(got.GetBonus()).To(BeZero())
			Expect(got.GetPin()).To(BeEmpty())
		})

		It("decides per row and per anchor", func() {
			owner := pulled("owner")
			Expect(owner).To(HaveKey("s1"))
			Expect(owner["s1"].GetHidden()).To(Equal([]string{"bonus", "pin", "review"}))
			Expect(decoded(owner["s1"]).GetSalary()).To(Equal(int64(5000)))
			Expect(plainBody(decoded(owner["s1"]).GetBio())).To(ContainSubstring("a biography"))

			admin := pulled("admin")
			Expect(admin["s1"].GetHidden()).To(Equal([]string{"bonus", "pin", "review"}))
			Expect(admin["s2"].GetHidden()).To(Equal([]string{"bonus", "pin", "review"}))
			Expect(decoded(admin["s2"]).GetSalary()).To(Equal(int64(6000)))

			member := pulled("member")
			Expect(member["s2"].GetHidden()).To(Equal([]string{"bio", "bonus", "pin", "review", "salary"}))
		})

		It("projects a bootstrap the same way", func() {
			req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest"})
			as("guest", req.Header())
			stream, err := f.sync.Bootstrap(f.ctx, req)
			Expect(err).NotTo(HaveOccurred())
			var rows []*syncplanev1.RowChange
			for stream.Receive() {
				if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
					rows = append(rows, chunk.Chunk.GetRows()...)
				}
			}
			Expect(stream.Err()).NotTo(HaveOccurred())
			rc := staffIn(rows)["s1"]
			Expect(rc.GetHidden()).To(Equal([]string{"bio", "bonus", "pin", "salary"}))
			Expect(decoded(rc).GetSalary()).To(BeZero())
		})

		It("sends no text delta state for a field the caller may not read", func() {
			first, err := pullFrom("guest", nil)
			Expect(err).NotTo(HaveOccurred())
			edited := editBody(bio, "owner", func(d *text.Doc) { Expect(d.SetString("a new biography")).To(Succeed()) })
			applied(patch("owner", "s1", &accessv1.Staff{Bio: edited, Name: "anna"}, "bio", "name"))

			next, err := pullFrom("guest", first.GetNextCursor())
			Expect(err).NotTo(HaveOccurred())
			rc := staffIn(next.GetPatch().GetChanges())["s1"]
			Expect(rc.GetTextDeltas()).To(BeEmpty())
			Expect(rc.GetHidden()).To(ContainElement("bio"))
			Expect(decoded(rc).GetBio()).To(BeEmpty())
			Expect(decoded(rc).GetName()).To(Equal("anna"))
		})

		It("sends a row again when a change of the caller's grants shows or hides a field", func() {
			first, err := pullFrom("guest", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(staffIn(first.GetPatch().GetChanges())["s1"].GetHidden()).To(ContainElement("salary"))

			f.seed(&accessv1.ProjectMember{Id: "pm-guest", Project: project("p1"), UserId: "guest", Role: "project_owner"})
			cursor := first.GetNextCursor()
			var changes []*syncplanev1.RowChange
			for range 10 {
				resp, err := pullFrom("guest", cursor)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetReset_()).To(BeFalse())
				changes = append(changes, resp.GetPatch().GetChanges()...)
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			rc := staffIn(changes)["s1"]
			Expect(rc).NotTo(BeNil(), "the grant shows salary and bio, so s1 is sent again")
			Expect(rc.GetHidden()).To(Equal([]string{"bonus", "pin"}))
			Expect(decoded(rc).GetSalary()).To(Equal(int64(5000)))
			Expect(plainBody(decoded(rc).GetBio())).To(ContainSubstring("a biography"))

			f.seed(&accessv1.ProjectMember{Id: "pm-guest", Project: project("p1"), UserId: "guest", Role: "project_reader"})
			changes = nil
			for range 10 {
				resp, err := pullFrom("guest", cursor)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.GetReset_()).To(BeFalse())
				changes = append(changes, resp.GetPatch().GetChanges()...)
				cursor = resp.GetNextCursor()
				if !resp.GetHasMore() {
					break
				}
			}
			rc = staffIn(changes)["s1"]
			Expect(rc).NotTo(BeNil(), "the grant hides salary and bio again")
			Expect(rc.GetHidden()).To(Equal([]string{"bio", "bonus", "pin", "salary"}))
			Expect(decoded(rc).GetSalary()).To(BeZero())
		})

		It("sends a row again when a write changes what a guard decides on the row", func() {
			first, err := pullFrom("guest", nil)
			Expect(err).NotTo(HaveOccurred())
			applied(patch("padmin", "s1", &accessv1.Staff{PersonId: "member"}, "person_id"))

			next, err := pullFrom("guest", first.GetNextCursor())
			Expect(err).NotTo(HaveOccurred())
			rc := staffIn(next.GetPatch().GetChanges())["s1"]
			Expect(rc.GetHidden()).To(ContainElement("review"))
			Expect(decoded(rc).GetReview()).To(BeEmpty())
		})

		It("refuses a subscription that names a field the caller does not read on every row", func() {
			_, err := pullFrom("guest", nil, &syncplanev1.Subscription{Entity: "staff", Predicate: query.Gt("salary", int64(0))})
			permissionDenied(err)
			_, err = pullFrom("guest", nil, &syncplanev1.Subscription{Entity: "staff", Window: &syncplanev1.Window{
				OrderBy: []*syncplanev1.OrderKey{query.Desc("salary")}, Limit: 1,
			}})
			permissionDenied(err)
			_, err = pullFrom("owner", nil, &syncplanev1.Subscription{Entity: "staff", Predicate: query.Gt("salary", int64(0))})
			permissionDenied(err)

			resp, err := pullFrom("admin", nil, &syncplanev1.Subscription{Entity: "staff", Predicate: query.Gt("salary", int64(5500))})
			Expect(err).NotTo(HaveOccurred())
			Expect(staffIn(resp.GetPatch().GetChanges())).To(HaveKey("s2"))
			_, err = pullFrom("guest", nil, &syncplanev1.Subscription{Entity: "staff", Predicate: query.Eq("name", "ann")})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Query", func() {
		It("clears each field a guard hides from the caller", func() {
			resp, err := queryAs("guest", &syncplanev1.QueryRequest{Entity: "staff"})
			Expect(err).NotTo(HaveOccurred())
			rc := staffIn(resp.GetRows())["s1"]
			Expect(rc.GetHidden()).To(Equal([]string{"bio", "bonus", "pin", "salary"}))
			Expect(decoded(rc).GetSalary()).To(BeZero())
		})

		DescribeTable("refuses a request that names a field the caller does not read on every row",
			func(user string, req *syncplanev1.QueryRequest) {
				req.Entity = "staff"
				_, err := queryAs(user, req)
				permissionDenied(err)
			},
			Entry("a predicate", "guest", &syncplanev1.QueryRequest{Predicate: query.Gt("salary", int64(0))}),
			Entry("a predicate inside not", "guest", &syncplanev1.QueryRequest{Predicate: query.Not(query.Eq("pin", "1234"))}),
			Entry("a field the caller reads at one anchor only", "owner",
				&syncplanev1.QueryRequest{Predicate: query.Gt("salary", int64(0))}),
			Entry("a field checked against the row", "guest",
				&syncplanev1.QueryRequest{Predicate: query.Eq("review", "good")}),
			Entry("an order", "guest", &syncplanev1.QueryRequest{OrderBy: []*syncplanev1.OrderKey{query.Asc("salary")}}),
			Entry("a grouping", "guest", &syncplanev1.QueryRequest{
				GroupBy: []string{"bonus"}, Aggregate: syncplanev1.AggregateOp_AGGREGATE_OP_COUNT,
			}),
			Entry("a facet", "guest", &syncplanev1.QueryRequest{Facets: []*syncplanev1.QueryFacet{{Path: []string{"pin"}}}}),
			Entry("a search over a field the guard can hide", "admin", &syncplanev1.QueryRequest{Predicate: query.Match("good")}),
		)

		It("answers a request on fields the caller reads on every row", func() {
			resp, err := queryAs("admin", &syncplanev1.QueryRequest{
				Entity: "staff", Predicate: query.Gt("salary", int64(5500)),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(staffIn(resp.GetRows())).To(HaveKey("s2"))
			Expect(staffIn(resp.GetRows())).NotTo(HaveKey("s1"))
		})
	})

	Describe("field history", func() {
		historyOf := func(user string, pred *syncplanev1.Condition) []*syncplanev1.FieldHistory {
			GinkgoHelper()
			cond := query.And(query.Eq("source_entity", "staff"), query.Eq("source_key", "s1"))
			if pred != nil {
				cond = query.And(cond, pred)
			}
			resp, err := queryAs(user, &syncplanev1.QueryRequest{
				Entity: "syncplane_field_history", Predicate: cond, OrderBy: []*syncplanev1.OrderKey{query.Asc("seq")},
			})
			Expect(err).NotTo(HaveOccurred())
			var out []*syncplanev1.FieldHistory
			for _, rc := range resp.GetRows() {
				var rec syncplanev1.FieldHistory
				Expect(proto.Unmarshal(rc.GetValue(), &rec)).To(Succeed())
				out = append(out, &rec)
			}
			return out
		}

		It("takes the fields a guard hides out of a record, and drops a record of hidden fields alone", func() {
			applied(patch("owner", "s1", &accessv1.Staff{Name: "anna", Salary: 5500}, "name", "salary"))
			applied(patch("admin", "s1", &accessv1.Staff{Salary: 6000}, "salary"))

			owner := historyOf("owner", nil)
			Expect(owner).NotTo(BeEmpty())
			last := owner[len(owner)-1]
			Expect(last.GetMask().GetPaths()).To(Equal([]string{"salary"}))

			guest := historyOf("guest", nil)
			for _, rec := range guest {
				Expect(rec.GetMask().GetPaths()).NotTo(ContainElement("salary"))
				if len(rec.GetPrior()) > 0 {
					var prior accessv1.Staff
					Expect(proto.Unmarshal(rec.GetPrior(), &prior)).To(Succeed())
					Expect(prior.GetSalary()).To(BeZero())
				}
			}
			Expect(guest).To(ContainElement(WithTransform(func(r *syncplanev1.FieldHistory) []string {
				return r.GetMask().GetPaths()
			}, Equal([]string{"name"}))))
			Expect(guest).To(HaveLen(len(owner)-1), "the record of salary alone is not shown")
		})

		It("matches a predicate against the record the caller receives", func() {
			applied(patch("owner", "s1", &accessv1.Staff{Name: "anna", Salary: 5500}, "name", "salary"))

			Expect(historyOf("owner", query.Has("mask.paths", "salary"))).NotTo(BeEmpty())
			Expect(historyOf("guest", query.Has("mask.paths", "salary"))).To(BeEmpty())
		})
	})

	Describe("writes", func() {
		It("keeps on a PUT the stored value of each field the caller may not read", func() {
			row := decoded(pulled("padmin")["s1"])
			row.Name = "anna"
			applied(put("padmin", row))

			got := stored("s1")
			Expect(got.GetName()).To(Equal("anna"))
			Expect(got.GetSalary()).To(Equal(int64(5000)))
			Expect(got.GetReview()).To(Equal("good"))
			Expect(got.GetBonus()).To(Equal(int64(7)))
			Expect(got.GetPin()).To(Equal("1234"), "padmin writes pin but did not read it")
			Expect(plainBody(got.GetBio())).To(ContainSubstring("a biography"))
		})

		It("lands on a PUT a value the caller sets in a field it writes and may not read", func() {
			row := decoded(pulled("owner")["s1"])
			row.Review = "great"
			applied(put("owner", row))

			Expect(stored("s1").GetReview()).To(Equal("great"))
		})

		It("lets a PATCH set a field the caller writes and may not read", func() {
			applied(patch("padmin", "s1", &accessv1.Staff{Pin: "9999"}, "pin"))
			Expect(stored("s1").GetPin()).To(Equal("9999"))
		})

		It("lands on a PUT each field the caller reads and writes", func() {
			row := decoded(pulled("owner")["s1"])
			row.Salary = 5100
			applied(put("owner", row))

			got := stored("s1")
			Expect(got.GetSalary()).To(Equal(int64(5100)))
			Expect(got.GetReview()).To(Equal("good"), "owner writes review but did not read it")
		})
	})

	Describe("the in-process API", func() {
		It("clears in a View each field a guard hides from the caller", func() {
			guest := server.WithPrincipal(f.ctx, server.Principal{Subject: "guest"})
			Expect(f.srv.View(guest, ws, func(tx *server.ViewTx) error {
				var row accessv1.Staff
				found, _, err := tx.Get(&row, "s1")
				Expect(found).To(BeTrue())
				Expect(row.GetName()).To(Equal("ann"))
				Expect(row.GetSalary()).To(BeZero())
				Expect(row.GetReview()).To(Equal("good"))
				return err
			})).To(Succeed())
		})
	})
})
