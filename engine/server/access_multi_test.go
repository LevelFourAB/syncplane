package server_test

import (
	"context"
	"fmt"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// multiEntities registers the multi-parent schema of the access test proto:
// stories below a product and a cycle, notes below stories, leaflets below
// each shelf they list, and topics below each topic they list.
func multiEntities() server.Option {
	return server.WithEntities(
		&accessv1.Product{}, &accessv1.Cycle{}, &accessv1.Shelf{}, &accessv1.Topic{},
		&accessv1.LaneMember{}, &accessv1.Story{}, &accessv1.StoryNote{}, &accessv1.Leaflet{},
	)
}

func newMultiFixtureOn(store storage.Adapter, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	return newSchemaFixtureOn(store, multiEntities(), opts...)
}

func story(id, product, cycle string) *accessv1.Story {
	s := &accessv1.Story{Id: id, Title: id}
	if product != "" {
		s.Product = &accessv1.Product{Id: product}
	}
	if cycle != "" {
		s.Cycle = &accessv1.Cycle{Id: cycle}
	}
	return s
}

func leaflet(id string, shelves ...string) *accessv1.Leaflet {
	l := &accessv1.Leaflet{Id: id, Title: id}
	for _, s := range shelves {
		l.Shelves = append(l.Shelves, &accessv1.Shelf{Id: s})
	}
	return l
}

func topic(id string, parents ...string) *accessv1.Topic {
	t := &accessv1.Topic{Id: id}
	for _, p := range parents {
		t.Parents = append(t.Parents, &accessv1.Topic{Id: p})
	}
	return t
}

func laneOnProduct(id, product, user, role string) *accessv1.LaneMember {
	return &accessv1.LaneMember{Id: id, Lane: &accessv1.LaneMember_Product{Product: &accessv1.Product{Id: product}}, UserId: user, Role: role}
}

func laneOnCycle(id, cycle, user, role string) *accessv1.LaneMember {
	return &accessv1.LaneMember{Id: id, Lane: &accessv1.LaneMember_Cycle{Cycle: &accessv1.Cycle{Id: cycle}}, UserId: user, Role: role}
}

func laneOnShelf(id, shelf, user, role string) *accessv1.LaneMember {
	return &accessv1.LaneMember{Id: id, Lane: &accessv1.LaneMember_Shelf{Shelf: &accessv1.Shelf{Id: shelf}}, UserId: user, Role: role}
}

func laneOnTopic(id, t, user, role string) *accessv1.LaneMember {
	return &accessv1.LaneMember{Id: id, Lane: &accessv1.LaneMember_Topic{Topic: &accessv1.Topic{Id: t}}, UserId: user, Role: role}
}

// seedLanes writes the rows every multi-parent spec starts from:
//
//   - products p1 and p2, cycles c1 and c2, shelves h1, h2 and h3;
//   - pview views p1, cview views c1, hview views h2;
//   - pedit edits in p1, cedit edits in c1, both edits in p1 and c1;
//   - planner plans in p1 and c1, and only views c2;
//   - s-pc sits below p1 and c1, s-p below p1, s-c below c1, s-p2 below p2,
//     and s-none below neither; n-pc is a note on s-pc;
//   - l-12 is on h1 and h2, l-1 on h1, and l-3 on h3.
func (f *accessFixture) seedLanes() {
	GinkgoHelper()
	f.seed(
		&accessv1.Product{Id: "p1"}, &accessv1.Product{Id: "p2"},
		&accessv1.Cycle{Id: "c1"}, &accessv1.Cycle{Id: "c2"},
		&accessv1.Shelf{Id: "h1"}, &accessv1.Shelf{Id: "h2"}, &accessv1.Shelf{Id: "h3"},
		laneOnProduct("lm-pview", "p1", "pview", "lane_viewer"),
		laneOnCycle("lm-cview", "c1", "cview", "lane_viewer"),
		laneOnShelf("lm-hview", "h2", "hview", "lane_viewer"),
		laneOnProduct("lm-pedit", "p1", "pedit", "lane_editor"),
		laneOnCycle("lm-cedit", "c1", "cedit", "lane_editor"),
		laneOnProduct("lm-both-p", "p1", "both", "lane_editor"),
		laneOnCycle("lm-both-c", "c1", "both", "lane_editor"),
		laneOnProduct("lm-plan-p", "p1", "planner", "lane_planner"),
		laneOnCycle("lm-plan-c", "c1", "planner", "lane_planner"),
		laneOnCycle("lm-plan-c2", "c2", "planner", "lane_viewer"),
		story("s-pc", "p1", "c1"), story("s-p", "p1", ""), story("s-c", "", "c1"),
		story("s-p2", "p2", ""), story("s-none", "", ""),
		&accessv1.StoryNote{Id: "n-pc", Story: &accessv1.Story{Id: "s-pc"}},
		leaflet("l-12", "h1", "h2"), leaflet("l-1", "h1"), leaflet("l-3", "h3"),
	)
}

var _ = Describe("Access model registration of several access parents", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), multiEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	It("registers the descriptor set", func() {
		Expect(register(nil)).To(Succeed())
	})

	It("accepts a marked field beside a marked oneof", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			card := messageNamed(file, "Card")
			card.Field = append(card.Field, &descriptorpb.FieldDescriptorProto{
				Name: proto.String("home"), JsonName: proto.String("home"), Number: proto.Int32(9),
				Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				TypeName: proto.String(".syncplane.testdata.access.v1.Board"), Options: accessParentMark(),
			})
		})).To(Succeed())
	})

	It("accepts a repeated polymorphic reference message", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			fieldNamed(messageNamed(file, "Pin"), "place").Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		})).To(Succeed())
	})

	DescribeTable("refuses a schema that breaks a rule of several access parents",
		func(change func(*descriptorpb.FileDescriptorProto), want string) {
			Expect(register(change)).To(MatchError(ContainSubstring(want)))
		},
		Entry("a grant entity with two access parents", func(file *descriptorpb.FileDescriptorProto) {
			member := messageNamed(file, "LaneMember")
			member.Field = append(member.Field, &descriptorpb.FieldDescriptorProto{
				Name: proto.String("extra"), JsonName: proto.String("extra"), Number: proto.Int32(9),
				Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				TypeName: proto.String(".syncplane.testdata.access.v1.Product"), Options: accessParentMark(),
			})
		}, `entity "lane_member": a grant entity has one access parent`),
		Entry("a grant entity with a repeated access parent", func(file *descriptorpb.FileDescriptorProto) {
			fieldNamed(messageNamed(file, "ProjectMember"), "project").Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, `entity "project_member": a grant entity has one access parent`),
	)

	It("treats a second access parent as a change to the access model", func() {
		productOnly := func(file *descriptorpb.FileDescriptorProto) {
			fieldNamed(messageNamed(file, "Story"), "cycle").Options = nil
		}
		Expect(register(productOnly)).To(Succeed())
		Expect(register(nil)).To(MatchError(ContainSubstring(`entity "story" changes its access model`)))
	})
})

var _ = Describe("Access model reads below several access parents", func() {
	var f *accessFixture

	for _, indexed := range []bool{false, true} {
		Context(map[bool]string{false: "before the indexes are built", true: "with the indexes built"}[indexed], func() {
			BeforeEach(func() {
				f = newMultiFixtureOn(newTestStore())
				f.seedLanes()
				if indexed {
					Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
				}
			})

			It("shows a row to the readers of each of its parents", func() {
				Expect(rowsOf("story", f.pull("pview"))).To(Equal([]string{"story/s-p", "story/s-pc"}))
				Expect(rowsOf("story", f.pull("cview"))).To(Equal([]string{"story/s-c", "story/s-pc"}))
				Expect(rowsOf("story_note", f.pull("pview"))).To(Equal([]string{"story_note/n-pc"}))
				Expect(rowsOf("story_note", f.pull("cview"))).To(Equal([]string{"story_note/n-pc"}))
			})

			It("shows a row once to a caller that reads it through two parents", func() {
				Expect(rowsOf("story", f.pull("both"))).To(Equal([]string{"story/s-c", "story/s-p", "story/s-pc"}))
			})

			It("shows a row to the readers of each row its list names", func() {
				Expect(rowsOf("leaflet", f.pull("hview"))).To(Equal([]string{"leaflet/l-12"}))
			})

			It("bootstraps what a pull shows", func() {
				for _, user := range []string{"pview", "cview", "hview", "both", "planner"} {
					Expect(f.bootstrapRows(user)).To(Equal(f.pull(user)), user)
				}
			})

			It("answers a Query with the rows below each parent", func() {
				Expect(f.queryKeys("cview", "story")).To(Equal([]string{"s-c", "s-pc"}))
				Expect(f.queryKeys("hview", "leaflet")).To(Equal([]string{"l-12"}))
				Expect(f.queryKeys("pview", "story_note")).To(Equal([]string{"n-pc"}))
			})
		})
	}

	It("bootstraps a caller below the root without scanning stories or leaflets", func() {
		counting := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
		f = newMultiFixtureOn(counting)
		f.seedLanes()
		for i := range 30 {
			f.seed(story(fmt.Sprintf("sx%02d", i), "p2", "c2"), leaflet(fmt.Sprintf("lx%02d", i), "h1", "h3"))
		}
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		counting.reset()
		Expect(rowsOf("leaflet", f.bootstrapRows("hview"))).To(Equal([]string{"leaflet/l-12"}))
		for _, entity := range []string{"story", "leaflet"} {
			Expect(counting.count(entity)).To(BeZero(), entity)
		}
	})

	It("reads a topic through any path of the topic graph", func() {
		f = newMultiFixtureOn(newTestStore())
		f.seed(
			topic("root-a"), topic("root-b"),
			topic("mid", "root-a"),
			topic("leaf", "mid", "root-b"),
			laneOnTopic("lm-a", "root-a", "aview", "lane_viewer"),
			laneOnTopic("lm-b", "root-b", "bview", "lane_viewer"),
		)
		Expect(rowsOf("topic", f.pull("aview"))).To(Equal([]string{"topic/leaf", "topic/mid", "topic/root-a"}))
		Expect(rowsOf("topic", f.pull("bview"))).To(Equal([]string{"topic/leaf", "topic/root-b"}))
	})

	It("reaches a row through a short path when a long path passes MaxAccessDepth", func() {
		f = newMultiFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
		// A write checks the depth of the row it writes and not of the rows
		// below it, so t3 ends up three hops below top through t2.
		f.seed(
			topic("top"), topic("t1", "top"), topic("t2"), topic("near"),
			topic("t3", "t2", "near"),
			laneOnTopic("lm-near", "near", "nview", "lane_viewer"),
		)
		f.seed(topic("t2", "t1"))
		Expect(rowsOf("topic", f.pull("nview"))).To(ContainElement("topic/t3"))
	})
})

var _ = Describe("Access model writes below several access parents", func() {
	var (
		f       *accessFixture
		clients int
	)

	one := func(user string, m func(client string, id uint64) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client, 1))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(entity, key string, value proto.Message) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return putMutation(c, id, entity, key, value) }
	}
	patch := func(entity, key string, value proto.Message, paths ...string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation {
			return patchMutation(c, id, entity, key, value, paths...)
		}
	}
	del := func(entity, key string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return deleteMutation(c, id, entity, key) }
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	refused := func(r *syncplanev1.MutationResult, code string) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(code), r.GetErrorMessage())
	}
	stored := func(entity, key string, into proto.Message) {
		GinkgoHelper()
		Expect(f.store.WithReadTx(f.ctx, ws, func(tx storage.ReadTx) error {
			row, err := tx.GetRow(entity, key)
			if err != nil {
				return err
			}
			return proto.Unmarshal(row.Value, into)
		})).To(Succeed())
	}

	BeforeEach(func() {
		f = newMultiFixtureOn(newTestStore())
		f.seedLanes()
	})

	It("checks a create through each parent it names", func() {
		applied(one("pedit", put("story", "n1", story("n1", "p1", ""))))
		refused(one("pedit", put("story", "n2", story("n2", "p1", "c1"))), server.CodeAccessDenied)
		refused(one("cedit", put("story", "n3", story("n3", "p1", "c1"))), server.CodeAccessDenied)
		applied(one("both", put("story", "n4", story("n4", "p1", "c1"))))
	})

	It("checks a create through each row its list names", func() {
		f.seed(laneOnShelf("lm-h1", "h1", "hedit", "lane_editor"))
		applied(one("hedit", put("leaflet", "n1", leaflet("n1", "h1"))))
		refused(one("hedit", put("leaflet", "n2", leaflet("n2", "h1", "h2"))), server.CodeAccessDenied)
		f.seed(laneOnShelf("lm-h2", "h2", "hedit", "lane_editor"))
		applied(one("hedit", put("leaflet", "n3", leaflet("n3", "h1", "h2"))))
	})

	It("lets a caller that writes through one parent change fields that place the row nowhere", func() {
		s := story("s-pc", "p1", "c1")
		s.Title = "renamed"
		applied(one("cedit", patch("story", "s-pc", s, "title")))
		applied(one("pedit", put("story", "s-pc", s)))
	})

	It("checks an added parent through that parent", func() {
		refused(one("pedit", patch("story", "s-p", story("s-p", "p1", "c1"), "cycle")), server.CodeAccessDenied)
		refused(one("pedit", put("story", "s-p", story("s-p", "p1", "c1"))), server.CodeAccessDenied)
		applied(one("both", patch("story", "s-p", story("s-p", "p1", "c1"), "cycle")))
		Expect(rowsOf("story", f.pull("cview"))).To(ContainElement("story/s-p"))
	})

	It("checks a removed parent through that parent", func() {
		refused(one("pedit", patch("story", "s-pc", story("s-pc", "p1", ""), "cycle")), server.CodeAccessDenied)
		refused(one("cedit", patch("story", "s-pc", story("s-pc", "", "c1"), "product")), server.CodeAccessDenied)
		applied(one("both", patch("story", "s-pc", story("s-pc", "p1", ""), "cycle")))
		Expect(rowsOf("story", f.pull("cview"))).NotTo(ContainElement("story/s-pc"))
	})

	It("checks a move to another parent at both ends", func() {
		f.seed(laneOnCycle("lm-cedit2", "c2", "cedit", "lane_editor"))
		applied(one("cedit", patch("story", "s-c", story("s-c", "", "c2"), "cycle")))
		// The caller writes s-pc through c1 alone, which is enough to move it
		// from c1 to c2.
		applied(one("cedit", patch("story", "s-pc", story("s-pc", "p1", "c2"), "cycle")))
		refused(one("pedit", patch("story", "s-p", story("s-p", "p1", "c2"), "cycle")), server.CodeAccessDenied)
	})

	It("lets a rule with a fields list add and remove the parents it names", func() {
		// The planner plans in p1 and c1 and only views c2.
		applied(one("planner", patch("story", "s-p", story("s-p", "p1", "c1"), "cycle")))
		applied(one("planner", patch("story", "s-pc", story("s-pc", "p1", ""), "cycle")))
		refused(one("planner", patch("story", "s-p", story("s-p", "p1", "c2"), "cycle")), server.CodeAccessDenied)
		refused(one("planner", patch("story", "s-p", story("s-p", "p2", "c1"), "product")), server.CodeAccessDenied)
	})

	It("adds and removes one row of a list with a PATCH, without the other rows", func() {
		f.seed(
			laneOnShelf("lm-h1", "h1", "hedit", "lane_editor"),
			laneOnShelf("lm-h3", "h3", "hedit", "lane_editor"),
			laneOnShelf("lm-h2", "h2", "other", "lane_editor"),
			laneOnShelf("lm-h1o", "h1", "other", "lane_editor"),
		)
		applied(one("hedit", patch("leaflet", "l-1", leaflet("l-1", "h3"), "shelves.h3")))
		applied(one("other", patch("leaflet", "l-1", leaflet("l-1", "h2"), "shelves.h2")))
		var got accessv1.Leaflet
		stored("leaflet", "l-1", &got)
		Expect(got.GetShelves()).To(HaveLen(3))
		refused(one("hedit", patch("leaflet", "l-1", leaflet("l-1"), "shelves.h2")), server.CodeAccessDenied)
		applied(one("other", patch("leaflet", "l-1", leaflet("l-1"), "shelves.h2")))
	})

	It("deletes a row a caller writes through one of its parents", func() {
		f.seed(story("s-del", "p1", "c1"))
		applied(one("cedit", del("story", "s-del")))
	})

	It("refuses to delete a parent while a live row lists it", func() {
		f = newMultiFixtureOn(newTestStore(), server.WithGlobalRoles(func(_ context.Context, p server.Principal) ([]string, error) {
			if p.Subject == "root" {
				return []string{"lane_admin"}, nil
			}
			return nil, nil
		}))
		f.seed(&accessv1.Shelf{Id: "h1"}, &accessv1.Shelf{Id: "h3"}, leaflet("l-13", "h1", "h3"))
		r := one("root", del("shelf", "h3"))
		refused(r, server.CodeConflict)
		Expect(r.GetErrorMessage()).To(ContainSubstring("access parent of a live leaflet row"))
		applied(one("root", patch("leaflet", "l-13", leaflet("l-13"), "shelves.h3")))
		applied(one("root", del("shelf", "h3")))
	})

	It("refuses a row with more access parents than MaxAccessParents, on every write path", func() {
		f = newMultiFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessParents: 2}))
		f.seed(&accessv1.Shelf{Id: "h1"}, &accessv1.Shelf{Id: "h2"}, &accessv1.Shelf{Id: "h3"},
			laneOnShelf("lm-1", "h1", "hedit", "lane_editor"),
			laneOnShelf("lm-2", "h2", "hedit", "lane_editor"),
			laneOnShelf("lm-3", "h3", "hedit", "lane_editor"))
		applied(one("hedit", put("leaflet", "n1", leaflet("n1", "h1", "h2"))))
		r := one("hedit", put("leaflet", "n2", leaflet("n2", "h1", "h2", "h3")))
		refused(r, server.CodeInvalidArgument)
		Expect(r.GetErrorMessage()).To(ContainSubstring("more than MaxAccessParents (2)"))
		err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(leaflet("n3", "h1", "h2", "h3"))
		})
		Expect(err).To(MatchError(ContainSubstring("more than MaxAccessParents (2)")))
	})

	It("counts one row named twice in a list as one parent", func() {
		f = newMultiFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessParents: 1}))
		f.seed(&accessv1.Shelf{Id: "h1"})
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(leaflet("n1", "h1", "h1"))
		})).To(Succeed())
	})

	Context("a topic graph", func() {
		BeforeEach(func() {
			f.seed(
				topic("top"), topic("side"),
				topic("mid", "top"),
				topic("low", "mid", "side"),
				laneOnTopic("lm-boss", "top", "boss", "lane_admin"),
				laneOnTopic("lm-boss-s", "side", "boss", "lane_admin"),
			)
		})

		It("refuses a parent that would put a topic below itself through any path, on every write path", func() {
			r := one("boss", patch("topic", "side", topic("side", "low"), "parents"))
			refused(r, server.CodeInvalidArgument)
			Expect(r.GetErrorMessage()).To(ContainSubstring("would sit below itself"))
			refused(one("boss", patch("topic", "top", topic("top", "side", "low"), "parents")), server.CodeInvalidArgument)
			err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Put(topic("top", "low"))
			})
			Expect(err).To(MatchError(ContainSubstring("would sit below itself")))
		})

		It("refuses a topic that any path puts deeper than MaxAccessDepth", func() {
			f = newMultiFixtureOn(newTestStore(), server.WithLimits(server.Limits{MaxAccessDepth: 2}))
			f.seed(
				topic("top"), topic("near"), topic("mid", "top"),
				laneOnTopic("lm-boss", "top", "boss", "lane_admin"),
				laneOnTopic("lm-boss-n", "near", "boss", "lane_admin"),
			)
			applied(one("boss", put("topic", "low", topic("low", "mid", "near"))))
			r := one("boss", put("topic", "lower", topic("lower", "near", "low")))
			refused(r, server.CodeInvalidArgument)
			Expect(r.GetErrorMessage()).To(ContainSubstring("more than MaxAccessDepth (2) hops"))
		})
	})
})

var _ = Describe("Access model sync below several access parents", func() {
	var f *accessFixture

	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, fn)).To(Succeed())
	}
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
		Expect(r.resets).To(BeZero())
	}

	BeforeEach(func() {
		f = newMultiFixtureOn(newTestStore())
		f.seedLanes()
		f.seed(&accessv1.StoryNote{Id: "n-p", Story: &accessv1.Story{Id: "s-p"}})
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("shows the rows below a row to the readers of a parent it gains, and hides them when it loses it", func() {
		cview := newAccessReplica(f, "cview")
		pview := newAccessReplica(f, "pview")
		cview.sync()
		pview.sync()
		Expect(cview.view()).NotTo(ContainElements("story/s-p", "story_note/n-p"))

		trusted(func(tx *server.EntityTx) error { return tx.Patch(story("s-p", "p1", "c1"), []string{"cycle"}, nil) })
		cview.sync()
		pview.sync()
		Expect(cview.view()).To(ContainElements("story/s-p", "story_note/n-p"))
		converged(cview)
		converged(pview)

		trusted(func(tx *server.EntityTx) error { return tx.Patch(story("s-p", "", "c1"), []string{"product"}, nil) })
		cview.sync()
		pview.sync()
		Expect(pview.view()).NotTo(ContainElements("story/s-p", "story_note/n-p"))
		Expect(cview.view()).To(ContainElements("story/s-p", "story_note/n-p"))
		converged(cview)
		converged(pview)
	})

	It("keeps the rows below a row for a reader of the parent it keeps", func() {
		both := newAccessReplica(f, "both")
		both.sync()
		trusted(func(tx *server.EntityTx) error { return tx.Patch(story("s-pc", "p1", ""), []string{"cycle"}, nil) })
		both.sync()
		Expect(both.view()).To(ContainElements("story/s-pc", "story_note/n-pc"))
		converged(both)
	})

	It("shows a leaflet to the readers of a shelf added to its list", func() {
		hview := newAccessReplica(f, "hview")
		hview.sync()
		Expect(rowsOf("leaflet", hview.view())).To(Equal([]string{"leaflet/l-12"}))
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(leaflet("l-3", "h2"), []string{"shelves.h2"}, nil)
		})
		hview.sync()
		Expect(rowsOf("leaflet", hview.view())).To(Equal([]string{"leaflet/l-12", "leaflet/l-3"}))
		converged(hview)
	})
})
