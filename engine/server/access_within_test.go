package server_test

import (
	"fmt"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
)

// withinEntities registers the within schema of the access test proto:
// dossiers whose fields decide what the rows below them show, and binders
// whose owners read everything below them.
func withinEntities() server.Option {
	return server.WithEntities(
		&accessv1.Clerk{}, &accessv1.Dossier{}, &accessv1.DossierMember{}, &accessv1.Matter{},
		&accessv1.MatterWatcher{}, &accessv1.Memo{}, &accessv1.Binder{}, &accessv1.Sheet{},
	)
}

func dossier(id string) *accessv1.Dossier { return &accessv1.Dossier{Id: id} }
func matter(id string) *accessv1.Matter   { return &accessv1.Matter{Id: id} }
func binder(id string) *accessv1.Binder   { return &accessv1.Binder{Id: id} }

// seedDossiers writes the rows every within spec starts from:
//
//   - reg is a registrar at the root; cora, olga, bea and ian are clerks;
//   - d-open is open and owned by olga, d-priv is private and owned by olga,
//     and d-arch is archived and owned by cora;
//   - mia is a member of d-priv, max a member of d-arch and ed an editor of
//     d-open, who edits the dossier but reads nothing below it;
//   - will watches m-open;
//   - each dossier holds one matter, and each matter one memo;
//   - b-top is bea's and holds ian's b-inner; b-other has no owner. Each
//     binder holds one sheet.
func (f *accessFixture) seedDossiers() {
	GinkgoHelper()
	f.seed(
		&accessv1.Clerk{Id: "c-reg", UserId: "reg", Role: "registrar"},
		&accessv1.Clerk{Id: "c-cora", UserId: "cora", Role: "clerk"},
		&accessv1.Clerk{Id: "c-olga", UserId: "olga", Role: "clerk"},
		&accessv1.Clerk{Id: "c-bea", UserId: "bea", Role: "clerk"},
		&accessv1.Clerk{Id: "c-ian", UserId: "ian", Role: "clerk"},
		&accessv1.Dossier{Id: "d-open", Title: "open", OwnerId: "olga"},
		&accessv1.Dossier{Id: "d-priv", Title: "private", Private: true, OwnerId: "olga"},
		&accessv1.Dossier{Id: "d-arch", Title: "archived", Archived: true, OwnerId: "cora"},
		&accessv1.DossierMember{Id: "dm-mia", Dossier: dossier("d-priv"), UserId: "mia", Role: "dossier_member"},
		&accessv1.DossierMember{Id: "dm-max", Dossier: dossier("d-arch"), UserId: "max", Role: "dossier_member"},
		&accessv1.DossierMember{Id: "dm-ed", Dossier: dossier("d-open"), UserId: "ed", Role: "dossier_editor"},
		&accessv1.Matter{Id: "m-open", Dossier: dossier("d-open"), Title: "open"},
		&accessv1.Matter{Id: "m-priv", Dossier: dossier("d-priv"), Title: "private"},
		&accessv1.Matter{Id: "m-arch", Dossier: dossier("d-arch"), Title: "archived"},
		&accessv1.MatterWatcher{Id: "mw-will", Matter: matter("m-open"), UserId: "will", Role: "matter_watcher"},
		&accessv1.Memo{Id: "memo-open", Matter: matter("m-open"), Title: "open"},
		&accessv1.Memo{Id: "memo-priv", Matter: matter("m-priv"), Title: "private"},
		&accessv1.Memo{Id: "memo-arch", Matter: matter("m-arch"), Title: "archived"},
		&accessv1.Binder{Id: "b-top", OwnerId: "bea", Title: "top"},
		&accessv1.Binder{Id: "b-inner", Parent: binder("b-top"), OwnerId: "ian", Title: "inner"},
		&accessv1.Binder{Id: "b-other", Title: "other"},
		&accessv1.Sheet{Id: "s-top", Binder: binder("b-top"), Title: "top"},
		&accessv1.Sheet{Id: "s-inner", Binder: binder("b-inner"), Title: "inner"},
		&accessv1.Sheet{Id: "s-other", Binder: binder("b-other"), Title: "other"},
	)
}

var _ = Describe("Access model registration of within entries", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}
	rulesOf := func(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.AccessOptions {
		GinkgoHelper()
		m := messageNamed(file, message)
		return proto.GetExtension(m.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	}
	// withinOn sets the within list of the first read rule of the matter.
	withinOn := func(entries ...*syncplanev1.AccessWithin) func(*descriptorpb.FileDescriptorProto) {
		return func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Matter").Allow[1].Within = entries
		}
	}
	open := query.Eq("private", false)

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), withinEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	DescribeTable("refuses an entry that cannot decide access",
		func(entry *syncplanev1.AccessWithin, want string) {
			Expect(register(withinOn(entry))).To(MatchError(And(
				ContainSubstring(`access allow[1] within[0]`), ContainSubstring(want))))
		},
		Entry("an entry without an entity", &syncplanev1.AccessWithin{When: open}, `names no entity`),
		Entry("an entry without a check", &syncplanev1.AccessWithin{Entity: "dossier"}, `sets no check; set caller_is or when`),
		Entry("an entity that is not registered",
			&syncplanev1.AccessWithin{Entity: "nothing", When: open},
			`names entity "nothing", which is not a registered entity in the access model`),
		Entry("an entity whose rows the rule's rows cannot sit below",
			&syncplanev1.AccessWithin{Entity: "memo", CallerIs: "title"},
			`names entity "memo", whose rows "matter" rows cannot sit below`),
		Entry("a subject field the entity does not have",
			&syncplanev1.AccessWithin{Entity: "dossier", CallerIs: "nobody"}, `field "nobody" not found`),
		Entry("a subject field that is not a string",
			&syncplanev1.AccessWithin{Entity: "dossier", CallerIs: "private"}, `field "private" is bool; it must be a string`),
		Entry("a condition on a field the entity does not have",
			&syncplanev1.AccessWithin{Entity: "dossier", When: query.Eq("closed", true)}, `closed`),
		Entry("a rel condition, which reads another row",
			&syncplanev1.AccessWithin{Entity: "dossier", When: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rel{
				Rel: &syncplanev1.RelCond{Path: []string{"owner_id"}, Cond: open},
			}}}, `reads another row`),
	)

	It("refuses an entry on the rule's own entity when its rows do not sit below rows of their own", func() {
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			rulesOf(file, "Dossier").Allow[1].Within = []*syncplanev1.AccessWithin{{Entity: "dossier", When: open}}
		})).To(MatchError(ContainSubstring(`names the rule's own entity, whose rows do not sit below rows of their own entity`)))
	})

	It("refuses an entry that repeats another of the same rule", func() {
		Expect(register(withinOn(
			&syncplanev1.AccessWithin{Entity: "dossier", When: open},
			&syncplanev1.AccessWithin{Entity: "dossier", When: open},
		))).To(MatchError(ContainSubstring(`access allow[1] within[1] repeats an earlier entry`)))
	})

	It("refuses an entry that carries fields this server does not know", func() {
		entry := &syncplanev1.AccessWithin{Entity: "dossier", When: open}
		entry.ProtoReflect().SetUnknown(unknownField())
		Expect(register(withinOn(entry))).To(MatchError(ContainSubstring(
			`access allow[1] within[0] carries option fields this server does not know`)))
	})

	It("accepts two entries, which must both hold", func() {
		Expect(register(withinOn(
			&syncplanev1.AccessWithin{Entity: "dossier", When: open},
			&syncplanev1.AccessWithin{Entity: "dossier", CallerIs: "owner_id"},
		))).To(Succeed())
	})

	It("changes the access spec with the entry, so the change needs the flag", func() {
		Expect(register(nil)).To(Succeed())
		change := withinOn(&syncplanev1.AccessWithin{Entity: "dossier", When: query.Eq("archived", false)})
		Expect(register(change)).To(MatchError(ContainSubstring(`entity "matter" changes its access model`)))
		Expect(register(change, server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("Within entries", func() {
	var (
		f       *accessFixture
		clients int
	)

	push := func(user string, m func(client string) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(user, entity, key string, value proto.Message) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return putMutation(c, 1, entity, key, value) })
	}
	patch := func(user, entity, key string, value proto.Message, paths ...string) *syncplanev1.MutationResult {
		GinkgoHelper()
		return push(user, func(c string) *syncplanev1.Mutation { return patchMutation(c, 1, entity, key, value, paths...) })
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	denied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetErrorMessage())
	}
	trusted := func(msgs ...proto.Message) { f.seed(msgs...) }

	BeforeEach(func() {
		f = newSchemaFixtureOn(newTestStore(), withinEntities())
		f.seedDossiers()
	})

	Describe("reads", func() {
		It("hides the rows below a private row from a caller who reads them through the root", func() {
			rows := f.pull("cora")
			Expect(rows).To(ContainElements("matter/m-open", "matter/m-arch", "memo/memo-open", "memo/memo-arch"))
			Expect(rows).To(lackAll("matter/m-priv", "memo/memo-priv", "dossier/d-priv"))
		})

		It("shows them to a member of the private row", func() {
			Expect(f.pull("mia")).To(ContainElements("dossier/d-priv", "matter/m-priv", "memo/memo-priv"))
		})

		It("shows the owner of a row the rows an owner rule reaches below it", func() {
			rows := f.pull("olga")
			Expect(rows).To(ContainElements("memo/memo-open", "memo/memo-priv", "memo/memo-arch"))
			// The owner rule is on memos only.
			Expect(rows).NotTo(ContainElement("matter/m-priv"))
			Expect(f.pull("cora")).NotTo(ContainElement("memo/memo-priv"))
		})

		It("still checks the entry for a caller who holds the permission below the row it reads", func() {
			Expect(f.pull("will")).To(ContainElements("matter/m-open", "memo/memo-open"))
		})

		It("reads a recursive tree from the row itself and every row above it", func() {
			Expect(f.pull("bea")).To(And(
				ContainElements("binder/b-top", "binder/b-inner", "sheet/s-top", "sheet/s-inner"),
				lackAll("binder/b-other", "sheet/s-other")))
			Expect(f.pull("ian")).To(And(
				ContainElements("binder/b-inner", "sheet/s-inner"),
				lackAll("binder/b-top", "sheet/s-top")))
		})

		It("narrows a bootstrap and a query the same way", func() {
			Expect(f.bootstrapRows("cora")).To(And(ContainElement("matter/m-open"), Not(ContainElement("matter/m-priv"))))
			Expect(f.queryKeys("cora", "matter")).To(Equal([]string{"m-arch", "m-open"}))
			Expect(f.queryKeys("olga", "memo")).To(Equal([]string{"memo-arch", "memo-open", "memo-priv"}))
			Expect(f.queryKeys("bea", "sheet")).To(Equal([]string{"s-inner", "s-top"}))
			Expect(f.bootstrapRows("ian")).To(And(ContainElement("sheet/s-inner"), Not(ContainElement("sheet/s-top"))))
		})
	})

	Describe("writes", func() {
		It("lets a member edit the rows below a row that passes the entry", func() {
			applied(patch("mia", "matter", "m-priv", &accessv1.Matter{Title: "renamed"}, "title"))
		})

		It("refuses every caller a write below a row that fails the entry", func() {
			denied(patch("max", "matter", "m-arch", &accessv1.Matter{Title: "renamed"}, "title"))
			denied(patch("reg", "matter", "m-arch", &accessv1.Matter{Title: "renamed"}, "title"))
			denied(put("max", "matter", "m-new", &accessv1.Matter{Id: "m-new", Dossier: dossier("d-arch")}))
		})

		It("needs the entry where a moved row is and where it goes", func() {
			denied(patch("reg", "matter", "m-open", &accessv1.Matter{Dossier: dossier("d-arch")}, "dossier"))
			denied(patch("reg", "matter", "m-arch", &accessv1.Matter{Dossier: dossier("d-open")}, "dossier"))
			applied(patch("reg", "matter", "m-open", &accessv1.Matter{Dossier: dossier("d-priv")}, "dossier"))
		})

		It("lets the owner of a row write the rows an owner rule reaches below it", func() {
			applied(patch("olga", "memo", "memo-priv", &accessv1.Memo{Title: "renamed"}, "title"))
			applied(put("olga", "memo", "memo-new", &accessv1.Memo{Id: "memo-new", Matter: matter("m-priv")}))
			denied(patch("cora", "memo", "memo-priv", &accessv1.Memo{Title: "renamed"}, "title"))
			applied(patch("cora", "memo", "memo-arch", &accessv1.Memo{Title: "renamed"}, "title"))
		})

		It("lets the owner of an outer row of a recursive tree write the rows below an inner one", func() {
			applied(patch("bea", "sheet", "s-inner", &accessv1.Sheet{Title: "renamed"}, "title"))
			denied(patch("ian", "sheet", "s-top", &accessv1.Sheet{Title: "renamed"}, "title"))
		})

		It("needs the read rules of the rows below to change what an entry reads of a row", func() {
			r := patch("ed", "dossier", "d-open", &accessv1.Dossier{Private: true}, "private")
			denied(r)
			Expect(r.GetErrorMessage()).To(ContainSubstring(`changing row "d-open" needs one of the permissions`))
			applied(patch("ed", "dossier", "d-open", &accessv1.Dossier{Title: "renamed"}, "title"))
			applied(patch("reg", "dossier", "d-open", &accessv1.Dossier{Private: true}, "private"))
		})
	})

	Describe("sync of a row an entry reads", func() {
		It("removes the rows below a row that stops passing, and sends them back when it passes again", func() {
			r := newAccessReplica(f, "cora")
			r.sync()
			Expect(r.view()).To(ContainElements("matter/m-open", "memo/memo-open"))

			trusted(&accessv1.Dossier{Id: "d-open", Title: "open", Private: true, OwnerId: "olga"})
			r.sync()
			Expect(r.view()).To(lackAll("dossier/d-open", "matter/m-open", "memo/memo-open"))

			trusted(&accessv1.Dossier{Id: "d-open", Title: "open", OwnerId: "olga"})
			r.sync()
			Expect(r.view()).To(ContainElements("dossier/d-open", "matter/m-open", "memo/memo-open"))
			Expect(r.resets).To(BeZero())
		})

		It("removes them from a caller who holds the permission at a row below the changed row", func() {
			r := newAccessReplica(f, "will")
			r.sync()
			Expect(r.view()).To(ContainElements("matter/m-open", "memo/memo-open"))

			trusted(&accessv1.Dossier{Id: "d-open", Title: "open", Private: true, OwnerId: "olga"})
			r.sync()
			Expect(r.view()).To(lackAll("matter/m-open", "memo/memo-open"))
		})

		It("moves the rows an owner rule reaches from the old owner to the new one", func() {
			olga, cora := newAccessReplica(f, "olga"), newAccessReplica(f, "cora")
			olga.sync()
			cora.sync()
			Expect(olga.view()).To(ContainElement("memo/memo-priv"))
			Expect(cora.view()).NotTo(ContainElement("memo/memo-priv"))

			trusted(&accessv1.Dossier{Id: "d-priv", Title: "private", Private: true, OwnerId: "cora"})
			olga.sync()
			cora.sync()
			Expect(olga.view()).NotTo(ContainElement("memo/memo-priv"))
			Expect(cora.view()).To(ContainElement("memo/memo-priv"))
			Expect(cora.view()).NotTo(ContainElement("matter/m-priv"))
		})

		It("removes the rows below a row that moves below a row that fails the entry", func() {
			r := newAccessReplica(f, "cora")
			r.sync()
			Expect(r.view()).To(ContainElements("matter/m-open", "memo/memo-open"))

			trusted(&accessv1.Matter{Id: "m-open", Dossier: dossier("d-priv"), Title: "open"})
			r.sync()
			Expect(r.view()).To(lackAll("matter/m-open", "memo/memo-open"))
		})

		It("changes what an owner reads below an outer row of a recursive tree", func() {
			bea, ian := newAccessReplica(f, "bea"), newAccessReplica(f, "ian")
			bea.sync()
			ian.sync()

			trusted(&accessv1.Binder{Id: "b-top", OwnerId: "ian", Title: "top"})
			bea.sync()
			ian.sync()
			Expect(bea.view()).To(lackAll("binder/b-top", "binder/b-inner", "sheet/s-top", "sheet/s-inner"))
			Expect(ian.view()).To(ContainElements("binder/b-top", "binder/b-inner", "sheet/s-top", "sheet/s-inner"))
		})

		It("names no row below a row that passed for a while between two pulls", func() {
			r := newAccessReplica(f, "cora")
			r.sync()

			trusted(&accessv1.Dossier{Id: "d-priv", Title: "private", OwnerId: "olga"})
			trusted(&accessv1.Dossier{Id: "d-priv", Title: "private", Private: true, OwnerId: "olga"})
			resp := r.pullOnce(0)
			Expect(resp.GetReset_()).To(BeFalse())
			for _, rc := range resp.GetPatch().GetChanges() {
				Expect(rc.GetEntity() + "/" + rc.GetKey()).NotTo(BeElementOf("matter/m-priv", "memo/memo-priv"))
			}
		})

		It("sends nothing new to a caller whose view the change leaves as it was", func() {
			r := newAccessReplica(f, "mia")
			r.sync()
			before := r.view()

			trusted(&accessv1.Dossier{Id: "d-priv", Title: "private", OwnerId: "olga"})
			r.sync()
			Expect(r.view()).To(Equal(before))
		})
	})
})
