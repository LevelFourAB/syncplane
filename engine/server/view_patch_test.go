package server_test

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/internal/pubsub"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("Server.View", func() {
	const ws = "ws-view"
	var (
		ctx      context.Context
		srv      *server.Server
		notifier *countingNotifier
	)

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "view.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		notifier = &countingNotifier{Local: pubsub.NewLocal()}
		srv = mustNewServer(store,
			server.WithEntities(&testdatav1.Widget{}),
			server.WithNotifier(notifier))
	})

	It("round-trips a row and its version, and pokes nothing on read", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "one", Count: 3})
		})).To(Succeed())
		pokesAfterWrite := notifier.notifies.Load()

		var got testdatav1.Widget
		var found bool
		var version uint64
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			f, v, err := tx.Get(&got, "w1")
			found, version = f, v
			return err
		})).To(Succeed())

		Expect(found).To(BeTrue())
		Expect(version).To(BeEquivalentTo(1))
		Expect(got.GetLabel()).To(Equal("one"))
		Expect(got.GetCount()).To(BeEquivalentTo(3))
		Expect(notifier.notifies.Load()).To(Equal(pokesAfterWrite), "a read sends no poke")
	})

	It("reports a missing row as not found", func() {
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			found, version, err := tx.Get(&testdatav1.Widget{}, "ghost")
			Expect(found).To(BeFalse())
			Expect(version).To(BeZero())
			return err
		})).To(Succeed())
	})

	It("pages rows in key order with their versions", func() {
		for _, id := range []string{"a", "b", "c"} {
			id := id
			Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Widget{Id: id})
			})).To(Succeed())
		}

		var keys []string
		versions := map[string]uint64{}
		var last string
		var more bool
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			l, m, err := tx.ScanPage(&testdatav1.Widget{}, "", 2, func(msg proto.Message, version uint64) error {
				w := msg.(*testdatav1.Widget)
				keys = append(keys, w.GetId())
				versions[w.GetId()] = version
				return nil
			})
			last, more = l, m
			return err
		})).To(Succeed())

		Expect(keys).To(Equal([]string{"a", "b"}))
		Expect(last).To(Equal("b"))
		Expect(more).To(BeTrue())
		Expect(versions["a"]).To(BeEquivalentTo(1))
		Expect(versions["b"]).To(BeEquivalentTo(2))
	})
})

var _ = Describe("EntityTx.Patch", func() {
	const ws = "ws-patch"
	var (
		ctx context.Context
		srv *server.Server
	)

	BeforeEach(func() {
		ctx = context.Background()
		srv = newTestServer()
	})

	get := func(key string) (*testdatav1.Widget, uint64, bool) {
		GinkgoHelper()
		var w testdatav1.Widget
		var version uint64
		var found bool
		Expect(srv.View(ctx, ws, func(tx *server.ViewTx) error {
			f, v, err := tx.Get(&w, key)
			found, version = f, v
			return err
		})).To(Succeed())
		return &w, version, found
	}

	It("merges only the masked fields, leaving the rest of the row intact", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "before", Count: 7})
		})).To(Succeed())

		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1", Label: "after"}, []string{"label"}, nil)
		})).To(Succeed())

		w, _, found := get("w1")
		Expect(found).To(BeTrue())
		Expect(w.GetLabel()).To(Equal("after"))
		Expect(w.GetCount()).To(BeEquivalentTo(7), "an unmasked field survives the patch")
	})

	It("honors keyed-list merge: a per-item mask adds without replacing the whole list", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Members: []*testdatav1.Member{{Uid: "u1", Name: "Ann"}}})
		})).To(Succeed())

		// The "members.u2" item path merges element u2 in by key, leaving u1
		// untouched — the same per-item merge the push PATCH path uses.
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{
				Id:      "w1",
				Members: []*testdatav1.Member{{Uid: "u2", Name: "Bob"}},
			}, []string{"members.u2"}, nil)
		})).To(Succeed())

		w, _, _ := get("w1")
		byUID := map[string]string{}
		for _, m := range w.GetMembers() {
			byUID[m.GetUid()] = m.GetName()
		}
		Expect(byUID).To(HaveKeyWithValue("u1", "Ann"), "the existing member is retained by key")
		Expect(byUID).To(HaveKeyWithValue("u2", "Bob"), "the patched member is added by key")
	})

	It("rejects a patch of a missing row as a conflict (delete-wins, not upsert)", func() {
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "ghost", Label: "x"}, []string{"label"}, nil)
		})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(rej.Code).To(Equal(server.CodeConflict))
	})

	It("rejects a patch of a deleted row as a conflict", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
		})).To(Succeed())
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Delete(&testdatav1.Widget{}, "w1")
		})).To(Succeed())

		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1", Label: "b"}, []string{"label"}, nil)
		})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(rej.Code).To(Equal(server.CodeConflict))
	})

	It("applies when expected_version matches and rejects when it is stale", func() {
		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Widget{Id: "w1", Label: "a"})
		})).To(Succeed())
		_, version, _ := get("w1")

		Expect(srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1", Label: "b"}, []string{"label"}, &version)
		})).To(Succeed(), "CAS succeeds at the current version")

		stale := version // the row has now advanced past this
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1", Label: "c"}, []string{"label"}, &stale)
		})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(rej.Code).To(Equal(server.CodeConflict))

		w, _, _ := get("w1")
		Expect(w.GetLabel()).To(Equal("b"), "the stale patch did not apply")
	})

	It("requires a non-empty field mask", func() {
		err := srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Widget{Id: "w1"}, nil, nil)
		})
		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue())
		Expect(rej.Code).To(Equal(server.CodeInvalidArgument))
	})
})

var _ = Describe("Server entity introspection", func() {
	It("lists entities sorted and looks them up by wire name", func() {
		srv := newTestServer()
		var names []string
		for _, e := range srv.Entities() {
			names = append(names, e.Name)
		}
		Expect(names).To(Equal([]string{"historied", "historied_all", "issue", "keyed_text", "pair", "project", "syncplane_field_history", "task", "todo", "user", "widget"}))

		meta, ok := srv.Entity("issue")
		Expect(ok).To(BeTrue())
		Expect(string(meta.Descriptor.Name())).To(Equal("Issue"))
		var keyNames []string
		for _, kf := range meta.KeyFields {
			keyNames = append(keyNames, string(kf.Name()))
		}
		Expect(keyNames).To(Equal([]string{"id"}))

		_, ok = srv.Entity("nope")
		Expect(ok).To(BeFalse())
	})

	It("marks the entities the engine registers for itself as managed", func() {
		srv := newTestServer()
		hist, ok := srv.Entity("syncplane_field_history")
		Expect(ok).To(BeTrue())
		Expect(hist.Managed).To(BeTrue())

		src, ok := srv.Entity("historied")
		Expect(ok).To(BeTrue())
		Expect(src.Managed).To(BeFalse(), "opting into history does not make an app entity managed")
	})

	It("classifies typed references via RefKeyFields", func() {
		fields := (&testdatav1.Issue{}).ProtoReflect().Descriptor().Fields()
		Expect(server.RefKeyFields(fields.ByName("project"))).To(HaveLen(1))
		Expect(server.RefKeyFields(fields.ByName("title"))).To(BeNil(), "a scalar is not a reference")
		Expect(server.RefKeyFields(fields.ByName("blocked_by"))).To(HaveLen(1), "a repeated self-ref is a reference")
	})
})

var _ = Describe("Manager app accessors", func() {
	var ctx context.Context

	noteSpec := server.DynamicEntity{
		Name: "note", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
		},
	}

	BeforeEach(func() { ctx = context.Background() })

	It("enumerates and looks up registered apps and their schemas", func() {
		store := newRootStore(filepath.Join(GinkgoT().TempDir(), "accessors.db"))
		m, err := server.NewManager(ctx, store)
		Expect(err).NotTo(HaveOccurred())

		_, err = m.Register(ctx, "app-b", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())
		_, err = m.Register(ctx, "app-a", descriptorSetBytes(noteSpec))
		Expect(err).NotTo(HaveOccurred())

		Expect(m.AppIDs()).To(Equal([]string{"app-a", "app-b"}), "sorted")
		Expect(m.App("app-a")).NotTo(BeNil())
		Expect(m.App("ghost")).To(BeNil())

		schemas, err := m.ListAppSchemas(ctx)
		Expect(err).NotTo(HaveOccurred())
		var ids []string
		for _, sc := range schemas {
			ids = append(ids, sc.AppID)
		}
		Expect(ids).To(ConsistOf("app-a", "app-b"))
	})
})
