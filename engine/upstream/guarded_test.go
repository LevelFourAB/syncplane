package upstream_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// A field whose guard admits no update is one only a trusted write changes at
// the origin. The local engine computes such fields with trusted code of its
// own, and the link keeps those values local instead of sending them.
var _ = Describe("Fields the origin lets the link only read", func() {
	var (
		o   *origin
		n   *local
		ctx context.Context
	)

	trusted := func() context.Context { return server.Trusted(context.Background()) }

	guarded := func(srv *server.Server, key string) *testdatav1.Guarded {
		GinkgoHelper()
		var g testdatav1.Guarded
		var found bool
		Expect(srv.View(trusted(), ws, func(tx *server.ViewTx) error {
			var err error
			found, _, err = tx.Get(&g, key)
			return err
		})).To(Succeed())
		if !found {
			return nil
		}
		return &g
	}

	// originCompute stands in for the origin's own trusted code, which sets
	// the guarded field.
	originCompute := func(key, owner string) {
		GinkgoHelper()
		Expect(o.srv.Apply(trusted(), ws, func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: key, Owner: owner}, []string{"owner"}, nil)
		})).To(Succeed())
	}

	// localApply writes as the local engine's own trusted code does.
	localApply := func(fn func(tx *server.ApplyTx) error) upstream.Receipt {
		GinkgoHelper()
		rc, err := n.ws().Apply(trusted(), fn)
		Expect(err).NotTo(HaveOccurred())
		return rc
	}

	owner := func(srv *server.Server, key string) string { return guarded(srv, key).GetOwner() }
	label := func(srv *server.Server, key string) string { return guarded(srv, key).GetLabel() }

	var originOpts []server.Option
	BeforeEach(func() { originOpts = nil })

	JustBeforeEach(func() {
		ctx = context.Background()
		o = newOrigin(append([]server.Option{server.WithEntities(&testdatav1.Guarded{})}, originOpts...)...)
		Expect(o.srv.Apply(trusted(), ws, func(tx *server.ApplyTx) error {
			return tx.Put(&testdatav1.Guarded{Id: "g1", Label: "first", Owner: "origin"})
		})).To(Succeed())
		n = newLocal(o, func(cfg *upstream.Config) {
			cfg.Bindings = append(cfg.Bindings, upstream.Binding{Prototype: &testdatav1.Guarded{}})
		}, server.WithEntities(&testdatav1.Guarded{}))
		n.add()
		n.flush()
		Expect(owner(n.srv, "g1")).To(Equal("origin"))
	})

	It("forwards the rest of a PATCH whose mask names such a field", func() {
		rc := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second", Owner: "local"}, []string{"label", "owner"}, nil)
		})
		Expect(rc.Forwarded).To(BeTrue())
		Expect(owner(n.srv, "g1")).To(Equal("local"), "the local value shows until the origin answers")

		n.flush()
		Expect(rc.Wait(ctx)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty())
		Expect(label(o.srv, "g1")).To(Equal("second"))
		Expect(owner(o.srv, "g1")).To(Equal("origin"))
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("origin"))
		Expect(label(n.srv, "g1")).To(Equal("second"))
	})

	It("forwards the other writes of a transaction that also writes such a field", func() {
		rc := localApply(func(tx *server.ApplyTx) error {
			if err := tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second"}, []string{"label"}, nil); err != nil {
				return err
			}
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local", State: &testdatav1.Guarded_StateName{StateName: "x"}},
				[]string{"owner", "state"}, nil)
		})
		n.flush()
		Expect(rc.Wait(ctx)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty())
		Expect(label(o.srv, "g1")).To(Equal("second"))
		Expect(owner(o.srv, "g1")).To(Equal("origin"))
		Expect(guarded(o.srv, "g1").GetState()).To(BeNil())

		originCompute("g1", "computed")
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("computed"))
		Expect(guarded(n.srv, "g1").GetState()).To(BeNil())
		Expect(label(n.srv, "g1")).To(Equal("second"))
	})

	It("sends nothing for a write that changes only such fields", func() {
		pushes := n.part.pushes.Load()
		rc := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local"}, []string{"owner"}, nil)
		})
		Expect(rc.Forwarded).To(BeFalse())
		n.flush()
		Expect(n.part.pushes.Load()).To(Equal(pushes))
		Expect(n.rejected()).To(BeEmpty())
		Expect(n.status().Pending).To(BeZero())
		Expect(owner(n.srv, "g1")).To(Equal("local"), "the local value stays until the origin changes the row")
		Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1), "a value kept local is no drift")

		originCompute("g1", "computed")
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("computed"))
		Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1))
	})

	It("forwards a later write of the row without taking the local value for drift", func() {
		localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local"}, []string{"owner"}, nil)
		})
		rc := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second"}, []string{"label"}, nil)
		})
		Expect(owner(n.srv, "g1")).To(Equal("local"))

		n.flush()
		Expect(rc.Wait(ctx)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty())
		Expect(label(o.srv, "g1")).To(Equal("second"))
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("origin"),
			"the origin's row for the forwarded write replaces the local value")
		Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1), "a value kept local is no drift")
	})

	Context("when the origin refuses a later write of the row", func() {
		BeforeEach(func() {
			originOpts = []server.Option{server.WithPermissions(protectGuardedLabel{})}
		})

		It("reverts that write to the local value, without taking the value for drift", func() {
			localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local"}, []string{"owner"}, nil)
			})
			rc := localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second"}, []string{"label"}, nil)
			})
			n.flush()
			Expect(rc.Wait(ctx)).To(MatchError(ContainSubstring(server.CodePermissionDenied)))
			Expect(label(n.srv, "g1")).To(Equal("first"))
			Consistently(func() string { return owner(n.srv, "g1") }, "300ms").Should(Equal("local"),
				"a value kept local is no drift, which a fresh snapshot would replace")
			Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1))
		})
	})

	It("keeps such a value on the pending write of its row until that write settles", func() {
		n.part.blockPush.Store(true)
		rc := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second"}, []string{"label"}, nil)
		})
		local := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local"}, []string{"owner"}, nil)
		})
		Expect(local.Forwarded).To(BeFalse())
		Expect(n.status().Pending).To(Equal(1))

		// An origin change of another field rebases the row; the pending
		// write replays the local value on top of it.
		Expect(o.srv.Apply(trusted(), ws, func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Settings: &testdatav1.Settings{}}, []string{"settings"}, nil)
		})).To(Succeed())
		Eventually(func() bool { return guarded(n.srv, "g1").GetSettings() != nil }).Should(BeTrue())
		Expect(owner(n.srv, "g1")).To(Equal("local"))
		Expect(label(n.srv, "g1")).To(Equal("second"))

		n.part.blockPush.Store(false)
		n.flush()
		Expect(rc.Wait(ctx)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty())
		Expect(label(o.srv, "g1")).To(Equal("second"))
		Expect(owner(o.srv, "g1")).To(Equal("origin"))
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("origin"))
	})

	It("leaves a collaborative-text field out of what it forwards", func() {
		doc := text.New("local")
		Expect(doc.Insert(0, "summary")).To(Succeed())
		rc := localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second", Summary: doc.Encode()}, []string{"label", "summary"}, nil)
		})
		Expect(guarded(n.srv, "g1").GetSummary()).NotTo(BeEmpty())

		n.flush()
		Expect(rc.Wait(ctx)).To(Succeed())
		Expect(n.rejected()).To(BeEmpty())
		Expect(label(o.srv, "g1")).To(Equal("second"))
		Expect(guarded(o.srv, "g1").GetSummary()).To(BeEmpty())
		Expect(guarded(n.srv, "g1").GetSummary()).NotTo(BeEmpty(), "the origin has no document of its own yet")

		originCompute("g1", "computed")
		Eventually(func() []byte { return guarded(n.srv, "g1").GetSummary() }).Should(BeEmpty())
	})

	It("forwards the rest of a write-through write and sends nothing for a write of such fields alone", func() {
		_, err := n.ws().Apply(upstream.WriteThrough(trusted()), func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second", Owner: "local"}, []string{"label", "owner"}, nil)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(label(o.srv, "g1")).To(Equal("second"), "a write-through write reaches the origin before it commits")
		Expect(owner(o.srv, "g1")).To(Equal("origin"))

		n.flush()
		pushes := n.part.pushes.Load()
		rc, err := n.ws().Apply(upstream.WriteThrough(trusted()), func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "through"}, []string{"owner"}, nil)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Forwarded).To(BeFalse())
		Expect(n.part.pushes.Load()).To(Equal(pushes))
		Expect(owner(n.srv, "g1")).To(Equal("through"))
		Expect(n.rejected()).To(BeEmpty())
	})

	// The origin leaves such a field empty until its own trusted code
	// computes it, in a change of its own. The settled write must not show
	// the field empty in between.
	Context("when the origin has not computed such a field yet", func() {
		createLocal := func(key string) upstream.Receipt {
			GinkgoHelper()
			return localApply(func(tx *server.ApplyTx) error {
				return tx.Put(&testdatav1.Guarded{Id: key, Label: "new", Owner: "local",
					State: &testdatav1.Guarded_StateName{StateName: "local"}})
			})
		}

		settle := func(rcs ...upstream.Receipt) {
			GinkgoHelper()
			n.flush()
			for _, rc := range rcs {
				Expect(rc.Wait(ctx)).To(Succeed())
			}
			Expect(n.rejected()).To(BeEmpty())
		}

		keepsLocal := func(key, want string) {
			GinkgoHelper()
			Expect(owner(n.srv, key)).To(Equal(want))
			Consistently(func() string { return owner(n.srv, key) }, "200ms").Should(Equal(want))
		}

		It("keeps the local values of a new row after the write settles", func() {
			rc := createLocal("g2")
			Expect(rc.Forwarded).To(BeTrue())
			settle(rc)
			Expect(label(o.srv, "g2")).To(Equal("new"))
			Expect(owner(o.srv, "g2")).To(BeEmpty(), "the origin clears a field its guard keeps from the link on a create")
			keepsLocal("g2", "local")
			Expect(guarded(n.srv, "g2").GetStateName()).To(Equal("local"))
			Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1), "a value kept local is no drift")
		})

		It("keeps the local value of a PATCH after the write settles", func() {
			originCompute("g1", "")
			Eventually(func() string { return owner(n.srv, "g1") }).Should(BeEmpty())
			rc := localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g1", Label: "second", Owner: "local"}, []string{"label", "owner"}, nil)
			})
			settle(rc)
			Expect(label(o.srv, "g1")).To(Equal("second"))
			keepsLocal("g1", "local")
		})

		It("keeps the local value across later writes of the row", func() {
			rc1 := createLocal("g2")
			rc2 := localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "second"}, []string{"label"}, nil)
			})
			settle(rc1, rc2)
			keepsLocal("g2", "local")

			rc3 := localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "third"}, []string{"label"}, nil)
			})
			settle(rc3)
			Expect(label(o.srv, "g2")).To(Equal("third"))
			keepsLocal("g2", "local")
			Expect(label(n.srv, "g2")).To(Equal("third"))
		})

		It("shows a newer local value of the field", func() {
			settle(createLocal("g2"))
			rc := localApply(func(tx *server.ApplyTx) error {
				if err := tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "second"}, []string{"label"}, nil); err != nil {
					return err
				}
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Owner: "newer"}, []string{"owner"}, nil)
			})
			settle(rc)
			keepsLocal("g2", "newer")

			local := localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Owner: "newest"}, []string{"owner"}, nil)
			})
			Expect(local.Forwarded).To(BeFalse())
			settle(localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "third"}, []string{"label"}, nil)
			}))
			keepsLocal("g2", "newest")
		})

		It("replaces the local value with the value the origin computes", func() {
			settle(createLocal("g2"))
			keepsLocal("g2", "local")

			originCompute("g2", "computed")
			Eventually(func() string { return owner(n.srv, "g2") }).Should(Equal("computed"))
			Expect(guarded(n.srv, "g2").GetState()).To(BeNil(), "the origin's change replaces every value kept local")
		})

		It("takes an empty value from a later origin write", func() {
			settle(createLocal("g2"))
			keepsLocal("g2", "local")

			Expect(o.srv.Apply(trusted(), ws, func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "origin"}, []string{"label", "owner"}, nil)
			})).To(Succeed())
			Eventually(func() string { return label(n.srv, "g2") }).Should(Equal("origin"))
			Expect(owner(n.srv, "g2")).To(BeEmpty())
		})

		It("keeps the local value across a restart", func() {
			settle(createLocal("g2"))
			n.restart()
			n.flush()
			keepsLocal("g2", "local")

			settle(localApply(func(tx *server.ApplyTx) error {
				return tx.Patch(&testdatav1.Guarded{Id: "g2", Label: "second"}, []string{"label"}, nil)
			}))
			keepsLocal("g2", "local")
		})
	})

	It("keeps such a value local across a restart", func() {
		localApply(func(tx *server.ApplyTx) error {
			return tx.Patch(&testdatav1.Guarded{Id: "g1", Owner: "local"}, []string{"owner"}, nil)
		})
		n.restart()
		n.flush()
		Expect(owner(n.srv, "g1")).To(Equal("local"))
		Expect(n.part.bootstraps.Load()).To(BeNumerically("==", 1))

		originCompute("g1", "computed")
		Eventually(func() string { return owner(n.srv, "g1") }).Should(Equal("computed"))
	})
})

// protectGuardedLabel keeps every caller from writing a guarded row's label: a
// refusal the link's own schema does not predict.
type protectGuardedLabel struct{ server.AllowAll }

func (protectGuardedLabel) ProtectedFields(_ context.Context, _ server.Principal, _, entity string) ([]string, error) {
	if entity == "guarded" {
		return []string{"label"}, nil
	}
	return nil, nil
}
