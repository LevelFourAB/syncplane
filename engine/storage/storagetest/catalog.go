package storagetest

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Catalog registers the behavior specs for an adapter that implements
// storage.Catalog. Registered separately from Conformance because the
// capability is optional; an adapter without one does not call this.
// newAdapter is called per spec and must return a fresh, empty store; it is
// closed afterwards.
func Catalog(name string, newAdapter func() storage.Adapter) bool {
	return Describe("storage.Catalog conformance: "+name, func() {
		var (
			ctx     context.Context
			catalog storage.Catalog
		)

		BeforeEach(func() {
			ctx = context.Background()
			adapter := newAdapter()
			DeferCleanup(func() { Expect(adapter.Close()).To(Succeed()) })
			var ok bool
			catalog, ok = storage.As[storage.Catalog](adapter)
			Expect(ok).To(BeTrue(), "adapter must implement storage.Catalog to run this suite")
		})

		It("starts empty", func() {
			Expect(catalog.ListCatalogEntries(ctx)).To(BeEmpty())
		})

		It("persists and lists entries in id order", func() {
			Expect(catalog.PutCatalogEntry(ctx, "beta", []byte("b-set"), "rev-b")).To(Succeed())
			Expect(catalog.PutCatalogEntry(ctx, "alpha", []byte("a-set"), "rev-a")).To(Succeed())

			got, err := catalog.ListCatalogEntries(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(HaveLen(2))
			Expect(got[0].ID).To(Equal("alpha"))
			Expect(got[0].Payload).To(Equal([]byte("a-set")))
			Expect(got[0].Revision).To(Equal("rev-a"))
			Expect(got[0].UpdatedAt).NotTo(BeZero())
			Expect(got[1].ID).To(Equal("beta"))
		})

		It("upserts an existing entry in place", func() {
			Expect(catalog.PutCatalogEntry(ctx, "app", []byte("v1"), "rev1")).To(Succeed())
			Expect(catalog.PutCatalogEntry(ctx, "app", []byte("v2"), "rev2")).To(Succeed())

			got, err := catalog.ListCatalogEntries(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(HaveLen(1))
			Expect(got[0].Payload).To(Equal([]byte("v2")))
			Expect(got[0].Revision).To(Equal("rev2"))
		})

		It("deletes an entry (and ignores a missing one)", func() {
			Expect(catalog.PutCatalogEntry(ctx, "app", []byte("v1"), "rev1")).To(Succeed())
			Expect(catalog.DeleteCatalogEntry(ctx, "app")).To(Succeed())
			Expect(catalog.DeleteCatalogEntry(ctx, "absent")).To(Succeed())

			Expect(catalog.ListCatalogEntries(ctx)).To(BeEmpty())
		})
	})
}
