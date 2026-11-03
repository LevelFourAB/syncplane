package upstream

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stored bootstrap state", func() {
	It("keeps the progress of an interrupted snapshot across a restart", func() {
		m := syncMeta{clientID: "c1", boot: &bootState{
			cursor: []byte{1}, token: []byte{2}, seq: 3,
			rows: 40, estimate: 120, entitiesDone: 1, entitiesTotal: 3,
		}}
		b, err := m.encode()
		Expect(err).NotTo(HaveOccurred())
		got, err := decodeSyncMeta(b)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.boot).NotTo(BeNil())
		Expect(got.boot.progress()).To(Equal(BootstrapProgress{
			RowsApplied: 40, RowsEstimated: 120, EntitiesDone: 1, EntitiesTotal: 3,
		}))
	})

	It("never reports an estimate below the rows applied", func() {
		b := &bootState{rows: 50, estimate: 40}
		Expect(b.progress().RowsEstimated).To(BeEquivalentTo(50))
		b = &bootState{rows: 50}
		Expect(b.progress().RowsEstimated).To(BeZero(), "an unknown estimate stays unknown")
	})
})

var _ = Describe("Status.Behind", func() {
	It("is the head minus the confirmed position, and 0 when the head is unknown or not ahead", func() {
		Expect(Status{OriginSeq: 7, OriginHeadSeq: 10}.Behind()).To(BeEquivalentTo(3))
		Expect(Status{OriginSeq: 10, OriginHeadSeq: 10}.Behind()).To(BeZero())
		Expect(Status{OriginSeq: 10}.Behind()).To(BeZero(), "an origin that reports no head")
		Expect(Status{OriginSeq: 12, OriginHeadSeq: 10}.Behind()).To(BeZero(), "a head from an older page")
	})
})
