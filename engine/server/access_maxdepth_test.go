package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/server"
)

// A client predicts access by walking the rows above a row, and stops where
// the server stops: after Limits.MaxAccessDepth hops. The summary carries the
// limit, so a server with another limit than the default is predicted right.
var _ = Describe("Access model limit on access parent hops in the CallerAccess summary", func() {
	It("sends the server's MaxAccessDepth", func() {
		// Given a server whose MaxAccessDepth is 5
		f := newAccessFixture(server.WithLimits(server.Limits{MaxAccessDepth: 5}))
		f.seedOrg()

		// When a client pulls
		got := newAccessReplica(f, "writer").pullOnce(0).GetCallerAccess()

		// Then the summary carries the limit
		Expect(got.GetMaxAccessDepth()).To(Equal(uint32(5)))
	})

	It("sends the default when the server sets no limit", func() {
		f := newAccessFixture()
		f.seedOrg()
		Expect(newAccessReplica(f, "writer").pullOnce(0).GetCallerAccess().GetMaxAccessDepth()).To(Equal(uint32(32)))
	})

	It("sends the whole summary, not a delta, when the limit changes", func() {
		// Given a client that holds the summary of a server with the default
		// limit
		f := newAccessFixture()
		f.seedOrg()
		r := newAccessReplica(f, "writer")
		r.sync()
		Expect(r.access.GetMaxAccessDepth()).To(Equal(uint32(32)))

		// When the server restarts over the same store with MaxAccessDepth 5,
		// and the client pulls again
		r.f = newAccessFixtureOn(f.store, server.WithLimits(server.Limits{MaxAccessDepth: 5}))
		if r.pullOnce(0).GetReset_() {
			r.cursor, r.access = nil, nil
			r.pullOnce(0)
		}

		// Then the client holds the new limit, which a delta cannot carry
		Expect(r.access.GetMaxAccessDepth()).To(Equal(uint32(5)))
	})
})
