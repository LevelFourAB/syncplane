package upstream_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	"github.com/levelfourab/syncplane/engine/upstream"
)

var _ = Describe("Link", func() {
	var (
		o *origin
		n *local
	)
	BeforeEach(func() {
		o = newOrigin()
	})

	It("bootstraps a workspace the origin already holds", func() {
		o.put(&todov1.Todo{Id: "t1", Title: "one"}, &todov1.Todo{Id: "t2", Title: "two"})
		n = newLocal(o, nil)
		n.add()
		n.flush()
		Expect(title(n.todo("t1"))).To(Equal("one"))
		Expect(title(n.todo("t2"))).To(Equal("two"))
		Expect(n.status().State).To(Equal(upstream.StateLive))
	})

	It("pulls origin writes and pushes local writes as they happen", func() {
		n = newLocal(o, nil)
		n.add()
		n.flush()

		o.put(&todov1.Todo{Id: "t1", Title: "from origin"})
		Eventually(func() string { return title(n.todo("t1")) }).Should(Equal("from origin"))

		rc := n.put(&todov1.Todo{Id: "t2", Title: "from local"})
		Expect(rc.Forwarded).To(BeTrue())
		Expect(rc.Wait(context.Background())).To(Succeed())
		Expect(title(o.todo("t2"))).To(Equal("from local"))
		n.flush()
		Expect(n.status().Pending).To(BeZero())
	})

	It("stops every goroutine it started on Close", func() {
		base := linkGoroutines()
		n = newLocal(o, nil)
		n.add()
		n.flush()
		Expect(linkGoroutines()).To(BeNumerically(">", base))
		n.stop()
		Eventually(linkGoroutines).WithTimeout(5*time.Second).Should(BeNumerically("<=", base), dumpLinkGoroutines)
	})
})
