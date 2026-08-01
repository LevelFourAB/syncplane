package server_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// New installs everything the declaration options carry before it hands back a
// server, so an app's whole schema is one error check. These specs pin what
// that buys: the failure is reported rather than deferred to first use, and
// nothing half-built escapes.
var _ = Describe("New", func() {
	It("reports a bad entity declaration instead of returning a server", func() {
		// A slash in the name forges an extra segment in the client's
		// `c/<entity>/<ws>/<key>` layout, so registration refuses it.
		srv, err := server.New(newTestStore(),
			server.WithEntities(dynamicpb.NewMessage(namedThing("a/b", "id"))))
		Expect(err).To(MatchError(ContainSubstring("not an identifier")))
		Expect(srv).To(BeNil(), "a server that could not be declared is never handed out")
	})

	It("reports a bad mutator declaration", func() {
		_, err := server.New(newTestStore(), server.WithMutator("", nil, nil))
		Expect(err).To(MatchError(ContainSubstring("needs a name and a func")))
	})

	It("declares entities across several WithEntities options", func() {
		srv, err := server.New(newTestStore(),
			server.WithEntities(&todov1.Todo{}),
			server.WithEntities(&testdatav1.Widget{}))
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.Mutate(context.Background(), "ws-new", func(tx *server.EntityTx) error {
			if err := tx.Put(&todov1.Todo{Id: "t1"}); err != nil {
				return err
			}
			return tx.Put(&testdatav1.Widget{Id: "w1"})
		})).To(Succeed())
	})

	It("rejects the same entity declared twice", func() {
		_, err := server.New(newTestStore(),
			server.WithEntities(&todov1.Todo{}, &todov1.Todo{}))
		Expect(err).To(MatchError(ContainSubstring(`entity "todo" already registered`)))
	})

	It("runs a mutator declared before the entity it writes", func() {
		srv, err := server.New(newTestStore(),
			server.WithMutator("complete", &todov1.Todo{},
				func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
					todo := args.(*todov1.Todo)
					todo.Completed = true
					return tx.Put(todo)
				}),
			server.WithEntities(&todov1.Todo{}))
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.Invoke(context.Background(), "ws-new", "complete", &todov1.Todo{Id: "t1"})).To(Succeed())
	})
})
