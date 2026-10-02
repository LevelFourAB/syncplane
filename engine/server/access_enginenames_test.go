package server_test

import (
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The engine keeps the access move log as rows of an entity of its own in each
// workspace. An entity the application registers must never share its
// storage, or the log's records reach clients as the application's rows.
var _ = Describe("The storage entity of the access move log", func() {
	It("keeps the access move log out of an entity the application names syncplane_access_moves", func() {
		// Given the org schema with comments below tasks, where guest reads
		// project p1 only, and task t2 with comment c2 sits in project p2
		f := newAccessFixture(server.WithEntities(&accessv1.Comment{}))
		f.seedOrg()
		f.seed(
			&accessv1.Project{Id: "p3", Name: "three"},
			&accessv1.Comment{Id: "c2", Task: &accessv1.Task{Id: "t2"}, Body: "on two"},
		)

		// And the application registers an entity named syncplane_access_moves
		err := f.srv.RegisterDynamicEntity(server.DynamicEntity{
			Name:      "syncplane_access_moves",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "entity", Number: 1, Kind: protoreflect.StringKind},
				{Name: "key", Number: 2, Kind: protoreflect.StringKind},
				{Name: "id", Number: 10, Kind: protoreflect.StringKind},
			},
		})
		if err != nil {
			// Refusing the name keeps the two apart as well.
			return
		}

		// When a connector moves t2 from p2 to p3, neither of which guest reads
		Expect(f.srv.Mutate(server.TrustedAs(f.ctx, "connector"), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.Task{Id: "t2", Project: project("p3")}, []string{"project"}, nil)
		})).To(Succeed())

		// Then a pull as guest carries no row of syncplane_access_moves, which
		// the application never wrote
		req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-guest"})
		as("guest", req.Header())
		resp, err := f.sync.Pull(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var leaked []string
		for _, rc := range resp.Msg.GetPatch().GetChanges() {
			if rc.GetEntity() == "syncplane_access_moves" {
				leaked = append(leaked, rc.GetKey()+" "+strings.ToValidUTF8(string(rc.GetValue()), "?"))
			}
		}
		Expect(leaked).To(BeEmpty(), "the move log of rows guest does not read reached guest")
	})
})
