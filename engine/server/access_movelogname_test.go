package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

var _ = Describe("Access model move log name", func() {
	It("refuses an entity that takes the name of the engine's access move log", func() {
		// Given the access schema, with Label renamed to the storage entity
		// the engine keeps its access move log under
		f := newSchemaFixtureOn(newTestStore(), server.WithEntities())
		set := accessFileSet(func(file *descriptorpb.FileDescriptorProto) {
			label := messageNamed(file, "Label")
			eo := proto.GetExtension(label.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
			eo.Name = "syncplane_access_moves"
		})

		// When it is registered
		_, err := f.srv.RegisterDescriptorSet(set, server.AllowAccessChange())

		// Then registration refuses it, as it refuses the name of the field
		// history entity, so that its rows and the log's never share a table
		Expect(err).To(MatchError(ContainSubstring("syncplane_access_moves")))
	})
})
