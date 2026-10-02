package server_test

import (
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// shelvesWithOwners adds to Shelf an owner_id, and to Leaflet a rule that
// lets the owner of a shelf a leaflet sits on update it.
func shelvesWithOwners(file *descriptorpb.FileDescriptorProto) {
	shelf := messageNamed(file, "Shelf")
	shelf.Field = append(shelf.Field, &descriptorpb.FieldDescriptorProto{
		Name:     proto.String("owner_id"),
		JsonName: proto.String("ownerId"),
		Number:   proto.Int32(2),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
	})
	leaflet := messageNamed(file, "Leaflet")
	access := proto.GetExtension(leaflet.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions).GetAccess()
	access.Allow = append(access.Allow, &syncplanev1.AccessRule{
		Update:   true,
		Admitted: &syncplanev1.AdmittedCaller{},
		Within:   []*syncplanev1.AccessWithin{{Entity: "shelf", CallerIs: "owner_id"}},
	})
}

// A parent is a share: a write that adds a parent needs its rule through that
// parent on its own, also for a rule whose within entries admit the caller.
var _ = Describe("Access model within rules on a row with several parents", func() {
	var (
		f            *accessFixture
		patchLeaflet func(shelves ...string) *syncplanev1.MutationResult
	)

	// Given: shelves h1 and h3 are u1's and h2 is u2's, leaflet l1 sits on
	// h1, and u1 views h1.
	BeforeEach(func() {
		f = &accessFixture{ctx: GinkgoT().Context(), store: newTestStore()}
		f.srv = mustNewServer(f.store, headerAuthenticator())
		_, err := f.srv.RegisterDescriptorSet(accessFileSet(shelvesWithOwners))
		Expect(err).NotTo(HaveOccurred())
		meta, ok := f.srv.Entity("shelf")
		Expect(ok).To(BeTrue())
		fd := meta.Descriptor.ParentFile()
		ts := httptest.NewServer(f.srv.HTTPHandler())
		DeferCleanup(ts.Close)
		f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)

		dynamic := func(msg proto.Message) *dynamicpb.Message {
			out := dynamicpb.NewMessage(fd.Messages().ByName(msg.ProtoReflect().Descriptor().Name()))
			Expect(proto.Unmarshal(mustMarshal(msg), out)).To(Succeed())
			return out
		}
		shelf := func(id, owner string) proto.Message {
			msg := dynamic(&accessv1.Shelf{Id: id})
			msg.Set(msg.Descriptor().Fields().ByName("owner_id"), protoreflect.ValueOfString(owner))
			return msg
		}
		f.seed(
			shelf("h1", "u1"), shelf("h2", "u2"), shelf("h3", "u1"),
			dynamic(&accessv1.Leaflet{Id: "l1", Shelves: []*accessv1.Shelf{{Id: "h1"}}, Title: "one"}),
			dynamic(&accessv1.LaneMember{Id: "lm-u1", Lane: &accessv1.LaneMember_Shelf{Shelf: &accessv1.Shelf{Id: "h1"}}, UserId: "u1", Role: "lane_viewer"}),
		)
		patchLeaflet = func(shelves ...string) *syncplanev1.MutationResult {
			GinkgoHelper()
			l := &accessv1.Leaflet{Id: "l1"}
			for _, s := range shelves {
				l.Shelves = append(l.Shelves, &accessv1.Shelf{Id: s})
			}
			results := f.push("u1", "c-u1", patchMutation("c-u1", 1, "leaflet", "l1", dynamic(l), "shelves"))
			Expect(results).To(HaveLen(1))
			return results[0]
		}
	})

	It("refuses an update that adds a parent where the within entry does not hold", func() {
		// When: u1 adds h2, which u1 does not own, to l1.
		r := patchLeaflet("h1", "h2")

		// Then: it is refused, as the rule does not hold through h2.
		Expect(r.GetErrorCode()).To(Equal(server.CodeAccessDenied), r.GetStatus().String())
	})

	It("applies an update that adds a parent where the within entry holds", func() {
		// When: u1 adds h3, which u1 owns, to l1.
		r := patchLeaflet("h1", "h3")

		// Then: it is applied.
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	})
})
