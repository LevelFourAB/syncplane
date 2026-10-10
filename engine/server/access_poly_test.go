package server_test

import (
	"fmt"
	"log/slog"
	"slices"
	"sort"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// polyEntities registers the polymorphic schema of the access test proto:
// cards and place members below a board or a space, items below cards, and
// pins that name their board or space through a reference message.
func polyEntities() server.Option {
	return server.WithEntities(
		&accessv1.Board{}, &accessv1.Space{}, &accessv1.PlaceMember{},
		&accessv1.Card{}, &accessv1.CardItem{}, &accessv1.Pin{},
	)
}

func newPolyFixtureOn(store storage.Adapter, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	return newSchemaFixtureOn(store, polyEntities(), opts...)
}

func cardOnBoard(id, board string) *accessv1.Card {
	return &accessv1.Card{Id: id, Place: &accessv1.Card_Board{Board: &accessv1.Board{Id: board}}}
}

func cardInSpace(id, space string) *accessv1.Card {
	return &accessv1.Card{Id: id, Place: &accessv1.Card_Space{Space: &accessv1.Space{Id: space}}}
}

func itemOn(id, card string) *accessv1.CardItem {
	return &accessv1.CardItem{Id: id, Card: &accessv1.Card{Id: card}}
}

func pinOnBoard(id, board string) *accessv1.Pin {
	return &accessv1.Pin{Id: id, Place: &accessv1.PlaceRef{Target: &accessv1.PlaceRef_Board{Board: &accessv1.Board{Id: board}}}}
}

func pinInSpace(id, space string) *accessv1.Pin {
	return &accessv1.Pin{Id: id, Place: &accessv1.PlaceRef{Target: &accessv1.PlaceRef_Space{Space: &accessv1.Space{Id: space}}}}
}

func memberOnBoard(id, board, user, role string) *accessv1.PlaceMember {
	return &accessv1.PlaceMember{
		Id: id, Place: &accessv1.PlaceMember_Board{Board: &accessv1.Board{Id: board}}, UserId: user, Role: role,
	}
}

func memberInSpace(id, space, user, role string) *accessv1.PlaceMember {
	return &accessv1.PlaceMember{
		Id: id, Place: &accessv1.PlaceMember_Space{Space: &accessv1.Space{Id: space}}, UserId: user, Role: role,
	}
}

// seedPlaces writes the rows every polymorphic spec starts from:
//
//   - boards b1, b2 and x, and spaces s1 and x, so that a board and a space
//     share the key x;
//   - bviewer views b1, sviewer views s1, xeditor edits the board x, and
//     admin administers b2;
//   - a card, an item on it and a pin on b1 and in s1, a card on each x, a
//     pin in the space x, a pin on b2, and a card on neither.
func (f *accessFixture) seedPlaces() {
	GinkgoHelper()
	f.seed(
		&accessv1.Board{Id: "b1"}, &accessv1.Board{Id: "b2"}, &accessv1.Board{Id: "x"},
		&accessv1.Space{Id: "s1"}, &accessv1.Space{Id: "x"},
		memberOnBoard("pm-bview", "b1", "bviewer", "place_viewer"),
		memberInSpace("pm-sview", "s1", "sviewer", "place_viewer"),
		memberOnBoard("pm-xedit", "x", "xeditor", "place_editor"),
		memberOnBoard("pm-admin", "b2", "admin", "place_admin"),
		cardOnBoard("c-b1", "b1"), cardInSpace("c-s1", "s1"),
		cardOnBoard("c-bx", "x"), cardInSpace("c-sx", "x"),
		&accessv1.Card{Id: "c-none"},
		itemOn("i-b1", "c-b1"), itemOn("i-s1", "c-s1"),
		pinOnBoard("p-b1", "b1"), pinInSpace("p-s1", "s1"), pinInSpace("p-sx", "x"), pinOnBoard("p-b2", "b2"),
	)
}

// bootstrapRows returns the rows a bootstrap from an empty cursor sends user,
// as "entity/key", sorted.
func (f *accessFixture) bootstrapRows(user string) []string {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user})
	as(user, req.Header())
	stream, err := f.sync.Bootstrap(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	var rows []string
	for stream.Receive() {
		if chunk, ok := stream.Msg().GetBody().(*syncplanev1.BootstrapResponse_Chunk_); ok {
			for _, rc := range chunk.Chunk.GetRows() {
				rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
			}
		}
	}
	Expect(stream.Err()).NotTo(HaveOccurred())
	sort.Strings(rows)
	return rows
}

// queryKeys returns the keys of the rows of entity a Query answers user
// with, sorted.
func (f *accessFixture) queryKeys(user, entity string) []string {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: entity})
	as(user, req.Header())
	resp, err := f.sync.Query(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	var keys []string
	for _, rc := range resp.Msg.GetRows() {
		keys = append(keys, rc.GetKey())
	}
	sort.Strings(keys)
	return keys
}

// fieldNamed returns the field of m with the given name, for a spec to change
// in place.
func fieldNamed(m *descriptorpb.DescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
	GinkgoHelper()
	for _, fd := range m.GetField() {
		if fd.GetName() == name {
			return fd
		}
	}
	Fail("no field named " + name)
	return nil
}

// withBehavior returns field options that carry b.
func withBehavior(b *syncplanev1.FieldBehavior) *descriptorpb.FieldOptions {
	opts := &descriptorpb.FieldOptions{}
	proto.SetExtension(opts, syncplanev1.E_FieldBehavior, b)
	return opts
}

// entityOptionsOf returns the entity options of the named message of file, for
// a change to edit in place.
func entityOptionsOf(file *descriptorpb.FileDescriptorProto, message string) *syncplanev1.EntityOptions {
	GinkgoHelper()
	return proto.GetExtension(messageNamed(file, message).GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
}

var _ = Describe("Access model registration of a polymorphic access parent", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(change func(*descriptorpb.FileDescriptorProto), opts ...server.RegisterOption) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(accessFileSet(change), opts...)
		return err
	}

	It("registers the compiled schema without a warning", func() {
		logs := &recordingHandler{}
		mustNewServer(newTestStore(), server.WithLogger(slog.New(logs)), polyEntities())
		Expect(logs.count("access model warning")).To(BeZero())
	})

	It("registers the descriptor set", func() {
		Expect(register(nil)).To(Succeed())
	})

	DescribeTable("refuses a schema that breaks a rule of a polymorphic access parent",
		func(change func(*descriptorpb.FileDescriptorProto), want string) {
			Expect(register(change)).To(MatchError(ContainSubstring(want)))
		},
		Entry("a member of a oneof named alone", func(file *descriptorpb.FileDescriptorProto) {
			entityOptionsOf(file, "Card").Access.Parents = []string{"board"}
		}, `access parents[0] names field "board", a member of oneof "place"; name the oneof`),
		Entry("a oneof member that is not a reference", func(file *descriptorpb.FileDescriptorProto) {
			// After space, since the members of a oneof are declared together.
			card := messageNamed(file, "Card")
			card.Field = slices.Insert(card.Field, 3, &descriptorpb.FieldDescriptorProto{
				Name: proto.String("url"), JsonName: proto.String("url"), Number: proto.Int32(9),
				Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				OneofIndex: proto.Int32(0),
			})
		}, `access parents[0]: field "url" must be a reference`),
		Entry("a polymorphic reference message with a workspace field", func(file *descriptorpb.FileDescriptorProto) {
			ref := messageNamed(file, "PlaceRef")
			ref.Field = append(ref.Field, &descriptorpb.FieldDescriptorProto{
				Name: proto.String("workspace_id"), JsonName: proto.String("workspaceId"), Number: proto.Int32(3),
				Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Options: withBehavior(&syncplanev1.FieldBehavior{Workspace: true}),
			})
		}, `access parents[0]: field "place" is a reference message with a workspace field`),
	)

	// A pin on a board or in a space sits one hop below the root, and a pin on
	// a card item three: the deepest case decides.
	It("refuses a case whose chain is longer than MaxAccessDepth", func() {
		srv = newBareServer(server.WithLimits(server.Limits{MaxAccessDepth: 2}))
		Expect(register(func(file *descriptorpb.FileDescriptorProto) {
			ref := messageNamed(file, "PlaceRef")
			ref.Field = append(ref.Field, &descriptorpb.FieldDescriptorProto{
				Name: proto.String("item"), JsonName: proto.String("item"), Number: proto.Int32(3),
				Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				TypeName: proto.String(".syncplane.testdata.access.v1.CardItem"), OneofIndex: proto.Int32(0),
			})
		})).To(MatchError(ContainSubstring(`entity "pin" can sit 3 access parent hops below an entity without a parent`)))
	})

	It("treats a new case as a change to the access model", func() {
		boardOnly := func(file *descriptorpb.FileDescriptorProto) {
			card := messageNamed(file, "Card")
			card.Field = slices.DeleteFunc(card.Field, func(fd *descriptorpb.FieldDescriptorProto) bool {
				return fd.GetName() == "space"
			})
		}
		Expect(register(boardOnly)).To(Succeed())
		Expect(register(nil)).To(MatchError(ContainSubstring(`entity "card" changes its access model`)))
		Expect(register(nil, server.AllowAccessChange())).To(Succeed())
	})
})

var _ = Describe("Access model reads below a polymorphic access parent", func() {
	var f *accessFixture

	for _, indexed := range []bool{false, true} {
		Context(map[bool]string{false: "before the indexes are built", true: "with the indexes built"}[indexed], func() {
			BeforeEach(func() {
				f = newPolyFixtureOn(newTestStore())
				f.seedPlaces()
				if indexed {
					Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
				}
			})

			It("shows a role on a board or a space the rows below whichever case names it", func() {
				Expect(f.pull("bviewer")).To(Equal([]string{
					"board/b1", "card/c-b1", "card_item/i-b1", "pin/p-b1", "place_member/pm-bview",
				}))
				Expect(f.pull("sviewer")).To(Equal([]string{
					"card/c-s1", "card_item/i-s1", "pin/p-s1", "place_member/pm-sview", "space/s1",
				}))
			})

			It("tells a board and a space with the same key apart", func() {
				Expect(f.pull("xeditor")).To(Equal([]string{"board/x", "card/c-bx", "place_member/pm-xedit"}))
			})

			It("bootstraps what a pull shows", func() {
				for _, user := range []string{"bviewer", "sviewer", "xeditor", "admin"} {
					Expect(f.bootstrapRows(user)).To(Equal(f.pull(user)), user)
				}
			})

			It("answers a Query with the rows below whichever case names the caller's anchor", func() {
				Expect(f.queryKeys("xeditor", "card")).To(Equal([]string{"c-bx"}))
				Expect(f.queryKeys("sviewer", "pin")).To(Equal([]string{"p-s1"}))
				Expect(f.queryKeys("bviewer", "card_item")).To(Equal([]string{"i-b1"}))
			})
		})
	}

	It("bootstraps a caller below the root without scanning cards or pins", func() {
		counting := &pageScanCountingStore{Adapter: newTestStore(), scanned: map[string]int{}}
		f = newPolyFixtureOn(counting)
		f.seedPlaces()
		for i := range 30 {
			f.seed(cardInSpace(fmt.Sprintf("cx%02d", i), "x"), pinInSpace(fmt.Sprintf("px%02d", i), "x"))
		}
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		counting.reset()
		Expect(f.bootstrapRows("xeditor")).To(Equal([]string{"board/x", "card/c-bx", "place_member/pm-xedit"}))
		for _, entity := range []string{"card", "pin"} {
			Expect(counting.count(entity)).To(BeZero(), entity)
		}
	})
})

var _ = Describe("Access model writes below a polymorphic access parent", func() {
	var (
		f       *accessFixture
		clients int
	)

	one := func(user string, m func(client string, id uint64) *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		clients++
		client := fmt.Sprintf("c%d", clients)
		results := f.push(user, client, m(client, 1))
		Expect(results).To(HaveLen(1))
		return results[0]
	}
	put := func(entity, key string, value proto.Message) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return putMutation(c, id, entity, key, value) }
	}
	patch := func(entity, key string, value proto.Message, paths ...string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation {
			return patchMutation(c, id, entity, key, value, paths...)
		}
	}
	del := func(entity, key string) func(string, uint64) *syncplanev1.Mutation {
		return func(c string, id uint64) *syncplanev1.Mutation { return deleteMutation(c, id, entity, key) }
	}
	applied := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
	}
	refused := func(r *syncplanev1.MutationResult, code string) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal(code), r.GetErrorMessage())
	}

	BeforeEach(func() {
		f = newPolyFixtureOn(newTestStore())
		f.seedPlaces()
		f.seed(
			memberOnBoard("pm-both-b", "x", "both", "place_editor"),
			memberInSpace("pm-both-s", "x", "both", "place_editor"),
		)
	})

	It("checks a create at the row the set case names", func() {
		applied(one("xeditor", put("card", "n1", cardOnBoard("n1", "x"))))
		refused(one("xeditor", put("card", "n2", cardInSpace("n2", "x"))), server.CodeAccessDenied)
		applied(one("xeditor", put("pin", "n3", pinOnBoard("n3", "x"))))
		refused(one("xeditor", put("pin", "n4", pinInSpace("n4", "x"))), server.CodeAccessDenied)
		applied(one("xeditor", put("card_item", "n5", itemOn("n5", "c-bx"))))
		refused(one("xeditor", put("card_item", "n6", itemOn("n6", "c-sx"))), server.CodeAccessDenied)
	})

	// The board x and the space x share a key, so only the entity tells the
	// two places apart.
	It("checks a move between rows of two entities that share a key at both ends", func() {
		refused(one("xeditor", patch("card", "c-bx", cardInSpace("c-bx", "x"), "place")), server.CodeAccessDenied)
		refused(one("xeditor", put("card", "c-bx", cardInSpace("c-bx", "x"))), server.CodeAccessDenied)
		applied(one("both", patch("card", "c-bx", cardInSpace("c-bx", "x"), "place")))
		Expect(f.pull("xeditor")).NotTo(ContainElement("card/c-bx"))
		applied(one("both", patch("pin", "p-sx", pinOnBoard("p-sx", "x"), "place")))
		Expect(f.pull("xeditor")).To(ContainElement("pin/p-sx"))
	})

	It("refuses to delete a board or a space while a row below it is live, through any case", func() {
		r := one("admin", del("board", "b2"))
		refused(r, server.CodeConflict)
		Expect(r.GetErrorMessage()).To(ContainSubstring("access parent of a live pin row"))
		applied(one("admin", del("pin", "p-b2")))
		r = one("admin", del("board", "b2"))
		refused(r, server.CodeConflict)
		Expect(r.GetErrorMessage()).To(ContainSubstring("access parent of a live place_member row"))
	})

	// A caller with a role on a board has a grant row below it, so only a
	// caller with a role at the root can delete a board. The board and the
	// space share a key, so only the entity tells the rows below the space
	// apart from rows below the board.
	It("deletes a board while rows sit below a space with the same key, with and without the indexes", func() {
		f = newPolyFixtureOn(newTestStore(), rootRolesBy(func(p server.Principal) []string {
			if p.Subject == "root" {
				return []string{"place_admin"}
			}
			return nil
		}))
		for _, id := range []string{"y1", "y2"} {
			f.seed(&accessv1.Board{Id: id}, &accessv1.Space{Id: id}, cardInSpace("c-"+id, id), pinInSpace("p-"+id, id))
		}
		applied(one("root", del("board", "y1")))
		refused(one("root", del("space", "y1")), server.CodeConflict)
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		applied(one("root", del("board", "y2")))
		refused(one("root", del("space", "y2")), server.CodeConflict)
	})

	It("grants a role at the row the set case names", func() {
		applied(one("admin", put("place_member", "pm-new", memberOnBoard("pm-new", "b2", "newbie", "place_viewer"))))
		Expect(f.pull("newbie")).To(ContainElements("board/b2", "pin/p-b2"))
		refused(one("admin", put("place_member", "pm-new2", memberInSpace("pm-new2", "x", "newbie", "place_viewer"))),
			server.CodeAccessDenied)
	})

	It("refuses a grant row that names no place, on every write path", func() {
		r := one("admin", put("place_member", "pm-new", &accessv1.PlaceMember{Id: "pm-new", UserId: "newbie", Role: "place_viewer"}))
		refused(r, server.CodeInvalidArgument)
		Expect(r.GetErrorMessage()).To(ContainSubstring(`names no "place"`))
		// A mask names a oneof by its group.
		err := f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			return tx.Patch(&accessv1.PlaceMember{Id: "pm-admin"}, []string{"place"}, nil)
		})
		Expect(err).To(MatchError(ContainSubstring(`names no "place"`)))
	})
})

var _ = Describe("Access model sync below a polymorphic access parent", func() {
	var f *accessFixture

	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, fn)).To(Succeed())
	}
	moveCard := func(card *accessv1.Card) {
		GinkgoHelper()
		trusted(func(tx *server.EntityTx) error { return tx.Patch(card, []string{"place"}, nil) })
	}
	converged := func(r *accessReplica) {
		GinkgoHelper()
		Expect(r.view()).To(Equal(f.pull(r.user)))
		Expect(r.resets).To(BeZero())
	}

	BeforeEach(func() {
		f = newPolyFixtureOn(newTestStore())
		f.seedPlaces()
		f.seed(itemOn("i-bx", "c-bx"))
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("moves the items of a card that moves from a board to a space", func() {
		bviewer := newAccessReplica(f, "bviewer")
		sviewer := newAccessReplica(f, "sviewer")
		bviewer.sync()
		sviewer.sync()

		moveCard(cardInSpace("c-b1", "s1"))
		bviewer.sync()
		sviewer.sync()
		Expect(bviewer.view()).NotTo(ContainElements("card/c-b1", "card_item/i-b1"))
		Expect(sviewer.view()).To(ContainElements("card/c-b1", "card_item/i-b1"))
		converged(bviewer)
		converged(sviewer)
	})

	It("moves the items of a card between a board and a space that share a key", func() {
		xeditor := newAccessReplica(f, "xeditor")
		xeditor.sync()
		Expect(xeditor.view()).To(ContainElements("card/c-bx", "card_item/i-bx"))

		moveCard(cardInSpace("c-bx", "x"))
		xeditor.sync()
		Expect(xeditor.view()).NotTo(ContainElements("card/c-bx", "card_item/i-bx"))
		converged(xeditor)
	})
})
