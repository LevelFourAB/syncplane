package server_test

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A row whose index entries the store cannot hold is a write the server
// refuses: an index missing the row would answer queries differently from the
// scan it stands in for. The refusal is a rejection, so a client is told and
// does not retry the same write until it is dead-lettered.
var _ = Describe("Index entry limits", func() {
	var (
		ctx context.Context
		srv *server.Server
		mt  protoreflect.MessageType
	)

	noteFields := []server.DynamicField{
		{Name: "id", Number: 1, Kind: protoreflect.StringKind},
		{Name: "body", Number: 2, Kind: protoreflect.StringKind},
		{Name: "labels", Number: 3, Kind: protoreflect.StringKind, Repeated: true},
	}
	indexed := server.DynamicEntity{
		Name:      "note",
		KeyFields: []string{"id"},
		Fields:    noteFields,
		Indexes: []server.DynamicIndex{
			{Name: "by_body", Key: []server.DynamicIndexKey{{Path: "body"}}},
			{Name: "by_label", Key: []server.DynamicIndexKey{{Path: "labels"}}},
		},
	}

	// use registers spec on s and makes it the server the spec talks to.
	// Messages are built from the registered descriptor, which in-process
	// writes require.
	use := func(s *server.Server, spec server.DynamicEntity) {
		GinkgoHelper()
		Expect(s.RegisterDynamicEntity(spec)).To(Succeed())
		meta, ok := s.Entity(spec.Name)
		Expect(ok).To(BeTrue())
		srv, mt = s, dynamicpb.NewMessageType(meta.Descriptor)
	}
	start := func(spec server.DynamicEntity, opts ...server.Option) {
		GinkgoHelper()
		use(newTestServer(opts...), spec)
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	push := func(m *syncplanev1.Mutation) *syncplanev1.MutationResult {
		GinkgoHelper()
		resp, err := srv.Push(ctx, connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{m})))
		Expect(err).NotTo(HaveOccurred())
		results := pushResults(resp.Msg)
		Expect(results).To(HaveLen(1))
		return results[0]
	}

	body := func(key string) (string, bool) {
		GinkgoHelper()
		var got string
		var found bool
		Expect(srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			m := mt.New().Interface()
			var err error
			found, _, err = tx.Get(m, key)
			got = m.ProtoReflect().Get(mt.Descriptor().Fields().ByName("body")).String()
			return err
		})).To(Succeed())
		return got, found
	}

	queryKeys := func(value string) []string {
		GinkgoHelper()
		resp, err := srv.Query(ctx, connect.NewRequest(&syncplanev1.QueryRequest{
			WorkspaceId: ws,
			Entity:      "note",
			Predicate: &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
				Path:  []string{"body"},
				Op:    syncplanev1.CmpOp_CMP_OP_EQ,
				Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: value}},
			}}},
		}))
		Expect(err).NotTo(HaveOccurred())
		keys := []string{}
		for _, r := range resp.Msg.GetRows() {
			keys = append(keys, r.GetKey())
		}
		return keys
	}

	long := strings.Repeat("a", storage.MaxIndexEntryBytes)
	fits := strings.Repeat("a", storage.MaxIndexEntryBytes/2)

	expectRejected := func(r *syncplanev1.MutationResult) {
		GinkgoHelper()
		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		Expect(r.GetErrorCode()).To(Equal("invalid_argument"))
		Expect(r.GetSeq()).To(BeZero(), "a rejection must not consume a sequence")
	}

	It("rejects a PUT whose indexed value makes an entry longer than the limit", func() {
		start(indexed)

		r := push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "body": long})))

		expectRejected(r)
		Expect(r.GetErrorMessage()).To(ContainSubstring("by_body"))
		_, found := body("n1")
		Expect(found).To(BeFalse())
	})

	It("rejects a PATCH that makes an indexed value too long and keeps the stored value", func() {
		start(indexed)
		Expect(push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "body": "short"}))).
			GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		expectRejected(push(patchMutation("c1", 2, "note", "n1", buildNote(mt, map[string]any{"body": long}), "body")))

		got, _ := body("n1")
		Expect(got).To(Equal("short"))
	})

	It("accepts an indexed value that leaves every entry within the limit", func() {
		start(indexed)

		r := push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "body": fits})))

		Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		Expect(queryKeys(fits)).To(Equal([]string{"n1"}))
	})

	It("rejects a trusted write past the limit with the same code", func() {
		start(indexed)

		err := srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(buildNote(mt, map[string]any{"id": "n1", "body": long}))
		})

		rej, ok := server.AsRejection(err)
		Expect(ok).To(BeTrue(), "got %v", err)
		Expect(rej.Code).To(Equal("invalid_argument"))
	})

	It("rejects a row with more index entries than MaxIndexEntriesPerRow", func() {
		start(indexed, server.WithLimits(server.Limits{MaxIndexEntriesPerRow: 3}))

		row := buildNote(mt, map[string]any{"id": "n1"})
		labels := row.ProtoReflect().Mutable(mt.Descriptor().Fields().ByName("labels")).List()
		for _, l := range []string{"a", "b", "c", "d"} {
			labels.Append(protoreflect.ValueOfString(l))
		}

		expectRejected(push(putMutation("c1", 1, "note", "n1", row)))
	})

	It("leaves an index over longer stored values unready, and queries still find the rows", func() {
		// A deployment that declared no index wrote the row, so nothing
		// checked its entries. The next deployment declares the index.
		first, store := newTestServerStore()
		use(first, server.DynamicEntity{Name: "note", KeyFields: []string{"id"}, Fields: noteFields})
		Expect(push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "body": long}))).
			GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		use(mustNewServer(store), indexed)

		Expect(srv.BuildIndexes(ctx, ws)).To(MatchError(ContainSubstring("by_body")))

		Expect(queryKeys(long)).To(Equal([]string{"n1"}), "the unready index must leave the read to the scan")
	})

	It("still builds the indexes of other entities when one entity cannot build", func() {
		first, store := newTestServerStore()
		use(first, server.DynamicEntity{Name: "note", KeyFields: []string{"id"}, Fields: noteFields})
		Expect(push(putMutation("c1", 1, "note", "n1", buildNote(mt, map[string]any{"id": "n1", "body": long}))).
			GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))

		// Map order decides which entity a pass visits first, so several
		// siblings make it likely that some come after the one that fails.
		second := mustNewServer(store)
		siblings := []string{"tag0", "tag1", "tag2", "tag3", "tag4", "tag5"}
		for i, name := range siblings {
			use(second, server.DynamicEntity{
				Name:      name,
				KeyFields: []string{"id"},
				Fields: []server.DynamicField{
					{Name: "id", Number: 1, Kind: protoreflect.StringKind},
					{Name: "body", Number: 2, Kind: protoreflect.StringKind},
				},
				Indexes: []server.DynamicIndex{{Name: "by_body", Key: []server.DynamicIndexKey{{Path: "body"}}}},
			})
			Expect(push(putMutation("c1", uint64(i+2), name, "t1", buildNote(mt, map[string]any{"id": "t1", "body": "red"}))).
				GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
		}
		Expect(second.RegisterDynamicEntity(indexed)).To(Succeed())

		Expect(second.BuildIndexes(ctx, ws)).To(MatchError(ContainSubstring(`entity "note"`)))

		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			for _, name := range siblings {
				_, ready, err := tx.IndexState(name, "by_body")
				Expect(err).NotTo(HaveOccurred())
				Expect(ready).To(BeTrue(), "%s", name)
			}
			return nil
		})).To(Succeed())
	})
})
