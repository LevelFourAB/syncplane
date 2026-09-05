package server

import (
	"context"
	"math"
	"path/filepath"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// rowReadTx counts the point reads a walk makes, which is the whole difference
// between the two walks: the keys-only one must make none.
type rowReadTx struct {
	storage.ReadTx
	rows int
}

func (t *rowReadTx) GetRow(entity, key string) (storage.Row, error) {
	t.rows++
	return t.ReadTx.GetRow(entity, key)
}

func (t *rowReadTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	t.rows += len(keys)
	return t.ReadTx.GetRows(entity, keys, fn)
}

// The keys-only walk is the row-resolving walk with the reads taken out, and
// nothing else: same readiness check, same plan, same dedupe, same budget. What
// it may differ on is documented and pinned here — it reports keys whose row no
// longer exists, because deciding that needs the row.
var _ = Describe("candidateKeysByIndex", func() {
	const ws = "ws1"
	var (
		ctx   context.Context
		store storage.Adapter
		info  *entityInfo
	)

	issues := []*testdatav1.Issue{
		{Id: "i1", State: "open", Rank: 1, Labels: []string{"red", "blue"}},
		{Id: "i2", State: "open", Rank: 2, Labels: []string{"red"}},
		{Id: "i3", State: "closed", Rank: 3, Labels: []string{"blue", "green"}},
		{Id: "i4", State: "done", Rank: 4},
	}

	BeforeEach(func() {
		ctx = context.Background()
		base, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(base.Close()).To(Succeed()) })
		store = base

		srv, err := New(store, WithEntities(&testdatav1.Issue{}))
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i, issue := range issues {
				raw, err := proto.Marshal(issue)
				if err != nil {
					return err
				}
				if err := tx.PutRow("issue", issue.GetId(), raw, uint64(i+1), storage.Actor{}); err != nil {
					return err
				}
			}
			return nil
		})).To(Succeed())
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		info = srv.catalogPtr().entities["issue"]
		Expect(info).NotTo(BeNil())
	})

	compile := func(cond *syncplanev1.Condition) *query.Compiled {
		GinkgoHelper()
		pred, err := query.Compile((&testdatav1.Issue{}).ProtoReflect().Descriptor(), cond)
		Expect(err).NotTo(HaveOccurred())
		return pred
	}

	// byRows is the row-resolving walk reduced to the keys it considered.
	byRows := func(cond *syncplanev1.Condition, limits Limits) ([]string, bool) {
		GinkgoHelper()
		var keys []string
		var served bool
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			served, err = queryByIndex(tx, info, compile(cond), nil, page{}, limits,
				func(row storage.Row, _ *query.Compiled) error {
					keys = append(keys, row.Key)
					return nil
				},
				func() int { return math.MaxInt },
				func() { keys = nil })
			return err
		})).To(Succeed())
		sort.Strings(keys)
		return keys, served
	}

	// byKeys is the keys-only walk, and fails the spec if it read a row.
	byKeys := func(cond *syncplanev1.Condition, limits Limits) ([]string, bool) {
		GinkgoHelper()
		var keys []string
		var served bool
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			counting := &rowReadTx{ReadTx: tx}
			var err error
			served, err = candidateKeysByIndex(counting, info, compile(cond), limits,
				func(rowKey string) error {
					keys = append(keys, rowKey)
					return nil
				})
			Expect(counting.rows).To(BeZero(), "a keys-only walk must resolve no row")
			return err
		})).To(Succeed())
		sort.Strings(keys)
		return keys, served
	}

	DescribeTable("names exactly the rows the row-resolving walk considers",
		func(cond *syncplanev1.Condition, want []string) {
			rowKeys, rowServed := byRows(cond, Limits{})
			keyKeys, keyServed := byKeys(cond, Limits{})
			Expect(rowServed).To(BeTrue(), "the fixture predicate must be index-servable")
			Expect(keyServed).To(Equal(rowServed))
			Expect(rowKeys).To(Equal(want))
			Expect(keyKeys).To(Equal(want))
		},
		Entry("an equality on an indexed scalar", query.Eq("state", "open"), []string{"i1", "i2"}),
		Entry("an in fan-out over several ranges",
			query.In("state", "open", "closed"), []string{"i1", "i2", "i3"}),
		Entry("membership of a repeated field", query.Has("labels", "red"), []string{"i1", "i2"}),
		// i1 carries both labels, so it takes an entry in each range: one key
		// out, or the caller would ship the row twice.
		Entry("a fan-out over a repeated field's entries",
			query.HasAny("labels", "red", "blue"), []string{"i1", "i2", "i3"}),
	)

	It("declines when the candidates outgrow the budget", func() {
		limits := Limits{MaxIndexScanCandidates: 1}
		_, rowServed := byRows(query.Eq("state", "open"), limits)
		_, keyServed := byKeys(query.Eq("state", "open"), limits)
		Expect(rowServed).To(BeFalse())
		Expect(keyServed).To(BeFalse(),
			"the caller drops what it collected and scans, which names those rows again")
	})

	It("names a key whose row is a tombstone", func() {
		// The engine clears an entry when it deletes a row; a row deleted
		// underneath the index leaves the entry behind, which is the state the
		// walk has to survive. The row-resolving walk drops it, and the caller
		// of the keys-only walk does the same once it reads the row.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.DeleteRow("issue", "i2", 100, storage.Actor{})
		})).To(Succeed())

		rowKeys, _ := byRows(query.Eq("state", "open"), Limits{})
		keyKeys, _ := byKeys(query.Eq("state", "open"), Limits{})
		Expect(rowKeys).To(Equal([]string{"i1"}))
		Expect(keyKeys).To(Equal([]string{"i1", "i2"}),
			"the keys are a superset: whether a row still exists needs the row")
	})

	It("declines in a workspace whose indexes are not built", func() {
		// An unbuilt index covers part of the entity, so walking it would
		// silently drop rows — the readiness check both walks share.
		Expect(store.WithReadTx(ctx, "ws2", func(tx storage.ReadTx) error {
			served, err := candidateKeysByIndex(tx, info, compile(query.Eq("state", "open")), Limits{},
				func(string) error { return nil })
			Expect(err).NotTo(HaveOccurred())
			Expect(served).To(BeFalse())
			return nil
		})).To(Succeed())
	})
})
