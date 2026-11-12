package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
)

// The search postings are kept by difference: each write removes and adds
// what changed between the stored row and the new one. These specs check,
// after every kind of write, that the posting index holds exactly the
// postings of the live rows — none missing, which would hide a row from a
// search, and none stale.
var _ = Describe("Search postings", func() {
	const (
		ws          = "ws-postings"
		searchIndex = "__search"
	)
	var (
		ctx    context.Context
		srv    *server.Server
		store  storage.Adapter
		client syncplanev1connect.SyncServiceClient
		mutID  uint64
	)

	start := func(st storage.Adapter, opts ...server.Option) {
		store = st
		srv = mustNewServer(store, append([]server.Option{
			server.WithEntities(&testdatav1.Article{}),
			server.WithIndexBuild(0, 0),
		}, opts...)...)
		DeferCleanup(func() { _ = srv.Close() })
		path, handler := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		DeferCleanup(ts.Close)
		client = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
		mutID = 0
	}

	BeforeEach(func() {
		ctx = context.Background()
		start(newTestStore())
	})

	push := func(muts ...func(id uint64) *syncplanev1.Mutation) {
		GinkgoHelper()
		batch := make([]*syncplanev1.Mutation, len(muts))
		for i, m := range muts {
			mutID++
			batch[i] = m(mutID)
		}
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, batch)))
		Expect(err).NotTo(HaveOccurred())
		for _, r := range pushResults(resp.Msg) {
			Expect(r.GetStatus()).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED), r.GetErrorMessage())
		}
	}
	put := func(a *testdatav1.Article) func(uint64) *syncplanev1.Mutation {
		return func(id uint64) *syncplanev1.Mutation { return putMutation("c1", id, "article", a.GetId(), a) }
	}
	patch := func(a *testdatav1.Article, paths ...string) func(uint64) *syncplanev1.Mutation {
		return func(id uint64) *syncplanev1.Mutation {
			return patchMutation("c1", id, "article", a.GetId(), a, paths...)
		}
	}
	del := func(key string) func(uint64) *syncplanev1.Mutation {
		return func(id uint64) *syncplanev1.Mutation { return deleteMutation("c1", id, "article", key) }
	}
	trusted := func(fn func(tx *server.EntityTx) error) {
		GinkgoHelper()
		Expect(srv.Mutate(server.Trusted(ctx), ws, fn)).To(Succeed())
	}
	exact := func(key []byte) storage.IndexRange {
		return storage.IndexRange{Lo: key, Hi: append(append([]byte{}, key...), 0)}
	}
	// expectPostings checks that every live row of entity that decodes as
	// prototype holds a posting under each key its value makes, and, with
	// exactCount, that the index holds nothing else. A key over
	// storage.MaxIndexEntryBytes is never stored, so it is not expected.
	expectPostings := func(entity string, prototype proto.Message, exactCount bool) {
		GinkgoHelper()
		decl, err := query.CompileSearchDecl(prototype.ProtoReflect().Descriptor())
		Expect(err).NotTo(HaveOccurred())
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			want := map[string][]string{}
			total := 0
			Expect(tx.ScanEntity(entity, func(row storage.Row) error {
				if row.Deleted {
					return nil
				}
				m := prototype.ProtoReflect().New().Interface()
				if proto.Unmarshal(row.Value, m) != nil {
					return nil
				}
				for _, k := range decl.SearchEntries(m.ProtoReflect()) {
					if len(k) > storage.MaxIndexEntryBytes {
						continue
					}
					want[string(k)] = append(want[string(k)], row.Key)
					total++
				}
				return nil
			})).To(Succeed())
			for k, rows := range want {
				var got []string
				Expect(tx.ScanPostings(entity, searchIndex, []storage.IndexRange{exact([]byte(k))}, nil, 0,
					func(rowKey string) error {
						got = append(got, rowKey)
						return nil
					})).To(Succeed())
				sort.Strings(got)
				sort.Strings(rows)
				Expect(got).To(Equal(rows), "rows under posting key %q", k)
			}
			if exactCount {
				counts, _, err := tx.PostingCounts(entity, searchIndex, []storage.IndexRange{{}}, 0)
				Expect(err).NotTo(HaveOccurred())
				Expect(counts[0]).To(Equal(total), "the index holds no stale posting")
			}
			return nil
		})).To(Succeed())
	}
	// expectExactPostings is expectPostings for articles, with no stale
	// posting allowed.
	expectExactPostings := func() {
		GinkgoHelper()
		expectPostings("article", &testdatav1.Article{}, true)
	}
	// searchEntity runs a match query over prototype's entity once over the
	// index and once as a scan, and checks that both return the same row ids.
	// entities is what a server needs registered to serve prototype.
	searchEntity := func(prototype proto.Message, entities []proto.Message, q string) []string {
		GinkgoHelper()
		idField := prototype.ProtoReflect().Descriptor().Fields().ByName("id")
		run := func(s *server.Server) []string {
			keys := []string{}
			Expect(s.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
				return tx.Query(prototype, query.Spec{Where: query.Match(q)}, func(m proto.Message, _ uint64) error {
					keys = append(keys, m.ProtoReflect().Get(idField).String())
					return nil
				})
			})).To(Succeed())
			sort.Strings(keys)
			return keys
		}
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		scanner := mustNewServer(store,
			server.WithEntities(entities...),
			server.WithIndexBuild(0, 0),
			server.WithLimits(server.Limits{DisableIndexes: true}))
		defer scanner.Close()
		indexed := run(srv)
		Expect(indexed).To(Equal(run(scanner)), "the index changes no search result")
		return indexed
	}
	search := func(q string) []string {
		GinkgoHelper()
		return searchEntity(&testdatav1.Article{}, []proto.Message{&testdatav1.Article{}}, q)
	}

	It("follows wire PUT, PATCH and DELETE", func() {
		push(
			put(&testdatav1.Article{Id: "a1", Title: "red apple", Summary: "fresh fruit"}),
			put(&testdatav1.Article{Id: "a2", Title: "green apple", Meta: &testdatav1.ArticleMeta{Author: "ann"}}),
		)
		expectExactPostings()
		push(put(&testdatav1.Article{Id: "a1", Title: "red pear", Summary: "fresh fruit"}))
		expectExactPostings()
		Expect(search("apple")).To(Equal([]string{"a2"}))
		push(patch(&testdatav1.Article{Id: "a2", Title: "yellow pear"}, "title"))
		expectExactPostings()
		push(patch(&testdatav1.Article{Id: "a2", State: "draft"}, "state"))
		expectExactPostings()
		push(patch(&testdatav1.Article{Id: "a2", Meta: &testdatav1.ArticleMeta{Author: "bob"}}, "meta.author"))
		expectExactPostings()
		Expect(search("pear")).To(Equal([]string{"a1", "a2"}))
		push(del("a1"))
		expectExactPostings()
		Expect(search("pear")).To(Equal([]string{"a2"}))
		Expect(search("fresh")).To(BeEmpty())
		push(put(&testdatav1.Article{Id: "a1", Title: "red pear again"}))
		expectExactPostings()
		Expect(search("pear")).To(Equal([]string{"a1", "a2"}))
	})

	It("follows a row that grows past the term cap and back", func() {
		push(put(&testdatav1.Article{Id: "a1", Title: "small", Summary: "needle"}))
		expectExactPostings()
		var big string
		for i := 0; i < query.SearchTermsPerRow+10; i++ {
			big += fmt.Sprintf("w%04d ", i)
		}
		push(patch(&testdatav1.Article{Id: "a1", Summary: big + "needle"}, "summary"))
		expectExactPostings()
		Expect(search("needle")).To(Equal([]string{"a1"}))
		push(patch(&testdatav1.Article{Id: "a1", Summary: "haystack"}, "summary"))
		expectExactPostings()
		Expect(search("needle")).To(BeEmpty())
	})

	It("follows trusted Put, Patch and Delete", func() {
		trusted(func(tx *server.EntityTx) error {
			if err := tx.Put(&testdatav1.Article{Id: "t1", Title: "alpha beta"}); err != nil {
				return err
			}
			return tx.Put(&testdatav1.Article{Id: "t2", Title: "beta gamma"})
		})
		expectExactPostings()
		trusted(func(tx *server.EntityTx) error {
			if err := tx.Put(&testdatav1.Article{Id: "t1", Title: "alpha delta"}); err != nil {
				return err
			}
			return tx.Patch(&testdatav1.Article{Id: "t2", Summary: "epsilon"}, []string{"summary"}, nil)
		})
		expectExactPostings()
		Expect(search("beta")).To(Equal([]string{"t2"}))
		trusted(func(tx *server.EntityTx) error {
			// Two writes of one row in one transaction: the second diffs
			// against the value the first stored.
			if err := tx.Patch(&testdatav1.Article{Id: "t2", Title: "zeta"}, []string{"title"}, nil); err != nil {
				return err
			}
			if err := tx.Patch(&testdatav1.Article{Id: "t2", Title: "eta"}, []string{"title"}, nil); err != nil {
				return err
			}
			return tx.Delete(&testdatav1.Article{}, "t1")
		})
		expectExactPostings()
		Expect(search("eta")).To(Equal([]string{"t2"}))
		Expect(search("alpha")).To(BeEmpty())
	})

	It("follows Replicate", func() {
		value := func(a *testdatav1.Article) []byte { return mustMarshal(a) }
		Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			if err := tx.Put("article", "r1", value(&testdatav1.Article{Id: "r1", Title: "copied row"})); err != nil {
				return err
			}
			return tx.Put("article", "r2", value(&testdatav1.Article{Id: "r2", Title: "other row"}))
		})).To(Succeed())
		expectExactPostings()
		Expect(srv.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			if err := tx.Put("article", "r1", value(&testdatav1.Article{Id: "r1", Title: "moved text"})); err != nil {
				return err
			}
			return tx.Delete("article", "r2")
		})).To(Succeed())
		expectExactPostings()
		Expect(search("row")).To(BeEmpty())
		Expect(search("moved")).To(Equal([]string{"r1"}))
	})

	It("follows text compaction", func() {
		start(newTestStore(),
			server.WithGC(time.Millisecond, 0),
			server.WithTextDeltaRetention(time.Millisecond),
			server.WithTextCompaction(1))
		full := bodyDoc("seed", "hello world wide")
		push(put(&testdatav1.Article{Id: "c1", Body: full}))
		trimmed := editBody(full, "seed", func(d *text.Doc) {
			Expect(d.Delete(11, 5)).To(Succeed())
		})
		push(patch(&testdatav1.Article{Id: "c1", Body: trimmed}, "body"))
		expectExactPostings()
		time.Sleep(5 * time.Millisecond)
		Expect(srv.CollectGarbage(ctx, ws)).To(Succeed())
		n, err := srv.CompactText(ctx, ws)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(BeNumerically(">", 0), "the row is compacted")
		expectExactPostings()
		Expect(search("world")).To(Equal([]string{"c1"}))
		Expect(search("wide")).To(BeEmpty())
	})

	It("clears and rebuilds the postings when the spec changes", func() {
		push(put(&testdatav1.Article{Id: "a1", Title: "kept term"}))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		var target string
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			target, _, err = tx.IndexState("article", searchIndex)
			return err
		})).To(Succeed())
		// A posting no row value makes, written under another spec, and the
		// state that spec leaves.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			if err := tx.PutPostings("article", searchIndex, "a1", nil, [][]byte{[]byte("\x01ghost\x00\x00")}); err != nil {
				return err
			}
			if err := tx.SetIndexState("article", searchIndex, "another-spec", true); err != nil {
				return err
			}
			return tx.SetIndexBuildMark("")
		})).To(Succeed())
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		expectExactPostings()
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			hash, ready, err := tx.IndexState("article", searchIndex)
			Expect(err).NotTo(HaveOccurred())
			Expect(hash).To(Equal(target))
			Expect(ready).To(BeTrue())
			return nil
		})).To(Succeed())
	})

	It("keeps the postings when a build resumes under the same spec", func() {
		push(put(&testdatav1.Article{Id: "a1", Title: "kept term"}))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		// The state a build that stopped part way leaves: the target spec,
		// not ready.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			hash, _, err := tx.IndexState("article", searchIndex)
			if err != nil {
				return err
			}
			if err := tx.SetIndexState("article", searchIndex, hash, false); err != nil {
				return err
			}
			return tx.SetIndexBuildMark("")
		})).To(Succeed())
		var clears int
		counting := &clearCountingStore{Adapter: store, clears: &clears}
		resumed := mustNewServer(counting, server.WithEntities(&testdatav1.Article{}), server.WithIndexBuild(0, 0))
		defer resumed.Close()
		Expect(resumed.BuildIndexes(ctx, ws)).To(Succeed())
		Expect(clears).To(BeZero())
		expectExactPostings()
	})

	It("rebuilds the postings of rows that a schema without search wrote", func() {
		push(put(&testdatav1.Article{Id: "a1", Title: "first words"}))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		// A node whose schema declares no search writes through the same
		// store, so its writes keep no posting.
		plain := mustNewServer(store, server.WithEntities(&testdatav1.Widget{}), server.WithIndexBuild(0, 0))
		defer plain.Close()
		Expect(plain.RegisterDynamicEntity(server.DynamicEntity{
			Name:      "article",
			KeyFields: []string{"id"},
			Fields: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			},
		})).To(Succeed())
		Expect(plain.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			if err := tx.Put("article", "a1", mustMarshal(&testdatav1.Article{Id: "a1", Title: "second words"})); err != nil {
				return err
			}
			return tx.Put("article", "a2", mustMarshal(&testdatav1.Article{Id: "a2", Title: "third words"}))
		})).To(Succeed())
		Expect(search("second")).To(Equal([]string{"a1"}))
		Expect(search("third")).To(Equal([]string{"a2"}))
		Expect(search("first")).To(BeEmpty())
		expectExactPostings()
	})

	It("keeps every posting when rows are written while a build walks", func() {
		var hook func()
		walking := &buildHookStore{Adapter: newTestStore(), after: func() {
			if hook != nil {
				h := hook
				hook = nil
				h()
			}
		}}
		start(walking, server.WithIndexBuild(0, 3))
		// Rows that predate the index: written straight to the store.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			for i := 0; i < 12; i++ {
				a := &testdatav1.Article{Id: fmt.Sprintf("a%02d", i), Title: fmt.Sprintf("shared word%02d", i)}
				if err := tx.PutRow("article", a.GetId(), mustMarshal(a), uint64(i+1), storage.Actor{}); err != nil {
					return err
				}
			}
			// A posting left from an earlier spec, which the build clears.
			return tx.PutPostings("article", searchIndex, "a00", nil, [][]byte{[]byte("\x01ghost\x00\x00")})
		})).To(Succeed())
		hook = func() {
			// After the first batch: a01 is behind the walk, a07 ahead of it.
			trusted(func(tx *server.EntityTx) error {
				if err := tx.Put(&testdatav1.Article{Id: "a01", Title: "shared changed"}); err != nil {
					return err
				}
				if err := tx.Put(&testdatav1.Article{Id: "a07", Title: "shared word07 more"}); err != nil {
					return err
				}
				if err := tx.Delete(&testdatav1.Article{}, "a09"); err != nil {
					return err
				}
				return tx.Put(&testdatav1.Article{Id: "a00b", Title: "shared fresh"})
			})
		}
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		Expect(hook).To(BeNil(), "the writes ran during the build")
		expectExactPostings()
		Expect(search("shared")).To(HaveLen(12))
		Expect(search("word07")).To(Equal([]string{"a07"}))
		Expect(search("changed")).To(Equal([]string{"a01"}))
	})

	// pushResult pushes one batch and returns the status of each mutation.
	pushResult := func(muts ...func(id uint64) *syncplanev1.Mutation) []syncplanev1.MutationStatus {
		GinkgoHelper()
		batch := make([]*syncplanev1.Mutation, len(muts))
		for i, m := range muts {
			mutID++
			batch[i] = m(mutID)
		}
		resp, err := client.Push(ctx, connect.NewRequest(pushOf(ws, batch)))
		Expect(err).NotTo(HaveOccurred())
		var out []syncplanev1.MutationStatus
		for _, r := range pushResults(resp.Msg) {
			out = append(out, r.GetStatus())
		}
		return out
	}
	custom := func(name string) func(uint64) *syncplanev1.Mutation {
		return func(id uint64) *syncplanev1.Mutation {
			args, err := anypb.New(&emptypb.Empty{})
			Expect(err).NotTo(HaveOccurred())
			return &syncplanev1.Mutation{ClientId: "c1", MutationId: id, Payload: &syncplanev1.Mutation_Custom{
				Custom: &syncplanev1.CustomMutation{Name: name, Args: args},
			}}
		}
	}
	const applied, rejected = syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED, syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED
	// plant writes value under key straight to the store, past every write
	// path, so the row holds no posting the engine made for it.
	plant := func(key string, value []byte) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			return tx.PutRow("article", key, value, seq, storage.Actor{})
		})).To(Succeed())
	}
	// poison is a value that does not decode: a field 1 whose length runs
	// past the end.
	poison := []byte{0x0a, 0xff}

	It("follows one push batch that writes one row five times", func() {
		push(
			put(&testdatav1.Article{Id: "a1", Title: "one two"}),
			patch(&testdatav1.Article{Id: "a1", Title: "three"}, "title"),
			del("a1"),
			put(&testdatav1.Article{Id: "a1", Title: "four five"}),
			patch(&testdatav1.Article{Id: "a1", Summary: "six"}, "summary"),
			put(&testdatav1.Article{Id: "a2", Title: "two four"}),
		)
		expectExactPostings()
		Expect(search("four")).To(Equal([]string{"a1", "a2"}))
		Expect(search("six")).To(Equal([]string{"a1"}))
		Expect(search("three")).To(BeEmpty())
		Expect(search("one")).To(BeEmpty())
	})

	It("restores the postings of a mutator whose savepoint rolls back", func() {
		start(newTestStore(), server.WithMutator("put_then_reject", &emptypb.Empty{},
			func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				if err := tx.Put(&testdatav1.Article{Id: "a1", Title: "rolled back"}); err != nil {
					return err
				}
				return server.Reject(server.CodeConflict, "refused after the put")
			}))
		push(put(&testdatav1.Article{Id: "a1", Title: "kept words"}))
		Expect(pushResult(
			custom("put_then_reject"),
			patch(&testdatav1.Article{Id: "a1", Summary: "patched"}, "summary"),
		)).To(Equal([]syncplanev1.MutationStatus{rejected, applied}))
		expectExactPostings()
		Expect(search("rolled")).To(BeEmpty())
		Expect(search("kept patched")).To(Equal([]string{"a1"}))
	})

	It("follows a mutator and a trusted transaction that write one row several times", func() {
		start(newTestStore(), server.WithMutator("churn", &emptypb.Empty{},
			func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				if err := tx.Delete(&testdatav1.Article{}, "a1"); err != nil {
					return err
				}
				if err := tx.Put(&testdatav1.Article{Id: "a1", Title: "middle value"}); err != nil {
					return err
				}
				return tx.Put(&testdatav1.Article{Id: "a1", Title: "final value"})
			}))
		push(
			put(&testdatav1.Article{Id: "a1", Title: "first value"}),
			put(&testdatav1.Article{Id: "a2", Title: "other row"}),
		)
		push(custom("churn"))
		expectExactPostings()
		Expect(search("final")).To(Equal([]string{"a1"}))
		Expect(search("middle")).To(BeEmpty())
		Expect(search("first")).To(BeEmpty())
		trusted(func(tx *server.EntityTx) error {
			if err := tx.Patch(&testdatav1.Article{Id: "a2", Title: "patched row"}, []string{"title"}, nil); err != nil {
				return err
			}
			if err := tx.Delete(&testdatav1.Article{}, "a2"); err != nil {
				return err
			}
			return tx.Put(&testdatav1.Article{Id: "a2", Summary: "recreated"})
		})
		expectExactPostings()
		Expect(search("row")).To(BeEmpty())
		Expect(search("recreated")).To(Equal([]string{"a2"}))
	})

	It("does not count search postings against the per-row entry cap", func() {
		// Issue has a fan-out index over labels next to its search, and a
		// row here has a few value entries and many postings.
		issueEntities := []proto.Message{&benchv1.Project{}, &benchv1.User{}, &benchv1.Issue{}}
		start(newTestStore(),
			server.WithEntities(issueEntities...),
			server.WithLimits(server.Limits{MaxIndexEntriesPerRow: 12}))
		var body string
		for i := 0; i < 60; i++ {
			body += fmt.Sprintf("word%02d ", i)
		}
		issue := func(labels ...string) *benchv1.Issue {
			return &benchv1.Issue{Id: "i1", Title: "cap check", Body: body, State: "open", Labels: labels}
		}
		write := func(fn func(tx *server.EntityTx) error) error {
			return srv.Mutate(server.Trusted(ctx), ws, fn)
		}
		Expect(write(func(tx *server.EntityTx) error { return tx.Put(issue("a", "b")) })).To(Succeed())
		Expect(write(func(tx *server.EntityTx) error {
			return tx.Patch(issue("a", "b", "c", "d"), []string{"labels"}, nil)
		})).To(Succeed(), "a PATCH of the fan-out field alone")
		Expect(write(func(tx *server.EntityTx) error { return tx.Put(issue("a", "b", "c", "d")) })).To(Succeed(),
			"a PUT of the same row counts the same entries")
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			states, err := tx.IndexStates("issue")
			Expect(err).NotTo(HaveOccurred())
			Expect(states).NotTo(BeEmpty())
			for _, st := range states {
				Expect(st.Ready).To(BeTrue(), "index %s", st.Index)
			}
			return nil
		})).To(Succeed())
		expectPostings("issue", &benchv1.Issue{}, true)
		Expect(searchEntity(&benchv1.Issue{}, issueEntities, "word07")).To(Equal([]string{"i1"}))
		err := write(func(tx *server.EntityTx) error { return tx.Put(issue("a", "b", "c", "d", "e", "f", "g")) })
		_, isRejection := server.AsRejection(err)
		Expect(isRejection).To(BeTrue(), "the cap still holds for value entries: %v", err)
	})

	It("follows PUT and DELETE over a stored row that does not decode", func() {
		plant("p1", poison)
		push(put(&testdatav1.Article{Id: "p1", Title: "repaired words"}))
		expectExactPostings()
		Expect(search("repaired")).To(Equal([]string{"p1"}))

		// Rows that held postings and then became poison: the write cannot
		// know the postings, so the old ones stay, and a search re-check
		// drops them.
		push(
			put(&testdatav1.Article{Id: "a1", Title: "alpha"}),
			put(&testdatav1.Article{Id: "a2", Title: "gamma"}),
		)
		plant("a1", poison)
		plant("a2", poison)
		push(put(&testdatav1.Article{Id: "a1", Title: "beta"}), del("a2"))
		expectPostings("article", &testdatav1.Article{}, false)
		Expect(search("alpha")).To(BeEmpty())
		Expect(search("gamma")).To(BeEmpty())
		Expect(search("beta")).To(Equal([]string{"a1"}))
	})

	It("rebuilds after a schema with a different search declaration wrote", func() {
		push(put(&testdatav1.Article{Id: "a1", Title: "title words", Summary: "summary words"}))
		Expect(srv.BuildIndexes(ctx, ws)).To(Succeed())
		var target string
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			var err error
			target, _, err = tx.IndexState("article", searchIndex)
			return err
		})).To(Succeed())

		foreign := mustNewServer(store, server.WithIndexBuild(0, 0))
		defer foreign.Close()
		_, err := foreign.RegisterDescriptorSet(articleSearchingSummaryOnly())
		Expect(err).NotTo(HaveOccurred())
		Expect(foreign.Replicate(ctx, ws, func(tx *server.ReplicaTx) error {
			if err := tx.Put("article", "a1", mustMarshal(&testdatav1.Article{Id: "a1", Title: "changed heading", Summary: "summary words"})); err != nil {
				return err
			}
			return tx.Put("article", "a2", mustMarshal(&testdatav1.Article{Id: "a2", Title: "second heading", Summary: "more text"}))
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			hash, ready, err := tx.IndexState("article", searchIndex)
			Expect(err).NotTo(HaveOccurred())
			Expect(ready && hash == target).To(BeFalse(), "the index is not ready under this schema")
			return nil
		})).To(Succeed())
		Expect(search("heading")).To(Equal([]string{"a1", "a2"}))
		Expect(search("title")).To(BeEmpty())
		expectExactPostings()
	})

	It("stores a word longer than any posting key may be under a key within the limit", func() {
		// The analyzer cuts a term at 64 code points, so no posting key reaches
		// storage.MaxIndexEntryBytes, whatever the text holds.
		long := strings.Repeat("\U0001F600", storage.MaxIndexEntryBytes)
		push(put(&testdatav1.Article{Id: "a1", Title: long + " " + strings.Repeat("x", 4*storage.MaxIndexEntryBytes)}))
		expectExactPostings()
		decl, err := query.CompileSearchDecl((&testdatav1.Article{}).ProtoReflect().Descriptor())
		Expect(err).NotTo(HaveOccurred())
		for _, k := range decl.SearchEntries((&testdatav1.Article{Title: long}).ProtoReflect()) {
			Expect(len(k)).To(BeNumerically("<=", storage.MaxIndexEntryBytes))
		}
		Expect(search(strings.Repeat("x", 4*storage.MaxIndexEntryBytes) + " ")).To(Equal([]string{"a1"}))
	})

	It("repairs a corrupt posting record with a rebuild", func() {
		path := filepath.Join(GinkgoT().TempDir(), "corrupt.db")
		st, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(st.Close()).To(Succeed()) })
		start(st)
		// More rows than a head holds, so the index has chunks, and rows
		// without the term, so the search takes the index and not the scan.
		trusted(func(tx *server.EntityTx) error {
			for i := 0; i < 600; i++ {
				if err := tx.Put(&testdatav1.Article{Id: fmt.Sprintf("a%03d", i), Title: fmt.Sprintf("shared word%03d", i)}); err != nil {
					return err
				}
			}
			for i := 0; i < 300; i++ {
				if err := tx.Put(&testdatav1.Article{Id: fmt.Sprintf("b%03d", i), Title: "apart"}); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(search("shared")).To(HaveLen(600))

		db, err := sql.Open("sqlite", path)
		Expect(err).NotTo(HaveOccurred())
		defer db.Close()
		_, err = db.Exec(`UPDATE posting_heads SET dir = x'01' WHERE dir IS NOT NULL`)
		Expect(err).NotTo(HaveOccurred())
		err = srv.View(server.Trusted(ctx), ws, func(tx *server.ViewTx) error {
			return tx.Query(&testdatav1.Article{}, query.Spec{Where: query.Match("shared ")}, func(proto.Message, uint64) error { return nil })
		})
		Expect(err).To(HaveOccurred(), "a read of the corrupt record fails")

		// What a spec change leaves, which makes the next build clear.
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			if err := tx.SetIndexState("article", searchIndex, "another-spec", false); err != nil {
				return err
			}
			return tx.SetIndexBuildMark("")
		})).To(Succeed())
		Expect(search("shared")).To(HaveLen(600))
		expectExactPostings()
	})

	It("follows text writes from plain strings on a searched text field", func() {
		trusted(func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Article{Id: "t1", Title: "doc"},
				server.WithTextString("body", "hello wide world"), server.WithTextPeer("p1"))
		})
		expectExactPostings()
		Expect(search("wide")).To(Equal([]string{"t1"}))
		trusted(func(tx *server.EntityTx) error {
			return tx.Patch(&testdatav1.Article{Id: "t1"}, nil, nil,
				server.WithTextString("body", "hello brave world"), server.WithTextPeer("p1"))
		})
		expectExactPostings()
		Expect(search("brave")).To(Equal([]string{"t1"}))
		Expect(search("wide")).To(BeEmpty())
	})
})

// articleSearchingSummaryOnly is a descriptor set that declares the article
// entity with a different search declaration: summary alone, where the
// compiled Article also searches title, body and meta.author.
func articleSearchingSummaryOnly() []byte {
	art := protodesc.ToDescriptorProto((&testdatav1.Article{}).ProtoReflect().Descriptor())
	fields := art.Field[:0]
	for _, f := range art.Field {
		if f.GetName() == "meta" {
			continue
		}
		if f.GetName() == "title" {
			f.Options = nil
		}
		fields = append(fields, f)
	}
	art.Field = fields
	eo := proto.Clone(proto.GetExtension(art.GetOptions(), syncplanev1.E_Entity).(*syncplanev1.EntityOptions)).(*syncplanev1.EntityOptions)
	eo.Search = &syncplanev1.SearchSpec{Field: []*syncplanev1.SearchField{{Path: "summary"}}}
	opts := &descriptorpb.MessageOptions{}
	proto.SetExtension(opts, syncplanev1.E_Entity, eo)
	art.Options = opts
	file := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("foreign/v1/article.proto"),
		Package:     proto.String("foreign.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"syncplane/v1/options.proto"},
		MessageType: []*descriptorpb.DescriptorProto{art},
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
		protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_query_proto),
		protodesc.ToFileDescriptorProto(syncplanev1.File_syncplane_v1_options_proto),
		file,
	}}
	b, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}
	return b
}

// clearCountingStore counts the ClearPostings calls of its write
// transactions.
type clearCountingStore struct {
	storage.Adapter
	clears *int
}

func (s *clearCountingStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	return s.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		return fn(&clearCountingTx{WriteTx: tx, clears: s.clears})
	})
}

type clearCountingTx struct {
	storage.WriteTx
	clears *int
}

func (t *clearCountingTx) ClearPostings(entity, index string) error {
	*t.clears++
	return t.WriteTx.ClearPostings(entity, index)
}

// buildHookStore runs after once after the first write transaction that
// walks an entity (a build batch) commits.
type buildHookStore struct {
	storage.Adapter
	after func()
}

func (s *buildHookStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	walked := false
	err := s.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		walked = false
		return fn(&walkSpyTx{WriteTx: tx, walked: &walked})
	})
	if err == nil && walked {
		s.after()
	}
	return err
}

type walkSpyTx struct {
	storage.WriteTx
	walked *bool
}

func (t *walkSpyTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	*t.walked = true
	return t.WriteTx.ScanEntityAfter(entity, afterKey, limit, fn)
}
