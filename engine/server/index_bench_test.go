package server_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// What a secondary index buys on the path it was built for.
//
// A reverse follow hop resolves "every issue whose project is this one", once
// per holder row, on every pull with a follow spec. Without an index that is a
// full scan and decode of the issue entity per project; with one it is a range
// probe plus a point read per hit. The two sub-benchmarks drive the SAME Pull
// over the SAME rows, differing only in whether the index is usable, so the
// ratio between them is the whole result.
//
// Sizes are the number of issue rows. A FIXED number of them point at the
// followed project, so the work the index avoids grows with the entity while
// the work it does stays flat — which is the shape the win has, and the shape
// a real reverse hop has as an entity grows.
//
// The crossover to know about: the index path costs one point read per MATCH,
// the scan path one sequential read and decode per ROW. So an index loses when
// nearly every row of the entity matches — a hop over a small entity where one
// parent holds everything — and wins by more the more selective the hop is.
// MaxReverseFanOut bounds the matches, so the index path's cost is bounded
// while the scan path's grows with the entity; that asymmetry is why the index
// is the default and the scan is the fallback rather than the other way round.
//
// The shape to expect (Apple M2 Pro, 10 matching rows):
//
//	rows     scan          index
//	 1000    1.4 ms        385 µs     510 KB  ->  49 KB
//	10000   12.6 ms        363 µs     4.8 MB  ->  48 KB
//	50000   65.8 ms        364 µs      24 MB  ->  47 KB
//
// The absolute numbers will drift; the shape is the point. Scan grows linearly
// in both time and allocation, index is flat in both. A regression here looks
// like the index column starting to slope.
var indexBenchSizes = []int{1000, 10000, 50000}

const indexBenchMatches = 10

const benchWS = "bench-ws"

// benchServer builds a server over a fresh on-disk store holding n issues,
// indexBenchMatches of which point at project "p0".
func benchServer(b *testing.B, n int, built bool, opts ...server.Option) *server.Server {
	b.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })

	srv, err := server.New(store, append([]server.Option{
		server.WithEntities(&testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{}),
	}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = srv.Close() })

	if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
		// Only p0 exists as a row, so the base scope — and therefore the set
		// of reverse hops the pull performs — is exactly one project. The
		// other issues point at a key with no project row, which stores and
		// indexes identically and simply resolves to nothing.
		if err := tx.Put(&testdatav1.Project{Id: "p0", Name: "target"}); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			// The matching rows are spread across the key space rather than
			// clustered, so the scan cannot get lucky and stop early. `state`
			// marks the same rows, so one fixture serves both directions: the
			// reverse hop finds them by their ref, a seed predicate by their
			// state.
			target, state := "absent", "done"
			if i%(n/indexBenchMatches) == 0 {
				target, state = "p0", "open"
			}
			if err := tx.Put(&testdatav1.Issue{
				Id:      fmt.Sprintf("i%07d", i),
				Title:   "issue",
				State:   state,
				Project: &testdatav1.Project{Id: target},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if built {
		if err := srv.BuildIndexes(ctx, benchWS); err != nil {
			b.Fatal(err)
		}
	}
	return srv
}

// followPull is a pull whose subscription follows project → issues. A pull
// whose cursor is behind the workspace rebuilds the closure from scratch —
// there is no row history to diff against — so this is the work a real client
// makes the server do every time anything at all changes.
func followPull(cursor []byte) *connect.Request[syncplanev1.PullRequest] {
	return connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: benchWS,
		Cursor:      cursor,
		Subscriptions: []*syncplanev1.Subscription{{
			Entity: "project",
			Follow: &syncplanev1.FollowSpec{
				Paths: []*syncplanev1.RefPath{{Segments: []string{"issues"}}},
			},
		}},
	})
}

// BenchmarkFollowReverse measures one following pull that has to rebuild, index
// against scan.
//
// The cursor is caught up and then left one write behind, which is the state a
// pull does the measured work in. Two boundaries this sits between, both
// deliberate:
//
//   - A pull whose cursor is AT the head rebuilds nothing at all (pull.go), so
//     benchmarking a fully caught-up cursor would time the skip and measure the
//     reverse hop not at all.
//   - Benchmarking from a nil cursor instead would fold a full first delivery
//     into the number and bury the term being measured.
//
// So each iteration delivers one row it has already delivered — constant, tiny,
// identical between the two modes — and rebuilds the closure, which is the
// reverse hop.
func BenchmarkFollowReverse(b *testing.B) {
	for _, n := range indexBenchSizes {
		for _, mode := range []struct {
			name  string
			built bool
		}{{"scan", false}, {"index", true}} {
			b.Run(fmt.Sprintf("%s/rows=%d", mode.name, n), func(b *testing.B) {
				srv := benchServer(b, n, mode.built)
				ctx := context.Background()
				cursor := drainPull(b, srv)
				// One write past the cursor: it puts the workspace ahead so
				// every pull below rebuilds. It is a USER row — an entity this
				// subscription neither scopes nor follows — so it adds no seed
				// and no hop. A second project would add both: every project in
				// base scope is a holder, and each holder pays its own reverse
				// hop over the whole issue entity.
				if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.User{Id: "u-idle", Name: "idle"})
				}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := srv.Pull(ctx, followPull(cursor)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkFollowSeeds measures the OTHER end of a following pull: finding the
// rows to walk from. A following subscription anchored on a predicate seeds
// from the rows that match it, and the same index that serves that predicate as
// a query serves it here — so a follow anchored on a small slice of a large
// entity stops paying for the whole entity just to find its anchors.
//
// Shape: the seed cost is O(entity) scanned and O(matches) indexed, the same
// asymmetry the reverse hop has, and for the same reason. The forward hop each
// seed then makes is a key point-lookup on both paths, so what differs between
// the two columns is the seed pass alone.
//
// The shape to expect (Apple M2 Pro, 10 matching rows):
//
//	rows     scan          index
//	 1000    1.7 ms        545 µs     595 KB  ->  62 KB
//	10000   15.9 ms        529 µs     5.6 MB  ->  62 KB
//	50000   72.8 ms        555 µs      28 MB  ->  62 KB
//
// A subscription with no predicate is absent from this table on purpose: its
// seeds ARE the entity, so there is nothing for an index to narrow and the scan
// is the right answer rather than a fallback.
func BenchmarkFollowSeeds(b *testing.B) {
	openIssues := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Cmp{Cmp: &syncplanev1.CmpCond{
		Path:  []string{"state"},
		Op:    syncplanev1.CmpOp_CMP_OP_EQ,
		Value: &syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_StringValue{StringValue: "open"}},
	}}}
	seedPull := func(cursor []byte) *connect.Request[syncplanev1.PullRequest] {
		return connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: benchWS,
			Cursor:      cursor,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity:    "issue",
				Predicate: openIssues,
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			}},
		})
	}
	for _, n := range indexBenchSizes {
		for _, mode := range []struct {
			name  string
			built bool
		}{{"scan", false}, {"index", true}} {
			b.Run(fmt.Sprintf("%s/rows=%d", mode.name, n), func(b *testing.B) {
				srv := benchServer(b, n, mode.built)
				ctx := context.Background()
				var cursor []byte
				for {
					resp, err := srv.Pull(ctx, seedPull(cursor))
					if err != nil {
						b.Fatal(err)
					}
					cursor = resp.Msg.GetNextCursor()
					if !resp.Msg.GetHasMore() {
						break
					}
				}
				// One row past the cursor, in an entity this subscription
				// neither scopes nor follows: enough to make every pull rebuild
				// without adding to what the rebuild walks. See
				// BenchmarkFollowReverse.
				if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
					return tx.Put(&testdatav1.User{Id: "u-idle", Name: "idle"})
				}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := srv.Pull(ctx, seedPull(cursor)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// What a windowed forward follow costs to MAINTAIN, which is the pull a client
// holding "the newest N issues, with their projects" makes every time anything
// at all changes.
//
// The shape that makes this its own benchmark: a window holds the NEWEST rows,
// row keys ascend with age, and a ref index is ordered by (target key, row key)
// — so a probe asking "does any window member still point at this project"
// meets the members LAST, after nearly the whole fan-in. The answer instead
// comes from the window itself, which is bounded by MaxWindowSize whatever the
// entity's size, so the column below has to stay FLAT in the number of issues.
//
// The shape to expect (Apple M5 Pro, 8 projects, window of 500):
//
//	issues   maintain      alloc
//	 10000   0.60 ms      460 KB
//	 50000   0.72 ms      462 KB
//
// A regression looks like the column sloping with the corpus: that is the probe
// per target coming back, and with it a cost proportional to the fan-in of every
// project the client holds.
func BenchmarkPullWindowedFollowMaintain(b *testing.B) {
	const projects = 8
	const window = 500

	sub := func(cursor []byte) *connect.Request[syncplanev1.PullRequest] {
		return connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: benchWS,
			Cursor:      cursor,
			Subscriptions: []*syncplanev1.Subscription{{
				Entity: "issue",
				Window: &syncplanev1.Window{ByRecency: true, Limit: window},
				Follow: &syncplanev1.FollowSpec{
					Paths: []*syncplanev1.RefPath{{Segments: []string{"project"}}},
				},
			}},
		})
	}

	for _, n := range []int{10000, 50000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			ctx := context.Background()
			store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			srv, err := server.New(store,
				server.WithEntities(&testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{}))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = srv.Close() })

			// Issues are spread over the projects round robin, so every project
			// carries an equal share of the fan-in and every one of them is in
			// the closure the window's newest rows reach.
			//
			// A transaction per chunk rather than one for the lot: every row a
			// single transaction writes shares one workspace sequence, and a
			// recency window breaks that tie by key ASCENDING — which would put
			// the OLDEST keys in the window and stand the shape being measured
			// on its head. Written in chunks, age tracks the key, so the window
			// holds the highest keys and sits at the far end of every project's
			// ref index.
			if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
				for p := 0; p < projects; p++ {
					id := fmt.Sprintf("p%d", p)
					if err := tx.Put(&testdatav1.Project{Id: id, Name: id}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			for lo := 0; lo < n; lo += window {
				if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
					for i := lo; i < lo+window && i < n; i++ {
						if err := tx.Put(&testdatav1.Issue{
							Id:      fmt.Sprintf("i%07d", i),
							Title:   "issue",
							Project: &testdatav1.Project{Id: fmt.Sprintf("p%d", i%projects)},
						}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			if err := srv.BuildIndexes(ctx, benchWS); err != nil {
				b.Fatal(err)
			}

			var cursor []byte
			for {
				resp, err := srv.Pull(ctx, sub(cursor))
				if err != nil {
					b.Fatal(err)
				}
				cursor = resp.Msg.GetNextCursor()
				if !resp.Msg.GetHasMore() {
					break
				}
			}
			// ONE issue edited past the caught-up cursor: the minimum that makes
			// a pull maintain anything, and the case the fixed cost shows up in
			// most plainly. It is an old row, so it enters the window and pushes
			// the weakest member out — every iteration replays the same page.
			if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Issue{
					Id: "i0000000", Title: "edited",
					Project: &testdatav1.Project{Id: "p0"},
				})
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := srv.Pull(ctx, sub(cursor)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// drainPull pulls until has_more clears and returns the caught-up cursor.
func drainPull(b *testing.B, srv *server.Server) []byte {
	b.Helper()
	ctx := context.Background()
	var cursor []byte
	for {
		resp, err := srv.Pull(ctx, followPull(cursor))
		if err != nil {
			b.Fatal(err)
		}
		cursor = resp.Msg.GetNextCursor()
		if !resp.Msg.GetHasMore() {
			return cursor
		}
	}
}

// BenchmarkIndexedWrite measures what maintenance costs the write path. It is
// the other half of the trade — the reverse hop's speed is paid for here, on
// every write to the indexed entity — and the three sub-benchmarks are the
// three cases the write path distinguishes.
//
// The row carries writeBenchLabels labels, and `labels` is BOTH marked
// `indexed` and the first component of by_label_rank, so one row occupies 2N
// entries for them plus one per singular index. That is deliberately the cost
// shape a per-row set of custom fields has, and it is also the warning: a
// collection declared on both surfaces pays for both.
//
//   - put — the whole entry set is replaced, because a PUT replaces the whole
//     row and the write path holds no pre-image to diff against.
//   - patch/indexed — the mask names a field an index reads, so the set is
//     replaced for the same reason a PUT replaces it.
//   - patch/untouched — the mask reaches no index component, so maintenance is
//     skipped entirely (entityInfo.maskTouchesIndexes) and the write costs what
//     it would on an entity with no indexes at all. The gap between this and
//     patch/indexed is the whole point of the guard, and it widens with the
//     number of entries a row occupies.
func BenchmarkIndexedWrite(b *testing.B) {
	ctx := context.Background()

	newServer := func(b *testing.B) *server.Server {
		b.Helper()
		store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = store.Close() })
		srv, err := server.New(store,
			server.WithEntities(&testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{}))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = srv.Close() })
		return srv
	}

	labels := make([]string, writeBenchLabels)
	for i := range labels {
		labels[i] = fmt.Sprintf("l%02d", i)
	}

	b.Run("put", func(b *testing.B) {
		srv := newServer(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
				return tx.Put(&testdatav1.Issue{
					Id:      fmt.Sprintf("i%07d", i),
					Title:   "issue",
					State:   "open",
					Labels:  labels,
					Project: &testdatav1.Project{Id: "p0"},
				})
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Both PATCH cases edit ONE row, over and over, so what differs between
	// them is the mask and nothing else.
	patch := func(b *testing.B, row *testdatav1.Issue, mask ...string) {
		b.Helper()
		srv := newServer(b)
		if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Issue{
				Id:      "i0000000",
				Title:   "issue",
				State:   "open",
				Labels:  labels,
				Project: &testdatav1.Project{Id: "p0"},
			})
		}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
				return tx.Patch(row, mask, nil)
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("patch/indexed", func(b *testing.B) {
		patch(b, &testdatav1.Issue{Id: "i0000000", State: "open"}, "state")
	})

	b.Run("patch/untouched", func(b *testing.B) {
		patch(b, &testdatav1.Issue{Id: "i0000000", Title: "edited"}, "title")
	})

	// The EAV row shape: the indexed collection is a MAP of user-defined fields
	// rather than a list, so a row's entry count is the number of custom fields
	// it holds times the number of map indexes declared over it.
	//
	// entries/row is reported alongside the timings because it is the number
	// MaxIndexEntriesPerRow (default 1024) is spent against, and the number the
	// one-index-vs-two decision turns on.
	//
	// The shape to expect, which is what a regression would change:
	//
	//   - entries/row is exactly (filled custom fields x map indexes), so the
	//     `two` layouts arm is exactly double the `one` arm at every width;
	//   - put grows with the width, roughly linearly once the entry set
	//     dominates the write — the cost an EAV index has and a singular one
	//     does not;
	//   - patch/untouched does NOT grow with it. A mask that reaches no index
	//     component skips maintenance entirely (maskTouchesIndexes), and that
	//     guard is what keeps an entity with wide custom fields affordable to
	//     edit. It is also unaffected by how many indexes are declared.
	//
	// So the second layout costs a doubling of the write path and buys the range
	// and ordering shapes only `[key, value]` serves.
	b.Run("eav", func(b *testing.B) {
		for _, layout := range eavLayouts {
			for _, width := range eavWidths {
				b.Run(fmt.Sprintf("%s/put/width=%d", layout.name, width), func(b *testing.B) {
					srv, store, mt := eavServer(b, layout.indexes)
					entries := eavEntriesPerRow(b, srv, store, mt, layout.indexes, width)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						row := eavRow(mt, fmt.Sprintf("r%07d", i), width, float64(i%7))
						if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
							return tx.Put(row)
						}); err != nil {
							b.Fatal(err)
						}
					}
					// Reported AFTER the loop: ResetTimer clears the extra
					// metrics, so a value stated before it never reaches the
					// output.
					b.StopTimer()
					b.ReportMetric(entries, "entries/row")
				})

				// Both PATCH cases edit ONE row over and over, so what differs
				// between them is the mask and nothing else.
				eavPatch := func(b *testing.B, field string, set func(protoreflect.Message)) {
					b.Helper()
					srv, _, mt := eavServer(b, layout.indexes)
					if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
						return tx.Put(eavRow(mt, "r0000000", width, 1))
					}); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						msg := mt.New()
						msg.Set(mt.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("r0000000"))
						set(msg)
						if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
							return tx.Patch(msg.Interface(), []string{field}, nil)
						}); err != nil {
							b.Fatal(err)
						}
					}
				}

				b.Run(fmt.Sprintf("%s/patch/indexed/width=%d", layout.name, width), func(b *testing.B) {
					eavPatch(b, "custom.f00", func(msg protoreflect.Message) {
						fd := msg.Descriptor().Fields().ByName("custom")
						m := msg.Mutable(fd).Map()
						m.Set(protoreflect.ValueOfString("f00").MapKey(), protoreflect.ValueOfFloat64(3))
					})
				})

				b.Run(fmt.Sprintf("%s/patch/untouched/width=%d", layout.name, width), func(b *testing.B) {
					eavPatch(b, "title", func(msg protoreflect.Message) {
						fd := msg.Descriptor().Fields().ByName("title")
						msg.Set(fd, protoreflect.ValueOfString("edited"))
					})
				})
			}
		}
	})
}

// BenchmarkCustomFieldQuery measures what a map index buys the two questions an
// EAV schema asks, against the scan that is always the fallback:
//
//   - CROSS-KEY, "which custom field holds this value" — an any() over the map,
//     which only a `[value, key]` layout can serve as a range; and
//   - NAMED-KEY, "does THIS custom field hold this value" — an addressed entry,
//     which either layout serves.
//
// The two arms drive the SAME query over the SAME rows, differing only in
// whether an index is usable, so the ratio between them is the whole result. A
// FIXED number of rows match, so the work the index avoids grows with the entity
// while the work it does stays flat.
//
// The shape to expect: the index column FLAT in the corpus size and the scan
// column linear in it, in both question shapes. If the cross-key column were not
// decisively flatter than its scan, the `[value, key]` layout would not be worth
// declaring and milestone 3 of this feature would not have paid for itself.
//
// The one-index-vs-two comparison lives here too. `one` declares `[value, key]`
// alone and `two` adds `[key, value]`; both serve both equality shapes at the
// same cost, because either layout pins both components from one term. What the
// second index buys is the shapes equality cannot express — a RANGE inside one
// named field, and an ordering by it — and its price is on the write path, in
// BenchmarkIndexedWrite above.
func BenchmarkCustomFieldQuery(b *testing.B) {
	ctx := context.Background()
	const width = 20
	const hit = 42.0

	specs := []struct {
		name string
		spec query.Spec
	}{
		// "Which custom field holds 42", with the field left open.
		{"cross_key", query.Spec{Where: query.Any("custom", query.Eq("value", hit))}},
		// "Does f00 hold 42", the addressed form.
		{"named_key", query.Spec{Where: query.Eq("custom.f00", hit)}},
	}

	for _, layout := range eavLayouts {
		for _, n := range indexBenchSizes {
			for _, mode := range []struct {
				name  string
				built bool
			}{{"scan", false}, {"index", true}} {
				for _, s := range specs {
					name := fmt.Sprintf("%s/%s/%s/rows=%d", s.name, layout.name, mode.name, n)
					b.Run(name, func(b *testing.B) {
						var opts []server.Option
						if !mode.built {
							opts = append(opts, server.WithLimits(server.Limits{DisableIndexes: true}))
						}
						srv, _, mt := eavServer(b, layout.indexes, opts...)
						if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
							for i := 0; i < n; i++ {
								// The matching rows are spread across the key
								// space rather than clustered, so the scan cannot
								// get lucky and stop early.
								v := float64(i % 7)
								if i%(n/indexBenchMatches) == 0 {
									v = hit
								}
								if err := tx.Put(eavRow(mt, fmt.Sprintf("r%07d", i), width, v)); err != nil {
									return err
								}
							}
							return nil
						}); err != nil {
							b.Fatal(err)
						}
						if mode.built {
							if err := srv.BuildIndexes(ctx, benchWS); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							rows := 0
							if err := srv.View(server.Trusted(ctx), benchWS, func(tx *server.ViewTx) error {
								return tx.Query(mt.New().Interface(), s.spec,
									func(proto.Message, uint64) error {
										rows++
										return nil
									})
							}); err != nil {
								b.Fatal(err)
							}
							if rows != indexBenchMatches {
								b.Fatalf("got %d rows, want %d", rows, indexBenchMatches)
							}
						}
					})
				}
			}
		}
	}
}

// The EAV benchmark fixture: an entity whose user-defined fields are a
// scalar-valued map, declared through the dynamic surface so the INDEX SET is a
// parameter rather than whatever the compiled test schema happens to carry.
//
// A scalar-valued map rather than the message-valued one Issue uses: the two
// index and probe identically (a map fan plus a leaf), and the scalar keeps the
// row construction here about the benchmark instead of about oneof variants.
var (
	eavWidths  = []int{3, 20, 100}
	eavLayouts = []struct {
		name    string
		indexes []server.DynamicIndex
	}{
		// The layout an equality-only app declares: one index, both axes.
		{"one", []server.DynamicIndex{{
			Name: "cf_by_value",
			Key: []server.DynamicIndexKey{
				{Path: "custom.*.value"}, {Path: "custom.*.key"},
			},
		}}},
		// Both layouts, which adds ranges and orderings within one named field.
		{"two", []server.DynamicIndex{
			{Name: "cf_by_value", Key: []server.DynamicIndexKey{
				{Path: "custom.*.value"}, {Path: "custom.*.key"},
			}},
			{Name: "cf_by_key", Key: []server.DynamicIndexKey{
				{Path: "custom.*.key"}, {Path: "custom.*.value"},
			}},
		}},
	}
)

// eavServer builds a server over a fresh on-disk store holding the EAV entity
// with the given indexes and no rows.
func eavServer(b *testing.B, indexes []server.DynamicIndex, opts ...server.Option) (
	*server.Server, storage.Adapter, protoreflect.MessageType,
) {
	b.Helper()
	store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	srv, err := server.New(store, opts...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = srv.Close() })

	spec := server.DynamicEntity{
		Name:      "record",
		KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "custom", Number: 3, MapKey: protoreflect.StringKind, MapValue: protoreflect.DoubleKind},
		},
		Indexes: indexes,
	}
	// Built ONCE and registered from the same type: RegisterDynamicEntity would
	// synthesize its own descriptor, and EntityTx.Put compares field descriptors
	// by identity, so two builds of the same spec are two different messages.
	mt, err := server.BuildDynamicMessageType(spec)
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.RegisterEntity(mt.New().Interface()); err != nil {
		b.Fatal(err)
	}
	return srv, store, mt
}

// eavRow builds one row with `width` custom fields. `hit` is the value the first
// field takes, which is what a probe selects on.
func eavRow(mt protoreflect.MessageType, id string, width int, hit float64) proto.Message {
	msg := mt.New()
	md := mt.Descriptor()
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfString(id))
	msg.Set(md.Fields().ByName("title"), protoreflect.ValueOfString("record"))
	m := msg.Mutable(md.Fields().ByName("custom")).Map()
	for f := 0; f < width; f++ {
		v := float64(f % 7)
		if f == 0 {
			v = hit
		}
		m.Set(protoreflect.ValueOfString(fmt.Sprintf("f%02d", f)).MapKey(), protoreflect.ValueOfFloat64(v))
	}
	return msg.Interface()
}

// eavEntriesPerRow writes one row and counts the index entries it occupies. It
// is the number MaxIndexEntriesPerRow is spent against, and the number that
// prices declaring a second layout.
func eavEntriesPerRow(
	b *testing.B,
	srv *server.Server,
	store storage.Adapter,
	mt protoreflect.MessageType,
	indexes []server.DynamicIndex,
	width int,
) float64 {
	b.Helper()
	ctx := context.Background()
	if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
		return tx.Put(eavRow(mt, "probe", width, 1))
	}); err != nil {
		b.Fatal(err)
	}
	n := 0
	if err := store.WithReadTx(ctx, benchWS, func(tx storage.ReadTx) error {
		for _, idx := range indexes {
			if err := tx.ScanIndex("record", idx.Name, nil, nil, 0, storage.ScanAsc, func(_ []byte, _ string) error {
				n++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return float64(n)
}

// writeBenchLabels is how many elements the fanned-out field of the benchmarked
// row holds. Ten is where the entry count (2N + singular indexes) is clearly
// past the point where maintenance dominates a small write, without being an
// outlier a real row would not reach.
const writeBenchLabels = 10
