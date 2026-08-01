package server_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
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

// BenchmarkIndexedWrite measures what maintenance costs the write path: the
// same PUT with the entity's indexes maintained and with index use disabled
// but maintenance still running, which is where the write amplification shows.
// It is the other half of the trade — the reverse hop's speed is paid for
// here, on every write to the indexed entity.
func BenchmarkIndexedWrite(b *testing.B) {
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := srv.Mutate(server.Trusted(ctx), benchWS, func(tx *server.EntityTx) error {
			return tx.Put(&testdatav1.Issue{
				Id:      fmt.Sprintf("i%07d", i),
				Title:   "issue",
				Project: &testdatav1.Project{Id: "p0"},
			})
		}); err != nil {
			b.Fatal(err)
		}
	}
}
