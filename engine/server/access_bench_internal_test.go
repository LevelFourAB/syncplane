package server

// What access control costs a caller with many grants.
//
// Every request and every Subscribe drain resolves the caller's grants and
// renders them twice: as the ReadAccess a cursor records and as the
// CallerAccess a client receives. A pull sends the CallerAccess whole when it
// changed. Each case gives the caller a role at a number of repos, directly or
// through teams whose grants name them, among the rows of other users and
// teams. Repo keys are UUID-shaped, so the reported sizes match a typical
// deployment. Run with:
//
//	go test ./engine/server/ -run '^$' -bench Access
//
// The store is SQLite. Set SYNCPLANE_BENCH_PG to 1 and SYNCPLANE_TEST_PG_DSN
// to a Postgres the benchmark may create schemas in, to measure pgstore
// instead. Each case seeds a fresh schema and drops it at the end.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// benchSchemaSeq numbers the Postgres schemas of one benchmark process.
var benchSchemaSeq atomic.Int64

// openAccessBenchStore opens an empty store for one benchmark case: SQLite in
// a temporary directory, or a fresh Postgres schema when SYNCPLANE_BENCH_PG is
// set. The store and the schema go away when b ends.
func openAccessBenchStore(b *testing.B) storage.Adapter {
	b.Helper()
	ctx := context.Background()
	if os.Getenv("SYNCPLANE_BENCH_PG") == "" {
		store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = store.Close() })
		return store
	}
	dsn := os.Getenv(pgtest.DSNEnv)
	if dsn == "" {
		b.Skipf("SYNCPLANE_BENCH_PG needs %s", pgtest.DSNEnv)
	}
	schema := fmt.Sprintf("sp_access_bench_%d_%d", time.Now().UnixNano(), benchSchemaSeq.Add(1))
	exec := func(sql string) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			b.Fatalf("connect to %s: %v", pgtest.DSNEnv, err)
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, sql); err != nil {
			b.Fatal(err)
		}
	}
	exec(`CREATE SCHEMA "` + schema + `"`)
	// Registered before the store's Close, so it runs after it: a DROP would
	// otherwise wait on the pool's open connections.
	b.Cleanup(func() { exec(`DROP SCHEMA "` + schema + `" CASCADE`) })
	u, err := url.Parse(dsn)
	if err != nil {
		b.Fatalf("%s must be a URI (postgres://...): %v", pgtest.DSNEnv, err)
	}
	q := u.Query()
	q.Set("options", "-c search_path="+schema)
	u.RawQuery = q.Encode()
	store, err := pgstore.Open(ctx, u.String())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	return store
}

// accessBenchCase is the access a benchmark gives its caller.
type accessBenchCase struct {
	repos int    // the repos the caller holds role at
	teams int    // 0 gives the caller one grant of its own per repo
	role  string // a role of repo_member; repo_writer when empty
}

func (c accessBenchCase) name() string {
	via := "direct"
	if c.teams > 0 {
		via = fmt.Sprintf("teams=%d", c.teams)
	}
	return fmt.Sprintf("repos=%d/%s", c.repos, via)
}

// repoKey is the key of repo r.
func repoKey(r int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", r, r) }

// accessBench is a server over the team schema of the access test proto,
// with every repo holding one issue.
type accessBench struct {
	ctx    context.Context
	store  storage.Adapter
	srv    *Server
	caller *accessCaller
	// as is ctx with the caller "me" as its principal.
	as context.Context
}

func newAccessBench(b *testing.B, c accessBenchCase) *accessBench {
	b.Helper()
	if c.role == "" {
		c.role = "repo_writer"
	}
	ctx := context.Background()
	store := openAccessBenchStore(b)
	srv, err := New(store, WithEntities(
		&accessv1.Squad{}, &accessv1.SquadMember{}, &accessv1.Repo{},
		&accessv1.RepoMember{}, &accessv1.RepoSquad{}, &accessv1.Issue{},
	))
	if err != nil {
		b.Fatal(err)
	}
	// The extra repo is the one BenchmarkPullAccessChange grants and revokes.
	repos := max(c.repos+1, 10)
	err = srv.Mutate(Trusted(ctx), "ws", func(tx *EntityTx) error {
		for r := range repos {
			if err := tx.Put(&accessv1.Repo{Id: repoKey(r)}); err != nil {
				return err
			}
			if err := tx.Put(&accessv1.Issue{
				Id: fmt.Sprintf("i-%d", r), Repo: &accessv1.Repo{Id: repoKey(r)}, Title: "issue",
			}); err != nil {
				return err
			}
		}
		// Rows of other users: 20 squads of 50 members each, every squad
		// with grants on 10 repos.
		for s := range 20 {
			sq := fmt.Sprintf("other-%d", s)
			if err := tx.Put(&accessv1.Squad{Id: sq}); err != nil {
				return err
			}
			for m := range 50 {
				if err := tx.Put(&accessv1.SquadMember{
					Id: fmt.Sprintf("sm-%s-%d", sq, m), Squad: &accessv1.Squad{Id: sq},
					UserId: fmt.Sprintf("user-%d-%d", s, m), Role: "squad_member",
				}); err != nil {
					return err
				}
			}
			for r := range 10 {
				if err := tx.Put(&accessv1.RepoMember{
					Id: fmt.Sprintf("rm-%s-%d", sq, r), Repo: &accessv1.Repo{Id: repoKey(r)},
					Member: &accessv1.RepoMember_Squad{Squad: &accessv1.Squad{Id: sq}}, Role: "repo_reader",
				}); err != nil {
					return err
				}
			}
		}
		if c.teams == 0 {
			for r := range c.repos {
				if err := tx.Put(&accessv1.RepoMember{
					Id: fmt.Sprintf("rm-me-%d", r), Repo: &accessv1.Repo{Id: repoKey(r)},
					Member: &accessv1.RepoMember_UserId{UserId: "me"}, Role: c.role,
				}); err != nil {
					return err
				}
			}
			return nil
		}
		for t := range c.teams {
			sq := fmt.Sprintf("mine-%d", t)
			if err := tx.Put(&accessv1.Squad{Id: sq}); err != nil {
				return err
			}
			if err := tx.Put(&accessv1.SquadMember{
				Id: "sm-me-" + sq, Squad: &accessv1.Squad{Id: sq}, UserId: "me", Role: "squad_member",
			}); err != nil {
				return err
			}
		}
		for r := range c.repos {
			sq := fmt.Sprintf("mine-%d", r%c.teams)
			if err := tx.Put(&accessv1.RepoMember{
				Id: fmt.Sprintf("rm-%s-%d", sq, r), Repo: &accessv1.Repo{Id: repoKey(r)},
				Member: &accessv1.RepoMember_Squad{Squad: &accessv1.Squad{Id: sq}}, Role: c.role,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.BuildIndexes(ctx, "ws"); err != nil {
		b.Fatal(err)
	}
	model := srv.catalogPtr().access
	// A scan would give the same set at a different cost, so the indexes the
	// resolution probes must be ready for the numbers to mean anything.
	if err := store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
		for _, g := range model.grantEntities {
			for _, c := range g.grant.subjects {
				ci := g.indexByName(c.indexName())
				if ready, err := ci.ready(tx, g.name); err != nil || !ready {
					return fmt.Errorf("index %s of %s is not ready: %v", ci.name, g.name, err)
				}
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	me := Principal{Subject: "me"}
	caller, err := srv.resolveCaller(ctx, me, model)
	if err != nil {
		b.Fatal(err)
	}
	ab := &accessBench{ctx: ctx, store: store, srv: srv, caller: caller, as: WithPrincipal(ctx, me)}
	if got := len(ab.resolve(b).perms["view_issues"].rows); got != c.repos {
		b.Fatalf("the caller holds view_issues at %d repos, want %d", got, c.repos)
	}
	return ab
}

// resolve resolves the caller's grants in a fresh read transaction.
func (ab *accessBench) resolve(b *testing.B) *accessSet {
	var set *accessSet
	if err := ab.store.WithReadTx(ab.ctx, "ws", func(tx storage.ReadTx) error {
		var err error
		set, err = resolveAccess(tx, ab.caller, ab.srv.limits)
		return err
	}); err != nil {
		b.Fatal(err)
	}
	return set
}

// pull pulls as the caller from cursor until no page remains, and returns the
// last cursor and every page.
func (ab *accessBench) pull(b *testing.B, cursor []byte) ([]byte, []*syncplanev1.PullResponse) {
	var pages []*syncplanev1.PullResponse
	for {
		resp, err := ab.srv.Pull(ab.as, connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: "ws", ClientId: "c-me", Cursor: cursor,
		}))
		if err != nil {
			b.Fatal(err)
		}
		pages = append(pages, resp.Msg)
		cursor = resp.Msg.GetNextCursor()
		if !resp.Msg.GetHasMore() {
			return cursor, pages
		}
	}
}

// grantExtra grants the caller the role at the extra repo, or revokes it.
func (ab *accessBench) grantExtra(b *testing.B, repos int, grant bool) {
	err := ab.srv.Mutate(Trusted(ab.ctx), "ws", func(tx *EntityTx) error {
		if !grant {
			return tx.Delete(&accessv1.RepoMember{}, "rm-me-extra")
		}
		return tx.Put(&accessv1.RepoMember{
			Id: "rm-me-extra", Repo: &accessv1.Repo{Id: repoKey(repos)},
			Member: &accessv1.RepoMember_UserId{UserId: "me"}, Role: "repo_writer",
		})
	})
	if err != nil {
		b.Fatal(err)
	}
}

// reportPages reports the encoded size of pages and of the CallerAccess they
// carry.
func reportPages(b *testing.B, pages []*syncplanev1.PullResponse) {
	var total, ca int
	for _, p := range pages {
		total += proto.Size(p)
		ca += proto.Size(p.GetCallerAccess())
	}
	b.ReportMetric(float64(total), "resp-B")
	b.ReportMetric(float64(ca), "caller-access-B")
}

var accessScales = []int{50, 1000, 5000}

// BenchmarkResolveAccess measures resolving the caller's grants, which every
// pull and every Subscribe drain pays.
func BenchmarkResolveAccess(b *testing.B) {
	for _, repos := range accessScales {
		for _, teams := range []int{0, 1, 10, 50} {
			c := accessBenchCase{repos: repos, teams: teams}
			b.Run(c.name(), func(b *testing.B) {
				ab := newAccessBench(b, c)
				for b.Loop() {
					ab.resolve(b)
				}
			})
		}
	}
}

// BenchmarkRenderAccess measures rendering a resolved set as a cursor records
// it and as a client receives it, hashes included, which every pull and every
// Subscribe drain pays. The roles grant 3, 4 and 6 permissions at each repo.
func BenchmarkRenderAccess(b *testing.B) {
	for _, repos := range accessScales {
		for _, role := range []string{"repo_reader", "repo_writer", "repo_admin"} {
			c := accessBenchCase{repos: repos, role: role}
			b.Run(fmt.Sprintf("repos=%d/%s", repos, role), func(b *testing.B) {
				ab := newAccessBench(b, c)
				set := ab.resolve(b)
				render := func() *rowEmitter {
					e := &rowEmitter{caller: ab.caller, access: set}
					e.boundReadAccess()
					e.boundCallerAccess()
					return e
				}
				var e *rowEmitter
				for b.Loop() {
					e = render()
				}
				b.ReportMetric(float64(proto.Size(e.callerAccess)), "caller-access-B")
				b.ReportMetric(float64(proto.Size(e.readAccess)), "read-access-B")
			})
		}
	}
}

// BenchmarkPullAccessIdle measures a pull from a current cursor when nothing
// changed: the floor every poll and poke pays.
func BenchmarkPullAccessIdle(b *testing.B) {
	for _, repos := range accessScales {
		c := accessBenchCase{repos: repos}
		b.Run(c.name(), func(b *testing.B) {
			ab := newAccessBench(b, c)
			cursor, _ := ab.pull(b, nil)
			var pages []*syncplanev1.PullResponse
			for b.Loop() {
				_, pages = ab.pull(b, cursor)
			}
			reportPages(b, pages)
		})
	}
}

// BenchmarkPullAccessChange measures a pull after one of the caller's grants
// changed: it resyncs the rows below the moved repo and sends the whole
// CallerAccess. Each iteration grants or revokes a role at one more repo,
// and only the pull is timed.
func BenchmarkPullAccessChange(b *testing.B) {
	for _, repos := range accessScales {
		c := accessBenchCase{repos: repos}
		b.Run(c.name(), func(b *testing.B) {
			ab := newAccessBench(b, c)
			cursor, _ := ab.pull(b, nil)
			var pages []*syncplanev1.PullResponse
			grant := true
			for b.Loop() {
				b.StopTimer()
				ab.grantExtra(b, repos, grant)
				grant = !grant
				b.StartTimer()
				cursor, pages = ab.pull(b, cursor)
				if !slices.ContainsFunc(pages, func(p *syncplanev1.PullResponse) bool { return p.GetCallerAccess() != nil }) {
					b.Fatal("a pull after a grant change sent no CallerAccess")
				}
			}
			reportPages(b, pages)
		})
	}
}

// BenchmarkPullAccessBootstrap measures a pull without a cursor, which sends
// every readable row and the whole CallerAccess.
func BenchmarkPullAccessBootstrap(b *testing.B) {
	for _, repos := range accessScales {
		c := accessBenchCase{repos: repos}
		b.Run(c.name(), func(b *testing.B) {
			ab := newAccessBench(b, c)
			var pages []*syncplanev1.PullResponse
			for b.Loop() {
				_, pages = ab.pull(b, nil)
			}
			reportPages(b, pages)
		})
	}
}
