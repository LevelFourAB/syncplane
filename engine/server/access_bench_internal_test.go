package server

// What resolving a caller's grants costs, with grants to the caller and with
// grants to the caller's teams.
//
// Every request and every Subscribe drain resolves the caller's grants, so
// this is paid on each pull. Each case gives the caller the same 50 repos:
// directly, or through teams whose grants name them, among the rows of other
// users and teams. Run with:
//
//	go test ./engine/server/ -run '^$' -bench ResolveAccess

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

func BenchmarkResolveAccess(b *testing.B) {
	const repos = 50
	for _, tc := range []struct {
		name  string
		teams int // 0 gives the caller one grant of its own per repo
	}{
		{"direct", 0},
		{"teams=1", 1},
		{"teams=10", 10},
		{"teams=50", 50},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			store, err := sqlitestore.Open(filepath.Join(b.TempDir(), "bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			srv, err := New(store, WithEntities(
				&accessv1.Squad{}, &accessv1.SquadMember{}, &accessv1.Repo{},
				&accessv1.RepoMember{}, &accessv1.RepoSquad{}, &accessv1.Issue{},
			))
			if err != nil {
				b.Fatal(err)
			}
			err = srv.Mutate(Trusted(ctx), "ws", func(tx *EntityTx) error {
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
							Id: fmt.Sprintf("rm-%s-%d", sq, r), Repo: &accessv1.Repo{Id: fmt.Sprintf("r%d", r)},
							Member: &accessv1.RepoMember_Squad{Squad: &accessv1.Squad{Id: sq}}, Role: "repo_reader",
						}); err != nil {
							return err
						}
					}
				}
				for r := range repos {
					if err := tx.Put(&accessv1.Repo{Id: fmt.Sprintf("r%d", r)}); err != nil {
						return err
					}
				}
				if tc.teams == 0 {
					for r := range repos {
						if err := tx.Put(&accessv1.RepoMember{
							Id: fmt.Sprintf("rm-me-%d", r), Repo: &accessv1.Repo{Id: fmt.Sprintf("r%d", r)},
							Member: &accessv1.RepoMember_UserId{UserId: "me"}, Role: "repo_writer",
						}); err != nil {
							return err
						}
					}
					return nil
				}
				for t := range tc.teams {
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
				for r := range repos {
					sq := fmt.Sprintf("mine-%d", r%tc.teams)
					if err := tx.Put(&accessv1.RepoMember{
						Id: fmt.Sprintf("rm-%s-%d", sq, r), Repo: &accessv1.Repo{Id: fmt.Sprintf("r%d", r)},
						Member: &accessv1.RepoMember_Squad{Squad: &accessv1.Squad{Id: sq}}, Role: "repo_writer",
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
			caller, err := srv.resolveCaller(ctx, Principal{Subject: "me"}, model)
			if err != nil {
				b.Fatal(err)
			}
			resolve := func() *accessSet {
				var set *accessSet
				if err := store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
					var err error
					set, err = resolveAccess(tx, caller, srv.limits)
					return err
				}); err != nil {
					b.Fatal(err)
				}
				return set
			}
			// A scan would give the same set at a different cost, so the indexes
			// the resolution probes must be ready for the numbers to mean
			// anything.
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
			if got := len(resolve().perms["edit_issues"].rows); got != repos {
				b.Fatalf("the caller holds edit_issues at %d repos, want %d", got, repos)
			}
			b.ResetTimer()
			for range b.N {
				resolve()
			}
		})
	}
}
