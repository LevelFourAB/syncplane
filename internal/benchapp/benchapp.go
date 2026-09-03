// Package benchapp holds the corpus the browser sync benchmarks measure
// against: the syncplane.bench.v1 issue tracker, its size tiers, and a
// deterministic generator that seeds a workspace server-side.
//
// It is split out of cmd/benchserver so the tier definitions are importable by
// anything that wants to reason about corpus size without spawning a server.
//
// # Determinism
//
// A benchmark is only comparable against its own history if the corpus is
// identical run to run, so generation is a pure function of (tier, seed):
// single-goroutine, seeded PRNG, no clock, no map iteration in a position that
// reaches a row. Change [GeneratorVersion] whenever a seeded database changes —
// the harness folds it into the cache fingerprint, so a corpus seeded by an
// older version is regenerated rather than silently compared against.
package benchapp

import (
	"github.com/levelfourab/syncplane/engine/server"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
)

// GeneratorVersion changes whenever a seeded database does — its rows, and the
// server-side state the seeder leaves behind, such as which indexes are built.
// It is the cache key for a seeded database: bump it and every cached corpus is
// regenerated.
const GeneratorVersion = 3

// Options returns the entity registrations for the bench corpus.
func Options() []server.Option {
	return []server.Option{
		server.WithEntities(
			&benchv1.Project{},
			&benchv1.User{},
			&benchv1.Label{},
			&benchv1.Issue{},
			&benchv1.Comment{},
			&benchv1.Note{},
			&benchv1.Flat{},
		),
	}
}

// Tier is one corpus size. The counts are chosen so issues and comments
// dominate — a real tracker's shape — with the remainder in the flat control
// entity, and so that the total lands near the tier's advertised row count.
// The json tags are load-bearing: /debug/tiers serves this struct to the
// benchmark harness, and TypeScript reads the field names.
type Tier struct {
	// Name as the harness and the workspace id spell it ("s", "m", "l", "xl").
	Name string `json:"name"`
	// Rough total, for reporting. The exact total is what Counts sums to.
	Approx int `json:"approx"`

	Projects int `json:"projects"`
	Users    int `json:"users"`
	Labels   int `json:"labels"`
	Issues   int `json:"issues"`
	Comments int `json:"comments"`
	Notes    int `json:"notes"`
	Flats    int `json:"flats"`

	// Characters in each note's collaborative-text body. Byte weight without
	// row weight: raising this moves bootstrap bytes without moving row counts,
	// which is what separates the two costs in a result table.
	NoteChars int `json:"noteChars"`
	// Characters in an issue body. The bulk entity's payload-weight dial.
	IssueBodyChars int `json:"issueBodyChars"`
}

// Total is the number of rows a seeded tier holds.
func (t Tier) Total() int {
	return t.Projects + t.Users + t.Labels + t.Issues + t.Comments + t.Notes + t.Flats
}

// Tiers are the corpus sizes the benchmarks run at. The names are what
// /debug/seed and the workspace ids use.
//
// xl exists to be run partial-only: a full-sync client is expected to be
// unhappy at half a million rows, and measuring how unhappy is the point of
// having the tier rather than a reason to leave it out.
var Tiers = map[string]Tier{
	"s": {
		Name: "s", Approx: 1_000,
		Projects: 5, Users: 25, Labels: 12,
		Issues: 500, Comments: 300, Notes: 10, Flats: 150,
		NoteChars: 2_000, IssueBodyChars: 160,
	},
	"m": {
		Name: "m", Approx: 10_000,
		Projects: 12, Users: 100, Labels: 20,
		Issues: 5_000, Comments: 3_000, Notes: 50, Flats: 1_800,
		NoteChars: 4_000, IssueBodyChars: 160,
	},
	"l": {
		Name: "l", Approx: 100_000,
		Projects: 40, Users: 400, Labels: 30,
		Issues: 50_000, Comments: 30_000, Notes: 200, Flats: 19_000,
		NoteChars: 4_000, IssueBodyChars: 160,
	},
	"xl": {
		Name: "xl", Approx: 500_000,
		Projects: 100, Users: 1_000, Labels: 40,
		Issues: 250_000, Comments: 150_000, Notes: 500, Flats: 98_000,
		NoteChars: 4_000, IssueBodyChars: 160,
	},
}

// TierNames lists the tiers smallest first — a stable order for seeding and
// reporting, which ranging over the map would not give.
var TierNames = []string{"s", "m", "l", "xl"}

// WorkspaceID is the workspace a tier's corpus is seeded into. One workspace
// per tier, stable across runs, so a seeded database can be cached and reused.
func WorkspaceID(tier string) string {
	return "bench-" + tier
}
