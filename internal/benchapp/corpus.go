package benchapp

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"

	"google.golang.org/protobuf/proto"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// baseTimeMs is the corpus's epoch, fixed so a regenerated corpus is
// byte-identical to the one it replaces. Reading the clock here would make
// every run a different corpus and every comparison meaningless.
const baseTimeMs int64 = 1_704_067_200_000 // 2024-01-01T00:00:00Z

// batchRows is how many rows share one write transaction. Every row in a batch
// takes the same workspace sequence, so this also sets how coarse the seeded
// history is — coarse enough that seeding half a million rows is not half a
// million transactions, fine enough that a pull page boundary still lands
// inside the corpus rather than swallowing all of it.
const batchRows = 1_000

// Counts is what a seed run wrote, per entity.
type Counts struct {
	Projects int `json:"projects"`
	Users    int `json:"users"`
	Labels   int `json:"labels"`
	Issues   int `json:"issues"`
	Comments int `json:"comments"`
	Notes    int `json:"notes"`
	Flats    int `json:"flats"`
	Total    int `json:"total"`
}

// Progress reports seeding progress. Seeding the largest tier takes minutes, so
// a caller that would otherwise look hung gets something to print.
type Progress func(entity string, done, total int)

// Seed generates a tier's corpus and writes it into workspaceID as trusted
// server-side writes, bypassing the client wire entirely — half a million rows
// pushed through Push would measure the seeding, not the benchmark.
//
// Deterministic in (tier, seed): the same pair always produces the same rows in
// the same order, which is what lets a seeded database be cached and reused
// (see GeneratorVersion).
//
// The workspace is assumed empty. Seeding a workspace twice writes the corpus
// twice under the same keys — harmless for row content, since every key is
// overwritten with the identical value, but it doubles the workspace's history
// and therefore what a bootstrap has to walk past. The harness seeds once into
// a cached database instead.
func Seed(
	ctx context.Context,
	srv *server.Server,
	workspaceID string,
	tier Tier,
	seed uint64,
	progress Progress,
) (Counts, error) {
	g := &generator{
		srv:      srv,
		ctx:      server.Trusted(ctx),
		ws:       workspaceID,
		tier:     tier,
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		progress: progress,
	}
	if err := g.run(); err != nil {
		return Counts{}, err
	}
	return Counts{
		Projects: tier.Projects,
		Users:    tier.Users,
		Labels:   tier.Labels,
		Issues:   tier.Issues,
		Comments: tier.Comments,
		Notes:    tier.Notes,
		Flats:    tier.Flats,
		Total:    tier.Total(),
	}, nil
}

type generator struct {
	srv      *server.Server
	ctx      context.Context
	ws       string
	tier     Tier
	rng      *rand.Rand
	progress Progress
}

func (g *generator) run() error {
	// Order matters: dimensions first, so an issue's refs point at rows that
	// already exist. Nothing enforces that — a reference stores only the target
	// key and resolves on read — but a corpus whose refs dangle would measure
	// the wrong thing on every follow.
	if err := g.rows("project", g.tier.Projects, g.project); err != nil {
		return err
	}
	if err := g.rows("user", g.tier.Users, g.user); err != nil {
		return err
	}
	if err := g.rows("label", g.tier.Labels, g.label); err != nil {
		return err
	}
	if err := g.rows("issue", g.tier.Issues, g.issue); err != nil {
		return err
	}
	if err := g.rows("comment", g.tier.Comments, g.comment); err != nil {
		return err
	}
	if err := g.notes(); err != nil {
		return err
	}
	return g.rows("flat", g.tier.Flats, g.flat)
}

// rows generates count messages with build and writes them in batches. build is
// called outside the write closure because a storage retry re-runs the closure,
// and re-running the generator inside it would consume the PRNG twice and make
// the corpus depend on how often SQLite decided to retry.
func (g *generator) rows(entity string, count int, build func(i int) proto.Message) error {
	for start := 0; start < count; start += batchRows {
		end := min(start+batchRows, count)
		batch := make([]proto.Message, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, build(i))
		}
		err := g.srv.Mutate(g.ctx, g.ws, func(tx *server.EntityTx) error {
			for _, msg := range batch {
				if err := tx.Put(msg); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("seed %s rows %d-%d: %w", entity, start, end, err)
		}
		if g.progress != nil {
			g.progress(entity, end, count)
		}
	}
	return nil
}

// notes are written apart from rows because a collaborative-text field is not
// set by assigning the message field: the body is handed over as a string and
// the engine builds the CRDT document for it, under one fixed peer identity.
func (g *generator) notes() error {
	count := g.tier.Notes
	for start := 0; start < count; start += batchRows {
		end := min(start+batchRows, count)
		type note struct {
			msg  *benchv1.Note
			body string
		}
		batch := make([]note, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, note{
				msg: &benchv1.Note{
					Id:          id("note", i),
					Title:       g.title(),
					Project:     g.someProject(),
					UpdatedAtMs: g.timestamp(i),
				},
				body: g.words(g.tier.NoteChars),
			})
		}
		err := g.srv.Mutate(g.ctx, g.ws, func(tx *server.EntityTx) error {
			for _, n := range batch {
				if err := tx.Put(n.msg,
					server.WithTextString("body", n.body),
					server.WithTextPeer("benchserver:seed"),
				); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("seed note rows %d-%d: %w", start, end, err)
		}
		if g.progress != nil {
			g.progress("note", end, count)
		}
	}
	return nil
}

func (g *generator) project(i int) proto.Message {
	return &benchv1.Project{
		Id:          id("project", i),
		Slug:        fmt.Sprintf("P%d", i),
		Name:        g.title(),
		Status:      pick(g.rng, projectStates),
		CreatedAtMs: g.timestamp(i),
	}
}

func (g *generator) user(i int) proto.Message {
	name := fmt.Sprintf("%s %s", pick(g.rng, firstNames), pick(g.rng, lastNames))
	return &benchv1.User{
		Id:        id("user", i),
		Name:      name,
		Email:     fmt.Sprintf("user%d@example.test", i),
		AvatarUrl: fmt.Sprintf("https://avatars.example.test/u/%d.png", i),
		// Skewed rather than balanced: a predicate that matches almost
		// everything and one that matches almost nothing cost different things,
		// and a 50/50 field can express neither.
		Active: g.rng.IntN(10) > 0,
	}
}

func (g *generator) label(i int) proto.Message {
	return &benchv1.Label{
		Id:    id("label", i),
		Name:  labelNames[i%len(labelNames)],
		Color: fmt.Sprintf("#%06x", g.rng.IntN(1<<24)),
	}
}

func (g *generator) issue(i int) proto.Message {
	issue := &benchv1.Issue{
		Id:          id("issue", i),
		Title:       g.title(),
		Body:        g.words(g.tier.IssueBodyChars),
		Project:     g.someProject(),
		Assignee:    g.someUser(),
		Reporter:    g.someUser(),
		State:       pick(g.rng, issueStates),
		Priority:    int32(g.rng.IntN(5)),
		Estimate:    int32(g.rng.IntN(13)),
		UpdatedAtMs: g.timestamp(i),
		CreatedAtMs: g.timestamp(i) - int64(g.rng.IntN(30*24))*3_600_000,
	}
	for n := g.rng.IntN(4); n > 0; n-- {
		issue.Labels = append(issue.Labels, id("label", g.rng.IntN(g.tier.Labels)))
	}
	issue.Labels = dedupe(issue.Labels)
	for n := g.rng.IntN(6); n > 0; n-- {
		k := len(issue.Checklist)
		issue.Checklist = append(issue.Checklist, &benchv1.ChecklistItem{
			Id:   fmt.Sprintf("c%d", k),
			Text: g.title(),
			Done: g.rng.IntN(3) == 0,
		})
	}
	issue.Body += g.searchMarkers(i)
	// Dependencies point BACKWARDS only. A chain that can only run toward lower
	// indices cannot contain a cycle, so following blocked_by transitively
	// terminates however deep a benchmark sets followDepth.
	if i > 0 && g.rng.IntN(4) == 0 {
		for n := 1 + g.rng.IntN(2); n > 0; n-- {
			issue.BlockedBy = append(issue.BlockedBy, &benchv1.Issue{
				Id: id("issue", g.rng.IntN(i)),
			})
		}
	}
	return issue
}

func (g *generator) comment(i int) proto.Message {
	return &benchv1.Comment{
		Id:          id("comment", i),
		Issue:       &benchv1.Issue{Id: id("issue", g.hotIssue())},
		Author:      g.someUser(),
		Body:        g.words(80 + g.rng.IntN(240)),
		CreatedAtMs: g.timestamp(i),
	}
}

func (g *generator) flat(i int) proto.Message {
	return &benchv1.Flat{
		Id:          id("flat", i),
		Name:        g.title(),
		Category:    pick(g.rng, categories),
		Note:        g.words(60),
		Amount:      int64(g.rng.IntN(1_000_000)),
		Count:       int32(g.rng.IntN(500)),
		Ratio:       g.rng.Float64(),
		Active:      g.rng.IntN(4) > 0,
		CreatedAtMs: g.timestamp(i),
		Tags:        dedupe([]string{pick(g.rng, categories), pick(g.rng, labelNames)}),
		Attrs: map[string]string{
			"source": pick(g.rng, categories),
			"batch":  fmt.Sprintf("b%d", i/500),
			"region": pick(g.rng, regions),
		},
	}
}

// searchMarkers is the text an issue body carries for the search benchmarks:
// terms whose SELECTIVITY is a property of the corpus rather than of the
// filler, which is what makes a search number interpretable. The filler words
// are drawn from a small vocabulary and so occur in nearly every row — the
// only selectivity they can express is "matches everything".
//
// Placement is by row index and consumes no randomness, so the markers land on
// the same rows for every seed, spread across the key space rather than
// clustered at one end.
//
//   - SearchRareTerm — RareTermRows rows, whatever the tier's size. The
//     selective search: what an index has to keep flat as a corpus grows.
//   - SearchMidTerm — one row in MidTermPer. Selective enough to be worth an
//     index, wide enough that the candidates dominate the walk.
//   - SearchCommonTerm — every row. The case no index can help with, and the
//     ceiling the other two are read against. Planted rather than taken from
//     the filler because a filler word occurs in MOST bodies, not all, and a
//     result set that drifts with the seed cannot be asserted on.
func (g *generator) searchMarkers(i int) string {
	var b strings.Builder
	b.WriteString(" " + SearchCommonTerm)
	if step := max(g.tier.Issues/RareTermRows, 1); i%step == 0 {
		b.WriteString(" " + SearchRareTerm)
	}
	if i%MidTermPer == 0 {
		b.WriteString(" " + SearchMidTerm)
	}
	return b.String()
}

// hotIssue picks the issue a comment hangs off, skewed so most comments land on
// the newest fifth. A uniform choice would give every issue the same handful of
// comments, and "load one issue's thread" — the reverse-follow a benchmark
// actually cares about — would cost the same whichever issue it picked.
func (g *generator) hotIssue() int {
	if g.tier.Issues == 0 {
		return 0
	}
	hot := max(g.tier.Issues/5, 1)
	if g.rng.IntN(10) < 7 {
		return g.tier.Issues - hot + g.rng.IntN(hot)
	}
	return g.rng.IntN(g.tier.Issues)
}

func (g *generator) someProject() *benchv1.Project {
	return &benchv1.Project{Id: id("project", g.rng.IntN(g.tier.Projects))}
}

func (g *generator) someUser() *benchv1.User {
	return &benchv1.User{Id: id("user", g.rng.IntN(g.tier.Users))}
}

// timestamp spreads rows over a synthetic year, ascending with the row index so
// that recency orderings and key order agree — a window ordered by
// updated_at_ms then holds a contiguous key range, which is the case worth
// measuring the maintenance of.
func (g *generator) timestamp(i int) int64 {
	return baseTimeMs + int64(i)*60_000 + int64(g.rng.IntN(60_000))
}

func (g *generator) title() string {
	var b strings.Builder
	for n := 3 + g.rng.IntN(5); n > 0; n-- {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(pick(g.rng, lorem))
	}
	return b.String()
}

// words builds roughly n characters of filler. Approximate on purpose: a body
// padded to an exact length would compress differently from real prose, and it
// is the order of magnitude that the payload dial is setting.
func (g *generator) words(n int) string {
	var b strings.Builder
	b.Grow(n + 8)
	for b.Len() < n {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(pick(g.rng, lorem))
	}
	return b.String()
}

// IssueKey is the row key of the i-th seeded issue. Exported because the
// benchserver's write endpoint addresses seeded rows by key: a caller building
// the key itself would, if the format ever drifted, silently create new rows
// where it meant to touch existing ones and still look like it worked.
func IssueKey(i int) string { return id("issue", i) }

// id is zero-padded so lexical row-key order matches numeric order. Row keys
// order the store, so unpadded ids would interleave issue-9 between issue-89
// and issue-90 and quietly scramble every range read the corpus is meant to
// make contiguous.
func id(prefix string, i int) string {
	return fmt.Sprintf("%s-%08d", prefix, i)
}

func pick(rng *rand.Rand, from []string) string {
	return from[rng.IntN(len(from))]
}

func dedupe(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:0]
	seen := make(map[string]struct{}, len(in))
	for _, s := range in {
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// The search markers, and how many rows carry them. Exported because the
// benchmark specs search for them by name and assert the row counts they get
// back: a search whose result set moved is measuring a different query.
const (
	SearchRareTerm   = "zygote"
	SearchMidTerm    = "quasar"
	SearchCommonTerm = "telemetry"
	// RareTermRows is fixed across tiers, so the rare search returns the same
	// rows from a thousand-row corpus and from a half-million-row one.
	RareTermRows = 10
	MidTermPer   = 100
)

var (
	issueStates   = []string{"open", "open", "open", "in_progress", "review", "done", "closed"}
	projectStates = []string{"active", "active", "archived"}
	categories    = []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	regions       = []string{"eu", "us", "apac"}
	labelNames    = []string{
		"bug", "feature", "chore", "docs", "perf", "security",
		"regression", "ux", "infra", "flaky",
	}
	firstNames = []string{"Ada", "Grace", "Alan", "Edsger", "Barbara", "Ken", "Radia", "Leslie"}
	lastNames  = []string{"Lovelace", "Hopper", "Turing", "Dijkstra", "Liskov", "Thompson", "Perlman", "Lamport"}
	lorem      = []string{
		"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
		"sed", "eiusmod", "tempor", "incididunt", "labore", "magna", "aliqua", "enim",
		"minim", "veniam", "quis", "nostrud", "exercitation", "ullamco", "laboris",
		"nisi", "aliquip", "commodo", "consequat", "duis", "aute", "irure", "velit",
	}
)
