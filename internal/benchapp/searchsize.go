package benchapp

import (
	"context"
	"encoding/base32"
	"math/rand/v2"
	"strings"

	"google.golang.org/protobuf/proto"

	benchv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/bench/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// The text-heavy search corpus. Its parameters and its PRNG draws are the same
// as the SQLite size fixture in engine/storage/sqlitestore/searchsize_test.go,
// so a title or a body here is word for word the one that fixture writes, and
// a client-side size compares with the server-side one. Only the keys differ:
// these are shaped like client ids (see SearchSizeKey).
const (
	// SearchSizeIssues is the number of issues SeedSearchSize writes.
	SearchSizeIssues = 2000
	// SearchSizeVocab is the number of distinct words the bodies draw from.
	SearchSizeVocab = 20000
	// SearchSizeBodyWords is the length of one issue body, in words.
	SearchSizeBodyWords = 300
	// SearchSizeTitleWords is the length of one issue title, in words.
	SearchSizeTitleWords = 8
	// SearchSizeProjects and SearchSizeUsers are the rows the issues refer to.
	SearchSizeProjects = 20
	SearchSizeUsers    = 50
)

// searchSizeBaseMs is the timestamp of the first key SearchSizeKey makes.
const searchSizeBaseMs int64 = 1_767_225_600_000 // 2026-01-01T00:00:00Z

// crockford is the encoding of client ids (client/src/id.ts, engine/id).
var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// SearchSizeWorkspace is the workspace SeedSearchSize writes into. It has the
// shape of a client id, the same as the row keys.
var SearchSizeWorkspace = SearchSizeKey(0, 0)

// SearchSizeKey returns the i-th key of a kind: a UUIDv7 in 26-character
// Crockford base32, the shape newId makes in the client. The timestamp
// increases with i and the random bits come from a PRNG seeded by kind and i,
// so the same arguments always give the same key.
func SearchSizeKey(kind, i int) string {
	rng := rand.New(rand.NewPCG(uint64(kind), uint64(i)))
	var b [16]byte
	for j := 6; j < 16; j++ {
		b[j] = byte(rng.Uint32())
	}
	ms := searchSizeBaseMs + int64(kind)*1_000_000 + int64(i)
	for j := 5; j >= 0; j-- {
		b[j] = byte(ms)
		ms >>= 8
	}
	b[6] = b[6]&0x0f | 0x70 // version 7
	b[8] = b[8]&0x3f | 0x80 // variant 10
	return crockford.EncodeToString(b[:])
}

// searchSizeVocabulary makes n pronounceable words of two to five syllables.
// It must draw from rng exactly as searchVocab in the SQLite size fixture.
func searchSizeVocabulary(rng *rand.Rand, n int) []string {
	syl := []string{"ka", "lo", "mi", "ne", "ru", "sa", "ti", "vo", "ze", "pra",
		"stel", "dor", "fin", "gal", "hus", "jen", "kor", "lin", "mor", "nes",
		"pol", "qui", "ras", "sor", "tan", "ul", "ver", "wen", "xi", "yor"}
	seen := map[string]struct{}{}
	out := make([]string, 0, n)
	for len(out) < n {
		var b strings.Builder
		for s := 2 + rng.IntN(4); s > 0; s-- {
			b.WriteString(syl[rng.IntN(len(syl))])
		}
		w := b.String()
		if _, dup := seen[w]; dup {
			continue
		}
		seen[w] = struct{}{}
		out = append(out, w)
	}
	return out
}

// SeedSearchSize writes the text-heavy search corpus into workspaceID as
// trusted writes and builds the workspace's indexes: SearchSizeProjects
// projects, SearchSizeUsers users and SearchSizeIssues issues, each issue with
// a title and a body of Zipf-distributed words. The result is the same on
// every call. The workspace is assumed empty.
func SeedSearchSize(ctx context.Context, srv *server.Server, workspaceID string) (Counts, error) {
	ctx = server.Trusted(ctx)
	rng := rand.New(rand.NewPCG(7, 11))
	vocab := searchSizeVocabulary(rng, SearchSizeVocab)
	zipf := rand.NewZipf(rng, 1.07, 2, SearchSizeVocab-1)
	text := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(vocab[zipf.Uint64()])
		}
		return b.String()
	}

	write := func(batch []proto.Message) error {
		return srv.Mutate(ctx, workspaceID, func(tx *server.EntityTx) error {
			for _, msg := range batch {
				if err := tx.Put(msg); err != nil {
					return err
				}
			}
			return nil
		})
	}

	var dims []proto.Message
	for i := 0; i < SearchSizeProjects; i++ {
		dims = append(dims, &benchv1.Project{Id: SearchSizeKey(1, i), Name: "project", Status: "active"})
	}
	for i := 0; i < SearchSizeUsers; i++ {
		dims = append(dims, &benchv1.User{Id: SearchSizeKey(2, i), Name: "user"})
	}
	if err := write(dims); err != nil {
		return Counts{}, err
	}
	// Built before each write, in the fixture's draw order (title, then
	// body, issue by issue): a storage retry re-runs the write closure, and
	// drawing inside it would change the corpus.
	for start := 0; start < SearchSizeIssues; start += 200 {
		batch := make([]proto.Message, 0, 200)
		for i := start; i < start+200; i++ {
			batch = append(batch, &benchv1.Issue{
				Id:          SearchSizeKey(3, i),
				Title:       text(SearchSizeTitleWords),
				Body:        text(SearchSizeBodyWords),
				Project:     &benchv1.Project{Id: SearchSizeKey(1, i%SearchSizeProjects)},
				Reporter:    &benchv1.User{Id: SearchSizeKey(2, i%SearchSizeUsers)},
				State:       "open",
				Priority:    int32(i % 4),
				UpdatedAtMs: int64(i),
			})
		}
		if err := write(batch); err != nil {
			return Counts{}, err
		}
	}
	if err := srv.BuildIndexes(ctx, workspaceID); err != nil {
		return Counts{}, err
	}
	return Counts{
		Projects: SearchSizeProjects,
		Users:    SearchSizeUsers,
		Issues:   SearchSizeIssues,
		Total:    SearchSizeProjects + SearchSizeUsers + SearchSizeIssues,
	}, nil
}
