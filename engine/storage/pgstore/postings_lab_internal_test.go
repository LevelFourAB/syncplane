package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// TestPostingsLab measures the posting chunks against a real Postgres with a
// search corpus: Zipf 1.07 over a 20,000-word vocabulary, an 8-word title and
// a 300-word body per document, at most 256 distinct terms per document, and
// 1% more documents with 2,000-word bodies, whose terms the engine replaces
// with one overflow key. It prints the size per relation and per posting, WAL
// bytes and median latency per write, and latency, round trips and buffers
// per read.
//
//	PGSTORE_POSTINGS_LAB=20000 SYNCPLANE_TEST_PG_DSN=... go test ./engine/storage/pgstore -run TestPostingsLab -v -timeout 0
//
// The value is the number of documents. PGSTORE_POSTINGS_CHUNK sets the most
// row numbers per chunk, PGSTORE_POSTINGS_PARTS the WithPartitions modulus,
// and PGSTORE_POSTINGS_WORKSPACES spreads the documents over that many
// workspaces, which then hold one posting index each; the probes read the
// first of them. The server should run with autovacuum off, so that
// dead tuples after the churn phase are counted and not removed, and the test
// needs the pgstattuple extension and CHECKPOINT, so a superuser.
func TestPostingsLab(t *testing.T) {
	docs, _ := strconv.Atoi(os.Getenv("PGSTORE_POSTINGS_LAB"))
	dsn := os.Getenv(pgtest.DSNEnv)
	if docs <= 0 || dsn == "" {
		t.Skipf("set PGSTORE_POSTINGS_LAB and %s", pgtest.DSNEnv)
	}
	if v, _ := strconv.Atoi(os.Getenv("PGSTORE_POSTINGS_CHUNK")); v > 0 {
		was := postingChunkMax
		postingChunkMax = v
		t.Cleanup(func() { postingChunkMax = was })
	}
	ctx := context.Background()
	schema := fmt.Sprintf("postings_lab_%d", time.Now().UnixNano())
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mustExec(`CREATE EXTENSION IF NOT EXISTS pgstattuple`)
	mustExec(`CREATE SCHEMA ` + schema)
	if os.Getenv("PGSTORE_POSTINGS_KEEP") == "" {
		defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	opts := "-c search_path=" + schema
	if os.Getenv("PGSTORE_POSTINGS_GENERIC") != "" {
		// Every statement runs its generic plan, as pgx's statement cache
		// gets after five runs, made with the statistics of empty tables.
		opts += " -c plan_cache_mode=force_generic_plan"
	}
	q.Set("options", opts)
	u.RawQuery = q.Encode()
	var opts2 []Option
	if parts, _ := strconv.Atoi(os.Getenv("PGSTORE_POSTINGS_PARTS")); parts > 0 {
		opts2 = append(opts2, WithPartitions(parts))
	}
	store, err := Open(ctx, u.String(), opts2...)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	tracer := &tripTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	traced := &Store{pool: pool, scopes: newScopeCache()}

	workspaces := max(1, atoiEnv("PGSTORE_POSTINGS_WORKSPACES"))
	w := buildLabWorkload(docs, workspaces)
	fmt.Printf("corpus: %d docs in %d workspaces, %d postings, %d overflowed\n", docs, workspaces, w.postings, w.overflowed)
	walNow := func() int64 {
		var n int64
		if err := admin.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0')::int8`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	tables := []string{"posting_chunks", "posting_rows", "posting_scopes"}
	vacuum := func() {
		for _, tb := range tables {
			mustExec(`VACUUM (ANALYZE) ` + schema + `.` + tb)
		}
		mustExec(`CHECKPOINT`)
	}
	sizes := func(label string) {
		rows, err := admin.Query(ctx, `SELECT c.relname, pg_relation_size(c.oid),
				CASE WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname LIKE 'posting%' AND c.relkind IN ('r', 'i')
			  AND NOT EXISTS (SELECT 1 FROM pg_partitioned_table p WHERE p.partrelid = c.oid)
			ORDER BY 1`, schema)
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		var parts, order []string
		byRel := map[string]int64{}
		for rows.Next() {
			var (
				name        string
				main, toast int64
			)
			if err := rows.Scan(&name, &main, &toast); err != nil {
				t.Fatal(err)
			}
			total += main + toast
			name = partSuffix.ReplaceAllString(name, "")
			if _, ok := byRel[name]; !ok {
				order = append(order, name)
			}
			byRel[name] += main + toast
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		for _, name := range order {
			parts = append(parts, fmt.Sprintf("%s=%.2fMB", name, float64(byRel[name])/(1<<20)))
		}
		fmt.Printf("%s size: %.2f MB, %.2f B/posting (%s)\n", label, float64(total)/(1<<20),
			float64(total)/float64(w.postings), strings.Join(parts, " "))
	}
	write := func(ops []labOp) {
		if err := traced.WithWriteTx(ctx, ops[0].ws, func(tx storage.WriteTx) error {
			for _, o := range ops {
				var err error
				if o.del {
					err = tx.DeletePostings(labEntity, labIndex, o.key, o.remove)
				} else {
					err = tx.PutPostings(labEntity, labIndex, o.key, o.remove, o.add)
				}
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	mustExec(`CHECKPOINT`)
	wal0, t0 := walNow(), time.Now()
	tracer.reset()
	for _, tx := range w.load {
		write(tx)
	}
	fmt.Printf("load: %v, WAL %.1f MB, %.1f round trips per transaction of 200 rows\n", time.Since(t0).Round(time.Millisecond),
		float64(walNow()-wal0)/(1<<20), float64(tracer.trips())/float64(len(w.load)))
	vacuum()
	sizes("load")

	probes(t, ctx, traced, tracer, admin, w)

	for _, kind := range []string{"insert", "replace3", "replaceAll", "delete"} {
		mustExec(`CHECKPOINT`)
		walA := walNow()
		planning := writePlanning(t, ctx, traced, tracer, w.incr[kind][0])
		var lat []time.Duration
		tracer.reset()
		for _, o := range w.incr[kind] {
			s := time.Now()
			write([]labOp{o})
			lat = append(lat, time.Since(s))
		}
		fmt.Printf("%-10s median %v, p95 %v, WAL %.1f KB/op, %d round trips/op, planning %v/op\n", kind, median(lat), pct(lat, 0.95),
			float64(walNow()-walA)/float64(len(lat))/1024, tracer.trips()/len(lat), planning)
	}
	mustExec(`CHECKPOINT`)
	walA := walNow()
	var lat []time.Duration
	for _, o := range w.churn {
		s := time.Now()
		write([]labOp{o})
		lat = append(lat, time.Since(s))
	}
	fmt.Printf("churn: %d ops, median %v, WAL %.1f MB\n", len(lat), median(lat), float64(walNow()-walA)/(1<<20))
	mustExec(`CHECKPOINT`)
	for _, tb := range tables {
		var live, dead int64
		if err := admin.QueryRow(ctx, `SELECT COALESCE(sum((pgstattuple(c.oid)).tuple_count), 0)::int8,
				COALESCE(sum((pgstattuple(c.oid)).dead_tuple_count), 0)::int8
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind = 'r' AND (c.relname = $2 OR c.relname ~ ('^' || $2 || '_p[0-9]+$'))`,
			schema, tb).Scan(&live, &dead); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("after churn %s: %d live, %d dead tuples\n", tb, live, dead)
	}
	var hot, upd int64
	if err := admin.QueryRow(ctx, `SELECT sum(n_tup_hot_upd)::int8, sum(n_tup_upd)::int8 FROM pg_stat_user_tables
		WHERE schemaname = $1 AND (relname = 'posting_chunks' OR relname ~ '^posting_chunks_p[0-9]+$')`,
		schema).Scan(&hot, &upd); err == nil {
		fmt.Printf("posting_chunks updates: %d, of them HOT: %d\n", upd, hot)
	}
	sizes("churn")
	vacuum()
	sizes("vacuum")
}

const (
	labWS     = "0b7c6a2e-4f1d-4e8a-9c3b-5d2e1f0a9b8c"
	labEntity = "issue"
	labIndex  = "__search"
)

type labOp struct {
	ws, key     string
	del         bool
	remove, add [][]byte
}

type labWorkload struct {
	probeWS              string
	postings, overflowed int
	load                 [][]labOp
	incr                 map[string][]labOp
	churn                []labOp
	rare, common         []string
	prefixes             []string
	triples              [][3]string
	pairs                [][2]string
}

func labKey(term string) []byte {
	if term == "" {
		return []byte{0x02}
	}
	return append(append([]byte{0x01}, term...), 0x00, 0x00)
}

func labKeys(terms []string) [][]byte {
	out := make([][]byte, len(terms))
	for i, t := range terms {
		out[i] = labKey(t)
	}
	return out
}

func atoiEnv(name string) int {
	n, _ := strconv.Atoi(os.Getenv(name))
	return n
}

var partSuffix = regexp.MustCompile(`_p[0-9]+`)

func buildLabWorkload(n, workspaces int) *labWorkload {
	wsName := func(i int) string {
		if workspaces == 1 {
			return labWS
		}
		return fmt.Sprintf("ws-%03d", i%workspaces)
	}
	rng := rand.New(rand.NewPCG(7, 11))
	syl := []string{"ka", "lo", "mi", "ne", "ru", "sa", "ti", "vo", "ze", "pra",
		"stel", "dor", "fin", "gal", "hus", "jen", "kor", "lin", "mor", "nes",
		"pol", "qui", "ras", "sor", "tan", "ul", "ver", "wen", "xi", "yor"}
	seen := map[string]bool{}
	var vocab []string
	for len(vocab) < 20000 {
		var b strings.Builder
		for s := 2 + rng.IntN(4); s > 0; s-- {
			b.WriteString(syl[rng.IntN(len(syl))])
		}
		if w := b.String(); !seen[w] {
			seen[w] = true
			vocab = append(vocab, w)
		}
	}
	zipf := rand.NewZipf(rng, 1.07, 2, 19999)
	words := func(k int, into []string) []string {
		for range k {
			into = append(into, vocab[zipf.Uint64()])
		}
		return into
	}
	// termsOf is the distinct terms of words, or the overflow key alone.
	termsOf := func(ws []string) []string {
		set := map[string]bool{}
		var out []string
		for _, w := range ws {
			if set[w] {
				continue
			}
			if len(set) == 256 {
				return []string{""}
			}
			set[w] = true
			out = append(out, w)
		}
		return out
	}
	w := &labWorkload{incr: map[string][]labOp{}, probeWS: wsName(0)}
	docs := map[string][]string{}
	wsOf := map[string]string{}
	pending := map[string][]labOp{}
	var wsOrder []string
	add := func(i int, key string, terms []string) {
		ws := wsName(i)
		docs[key], wsOf[key] = terms, ws
		w.postings += len(terms)
		if len(terms) == 1 && terms[0] == "" {
			w.overflowed++
		}
		if _, ok := pending[ws]; !ok {
			wsOrder = append(wsOrder, ws)
		}
		pending[ws] = append(pending[ws], labOp{ws: ws, key: key, add: labKeys(terms)})
		if len(pending[ws]) == 200 {
			w.load = append(w.load, pending[ws])
			pending[ws] = []labOp{}
		}
	}
	for i := range n {
		add(i, fmt.Sprintf("%08x-0000-4000-8000-%012x", 3, i), termsOf(words(300, words(8, nil))))
	}
	for i := range n / 100 {
		add(i, fmt.Sprintf("%08x-0000-4000-8000-%012x", 5, i), termsOf(words(2000, words(8, nil))))
	}
	for _, ws := range wsOrder {
		if len(pending[ws]) > 0 {
			w.load = append(w.load, pending[ws])
		}
	}

	counts := map[string]int{}
	for key, ts := range docs {
		if wsOf[key] != w.probeWS {
			continue
		}
		for _, t := range ts {
			if t != "" {
				counts[t]++
			}
		}
	}
	terms := make([]string, 0, len(counts))
	for t := range counts {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	// Rare is 8 to 12 rows in 20,000, scaled to the documents of the probed
	// workspace, and at least one row.
	perWS := max(1, n/workspaces)
	rareLo, rareHi := max(1, 8*perWS/20000), max(1, 12*perWS/20000)
	for _, t := range terms {
		if c := counts[t]; c >= rareLo && c <= rareHi && len(w.rare) < 40 {
			w.rare = append(w.rare, t)
		}
	}
	byCount := slices.Clone(terms)
	sort.SliceStable(byCount, func(i, j int) bool { return counts[byCount[i]] > counts[byCount[j]] })
	w.common = byCount[:10]
	pc := map[string]int{}
	for _, t := range terms {
		for _, k := range []int{3, 4} {
			if len(t) > k {
				pc[t[:k]]++
			}
		}
	}
	var pfx []string
	for p, c := range pc {
		if c >= 20 && c <= 60 {
			pfx = append(pfx, p)
		}
	}
	for lo := 19; len(pfx) < 20 && lo > 1; lo-- {
		for p, c := range pc {
			if c == lo {
				pfx = append(pfx, p)
			}
		}
	}
	sort.Strings(pfx)
	pr := rand.New(rand.NewPCG(5, 5))
	pr.Shuffle(len(pfx), func(i, j int) { pfx[i], pfx[j] = pfx[j], pfx[i] })
	w.prefixes = pfx[:min(20, len(pfx))]
	var mid []string
	midLo, midHi := max(2, 100*perWS/20000), max(3, 400*perWS/20000)
	for _, t := range terms {
		if c := counts[t]; c >= midLo && c <= midHi {
			mid = append(mid, t)
		}
	}
	for i := range 10 {
		w.triples = append(w.triples, [3]string{w.common[i%len(w.common)], mid[(i*37)%len(mid)], w.rare[i%len(w.rare)]})
		w.pairs = append(w.pairs, [2]string{w.common[i%len(w.common)], w.rare[i%len(w.rare)]})
	}

	// Writes after the load, from a generator of their own.
	r := rand.New(rand.NewPCG(99, 1))
	next := n
	keys := func() []string {
		ks := make([]string, 0, len(docs))
		for k := range docs {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return ks
	}
	fresh := func() []string {
		for {
			if ts := termsOf(words(300, words(8, nil))); len(ts) >= 150 {
				return ts
			}
		}
	}
	randomLive := func() string {
		ks := keys()
		for {
			k := ks[r.IntN(len(ks))]
			if docs[k][0] != "" {
				return k
			}
		}
	}
	diff := func(old, nu []string) (remove, add [][]byte) {
		o, n := map[string]bool{}, map[string]bool{}
		for _, t := range old {
			o[t] = true
		}
		for _, t := range nu {
			n[t] = true
			if !o[t] {
				add = append(add, labKey(t))
			}
		}
		for _, t := range old {
			if !n[t] {
				remove = append(remove, labKey(t))
			}
		}
		return remove, add
	}
	makers := map[string]func() labOp{
		"insert": func() labOp {
			key := fmt.Sprintf("%08x-0000-4000-8000-%012x", 4, next)
			next++
			docs[key] = fresh()
			wsOf[key] = wsName(next)
			return labOp{ws: wsOf[key], key: key, add: labKeys(docs[key])}
		},
		"replace3": func() labOp {
			key := randomLive()
			old := docs[key]
			nu := slices.Clone(old)
			for range 3 {
				j := r.IntN(len(nu))
				nu = slices.Delete(nu, j, j+1)
			}
			have := map[string]bool{}
			for _, t := range old {
				have[t] = true
			}
			for range 3 {
				for {
					t := vocab[r.IntN(len(vocab))]
					if !have[t] {
						have[t] = true
						nu = append(nu, t)
						break
					}
				}
			}
			docs[key] = nu
			remove, add := diff(old, nu)
			return labOp{ws: wsOf[key], key: key, remove: remove, add: add}
		},
		"replaceAll": func() labOp {
			key := randomLive()
			old := docs[key]
			have := map[string]bool{}
			for _, t := range old {
				have[t] = true
			}
			var nu []string
			for _, t := range fresh() {
				for have[t] {
					t = vocab[r.IntN(len(vocab))]
				}
				have[t] = true
				nu = append(nu, t)
			}
			docs[key] = nu
			remove, add := diff(old, nu)
			return labOp{ws: wsOf[key], key: key, remove: remove, add: add}
		},
		"delete": func() labOp {
			ks := keys()
			key := ks[r.IntN(len(ks))]
			old := docs[key]
			delete(docs, key)
			return labOp{ws: wsOf[key], key: key, del: true, remove: labKeys(old)}
		},
	}
	for _, kind := range []string{"insert", "replace3", "replaceAll", "delete"} {
		for range 200 {
			w.incr[kind] = append(w.incr[kind], makers[kind]())
		}
	}
	var kinds []string
	for range 2000 {
		kinds = append(kinds, "replace3")
	}
	for range 200 {
		kinds = append(kinds, "delete", "insert")
	}
	r.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
	for _, kind := range kinds {
		w.churn = append(w.churn, makers[kind]())
	}
	return w
}

func termRange(term string) storage.IndexRange {
	k := labKey(term)
	return storage.IndexRange{Lo: k, Hi: append(slices.Clone(k), 0)}
}

func probes(t *testing.T, ctx context.Context, store *Store, tracer *tripTracer, admin *pgx.Conn, w *labWorkload) {
	type probe struct {
		name string
		n    int
		run  func(tx storage.ReadTx, i int) (int, error)
	}
	scan := func(tx storage.ReadTx, all, anyR []storage.IndexRange, limit int) (int, error) {
		n := 0
		err := tx.ScanPostings(labEntity, labIndex, all, anyR, limit, func(string) error { n++; return nil })
		return n, err
	}
	list := []probe{
		{"rare", len(w.rare), func(tx storage.ReadTx, i int) (int, error) {
			return scan(tx, []storage.IndexRange{termRange(w.rare[i])}, nil, 0)
		}},
		{"common limit 65", len(w.common), func(tx storage.ReadTx, i int) (int, error) {
			return scan(tx, []storage.IndexRange{termRange(w.common[i])}, nil, 65)
		}},
		{"common all", len(w.common), func(tx storage.ReadTx, i int) (int, error) {
			return scan(tx, []storage.IndexRange{termRange(w.common[i])}, nil, 0)
		}},
		{"prefix", len(w.prefixes), func(tx storage.ReadTx, i int) (int, error) {
			lo := append([]byte{0x01}, w.prefixes[i]...)
			hi := slices.Clone(lo)
			hi[len(hi)-1]++
			return scan(tx, []storage.IndexRange{{Lo: lo, Hi: hi}}, nil, 0)
		}},
		{"overflow", 1, func(tx storage.ReadTx, i int) (int, error) {
			return scan(tx, []storage.IndexRange{termRange("")}, nil, 0)
		}},
		{"counts3", len(w.triples), func(tx storage.ReadTx, i int) (int, error) {
			tr := w.triples[i]
			c, _, err := tx.PostingCounts(labEntity, labIndex, []storage.IndexRange{termRange(tr[0]), termRange(tr[1]), termRange(tr[2])}, 0)
			return len(c), err
		}},
		{"all common+rare", len(w.pairs), func(tx storage.ReadTx, i int) (int, error) {
			p := w.pairs[i]
			return scan(tx, []storage.IndexRange{termRange(p[0]), termRange(p[1])}, nil, 0)
		}},
	}
	for _, p := range list {
		var lat []time.Duration
		trips, rows := 0, 0
		var stmts []tracedQuery
		for round := 0; round <= 5; round++ {
			for i := range p.n {
				err := store.WithReadTx(ctx, w.probeWS, func(tx storage.ReadTx) error {
					tracer.reset()
					s := time.Now()
					n, err := p.run(tx, i)
					if round > 0 {
						lat = append(lat, time.Since(s))
						rows += n
						trips = max(trips, tracer.trips())
					}
					if round == 1 && i == 0 {
						stmts = tracer.taken()
					}
					return err
				})
				if err != nil {
					t.Fatalf("%s: %v", p.name, err)
				}
			}
		}
		var (
			buffers  int64
			planning time.Duration
		)
		for _, s := range stmts {
			b, pt, err := explainBuffers(ctx, admin, store, s)
			if err != nil {
				t.Fatalf("%s: explain: %v", p.name, err)
			}
			buffers += b
			planning += pt
		}
		fmt.Printf("probe %-16s median %8v  round trips %d  rows %7.1f  buffers %d  planning %v\n", p.name, median(lat), trips,
			float64(rows)/float64(len(lat)), buffers, planning.Round(time.Microsecond))
	}
}

// explainBuffers runs one traced statement under EXPLAIN (ANALYZE, BUFFERS)
// on a connection of store, and returns the shared buffers it touched and
// the time it took to plan.
func explainBuffers(ctx context.Context, admin *pgx.Conn, store *Store, s tracedQuery) (int64, time.Duration, error) {
	return explainStmt(ctx, store.pool, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) `, s)
}

func explainStmt(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, prefix string, s tracedQuery) (int64, time.Duration, error) {
	var raw []byte
	if err := q.QueryRow(ctx, prefix+s.sql, s.args...).Scan(&raw); err != nil {
		return 0, 0, err
	}
	var out []struct {
		Plan     map[string]any `json:"Plan"`
		Planning float64        `json:"Planning Time"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || len(out) == 0 {
		return 0, 0, fmt.Errorf("plan: %v", err)
	}
	num := func(k string) int64 {
		v, _ := out[0].Plan[k].(float64)
		return int64(v)
	}
	return num("Shared Hit Blocks") + num("Shared Read Blocks"), time.Duration(out[0].Planning * float64(time.Millisecond)), nil
}

// writePlanning replays the posting statements of one write without
// committing them, and returns the time it takes to plan them.
func writePlanning(t *testing.T, ctx context.Context, store *Store, tracer *tripTracer, o labOp) time.Duration {
	var stmts []tracedQuery
	rollback := fmt.Errorf("roll back")
	err := store.WithWriteTx(ctx, o.ws, func(tx storage.WriteTx) error {
		var err error
		if o.del {
			err = tx.DeletePostings(labEntity, labIndex, o.key, o.remove)
		} else {
			err = tx.PutPostings(labEntity, labIndex, o.key, o.remove, o.add)
		}
		if err != nil {
			return err
		}
		pt := tx.(*pgTx)
		tracer.reset()
		if err := pt.resolvePostings(); err != nil {
			return err
		}
		stmts = append(tracer.taken(), postingQueuedStmts(pt.queued)...)
		return rollback
	})
	if err != rollback {
		t.Fatal(err)
	}
	var total time.Duration
	for _, s := range stmts {
		_, pt, err := explainStmt(ctx, store.pool, `EXPLAIN (SUMMARY, FORMAT JSON) `, s)
		if err != nil {
			t.Fatalf("explain %s: %v", s.sql, err)
		}
		total += pt
	}
	return total.Round(time.Microsecond)
}

func postingQueuedStmts(queued []queuedStmt) []tracedQuery {
	var out []tracedQuery
	for _, q := range queued {
		if strings.Contains(q.sql, "posting_") {
			out = append(out, tracedQuery{sql: q.sql, args: q.args})
		}
	}
	return out
}

type tracedQuery struct {
	sql  string
	args []any
}

// tripTracer counts round trips: each query sent alone and each batch is one.
// It keeps the posting statements it saw, to explain them later.
type tripTracer struct {
	mu    sync.Mutex
	n     int
	stmts []tracedQuery
}

func (tr *tripTracer) reset() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n, tr.stmts = 0, nil
}

func (tr *tripTracer) trips() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.n
}

func (tr *tripTracer) taken() []tracedQuery {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.stmts)
}

func (tr *tripTracer) keep(sql string, args []any) {
	if strings.Contains(sql, "posting_") {
		tr.stmts = append(tr.stmts, tracedQuery{sql: sql, args: args})
	}
}

func (tr *tripTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	tr.keep(d.SQL, d.Args)
	return ctx
}

func (tr *tripTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tr *tripTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	for _, q := range d.Batch.QueuedQueries {
		tr.keep(q.SQL, q.Arguments)
	}
	return ctx
}

func (tr *tripTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (tr *tripTracer) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

func median(d []time.Duration) time.Duration { return pct(d, 0.5) }

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	i := int(float64(len(s)-1) * p)
	return s[i].Round(time.Microsecond)
}
