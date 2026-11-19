package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/levelfourab/syncplane/engine/storage"
)

// TestCorpus replays the stored state of an existing store file into a new
// Store and measures the time of a load, of single-row edits and of common
// reads, and the size of the file the load leaves. It runs only when
// SQLITESTORE_CORPUS names a store file; the file is opened read-only and is
// not changed. CORPUS_RUNS sets the number of runs (default 3), and each phase
// prints the median of the runs and their range.
//
// The replay writes what the file holds, through the adapter: every row with
// its version, value and actor, every index entry from the per-row entry
// lists, every search posting, and every consumer shadow. It does not need the
// schema of the application that wrote the file, because it does not compute
// entries; it copies them.
//
//	SQLITESTORE_CORPUS=/path/to/store.db go test ./engine/storage/sqlitestore -run TestCorpus -v
func TestCorpus(t *testing.T) {
	path := os.Getenv("SQLITESTORE_CORPUS")
	if path == "" {
		t.Skip("SQLITESTORE_CORPUS is not set")
	}
	runs := 3
	if v := os.Getenv("CORPUS_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("CORPUS_RUNS=%q", v)
		}
		runs = n
	}
	c, err := loadCorpus(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("corpus: %d workspaces, %d rows, %d entries, %d postings, %d shadows",
		len(c.workspaces), c.rowCount(), c.entryCount(), c.postingCount(), c.shadowCount())

	phases := map[string][]time.Duration{}
	var sizes []int64
	var lastDir string
	for run := range runs {
		dir := t.TempDir()
		got, size, err := c.run(filepath.Join(dir, "corpus.db"), uint64(run))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range got {
			phases[k] = append(phases[k], v)
		}
		sizes = append(sizes, size)
		lastDir = dir
	}
	names := make([]string, 0, len(phases))
	for k := range phases {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		d := phases[k]
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		t.Logf("phase %-8s median %8.1f ms  range %8.1f .. %8.1f ms", k,
			ms(d[len(d)/2]), ms(d[0]), ms(d[len(d)-1]))
	}
	t.Logf("file size %d bytes (%.2f MiB)", sizes[0], float64(sizes[0])/(1<<20))
	if err := logObjectSizes(t, filepath.Join(lastDir, "corpus.db")); err != nil {
		t.Fatal(err)
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// corpus is the state of a store file, held in memory.
type corpus struct {
	workspaces []*corpusWorkspace
}

type corpusWorkspace struct {
	id   string
	rows []corpusRow
	// postings maps entity, then index, to the posting keys of each row.
	postings map[string]map[string]map[string][][]byte
	// shadows maps a consumer to its snapshots.
	shadows map[string][]corpusShadow
	// indexes names the indexes of each entity that hold entries.
	indexes map[string][]string
}

type corpusRow struct {
	entity, key string
	value       []byte
	version     uint64
	deleted     bool
	actor       storage.Actor
	entries     []storage.IndexEntry
}

type corpusShadow struct {
	entity, key string
	snapshot    []byte
}

func (c *corpus) rowCount() (n int) {
	for _, w := range c.workspaces {
		n += len(w.rows)
	}
	return n
}

func (c *corpus) entryCount() (n int) {
	for _, w := range c.workspaces {
		for _, r := range w.rows {
			n += len(r.entries)
		}
	}
	return n
}

func (c *corpus) postingCount() (n int) {
	for _, w := range c.workspaces {
		for _, byIdx := range w.postings {
			for _, byKey := range byIdx {
				for _, ks := range byKey {
					n += len(ks)
				}
			}
		}
	}
	return n
}

func (c *corpus) shadowCount() (n int) {
	for _, w := range c.workspaces {
		for _, s := range w.shadows {
			n += len(s)
		}
	}
	return n
}

// loadCorpus reads the state of the store file at path.
func loadCorpus(path string) (*corpus, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	byID := map[string]*corpusWorkspace{}
	ws := func(id string) *corpusWorkspace {
		w, ok := byID[id]
		if !ok {
			w = &corpusWorkspace{
				id:       id,
				postings: map[string]map[string]map[string][][]byte{},
				shadows:  map[string][]corpusShadow{},
				indexes:  map[string][]string{},
			}
			byID[id] = w
		}
		return w
	}

	scopes := map[int64][2]string{} // scope id → entity, index
	scopeWS := map[int64]string{}
	err = each(db, `SELECT id, workspace_id, entity, idx FROM index_scopes`, func(r *sql.Rows) error {
		var id int64
		var w, e, i string
		if err := r.Scan(&id, &w, &e, &i); err != nil {
			return err
		}
		scopes[id] = [2]string{e, i}
		scopeWS[id] = w
		ws(w).indexes[e] = append(ws(w).indexes[e], i)
		return nil
	})
	if err != nil {
		return nil, err
	}

	type rowRef struct{ ws, entity, key string }
	entries := map[rowRef][]storage.IndexEntry{}
	err = each(db, `SELECT i.workspace_id, i.entity, i.key, e.entries
		FROM index_row_ids i JOIN index_row_entries e ON e.id = i.id`, func(r *sql.Rows) error {
		var ref rowRef
		var blob []byte
		if err := r.Scan(&ref.ws, &ref.entity, &ref.key, &blob); err != nil {
			return err
		}
		set, err := decodeRowEntries(ref.key, blob)
		if err != nil {
			return err
		}
		list := make([]storage.IndexEntry, 0, len(set))
		for e := range set {
			sc, ok := scopes[e.scope]
			if !ok {
				return fmt.Errorf("entry of unknown scope %d", e.scope)
			}
			list = append(list, storage.IndexEntry{Index: sc[1], Key: []byte(e.k)})
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Index != list[j].Index {
				return list[i].Index < list[j].Index
			}
			return string(list[i].Key) < string(list[j].Key)
		})
		entries[ref] = list
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = each(db, `SELECT workspace_id, entity, key, value, version, deleted, actor_subject, actor_system
		FROM rows ORDER BY workspace_id, version, entity, key`, func(r *sql.Rows) error {
		var row corpusRow
		var w string
		var version int64
		if err := r.Scan(&w, &row.entity, &row.key, &row.value, &version, &row.deleted,
			&row.actor.Subject, &row.actor.System); err != nil {
			return err
		}
		row.version = uint64(version)
		row.entries = entries[rowRef{w, row.entity, row.key}]
		ws(w).rows = append(ws(w).rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if err := loadPostings(db, ws); err != nil {
		return nil, err
	}

	err = each(db, `SELECT c.workspace_id, c.consumer, s.entity, s.key, s.snapshot
		FROM consumer_shadows s JOIN consumer_scopes c ON c.id = s.scope`, func(r *sql.Rows) error {
		var w, consumer string
		var s corpusShadow
		if err := r.Scan(&w, &consumer, &s.entity, &s.key, &s.snapshot); err != nil {
			return err
		}
		ws(w).shadows[consumer] = append(ws(w).shadows[consumer], s)
		return nil
	})
	if err != nil {
		return nil, err
	}

	c := &corpus{}
	for _, w := range byID {
		c.workspaces = append(c.workspaces, w)
	}
	sort.Slice(c.workspaces, func(i, j int) bool { return c.workspaces[i].id < c.workspaces[j].id })
	return c, nil
}

// loadPostings reads the posting keys of every row, from the heads with their
// chunks and from the pages.
func loadPostings(db *sql.DB, ws func(string) *corpusWorkspace) error {
	type scopeName struct{ ws, entity, idx string }
	scopes := map[int64]scopeName{}
	err := each(db, `SELECT id, workspace_id, entity, idx FROM posting_scopes`, func(r *sql.Rows) error {
		var id int64
		var s scopeName
		if err := r.Scan(&id, &s.ws, &s.entity, &s.idx); err != nil {
			return err
		}
		scopes[id] = s
		return nil
	})
	if err != nil {
		return err
	}
	keys := map[[2]int64]string{}
	err = each(db, `SELECT scope, num, key FROM posting_nums`, func(r *sql.Rows) error {
		var scope, num int64
		var key string
		if err := r.Scan(&scope, &num, &key); err != nil {
			return err
		}
		keys[[2]int64{scope, num}] = key
		return nil
	})
	if err != nil {
		return err
	}
	chunks := map[int64][]byte{}
	err = each(db, `SELECT id, nums FROM posting_chunks`, func(r *sql.Rows) error {
		var id int64
		var b []byte
		if err := r.Scan(&id, &b); err != nil {
			return err
		}
		chunks[id] = b
		return nil
	})
	if err != nil {
		return err
	}
	add := func(scope int64, k []byte, nums []int64) error {
		s, ok := scopes[scope]
		if !ok {
			return fmt.Errorf("posting of unknown scope %d", scope)
		}
		w := ws(s.ws)
		byIdx := w.postings[s.entity]
		if byIdx == nil {
			byIdx = map[string]map[string][][]byte{}
			w.postings[s.entity] = byIdx
		}
		byKey := byIdx[s.idx]
		if byKey == nil {
			byKey = map[string][][]byte{}
			byIdx[s.idx] = byKey
		}
		for _, n := range nums {
			key, ok := keys[[2]int64{scope, n}]
			if !ok {
				return fmt.Errorf("posting names row number %d of scope %d, which no row holds", n, scope)
			}
			byKey[key] = append(byKey[key], append([]byte(nil), k...))
		}
		return nil
	}
	err = each(db, `SELECT scope, k, dir, tail FROM posting_heads`, func(r *sql.Rows) error {
		var scope int64
		var k, dir, tail []byte
		if err := r.Scan(&scope, &k, &dir, &tail); err != nil {
			return err
		}
		cs, err := decodeDir(dir)
		if err != nil {
			return err
		}
		var nums []int64
		for _, c := range cs {
			if nums, err = decodeChunk(c, chunks[c.id], nums); err != nil {
				return err
			}
		}
		if nums, err = decodeNums(tail, nums); err != nil {
			return err
		}
		return add(scope, k, nums)
	})
	if err != nil {
		return err
	}
	return each(db, `SELECT scope, body FROM posting_pages`, func(r *sql.Rows) error {
		var scope int64
		var body []byte
		if err := r.Scan(&scope, &body); err != nil {
			return err
		}
		var addErr error
		err := walkPage(body, func(k []byte, _ int, coded []byte) bool {
			nums, err := decodeNums(coded, nil)
			if err == nil {
				err = add(scope, k, nums)
			}
			addErr = err
			return err == nil
		})
		if err != nil {
			return err
		}
		return addErr
	})
}

func each(db *sql.DB, query string, fn func(*sql.Rows) error) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// corpusBatch is the number of rows the load writes per transaction.
const corpusBatch = 200

// run replays the corpus into a new store at path and times each phase. It
// returns the duration of each phase and the size of the file after Close.
func (c *corpus) run(path string, seed uint64) (map[string]time.Duration, int64, error) {
	ctx := context.Background()
	store, err := Open(path)
	if err != nil {
		return nil, 0, err
	}
	out := map[string]time.Duration{}
	timed := func(name string, fn func() error) error {
		start := time.Now()
		err := fn()
		out[name] += time.Since(start)
		return err
	}

	err = timed("load", func() error {
		for _, w := range c.workspaces {
			if err := c.load(ctx, store, w); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		store.Close()
		return nil, 0, err
	}

	rng := rand.New(rand.NewPCG(seed, 99))
	main := c.workspaces[0]
	for _, w := range c.workspaces {
		if len(w.rows) > len(main.rows) {
			main = w
		}
	}
	steps := []struct {
		name string
		fn   func() error
	}{
		{"edit", func() error { return c.edit(ctx, store, main, rng) }},
		{"page", func() error { return c.readPages(ctx, store, main, rng) }},
		{"pull", func() error { return c.pull(ctx, store, main) }},
		{"point", func() error { return c.points(ctx, store, main, rng) }},
		{"search", func() error { return c.search(ctx, store, main, rng) }},
	}
	for _, s := range steps {
		if err := timed(s.name, s.fn); err != nil {
			store.Close()
			return nil, 0, fmt.Errorf("%s: %w", s.name, err)
		}
	}
	if err := store.Close(); err != nil {
		return nil, 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	return out, fi.Size(), nil
}

// load writes the rows of w in version order, corpusBatch rows per
// transaction, with their entries and postings, then the shadows, then marks
// every index ready.
func (c *corpus) load(ctx context.Context, store *Store, w *corpusWorkspace) error {
	for start := 0; start < len(w.rows); start += corpusBatch {
		batch := w.rows[start:min(start+corpusBatch, len(w.rows))]
		err := store.WithWriteTx(ctx, w.id, func(tx storage.WriteTx) error {
			for _, r := range batch {
				if r.deleted {
					if err := tx.DeleteRow(r.entity, r.key, r.version, r.actor); err != nil {
						return err
					}
					continue
				}
				if err := tx.PutRow(r.entity, r.key, r.value, r.version, r.actor); err != nil {
					return err
				}
				if len(r.entries) > 0 {
					if err := tx.PutIndexEntries(r.entity, r.key, r.entries); err != nil {
						return err
					}
				}
				for idx, byKey := range w.postings[r.entity] {
					if ks := byKey[r.key]; len(ks) > 0 {
						if err := tx.PutPostings(r.entity, idx, r.key, nil, ks); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	for consumer, shadows := range w.shadows {
		for start := 0; start < len(shadows); start += corpusBatch {
			batch := shadows[start:min(start+corpusBatch, len(shadows))]
			err := store.WithWriteTx(ctx, w.id, func(tx storage.WriteTx) error {
				for _, s := range batch {
					if err := tx.PutConsumerShadow(consumer, s.entity, s.key, s.snapshot); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return store.WithWriteTx(ctx, w.id, func(tx storage.WriteTx) error {
		for entity, idxs := range w.indexes {
			for _, idx := range idxs {
				if err := tx.SetIndexState(entity, idx, "corpus", true); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// corpusEdits is the number of single-row edits the edit phase makes.
const corpusEdits = 2000

// edit rewrites random live rows one transaction each, the way a client's
// pushed PATCH arrives: a new version and value, the same entries, and the
// shadow of the row replaced.
func (c *corpus) edit(ctx context.Context, store *Store, w *corpusWorkspace, rng *rand.Rand) error {
	live := liveRows(w)
	var version uint64
	for _, r := range w.rows {
		version = max(version, r.version)
	}
	for range corpusEdits {
		r := live[rng.IntN(len(live))]
		version++
		value := append(append([]byte(nil), r.value...), byte(version))
		err := store.WithWriteTx(ctx, w.id, func(tx storage.WriteTx) error {
			if _, err := tx.NextSeq(); err != nil {
				return err
			}
			if err := tx.PutRow(r.entity, r.key, value, version, r.actor); err != nil {
				return err
			}
			if err := tx.PutIndexEntries(r.entity, r.key, r.entries); err != nil {
				return err
			}
			for consumer := range w.shadows {
				if err := tx.PutConsumerShadow(consumer, r.entity, r.key, value[:min(len(value), 12)]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func liveRows(w *corpusWorkspace) []corpusRow {
	var out []corpusRow
	for _, r := range w.rows {
		if !r.deleted && len(r.entries) > 0 {
			out = append(out, r)
		}
	}
	return out
}

// corpusPageReads is the number of index-range reads the page phase makes.
const corpusPageReads = 1000

// readPages picks random entries of random indexes and reads the rows that
// share the entry's value: the range of entries that start with the entry
// minus its row key, then the rows those entries name. It is the read that
// opens a list of children, or every row with one field value.
func (c *corpus) readPages(ctx context.Context, store *Store, w *corpusWorkspace, rng *rand.Rand) error {
	live := liveRows(w)
	for range corpusPageReads {
		r := live[rng.IntN(len(live))]
		e := r.entries[rng.IntN(len(r.entries))]
		prefix := e.Key
		if endsWithRowKey(string(prefix), r.key) {
			prefix = prefix[:len(prefix)-len(rowKeySuffix(r.key))]
		}
		hi := append(append([]byte(nil), prefix...), 0xff)
		err := store.WithReadTx(ctx, w.id, func(tx storage.ReadTx) error {
			var keys []string
			err := tx.ScanIndex(r.entity, e.Index, prefix, hi, 500, storage.ScanAsc, func(_ []byte, key string) error {
				keys = append(keys, key)
				return nil
			})
			if err != nil {
				return err
			}
			return tx.GetRows(r.entity, keys, func(storage.Row) error { return nil })
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// pull reads every change of w, 500 rows per transaction, the way a client
// bootstraps.
func (c *corpus) pull(ctx context.Context, store *Store, w *corpusWorkspace) error {
	var seq uint64
	var entity, key string
	for {
		n := 0
		err := store.WithReadTx(ctx, w.id, func(tx storage.ReadTx) error {
			return tx.ScanChanges(seq, entity, key, 500, func(r storage.Row) error {
				seq, entity, key = r.Version, r.Entity, r.Key
				n++
				return nil
			})
		})
		if err != nil {
			return err
		}
		if n < 500 {
			return nil
		}
	}
}

// corpusPoints is the number of single-row reads the point phase makes, in
// transactions of corpusPointBatch.
const (
	corpusPoints     = 20000
	corpusPointBatch = 20
)

func (c *corpus) points(ctx context.Context, store *Store, w *corpusWorkspace, rng *rand.Rand) error {
	for i := 0; i < corpusPoints; i += corpusPointBatch {
		err := store.WithReadTx(ctx, w.id, func(tx storage.ReadTx) error {
			for range corpusPointBatch {
				r := w.rows[rng.IntN(len(w.rows))]
				if _, err := tx.GetRow(r.entity, r.key); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// corpusSearches is the number of searches the search phase makes.
const corpusSearches = 500

// search looks up the rows that hold one random posting key of one random
// row, then the rows that hold two.
func (c *corpus) search(ctx context.Context, store *Store, w *corpusWorkspace, rng *rand.Rand) error {
	type target struct {
		entity, idx string
		keys        [][]byte
	}
	var targets []target
	for entity, byIdx := range w.postings {
		for idx, byKey := range byIdx {
			for _, ks := range byKey {
				if len(ks) >= 2 {
					targets = append(targets, target{entity, idx, ks})
				}
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	sort.Slice(targets, func(i, j int) bool {
		a, b := targets[i], targets[j]
		if a.entity != b.entity {
			return a.entity < b.entity
		}
		return string(a.keys[0]) < string(b.keys[0])
	})
	exact := func(k []byte) storage.IndexRange {
		return storage.IndexRange{Lo: k, Hi: append(append([]byte(nil), k...), 0)}
	}
	for range corpusSearches {
		tg := targets[rng.IntN(len(targets))]
		a := tg.keys[rng.IntN(len(tg.keys))]
		b := tg.keys[rng.IntN(len(tg.keys))]
		err := store.WithReadTx(ctx, w.id, func(tx storage.ReadTx) error {
			if err := tx.ScanPostings(tg.entity, tg.idx, []storage.IndexRange{exact(a)}, nil, 50,
				func(string) error { return nil }); err != nil {
				return err
			}
			return tx.ScanPostings(tg.entity, tg.idx, []storage.IndexRange{exact(a), exact(b)}, nil, 50,
				func(string) error { return nil })
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// logObjectSizes prints the bytes of each table and index of the file at
// path, largest first.
func logObjectSizes(t *testing.T, path string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var lines []string
	err = each(db, `SELECT name, SUM(pgsize), SUM(payload), SUM(unused) FROM dbstat
		GROUP BY name ORDER BY SUM(pgsize) DESC LIMIT 20`, func(r *sql.Rows) error {
		var name string
		var size, payload, unused int64
		if err := r.Scan(&name, &size, &payload, &unused); err != nil {
			return err
		}
		lines = append(lines, fmt.Sprintf("  %-34s %10d  payload %10d  unused %9d", name, size, payload, unused))
		return nil
	})
	if err != nil {
		return err
	}
	t.Log("objects:\n" + strings.Join(lines, "\n"))
	return nil
}
