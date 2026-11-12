package pgstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Posting indexes (see storage.WriteTx.PutPostings) are stored as packed
// pages and chunks rather than as one row per (posting key, row key) pair.
//
// posting_scopes gives each (workspace, entity, index) an int scope id from a
// sequence, a counter of row numbers and the number of rows posting_rows
// holds for the scope. posting_rows maps each row key of a
// scope to a row number. Row numbers are dense within the scope, so the gaps
// between the row numbers of one posting key stay small.
//
// Each posting key with postings is in exactly one of two tables. A key with
// few row numbers is in posting_pages, together with the keys next to it; a
// key with many is in posting_chunks, alone. A small scope thus holds its
// rare keys in a few tuples and not in one tuple per key.
//
// posting_pages holds the pages. One tuple (scope, lo, body) holds every
// page key of the scope from lo up to the lo of the next page, in the format
// of postings_page.go. No page key is below the lo of the first page: a key
// below every page goes into a new page with the empty key as lo. lo is not
// always a key of the page: a removal does not change it. A page that a write
// makes larger than postingPageBudget bytes splits into parts of about equal
// size, each part after the first with its first key as lo; a page of one
// key may be larger. A changed page smaller than a quarter of the budget
// merges into the page before it when the write read that page, which it does
// for a removal, and the two fit in the budget. A key longer than about the
// budget thus has a page of its own.
//
// A page key moves to posting_chunks when it has more than
// postingPageMaxRows row numbers. A key in posting_chunks moves back to a
// page when it has one chunk and fewer than half of postingPageMaxRows row
// numbers. The gap between the two limits stops a key that gains and loses
// one row from moving at each write.
//
// posting_chunks holds the other keys. One tuple (scope, k, chunk) holds the
// sorted row numbers of key k from chunk up to the next chunk of k, as
// unsigned varints of the difference to the previous number (the first one
// to chunk). n is their count. Every key that holds postings has a chunk 0, so
// each row number has a chunk: the one with the greatest chunk value not above
// it. Chunk 0 may be empty while the key has other chunks; no other chunk is.
// A chunk that a removal leaves small merges into the chunk before it.
//
// A key with two chunks or more also has a header tuple at chunk -1 (see
// postingHeader). Its n is the number of postings of the key, and its ids are
// the chunk values of the key, coded the same way. A key with one chunk has no
// header, and its chunk 0 carries the count. A count therefore reads one tuple
// per key, and a reader that intersects finds the chunks that can hold a
// candidate without reading the other chunks.
//
// Row numbers and scope ids are never used again: a scope's counter only goes
// up, and ClearPostings drops the scope, so the next write takes a new scope
// id from the sequence. A number therefore names one row key for as long as
// any chunk or page holds it. Row numbers are BIGINT; scope ids are INTEGER, and the
// sequence fails with an error when it runs out.
//
// Every statement on posting_rows, posting_chunks and posting_pages binds
// scope to equality as a parameter, so a partitioned layout, which hashes the
// three tables by scope, prunes it to one partition when it plans it, and
// also when it runs a generic plan. The scope id comes from the store's cache (see scopeCache),
// checked in the same batch. The pruning specs in
// postings_prune_internal_test.go hold every statement to one partition.

// postingHeader is the chunk value of a key's header tuple.
const postingHeader = -1

// postingChunkMax is the most row numbers one chunk holds, and
// postingChunkMaxBytes the most bytes its coded numbers take. Together they
// keep a chunk tuple below the size at which Postgres moves a value to TOAST,
// where every edit of the chunk would write the whole value again.
//
// 512 numbers made the table 5% smaller than 256 on a 20,000-document corpus
// and every write 5 to 10% more WAL, because an edit writes the whole chunk
// again. Writes run on every push, so the smaller chunk wins.
var (
	postingChunkMax      = 256
	postingChunkMaxBytes = 1024
)

// postingPageBudget is the most bytes a page body with two keys or more
// takes, and postingPageMaxRows the most row numbers a key in a page has.
//
// A page of 1,024 bytes keeps the tuple below the size at which Postgres
// moves a value to TOAST, and leaves room for several tuples on a heap page,
// so that the new version of a page an edit rewrites can stay on the same
// heap page as a HOT update. A larger page writes more bytes per edit; a
// smaller one costs more tuples and index entries.
var (
	postingPageBudget  = 1024
	postingPageMaxRows = 128
)

// postingKeyMax is an upper bound greater than every posting key: a key is at
// most storage.MaxIndexEntryBytes long, and this is one 0xff byte longer. It
// stands in for a nil Hi so that every range read is an index range.
var postingKeyMax = bytes.Repeat([]byte{0xff}, storage.MaxIndexEntryBytes+1)

// postingOp is one PutPostings or DeletePostings call that the transaction
// has not written yet. Its keys are copies the transaction owns.
type postingOp struct {
	entity, index, key string
	remove, add        [][]byte
	// forget marks a DeletePostings call: after remove, the row is forgotten.
	forget bool
}

// cloneKeys copies keys into one allocation, so the caller may reuse them.
func cloneKeys(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	size := 0
	for _, k := range keys {
		size += len(k)
	}
	buf := make([]byte, 0, size)
	out := make([][]byte, len(keys))
	for i, k := range keys {
		start := len(buf)
		buf = append(buf, k...)
		out[i] = buf[start:len(buf):len(buf)]
	}
	return out
}

// PutPostings and DeletePostings do not reach the server when they are called.
// A change to a chunk is a read, an edit in Go and a write, and a read per
// call would cost a round trip per row of a push. The calls go to postingLog
// instead, and resolvePostings writes every logged call of the transaction
// with one read: before COMMIT (see flush), and before a posting read of this
// transaction. Registration happens at the call, so that InSavepoint rewinds
// it with the rest of the call.
func (t *pgTx) PutPostings(entity, index, key string, remove, add [][]byte) error {
	if t.failed != nil {
		return t.failed
	}
	if len(add) > 0 {
		if err := t.register(); err != nil {
			return err
		}
	}
	if len(remove) == 0 && len(add) == 0 {
		return nil
	}
	t.postingLog = append(t.postingLog, postingOp{
		entity: entity, index: index, key: key,
		remove: cloneKeys(remove), add: cloneKeys(add),
	})
	return nil
}

func (t *pgTx) DeletePostings(entity, index, key string, held [][]byte) error {
	if t.failed != nil {
		return t.failed
	}
	t.postingLog = append(t.postingLog, postingOp{
		entity: entity, index: index, key: key,
		remove: cloneKeys(held), forget: true,
	})
	return nil
}

func (t *pgTx) ClearPostings(entity, index string) error {
	// The logged calls of this index go before the clear, and the clear makes
	// them void. Writing them first keeps the log a plain sequence for
	// InSavepoint to rewind; a rebuild clears once, so the cost is small.
	if err := t.resolvePostings(); err != nil {
		return err
	}
	t.scopes.drop(t.ws, entity, index)
	const scope = `(SELECT scope FROM posting_scopes WHERE workspace_id = $1 AND entity = $2 AND idx = $3)`
	if err := t.queue(`DELETE FROM posting_chunks WHERE scope = `+scope, t.ws, entity, index); err != nil {
		return err
	}
	if err := t.queue(`DELETE FROM posting_pages WHERE scope = `+scope, t.ws, entity, index); err != nil {
		return err
	}
	if err := t.queue(`DELETE FROM posting_rows WHERE scope = `+scope, t.ws, entity, index); err != nil {
		return err
	}
	return t.queue(`DELETE FROM posting_scopes WHERE workspace_id = $1 AND entity = $2 AND idx = $3`,
		t.ws, entity, index)
}

// postingRowPlan is the net effect of the logged calls on one row key.
type postingRowPlan struct {
	// old holds the postings the calls change on the row's stored number:
	// true for held after the calls, false for not held.
	old map[string]bool
	// forget is set when a DeletePostings call forgot the stored number.
	forget bool
	// fresh holds the postings of the row after the last forget, which take a
	// new number. nil when no call forgot the row.
	fresh map[string]bool
	// oldAdd is set when a call added a posting before any forget, and
	// freshAdd when a call added one after the last forget. A row that got a
	// posting has a place in the index, also when a later call of the same
	// transaction removed the posting again.
	oldAdd, freshAdd bool
}

// postingScopePlan collects the logged calls of one (entity, index).
type postingScopePlan struct {
	entity, index string
	rows          map[string]*postingRowPlan
	keys          []string
	hasAdd        bool

	// What the read returned. found is false when there is no scope and the
	// calls add nothing; exists is false when scope is a new id that no row
	// of posting_scopes holds yet.
	scope    int32
	nextRow  int64
	exists   bool
	found    bool
	nums     map[string]int64
	keyState map[string]*postingKeyState
	// pages holds the pages the read returned and the pages the write makes,
	// by lo, and los their lo values in order.
	pages map[string]*postingPage
	los   [][]byte

	// guess is the scope id the cache held when guessed is set, and the read
	// of the row numbers and chunks went out with it.
	guess   int32
	guessed bool
	// The pairs postingChunksSQL reads, as parallel arrays.
	pKeys        []string
	pKs          [][]byte
	pFresh, pNxt []bool
	// The posting keys the calls change, in order, and for each one the
	// number of removals that can apply to it, which postingPagesSQL reads.
	cKs       [][]byte
	cRemovals []int32
}

// postingPage is a page that a writer read or makes.
type postingPage struct {
	lo      []byte
	entries []pageEntry
	stored  bool
	dirty   bool
	// prev is the lo of the page before this one, when hasPrev says that the
	// read returned that page.
	prev    []byte
	hasPrev bool
}

func (pg *postingPage) get(k []byte) []int64 {
	if i, ok := findPageEntry(pg.entries, k); ok {
		return pg.entries[i].nums
	}
	return nil
}

// set makes nums the row numbers of k in the page; empty nums removes k.
func (pg *postingPage) set(k []byte, nums []int64) {
	i, ok := findPageEntry(pg.entries, k)
	switch {
	case ok && len(nums) == 0:
		pg.entries = slices.Delete(pg.entries, i, i+1)
	case ok:
		pg.entries[i].nums = nums
	case len(nums) > 0:
		pg.entries = slices.Insert(pg.entries, i, pageEntry{k: k, nums: nums})
	default:
		return
	}
	pg.dirty = true
}

func (p *postingScopePlan) row(key string) *postingRowPlan {
	r := p.rows[key]
	if r == nil {
		r = &postingRowPlan{old: map[string]bool{}}
		p.rows[key] = r
		p.keys = append(p.keys, key)
	}
	return r
}

func (p *postingScopePlan) add(op postingOp) {
	r := p.row(op.key)
	cur := r.old
	if r.fresh != nil {
		cur = r.fresh
	}
	for _, k := range op.remove {
		cur[string(k)] = false
	}
	if !op.forget {
		for _, k := range op.add {
			cur[string(k)] = true
			p.hasAdd = true
		}
		if len(op.add) > 0 {
			if r.fresh != nil {
				r.freshAdd = true
			} else {
				r.oldAdd = true
			}
		}
		return
	}
	// A posting this transaction added and the caller did not name in held
	// is known, so it goes too.
	for k := range cur {
		cur[k] = false
	}
	if r.fresh == nil {
		r.forget = true
		r.oldAdd = false
	}
	r.fresh = map[string]bool{}
	r.freshAdd = false
}

// postingKeyState is what a writer knows about the chunks of one posting key:
// the chunks the read returned, and every chunk value of the key when the key
// has a header.
type postingKeyState struct {
	header bool
	// total is the number of postings of the key, and origTotal the number
	// before the edits.
	total, origTotal int
	// starts holds every chunk value of the key in order: the header's list,
	// or [0] for a key with one chunk, or nothing for a new key.
	starts []int64
	chunks map[int64][]int64
	stored map[int64]bool
	dirty  map[int64]bool
	edits  []postingEdit
	// oldStarts is starts before settleAll.
	oldStarts []int64
}

type postingEdit struct {
	num int64
	add bool
}

// resolvePostings writes the logged calls that are not written yet. It reads
// what every (entity, index) of the calls needs in one batch, which carries
// the queued statements before it, and queues the writes.
func (t *pgTx) resolvePostings() error {
	if t.failed != nil {
		return t.failed
	}
	ops := t.postingLog[t.postingDone:]
	if len(ops) == 0 {
		return nil
	}
	type scopeKey struct{ entity, index string }
	plans := map[scopeKey]*postingScopePlan{}
	var order []*postingScopePlan
	for _, op := range ops {
		sk := scopeKey{op.entity, op.index}
		p := plans[sk]
		if p == nil {
			p = &postingScopePlan{entity: op.entity, index: op.index, rows: map[string]*postingRowPlan{}}
			plans[sk] = p
			order = append(order, p)
		}
		p.add(op)
	}
	// One batch reads every scope, and the chunks of each scope whose id the
	// cache holds. A scope the cache does not hold, or holds stale, reads its
	// chunks in a second batch, with the id the first one returned.
	var reads []queuedStmt
	for _, p := range order {
		p.pairs()
		reads = append(reads, p.scopeStmt(t.ws))
		if id, ok := t.scopes.get(t.ws, p.entity, p.index); ok {
			p.guess, p.guessed = id, true
			reads = append(reads, p.dataStmts(id)...)
		}
	}
	if err := t.readPostingPlans(order, reads, func(p *postingScopePlan, br pgx.BatchResults) error {
		if err := p.decodeScope(br); err != nil {
			return err
		}
		if p.guessed {
			return p.decodeData(br)
		}
		return nil
	}); err != nil {
		return err
	}
	var again []*postingScopePlan
	reads = reads[:0]
	for _, p := range order {
		switch {
		case !p.found:
			t.scopes.drop(t.ws, p.entity, p.index)
		case !p.exists:
			// A new scope holds nothing yet, whatever the guess read.
			p.resetData()
			t.scopes.put(t.ws, p.entity, p.index, p.scope)
		case !p.guessed || p.guess != p.scope:
			p.resetData()
			t.scopes.put(t.ws, p.entity, p.index, p.scope)
			again = append(again, p)
			reads = append(reads, p.dataStmts(p.scope)...)
		}
	}
	if len(again) > 0 {
		if err := t.readPostingPlans(again, reads, (*postingScopePlan).decodeData); err != nil {
			return err
		}
	}
	t.postingDone = len(t.postingLog)
	for _, p := range order {
		if err := p.write(t); err != nil {
			return err
		}
	}
	return nil
}

// The statements a writer reads with.
const (
	// postingScopeSQL reads the scope of workspace $1, entity $2 and index
	// $3, or takes a new scope id when there is
	// none and $4 says the calls add postings. A scope id the transaction
	// does not use is lost, which costs nothing: only uniqueness matters.
	postingScopeSQL = `SELECT scope, next_row, TRUE FROM posting_scopes
		WHERE workspace_id = $1 AND entity = $2 AND idx = $3
		UNION ALL
		SELECT nextval('posting_scope_ids')::int4, 0, FALSE
		WHERE $4::bool AND NOT EXISTS (
			SELECT 1 FROM posting_scopes WHERE workspace_id = $1 AND entity = $2 AND idx = $3)`
	// postingRowsSQL reads the row numbers of the row keys $2 in scope $1.
	postingRowsSQL = `SELECT r.key, r.num
		FROM unnest($2::text[]) AS u(key)
		CROSS JOIN LATERAL (
			SELECT key, num FROM posting_rows WHERE scope = $1 AND key = u.key LIMIT 1
		) r`
	// postingChunksSQL reads, for each (row key, posting key) pair of scope
	// $1, the chunk that holds or would hold the row's number, the chunk
	// before it when $5 marks the pair as a removal, which may merge the two,
	// and the key's header. A pair whose row has no number, or that $4 marks
	// fresh, will
	// take a number above every number of the scope, so it reads the last
	// chunk.
	//
	// The chunk before comes from the same backward scan of the primary key,
	// so it costs no descent of its own. The chunk after would cost one per
	// removed posting, a sixth of the time of a row delete, so a small chunk
	// merges only into the chunk before it.
	//
	// Every part is a LATERAL probe with LIMIT, which leaves the planner no
	// plan but a descent of the primary key per pair, whatever the table
	// statistics were when the plan was made. Without the LIMIT the planner
	// may turn a probe into a join, which in a partitioned layout reads every
	// partition instead of the one of the scope.
	postingChunksSQL = `SELECT c.k, c.chunk, c.n, c.ids
		FROM unnest($2::text[], $3::bytea[], $4::bool[], $5::bool[]) AS u(key, k, fresh, nxt)
		LEFT JOIN LATERAL (
			SELECT num FROM posting_rows WHERE scope = $1 AND key = u.key AND NOT u.fresh LIMIT 1
		) r ON TRUE
		CROSS JOIN LATERAL (
			(SELECT k, chunk, n, ids FROM posting_chunks
			 WHERE scope = $1 AND k = u.k AND chunk >= 0 AND chunk <= COALESCE(r.num, 9223372036854775807)
			 ORDER BY chunk DESC LIMIT CASE WHEN u.nxt AND r.num IS NOT NULL THEN 2 ELSE 1 END)
			UNION ALL
			(SELECT k, chunk, n, ids FROM posting_chunks
			 WHERE scope = $1 AND k = u.k AND chunk = -1)
		) c`
	// postingPagesSQL reads, for each posting key $2 of scope $1, the page
	// that holds or would hold it, and when $3 gives the key removals, also
	// the page before it, which a page that the removals leave small may
	// merge into. prev is the lo of the page before, when the same scan read
	// it. The two pages come from one backward scan of the primary key, as in
	// postingChunksSQL, and a page that several keys read comes back once.
	//
	// A key in chunks whose count stays at $4 or above after $3 removals
	// reads no page: it cannot move back to a page (see editKey), and the
	// probe of its first chunk tuple costs less than the read of a page.
	postingPagesSQL = `SELECT DISTINCT ON (p.lo) p.lo, p.body, p.prev
		FROM unnest($2::bytea[], $3::int4[]) AS u(k, removals)
		CROSS JOIN LATERAL (
			SELECT lo, body, lead(lo) OVER (ORDER BY lo DESC) AS prev FROM (
				SELECT lo, body FROM posting_pages WHERE scope = $1 AND lo <= u.k
				ORDER BY lo DESC LIMIT CASE WHEN u.removals > 0 THEN 2 ELSE 1 END) q
			WHERE NOT EXISTS (
				SELECT 1 FROM posting_chunks
				WHERE scope = $1 AND k = u.k AND chunk <= 0 AND n >= $4 + u.removals)
		) p
		ORDER BY p.lo, p.prev NULLS LAST`
)

// pairs builds the (row key, posting key) pairs postingChunksSQL reads.
func (p *postingScopePlan) pairs() {
	p.pKeys, p.pKs, p.pFresh, p.pNxt = nil, nil, nil, nil
	p.cKs, p.cRemovals = nil, nil
	type pair struct {
		key, k     string
		fresh, nxt bool
	}
	seen := map[pair]bool{}
	changed := map[string]int32{}
	push := func(pr pair) {
		if seen[pr] {
			return
		}
		if pr.nxt {
			changed[pr.k]++
		} else if _, ok := changed[pr.k]; !ok {
			changed[pr.k] = 0
		}
		seen[pr] = true
		p.pKeys, p.pKs = append(p.pKeys, pr.key), append(p.pKs, []byte(pr.k))
		p.pFresh, p.pNxt = append(p.pFresh, pr.fresh), append(p.pNxt, pr.nxt)
	}
	for _, key := range p.keys {
		r := p.rows[key]
		for k, held := range r.old {
			push(pair{key: key, k: k, nxt: !held})
		}
		for k, held := range r.fresh {
			if held {
				// Every fresh number reads the same last chunk, so the row key
				// does not matter.
				push(pair{k: k, fresh: true})
			}
		}
	}
	ks := make([]string, 0, len(changed))
	for k := range changed {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	for _, k := range ks {
		p.cKs, p.cRemovals = append(p.cKs, []byte(k)), append(p.cRemovals, changed[k])
	}
}

func (p *postingScopePlan) scopeStmt(ws string) queuedStmt {
	return queuedStmt{sql: postingScopeSQL, args: []any{ws, p.entity, p.index, p.hasAdd}}
}

// dataStmts read the row numbers, chunks and pages of the plan in scope.
func (p *postingScopePlan) dataStmts(scope int32) []queuedStmt {
	return []queuedStmt{
		{sql: postingRowsSQL, args: []any{scope, p.keys}},
		{sql: postingChunksSQL, args: []any{scope, p.pKeys, p.pKs, p.pFresh, p.pNxt}},
		{sql: postingPagesSQL, args: []any{scope, p.cKs, p.cRemovals, int32(postingPageMaxRows / 2)}},
	}
}

func (p *postingScopePlan) decodeScope(br pgx.BatchResults) error {
	p.resetData()
	err := br.QueryRow().Scan(&p.scope, &p.nextRow, &p.exists)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		p.found = false
	case err != nil:
		return err
	default:
		p.found = true
	}
	return nil
}

// resetData forgets the row numbers, chunks and pages a read returned.
func (p *postingScopePlan) resetData() {
	p.nums = map[string]int64{}
	p.keyState = map[string]*postingKeyState{}
	p.pages = map[string]*postingPage{}
	p.los = nil
}

func (p *postingScopePlan) decodeData(br pgx.BatchResults) error {
	rows, err := br.Query()
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			key string
			num int64
		)
		if err := rows.Scan(&key, &num); err != nil {
			rows.Close()
			return err
		}
		p.nums[key] = num
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if err := p.decodeChunks(br); err != nil {
		return err
	}
	rows, err = br.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var lo, body, prev []byte
		if err := rows.Scan(&lo, &body, &prev); err != nil {
			return err
		}
		pg := &postingPage{lo: notNilBytes(lo), stored: true, prev: prev, hasPrev: prev != nil}
		if pg.entries, err = decodePage(body); err != nil {
			return fmt.Errorf("%w: scope %d page %x", err, p.scope, lo)
		}
		p.pages[string(pg.lo)] = pg
		// The rows come in lo order.
		p.los = append(p.los, pg.lo)
	}
	return rows.Err()
}

func (p *postingScopePlan) decodeChunks(br pgx.BatchResults) error {
	rows, err := br.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			k     []byte
			chunk int64
			n     int32
			ids   []byte
		)
		if err := rows.Scan(&k, &chunk, &n, &ids); err != nil {
			return err
		}
		ks := p.state(string(k))
		if chunk == postingHeader {
			if ks.header {
				continue
			}
			starts, err := decodeIDs(ids, 0, nil)
			if err != nil {
				return err
			}
			ks.header, ks.total, ks.starts = true, int(n), starts
			continue
		}
		if _, ok := ks.chunks[chunk]; ok {
			continue
		}
		decoded, err := decodeIDs(ids, chunk, nil)
		if err != nil {
			return err
		}
		ks.chunks[chunk] = decoded
		ks.stored[chunk] = true
	}
	return rows.Err()
}

func (p *postingScopePlan) state(k string) *postingKeyState {
	ks := p.keyState[k]
	if ks == nil {
		ks = &postingKeyState{chunks: map[int64][]int64{}, stored: map[int64]bool{}, dirty: map[int64]bool{}}
		p.keyState[k] = ks
	}
	return ks
}

// readPostingPlans sends reads with the queue and lets decode read the
// results of each plan of plans, in order.
func (t *pgTx) readPostingPlans(plans []*postingScopePlan, reads []queuedStmt, decode func(*postingScopePlan, pgx.BatchResults) error) error {
	br, err := t.sendWith(reads...)
	if err != nil {
		return err
	}
	for _, p := range plans {
		if err := decode(p, br); err != nil {
			_ = br.Close()
			return err
		}
	}
	return br.Close()
}

// write turns the plan and what the read returned into queued statements.
func (p *postingScopePlan) write(t *pgTx) error {
	if !p.found {
		// No scope and nothing to add: nothing is stored to change.
		return nil
	}
	next := p.nextRow
	// An int64 counter does not run out: at a million rows a second, it
	// lasts for 290,000 years.
	alloc := func() int64 {
		n := next
		next++
		return n
	}
	edits := map[string][]postingEdit{}
	edit := func(k string, num int64, add bool) {
		edits[k] = append(edits[k], postingEdit{num: num, add: add})
	}
	var (
		forgetKeys []string
		newNums    []int64
		newKeys    []string
	)
	// Numbers go out in key order, so the same calls give the same numbers.
	keys := slices.Clone(p.keys)
	slices.Sort(keys)
	for _, key := range keys {
		r := p.rows[key]
		num, stored := p.nums[key]
		if stored {
			for k, held := range r.old {
				edit(k, num, held)
			}
			if r.forget {
				forgetKeys = append(forgetKeys, key)
			}
		} else if r.oldAdd {
			n := alloc()
			newNums, newKeys = append(newNums, n), append(newKeys, key)
			for k, held := range r.old {
				if held {
					edit(k, n, true)
				}
			}
		}
		if r.freshAdd {
			n := alloc()
			newNums, newKeys = append(newNums, n), append(newKeys, key)
			for k, held := range r.fresh {
				if held {
					edit(k, n, true)
				}
			}
		}
	}

	var w postingWrites
	// Keys in order, so the same calls give the same pages.
	changed := make([]string, 0, len(edits))
	for k := range edits {
		changed = append(changed, k)
	}
	slices.Sort(changed)
	for _, k := range changed {
		if err := p.editKey(k, edits[k], &w); err != nil {
			return err
		}
	}
	p.settlePages(&w)
	changedPosting := len(w.upK) > 0 || len(w.delK) > 0 || len(w.pageLo) > 0 || len(w.pageDel) > 0

	// Each new number is a row inserted into posting_rows, and each
	// forgotten key a row deleted from it.
	rowDelta := len(newNums) - len(forgetKeys)
	if !p.exists {
		if len(newNums) == 0 && !changedPosting {
			return nil
		}
		if err := t.queue(`INSERT INTO posting_scopes (workspace_id, entity, idx, scope, next_row, row_count) VALUES ($1, $2, $3, $4, $5, $6)`,
			t.ws, p.entity, p.index, p.scope, next, rowDelta); err != nil {
			return err
		}
	} else if next != p.nextRow || rowDelta != 0 {
		if err := t.queue(`UPDATE posting_scopes SET next_row = $4, row_count = row_count + $5 WHERE workspace_id = $1 AND entity = $2 AND idx = $3`,
			t.ws, p.entity, p.index, next, rowDelta); err != nil {
			return err
		}
	}
	// A forgotten row that comes back in the same transaction is deleted and
	// inserted with its new number, in that order.
	if len(forgetKeys) > 0 {
		if err := t.queue(`DELETE FROM posting_rows WHERE scope = $1 AND key = ANY($2::text[])`,
			p.scope, forgetKeys); err != nil {
			return err
		}
	}
	if len(newNums) > 0 {
		if err := t.queue(`INSERT INTO posting_rows (scope, num, key)
			SELECT $1, u.num, u.key FROM unnest($2::int8[], $3::text[]) AS u(num, key)`,
			p.scope, newNums, newKeys); err != nil {
			return err
		}
	}
	if len(w.delK) > 0 {
		if err := t.queue(`DELETE FROM posting_chunks c
			USING unnest($2::bytea[], $3::int8[]) AS d(k, chunk)
			WHERE c.scope = $1 AND c.k = d.k AND c.chunk = d.chunk`,
			p.scope, w.delK, w.delChunk); err != nil {
			return err
		}
	}
	if len(w.upK) > 0 {
		// A chunk the read returned is updated in place. The update changes
		// no indexed column, so Postgres can keep the new version on the same
		// page (a HOT update) when the page has room, which the table's
		// fillfactor leaves.
		if err := t.queue(`INSERT INTO posting_chunks (scope, chunk, n, k, ids)
			SELECT $1, u.chunk, u.n, u.k, u.ids
			FROM unnest($2::bytea[], $3::int8[], $4::int4[], $5::bytea[]) AS u(k, chunk, n, ids)
			ON CONFLICT (scope, k, chunk) DO UPDATE SET n = excluded.n, ids = excluded.ids`,
			p.scope, w.upK, w.upChunk, w.upN, w.upIDs); err != nil {
			return err
		}
	}
	if len(w.pageDel) > 0 {
		if err := t.queue(`DELETE FROM posting_pages p
			USING unnest($2::bytea[]) AS d(lo)
			WHERE p.scope = $1 AND p.lo = d.lo`,
			p.scope, w.pageDel); err != nil {
			return err
		}
	}
	if len(w.pageLo) > 0 {
		// As for chunks, a page the read returned is updated in place.
		if err := t.queue(`INSERT INTO posting_pages (scope, lo, body)
			SELECT $1, u.lo, u.body FROM unnest($2::bytea[], $3::bytea[]) AS u(lo, body)
			ON CONFLICT (scope, lo) DO UPDATE SET body = excluded.body`,
			p.scope, w.pageLo, w.pageBody); err != nil {
			return err
		}
	}
	return nil
}

// postingWrites collects the tuples a write puts and deletes, as the
// parallel arrays of the statements that write them.
type postingWrites struct {
	upK      [][]byte
	upChunk  []int64
	upN      []int32
	upIDs    [][]byte
	delK     [][]byte
	delChunk []int64
	pageLo   [][]byte
	pageBody [][]byte
	pageDel  [][]byte
}

func (w *postingWrites) chunkFuncs(k string) (func(chunk int64, n int, ids []byte), func(chunk int64)) {
	return func(chunk int64, n int, ids []byte) {
			w.upK, w.upChunk, w.upN, w.upIDs = append(w.upK, []byte(k)), append(w.upChunk, chunk), append(w.upN, int32(n)), append(w.upIDs, ids)
		}, func(chunk int64) {
			w.delK, w.delChunk = append(w.delK, []byte(k)), append(w.delChunk, chunk)
		}
}

// editKey makes the edits es on posting key k, in its page or its chunks,
// and moves the key between the two when its size asks for it.
func (p *postingScopePlan) editKey(k string, es []postingEdit, w *postingWrites) error {
	kb := []byte(k)
	if ks := p.keyState[k]; ks != nil && (ks.header || len(ks.chunks) > 0) {
		ks.edits = es
		if err := ks.apply(k); err != nil {
			return err
		}
		ks.settleAll()
		// The check comes after settleAll, so a key that merges down to one
		// chunk moves in the same write. chunk 0 is the only chunk then, but
		// the read may not have returned it. A key that can pass the check
		// had a count below half the limit plus its removals, so the read
		// returned its page (see postingPagesSQL).
		if nums, ok := ks.chunks[0]; ok && len(ks.starts) == 1 && ks.total < postingPageMaxRows/2 {
			ks.chunks, ks.starts = map[int64][]int64{}, nil
			p.pageFor(kb).set(kb, nums)
		}
		ks.emit(w.chunkFuncs(k))
		return nil
	}
	pg := p.pageFor(kb)
	var add, remove []int64
	for _, e := range es {
		if e.add {
			add = append(add, e.num)
		} else {
			remove = append(remove, e.num)
		}
	}
	nums := applyChange(pg.get(kb), remove, add)
	if len(nums) <= postingPageMaxRows {
		pg.set(kb, nums)
		return nil
	}
	pg.set(kb, nil)
	ks := p.state(k)
	for _, n := range nums {
		ks.edits = append(ks.edits, postingEdit{num: n, add: true})
	}
	if err := ks.apply(k); err != nil {
		return err
	}
	ks.plan(w.chunkFuncs(k))
	return nil
}

// pageFor returns the page that holds or would hold k. The read returned
// that page, if it is stored, as it reads the page of each changed key. When
// no page has a lo at or below k, a new page from the empty key on takes it.
func (p *postingScopePlan) pageFor(k []byte) *postingPage {
	i := sort.Search(len(p.los), func(i int) bool { return bytes.Compare(p.los[i], k) > 0 }) - 1
	if i >= 0 {
		return p.pages[string(p.los[i])]
	}
	pg := &postingPage{lo: []byte{}}
	p.pages[""] = pg
	p.los = slices.Insert(p.los, 0, pg.lo)
	return pg
}

// settlePages merges the small pages into the pages before them, splits the
// large ones, and reports each page to write and each page to delete to w.
func (p *postingScopePlan) settlePages(w *postingWrites) {
	// p.los holds every page in lo order. Merges go from the last page to the
	// first, so a page that takes the page after it can still merge into the
	// page before it.
	for i := len(p.los) - 1; i >= 0; i-- {
		pg := p.pages[string(p.los[i])]
		if !pg.dirty || len(pg.entries) == 0 || !pg.hasPrev {
			continue
		}
		size := pageLen(pg.entries)
		if size >= postingPageBudget/4 {
			continue
		}
		q := p.pages[string(pg.prev)]
		if q == nil || pageLen(q.entries)+size > postingPageBudget {
			continue
		}
		q.entries = append(slices.Clone(q.entries), pg.entries...)
		q.dirty = true
		pg.entries = nil
	}
	for _, lo := range p.los {
		pg := p.pages[string(lo)]
		if !pg.dirty {
			continue
		}
		if len(pg.entries) == 0 {
			if pg.stored {
				w.pageDel = append(w.pageDel, pg.lo)
			}
			continue
		}
		if len(pg.entries) == 1 || pageLen(pg.entries) <= postingPageBudget {
			w.pageLo, w.pageBody = append(w.pageLo, pg.lo), append(w.pageBody, encodePage(pg.entries))
			continue
		}
		for i, part := range splitPage(pg.entries, postingPageBudget) {
			lo := pg.lo
			if i > 0 {
				lo = part[0].k
			}
			w.pageLo, w.pageBody = append(w.pageLo, lo), append(w.pageBody, encodePage(part))
		}
	}
}

// apply makes the edits on the chunks they fall in.
func (ks *postingKeyState) apply(k string) error {
	if !ks.header {
		if _, ok := ks.chunks[0]; ok {
			ks.starts = []int64{0}
			ks.total = len(ks.chunks[0])
		}
	}
	ks.origTotal = ks.total
	type change struct{ add, remove []int64 }
	byChunk := map[int64]*change{}
	for _, e := range ks.edits {
		if len(ks.starts) == 0 {
			if !e.add {
				continue
			}
			ks.starts = []int64{0}
			ks.chunks[0] = nil
		}
		// The chunk of a number is the one with the greatest start not above
		// it. Chunk 0 always exists, so there is one.
		i := sort.Search(len(ks.starts), func(i int) bool { return ks.starts[i] > e.num }) - 1
		start := ks.starts[i]
		if _, ok := ks.chunks[start]; !ok {
			return fmt.Errorf("pgstore: posting key %x: chunk %d was not read", k, start)
		}
		c := byChunk[start]
		if c == nil {
			c = &change{}
			byChunk[start] = c
		}
		if e.add {
			c.add = append(c.add, e.num)
		} else {
			c.remove = append(c.remove, e.num)
		}
	}
	for start, c := range byChunk {
		before := ks.chunks[start]
		after := applyChange(before, c.remove, c.add)
		ks.total += len(after) - len(before)
		if !slices.Equal(before, after) || !ks.stored[start] {
			ks.chunks[start] = after
			ks.dirty[start] = true
		}
	}
	return nil
}

// applyChange returns ids without remove and with add, sorted and without
// duplicates. ids is sorted. It does not change ids.
func applyChange(ids, remove, add []int64) []int64 {
	out := make([]int64, 0, len(ids)+len(add))
	if len(remove) > 0 {
		slices.Sort(remove)
		for _, id := range ids {
			if _, found := slices.BinarySearch(remove, id); !found {
				out = append(out, id)
			}
		}
	} else {
		out = append(out, ids...)
	}
	if len(add) > 0 {
		out = append(out, add...)
		slices.Sort(out)
		out = slices.Compact(out)
	}
	return out
}

// plan splits and merges the changed chunks and reports each tuple to write
// and each tuple to delete.
func (ks *postingKeyState) plan(upsert func(chunk int64, n int, ids []byte), del func(chunk int64)) {
	ks.settleAll()
	ks.emit(upsert, del)
}

// settleAll splits and merges the changed chunks.
func (ks *postingKeyState) settleAll() {
	ks.oldStarts = slices.Clone(ks.starts)
	dirty := make([]int64, 0, len(ks.dirty))
	for s := range ks.dirty {
		dirty = append(dirty, s)
	}
	slices.Sort(dirty)
	for _, s := range dirty {
		ks.settle(s)
	}
	// An empty chunk 0 may stay only while the key has other chunks.
	if len(ks.starts) == 1 {
		if ids, ok := ks.chunks[0]; ok && len(ids) == 0 {
			ks.drop(0, 0)
		}
	}
}

// emit reports each tuple to write and each tuple to delete, after
// settleAll.
func (ks *postingKeyState) emit(upsert func(chunk int64, n int, ids []byte), del func(chunk int64)) {
	for s := range ks.dirty {
		if ids, ok := ks.chunks[s]; ok {
			upsert(s, len(ids), encodeIDs(ids, s))
		}
	}
	for s := range ks.stored {
		if _, ok := ks.chunks[s]; !ok {
			del(s)
		}
	}
	switch {
	case len(ks.starts) >= 2:
		if !ks.header || ks.total != ks.origTotal || !slices.Equal(ks.oldStarts, ks.starts) {
			upsert(postingHeader, ks.total, encodeIDs(ks.starts, 0))
		}
	case ks.header:
		del(postingHeader)
	}
}

// settle splits or merges chunk s when its size asks for it. It repeats
// while a merge leaves chunk s empty or small and the next chunk was read.
func (ks *postingKeyState) settle(s int64) {
	maxN := postingChunkMax
	for {
		ids, ok := ks.chunks[s]
		if !ok {
			return // merged into the chunk before it
		}
		pos, _ := slices.BinarySearch(ks.starts, s)
		var (
			next        int64
			hasNext     bool
			nextIDs     []int64
			nextFetched bool
		)
		if pos+1 < len(ks.starts) {
			next, hasNext = ks.starts[pos+1], true
			nextIDs, nextFetched = ks.chunks[next]
		}
		switch {
		case len(ids) == 0 && (s != 0 || !hasNext):
			ks.drop(pos, s)
			return
		case len(ids) == 0 && nextFetched:
			// Chunk 0 must stay, so it takes the chunk after it.
			ks.chunks[0] = nextIDs
			ks.drop(pos+1, next)
		case len(ids) == 0:
			// Chunk 0 is empty and the chunk after it was not read. It stays
			// empty until an edit adds to it or merges the next chunk into it.
			return
		case len(ids) < maxN/4 && nextFetched && len(ids)+len(nextIDs) <= maxN &&
			codedLen(ids, s)+codedLen(nextIDs, ids[len(ids)-1]) <= postingChunkMaxBytes:
			ks.chunks[s] = append(slices.Clone(ids), nextIDs...)
			ks.drop(pos+1, next)
		case len(ids) < maxN/4 && pos > 0 && ks.fits(ks.starts[pos-1], ids):
			prev := ks.starts[pos-1]
			ks.chunks[prev] = append(slices.Clone(ks.chunks[prev]), ids...)
			ks.dirty[prev] = true
			ks.drop(pos, s)
			return
		case len(ids) > maxN || codedLen(ids, s) > postingChunkMaxBytes:
			ks.split(pos, s, ids)
			return
		default:
			return
		}
	}
}

// fits reports whether chunk prev was read and has room for ids, which all
// sort after it.
func (ks *postingKeyState) fits(prev int64, ids []int64) bool {
	prevIDs, ok := ks.chunks[prev]
	if !ok || len(prevIDs)+len(ids) > postingChunkMax {
		return false
	}
	last := prev
	if len(prevIDs) > 0 {
		last = prevIDs[len(prevIDs)-1]
	}
	return codedLen(prevIDs, prev)+codedLen(ids, last) <= postingChunkMaxBytes
}

// drop removes the chunk at position pos of starts.
func (ks *postingKeyState) drop(pos int, s int64) {
	ks.starts = slices.Delete(ks.starts, pos, pos+1)
	delete(ks.chunks, s)
}

// split cuts an oversized chunk into chunks that fit. The last chunk of a key
// takes appends, so it is cut full from the left, and the new numbers start
// a new chunk; a chunk in the middle is cut into equal parts, which leaves
// each of them room for later inserts.
func (ks *postingKeyState) split(pos int, s int64, ids []int64) {
	size := postingChunkMax
	if pos < len(ks.starts)-1 {
		parts := (len(ids) + postingChunkMax - 1) / postingChunkMax
		size = (len(ids) + parts - 1) / parts
	}
	var pieces [][]int64
	for rest := ids; len(rest) > 0; {
		base := s
		if len(pieces) > 0 {
			base = rest[0]
		}
		n := min(size, len(rest))
		for n > 1 && codedLen(rest[:n], base) > postingChunkMaxBytes {
			n--
		}
		pieces = append(pieces, rest[:n:n])
		rest = rest[n:]
	}
	ks.chunks[s] = pieces[0]
	ks.dirty[s] = true
	at := pos + 1
	for _, piece := range pieces[1:] {
		ks.chunks[piece[0]] = piece
		ks.dirty[piece[0]] = true
		ks.starts = slices.Insert(ks.starts, at, piece[0])
		at++
	}
}

// encodeIDs codes sorted numbers as varints of the difference to the number
// before, the first one to base.
func encodeIDs(ids []int64, base int64) []byte {
	out := make([]byte, 0, len(ids)*2)
	prev := base
	for _, id := range ids {
		out = binary.AppendUvarint(out, uint64(id-prev))
		prev = id
	}
	return out
}

// codedLen is len(encodeIDs(ids, base)).
func codedLen(ids []int64, base int64) int {
	n, prev := 0, base
	for _, id := range ids {
		d := uint64(id - prev)
		n++
		for d >= 1<<7 {
			d >>= 7
			n++
		}
		prev = id
	}
	return n
}

// decodeIDs appends the numbers encodeIDs coded to dst.
func decodeIDs(b []byte, base int64, dst []int64) ([]int64, error) {
	prev := base
	for len(b) > 0 {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errors.New("pgstore: corrupt posting chunk")
		}
		b = b[n:]
		prev += int64(v)
		dst = append(dst, prev)
	}
	return dst, nil
}
