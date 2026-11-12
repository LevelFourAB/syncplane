package pgstore

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/storage"
)

// postingBounds gives the bounds of ranges as the two arrays the read
// statements take. A nil Lo becomes the empty key, which sorts first, and a
// nil Hi becomes postingKeyMax, so that each range is an index range.
func postingBounds(ranges []storage.IndexRange) (los, his [][]byte) {
	los = make([][]byte, len(ranges))
	his = make([][]byte, len(ranges))
	for i, r := range ranges {
		los[i] = notNilBytes(r.Lo)
		his[i] = r.Hi
		if his[i] == nil {
			his[i] = postingKeyMax
		}
	}
	return los, his
}

// postingScopeIDSQL reads the scope and the row count of workspace $1,
// entity $2 and index $3.
const postingScopeIDSQL = `SELECT scope, row_count FROM posting_scopes WHERE workspace_id = $1 AND entity = $2 AND idx = $3`

// withScope runs one read of a posting index with its scope id bound. The id
// comes from the store's cache, and the read travels in one batch with a read
// of the current id: when the two differ, the cache was stale, and withScope
// reads again with the current id. decode gets the scope id, the row count of
// the scope and the results of the batch after the check. It reports false,
// and does not call decode, when the index has no scope.
//
// When the cache does not hold the id, cold, if not nil, gives a read that
// finds the scope itself, which travels with the check in one batch. With a
// nil cold, the id is read first, which costs one more round trip.
func (t *pgTx) withScope(entity, index string, read func(scope int32) queuedStmt, cold func() queuedStmt, decode func(scope int32, rows int64, br pgx.BatchResults) error) (bool, error) {
	check := queuedStmt{sql: postingScopeIDSQL, args: []any{t.ws, entity, index}}
	for {
		var stmt queuedStmt
		id, ok := t.scopes.get(t.ws, entity, index)
		switch {
		case ok:
			stmt = read(id)
		case cold != nil:
			stmt = cold()
		default:
			var rows int64
			err := t.queryRow(check.sql, check.args...).Scan(&id, &rows)
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			t.scopes.put(t.ws, entity, index, id)
			ok, stmt = true, read(id)
		}
		br, err := t.sendWith(check, stmt)
		if err != nil {
			return false, err
		}
		var (
			current int32
			rows    int64
		)
		err = br.QueryRow().Scan(&current, &rows)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			t.scopes.drop(t.ws, entity, index)
			return false, br.Close()
		case err != nil:
			_ = br.Close()
			return false, err
		case !ok:
			// The cold read found the scope by itself, so its result belongs
			// to current.
			t.scopes.put(t.ws, entity, index, current)
		case current != id:
			t.scopes.put(t.ws, entity, index, current)
			if err := br.Close(); err != nil {
				return false, err
			}
			continue
		}
		if err := decode(current, rows, br); err != nil {
			_ = br.Close()
			return false, err
		}
		return true, br.Close()
	}
}

// postingCountsSQL counts each range of scope $1. For the keys in chunks, it
// sums the counts of the first tuple of each key: the header of a key with
// several chunks, or the only chunk of a key with one. The test of chunk is
// part of the index condition, so the other chunks of a key cost an index
// entry each and no heap read. Every key holds one posting or more, so LIMIT
// $4 keys is a count above $4 - 1 whenever it cuts the range short.
//
// For the keys in pages, it returns the page that holds or would hold the
// low bound of the range, and the pages whose lo is in the range, which the
// caller counts. Each of the second kind holds a key in the range, except
// the last one of the range, whose keys may all be at or above the high
// bound. When LIMIT $4 pages cuts the range short, the last one is not among
// them, so the pages also count above $4 - 1.
const postingCountsSQL = `SELECT r.i::int4, x.n, x.body
	FROM unnest($2::bytea[], $3::bytea[]) WITH ORDINALITY AS r(lo, hi, i)
	CROSS JOIN LATERAL (
		(SELECT COALESCE(sum(h.n), 0)::int8 AS n, NULL::bytea AS body FROM (
			SELECT DISTINCT ON (c.k) c.n FROM posting_chunks c
			WHERE c.scope = $1 AND c.k >= r.lo AND c.k < r.hi AND c.chunk <= 0
			ORDER BY c.k, c.chunk
			LIMIT $4
		) h)
		UNION ALL
		(SELECT 0::int8, p.body FROM posting_pages p
		 WHERE p.scope = $1 AND p.lo <= r.lo
		 ORDER BY p.lo DESC LIMIT 1)
		UNION ALL
		(SELECT 0::int8, p.body FROM posting_pages p
		 WHERE p.scope = $1 AND p.lo > r.lo AND p.lo < r.hi
		 ORDER BY p.lo LIMIT $4)
	) x`

// postingCountsColdSQL is postingCountsSQL for a scope that the cache does
// not hold: it finds the scope of workspace $1, entity $5 and index $6
// itself. The scope is then known only when the statement runs, so a
// partitioned layout prunes the partitions at run time and not when it plans.
var postingCountsColdSQL = strings.ReplaceAll(postingCountsSQL, "$1",
	"(SELECT scope FROM posting_scopes WHERE workspace_id = $1 AND entity = $5 AND idx = $6)")

// countPage adds to *n the number of postings of page body b whose key is in
// [lo, hi).
func countPage(b, lo, hi []byte, n *int) error {
	return walkPage(b, false, func(k []byte, count int, _ []int64) bool {
		if bytes.Compare(k, hi) >= 0 {
			return false
		}
		if bytes.Compare(k, lo) >= 0 {
			*n += count
		}
		return true
	})
}

// PostingCounts answers every range in one statement. It reads one tuple per
// key in chunks, which for a key with one chunk is that chunk: the count is a
// column of it, and its numbers are not decoded. It reads the pages that can
// hold keys of the range and counts their keys without decoding their
// numbers. A range of one key thus reads the page or the chunk tuple of that
// key, and the page before the key when the key is in chunks. The row count
// comes with the check of the scope id in the same batch, so the call is one
// round trip, also when the cache does not hold the scope id.
func (t *pgTx) PostingCounts(entity, index string, ranges []storage.IndexRange, max int) ([]int, int, error) {
	out := make([]int, len(ranges))
	if err := t.resolvePostings(); err != nil {
		return nil, 0, err
	}
	los, his := postingBounds(ranges)
	var keyLimit *int64
	if max > 0 {
		keyLimit = limitArg(max + 1)
	}
	var rowCount int64
	_, err := t.withScope(entity, index, func(scope int32) queuedStmt {
		return queuedStmt{sql: postingCountsSQL, args: []any{scope, los, his, keyLimit}}
	}, func() queuedStmt {
		return queuedStmt{sql: postingCountsColdSQL, args: []any{t.ws, los, his, keyLimit, entity, index}}
	}, func(_ int32, n int64, br pgx.BatchResults) error {
		rowCount = n
		rows, err := br.Query()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				i    int32
				n    int64
				body []byte
			)
			if err := rows.Scan(&i, &n, &body); err != nil {
				return err
			}
			i-- // WITH ORDINALITY counts from 1
			if body == nil {
				out[i] += int(n)
				continue
			}
			if err := countPage(body, los[i], his[i], &out[i]); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, 0, err
	}
	return out, int(rowCount), nil
}

// postingHeadsSQL reads the first tuple of each key in chunks of each range
// of scope $1, as postingCountsSQL counts them, with the numbers of a key
// that has one chunk and the chunk list of a key that has more. It also reads
// the pages postingCountsSQL reads, with a NULL chunk. $4 is always NULL: a
// LIMIT keeps the reads of the chunks and of the pages ordered scans of the
// primary key in every plan, as in postingCountsSQL, where a generic plan for
// large partitioned tables otherwise sorts the rows of all partitions.
const postingHeadsSQL = `SELECT r.i::int4, h.k, h.chunk, h.n, h.ids
	FROM unnest($2::bytea[], $3::bytea[]) WITH ORDINALITY AS r(lo, hi, i)
	CROSS JOIN LATERAL (
		(SELECT DISTINCT ON (c.k) c.k, c.chunk, c.n, c.ids FROM posting_chunks c
		 WHERE c.scope = $1 AND c.k >= r.lo AND c.k < r.hi AND c.chunk <= 0
		 ORDER BY c.k, c.chunk LIMIT $4)
		UNION ALL
		(SELECT p.lo, NULL::int8, 0, p.body FROM posting_pages p
		 WHERE p.scope = $1 AND p.lo <= r.lo
		 ORDER BY p.lo DESC LIMIT 1)
		UNION ALL
		(SELECT p.lo, NULL::int8, 0, p.body FROM posting_pages p
		 WHERE p.scope = $1 AND p.lo > r.lo AND p.lo < r.hi
		 ORDER BY p.lo LIMIT $4)
	) h`

// postingFetchSQL reads named chunks, a primary-key probe each. The LIMIT
// keeps the probe in every plan: without it, a generic plan made on empty
// tables joins with a hash and reads every chunk of the scope.
const postingFetchSQL = `SELECT c.k, c.chunk, c.ids
	FROM unnest($2::bytea[], $3::int8[]) AS u(k, chunk)
	CROSS JOIN LATERAL (
		SELECT k, chunk, ids FROM posting_chunks WHERE scope = $1 AND k = u.k AND chunk = u.chunk LIMIT 1
	) c`

// postingResolveSQL maps row numbers to row keys. A number with no row, which
// DeletePostings forgot, returns nothing. The LIMIT is there for the reason of
// postingFetchSQL.
const postingResolveSQL = `SELECT r.key
	FROM unnest($2::int8[]) AS u(num)
	CROSS JOIN LATERAL (SELECT key FROM posting_rows WHERE scope = $1 AND num = u.num LIMIT 1) r`

// postingMulti is a key with several chunks, as its header lists them.
type postingMulti struct {
	k      string
	starts []int64
}

// postingRangeRead is what a reader knows about one range.
type postingRangeRead struct {
	total int
	// ids holds the numbers of the keys with one chunk, unsorted and with
	// duplicates.
	ids   []int64
	multi []postingMulti
}

type postingChunkRef struct {
	k     string
	chunk int64
}

// ScanPostings works in row-number space: it reads the first tuple of each
// key in chunks and the pages of each range, takes the smallest range of all
// as the source of candidates, reads from each other range of all only the
// chunks that can hold a candidate, intersects in Go and reads the row keys
// of the result. A page holds the numbers of its keys, so a key in a page
// costs what a key with one chunk costs. That is one round trip for the
// first tuples and the pages, one or two for chunks of keys with several
// chunks (none when there are no such keys), and one for the row keys. A
// scope id the store has not cached yet, or has cached stale, costs one more
// (see withScope).
func (t *pgTx) ScanPostings(entity, index string, all, anyRanges []storage.IndexRange, limit int, fn func(rowKey string) error) error {
	if len(all) == 0 && len(anyRanges) == 0 {
		return nil
	}
	if err := t.resolvePostings(); err != nil {
		return err
	}
	ranges := make([]storage.IndexRange, 0, len(all)+len(anyRanges))
	ranges = append(append(ranges, all...), anyRanges...)
	los, his := postingBounds(ranges)
	reads := make([]postingRangeRead, len(ranges))
	var scope int32
	found, err := t.withScope(entity, index, func(scope int32) queuedStmt {
		return queuedStmt{sql: postingHeadsSQL, args: []any{scope, los, his, (*int64)(nil)}}
	}, nil, func(id int32, _ int64, br pgx.BatchResults) error {
		scope = id
		rows, err := br.Query()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				i, n   int32
				chunkp *int64
				k, ids []byte
			)
			if err := rows.Scan(&i, &k, &chunkp, &n, &ids); err != nil {
				return err
			}
			i-- // WITH ORDINALITY counts from 1
			r := &reads[i]
			if chunkp == nil {
				if err := walkPage(ids, true, func(pk []byte, count int, nums []int64) bool {
					if bytes.Compare(pk, his[i]) >= 0 {
						return false
					}
					if bytes.Compare(pk, los[i]) >= 0 {
						r.total += count
						r.ids = append(r.ids, nums...)
					}
					return true
				}); err != nil {
					return err
				}
				continue
			}
			chunk := *chunkp
			r.total += int(n)
			if chunk == postingHeader {
				starts, err := decodeIDs(ids, 0, nil)
				if err != nil {
					return err
				}
				r.multi = append(r.multi, postingMulti{k: string(k), starts: starts})
				continue
			}
			if r.ids, err = decodeIDs(ids, chunk, r.ids); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	if err != nil || !found {
		return err
	}

	chunks := map[postingChunkRef][]int64{}
	var need []postingChunkRef
	wantAll := func(m postingMulti) {
		for _, s := range m.starts {
			ref := postingChunkRef{m.k, s}
			if _, ok := chunks[ref]; !ok {
				chunks[ref] = nil
				need = append(need, ref)
			}
		}
	}
	fetch := func() error {
		if len(need) == 0 {
			return nil
		}
		ks := make([][]byte, len(need))
		cs := make([]int64, len(need))
		for i, ref := range need {
			ks[i], cs[i] = []byte(ref.k), ref.chunk
		}
		need = need[:0]
		rows, err := t.query(postingFetchSQL, scope, ks, cs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				k, ids []byte
				chunk  int64
			)
			if err := rows.Scan(&k, &chunk, &ids); err != nil {
				return err
			}
			decoded, err := decodeIDs(ids, chunk, nil)
			if err != nil {
				return err
			}
			chunks[postingChunkRef{string(k), chunk}] = decoded
		}
		return rows.Err()
	}
	// rangeIDs is every number of range r that the chunks read so far hold.
	rangeIDs := func(r *postingRangeRead) []int64 {
		out := slices.Clone(r.ids)
		for _, m := range r.multi {
			for _, s := range m.starts {
				out = append(out, chunks[postingChunkRef{m.k, s}]...)
			}
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	// wantFor asks for the chunks of the keys of range r with several chunks
	// that can hold one of candidates, which is sorted.
	wantFor := func(r *postingRangeRead, candidates []int64) {
		for _, m := range r.multi {
			for _, c := range candidates {
				i := sort.Search(len(m.starts), func(i int) bool { return m.starts[i] > c }) - 1
				ref := postingChunkRef{m.k, m.starts[i]}
				if _, ok := chunks[ref]; !ok {
					chunks[ref] = nil
					need = append(need, ref)
				}
			}
		}
	}

	// The ranges of all, smallest first. The first one gives the candidates.
	order := make([]int, len(all))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return reads[order[a]].total < reads[order[b]].total })
	allEmpty := len(all) == 0 || reads[order[0]].total == 0
	for i := len(all); i < len(ranges); i++ {
		for _, m := range reads[i].multi {
			wantAll(m)
		}
	}
	var candidates []int64
	if !allEmpty {
		driver := &reads[order[0]]
		for _, m := range driver.multi {
			wantAll(m)
		}
		if len(driver.multi) > 0 {
			if err := fetch(); err != nil {
				return err
			}
		}
		candidates = rangeIDs(driver)
		for _, j := range order[1:] {
			wantFor(&reads[j], candidates)
		}
	}
	if err := fetch(); err != nil {
		return err
	}
	if !allEmpty {
		for _, j := range order[1:] {
			if len(candidates) == 0 {
				break
			}
			candidates = intersectSorted(candidates, rangeIDs(&reads[j]))
		}
	}
	result := candidates
	for i := len(all); i < len(ranges); i++ {
		result = append(result, rangeIDs(&reads[i])...)
	}
	slices.Sort(result)
	result = slices.Compact(result)
	// The lowest numbers, so that a limited scan yields the same rows while
	// the index does not change.
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	if len(result) == 0 {
		return nil
	}
	keys, err := queryBatch(t, postingResolveSQL, []any{scope, result}, len(result), func(rows pgx.Rows) (string, error) {
		var key string
		err := rows.Scan(&key)
		return key, err
	})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

// intersectSorted returns the numbers in both a and b, which are sorted and
// hold no duplicates.
func intersectSorted(a, b []int64) []int64 {
	out := a[:0:0]
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}
