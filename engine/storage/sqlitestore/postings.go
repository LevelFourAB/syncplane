package sqlitestore

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Posting indexes (see storage.WriteTx.PutPostings) are stored as packed lists
// of row numbers:
//
//	posting_scopes  (workspace, entity, index) -> scope id, next row number,
//	                rows with a number
//	posting_rows    (scope, row key) -> row number
//	posting_nums    (scope, row number) -> row key
//	posting_heads   (scope, posting key) -> count, chunk directory, tail
//	posting_chunks  chunk id -> scope, row numbers
//
// A row gets a number the first time a posting is added for it. Numbers are
// dense per scope and given out in ascending order, so the numbers of one
// posting key are small deltas apart and a row new to the index goes at the
// end of every list it joins. A number is never given out twice in one scope:
// ClearPostings drops the scope, and the next add creates a scope with a new
// id. So a committed (scope, number) pair names one row key for the life of
// the file.
//
// The numbers of one posting key are, in ascending order, the numbers of each
// chunk of its directory and then the numbers of its tail. Each directory
// entry names a chunk record, the highest number the chunk holds and how many
// numbers it holds; every number of a chunk is above the highest number of the
// chunk before it, and every number of the tail is above the highest number of
// the last chunk. A number added above the last chunk goes into the tail. A
// full tail moves into the last chunk when it fits, or else becomes a new
// chunk. A number added below goes into the chunk whose range holds it, and a
// chunk that grows past postingChunkCap splits in half. A chunk that shrinks
// below postingMergeBelow merges with a neighbour when the two fit in one.
//
// The count of a head is the number of numbers it holds. PostingCounts reads
// it without the chunks, and ScanPostings reads only the chunks of a large
// key whose range holds a candidate.
//
// There is no list of the keys each row holds: the caller names the keys it
// removes. DeletePostings removes the row's numbers from posting_rows and
// posting_nums, so a number that the caller did not remove from a head names
// no row key any more, and ScanPostings skips it.

const (
	// postingTailCap is how many numbers a head holds before they move into a
	// chunk. A key that few rows hold is then one record.
	postingTailCap = 256
	// postingChunkCap is how many numbers a chunk holds before it splits.
	postingChunkCap = 512
	// postingMergeBelow is the size below which a chunk merges with a
	// neighbour. It is far below half of postingChunkCap, so that a chunk
	// that a split made does not merge again on the next removal.
	postingMergeBelow = postingChunkCap / 4
)

// postingChunk is one entry of a head's chunk directory. nums is nil until
// the chunk is loaded.
type postingChunk struct {
	id    int64 // 0 for a chunk that is not stored yet
	hi    int64
	n     int
	nums  []int64
	dirty bool
}

// postingHead is the head of one posting key, as the write path changes it.
type postingHead struct {
	k      []byte
	exists bool
	cnt    int
	chunks []*postingChunk
	tail   []int64
	freed  []int64
	dirty  bool
}

// appendNums appends nums, which must be strictly ascending and above 0, as
// uvarint deltas, the first from 0.
func appendNums(out []byte, nums []int64) []byte {
	prev := int64(0)
	for _, n := range nums {
		out = binary.AppendUvarint(out, uint64(n-prev))
		prev = n
	}
	return out
}

// decodeNums reverses appendNums and appends the numbers to out. It returns an
// error for bytes that appendNums cannot have made.
func decodeNums(b []byte, out []int64) ([]int64, error) {
	prev := int64(0)
	for len(b) > 0 {
		d, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errors.New("truncated row number")
		}
		if d == 0 || d > math.MaxInt64-uint64(prev) {
			return nil, fmt.Errorf("row number delta %d after %d is out of range", d, prev)
		}
		prev += int64(d)
		out = append(out, prev)
		b = b[n:]
	}
	return out, nil
}

// encodeDir packs a chunk directory: the chunk count, then per chunk its
// record id, its highest number as a delta from the chunk before, and its
// size. An empty directory packs to nil.
func encodeDir(chunks []*postingChunk) []byte {
	if len(chunks) == 0 {
		return nil
	}
	out := binary.AppendUvarint(make([]byte, 0, 1+len(chunks)*8), uint64(len(chunks)))
	prev := int64(0)
	for _, c := range chunks {
		out = binary.AppendUvarint(out, uint64(c.id))
		out = binary.AppendUvarint(out, uint64(c.hi-prev))
		out = binary.AppendUvarint(out, uint64(c.n))
		prev = c.hi
	}
	return out
}

// decodeDir reverses encodeDir. It returns an error for bytes that encodeDir
// cannot have made.
func decodeDir(b []byte) ([]*postingChunk, error) {
	if len(b) == 0 {
		return nil, nil
	}
	next := func(what string) (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, fmt.Errorf("truncated %s", what)
		}
		b = b[n:]
		return v, nil
	}
	cnt, err := next("chunk count")
	if err != nil {
		return nil, err
	}
	// Three bytes at least per chunk, so a count past that is corrupt and
	// must not size an allocation.
	if cnt == 0 || cnt > uint64(len(b)/3) {
		return nil, fmt.Errorf("directory of %d chunks in %d bytes", cnt, len(b))
	}
	out := make([]*postingChunk, 0, cnt)
	prev := int64(0)
	for range cnt {
		id, err := next("chunk id")
		if err != nil {
			return nil, err
		}
		d, err := next("chunk bound")
		if err != nil {
			return nil, err
		}
		size, err := next("chunk size")
		if err != nil {
			return nil, err
		}
		if id == 0 || id > math.MaxInt64 {
			return nil, fmt.Errorf("chunk id %d is out of range", id)
		}
		if d == 0 || d > math.MaxInt64-uint64(prev) {
			return nil, fmt.Errorf("chunk bound delta %d after %d is out of range", d, prev)
		}
		if size == 0 || size > d || size > 2*postingChunkCap {
			return nil, fmt.Errorf("chunk of %d row numbers", size)
		}
		prev += int64(d)
		out = append(out, &postingChunk{id: int64(id), hi: prev, n: int(size)})
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("%d bytes after the chunk directory", len(b))
	}
	return out, nil
}

// fillHead decodes the stored directory and tail of a head and checks them
// against its count.
func fillHead(h *postingHead, dir, tail []byte) error {
	var err error
	if h.chunks, err = decodeDir(dir); err != nil {
		return err
	}
	if h.tail, err = decodeNums(tail, nil); err != nil {
		return fmt.Errorf("tail: %w", err)
	}
	return checkHead(h.cnt, h.chunks, h.tail)
}

// checkHead checks that a head's count is what its directory and tail hold,
// and that the tail is above the last chunk.
func checkHead(cnt int, chunks []*postingChunk, tail []int64) error {
	total := len(tail)
	for _, c := range chunks {
		total += c.n
	}
	if total != cnt {
		return fmt.Errorf("head counts %d row numbers and holds %d", cnt, total)
	}
	if len(chunks) > 0 && len(tail) > 0 && tail[0] <= chunks[len(chunks)-1].hi {
		return fmt.Errorf("tail starts at %d, at or below chunk bound %d", tail[0], chunks[len(chunks)-1].hi)
	}
	return nil
}

// postingScope returns the scope id of one posting index, or 0 when there is
// none.
func (t *sqliteTx) postingScope(entity, index string) (int64, error) {
	var id int64
	err := t.queryRow(
		`SELECT id FROM posting_scopes WHERE workspace_id = ? AND entity = ? AND idx = ?`,
		t.ws, entity, index,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// postingRowNum returns the number of a row in a scope, or 0 when the row has
// none.
func (t *sqliteTx) postingRowNum(scope int64, key string) (int64, error) {
	var num int64
	err := t.queryRow(`SELECT num FROM posting_rows WHERE scope = ? AND key = ?`, scope, key).Scan(&num)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return num, err
}

func (t *sqliteTx) PutPostings(entity, index, key string, remove, add [][]byte) error {
	if len(add) == 0 && len(remove) == 0 {
		return nil
	}
	// remove applies before add, so a key in both is held after the call: it
	// is an add and not a remove.
	adds := make(map[string]struct{}, len(add))
	for _, k := range add {
		adds[string(k)] = struct{}{}
	}
	removes := make(map[string]struct{}, len(remove))
	for _, k := range remove {
		if _, both := adds[string(k)]; !both {
			removes[string(k)] = struct{}{}
		}
	}
	var (
		scope, num int64
		err        error
	)
	if len(adds) > 0 {
		if err := t.register(); err != nil {
			return err
		}
		if scope, err = t.createPostingScope(entity, index); err != nil {
			return err
		}
		if num, err = t.postingRowNum(scope, key); err != nil {
			return err
		}
		if num == 0 {
			if num, err = t.newPostingRow(scope, key); err != nil {
				return err
			}
		}
	} else {
		// No register(): a call that only removes must leave no trace in a
		// workspace nothing has written, and a row without a number holds
		// nothing to remove.
		if scope, err = t.postingScope(entity, index); err != nil || scope == 0 {
			return err
		}
		if num, err = t.postingRowNum(scope, key); err != nil || num == 0 {
			return err
		}
	}
	// Sorted so that the statements of one call run in the same order every
	// time, which keeps a retried transaction identical to its first attempt.
	for _, k := range sortedKeys(removes) {
		if err := t.changePosting(scope, []byte(k), num, false); err != nil {
			return err
		}
	}
	for _, k := range sortedKeys(adds) {
		if err := t.changePosting(scope, []byte(k), num, true); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// createPostingScope returns the scope id of one posting index and creates it
// when there is none.
func (t *sqliteTx) createPostingScope(entity, index string) (int64, error) {
	id, err := t.postingScope(entity, index)
	if err != nil || id != 0 {
		return id, err
	}
	res, err := t.exec(
		`INSERT INTO posting_scopes (workspace_id, entity, idx) VALUES (?, ?, ?)`,
		t.ws, entity, index,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// newPostingRow gives a row the next number of its scope.
func (t *sqliteTx) newPostingRow(scope int64, key string) (int64, error) {
	var num int64
	if err := t.queryRow(
		`UPDATE posting_scopes SET next_num = next_num + 1, row_count = row_count + 1 WHERE id = ? RETURNING next_num - 1`, scope,
	).Scan(&num); err != nil {
		return 0, err
	}
	if _, err := t.exec(`INSERT INTO posting_rows (scope, key, num) VALUES (?, ?, ?)`, scope, key, num); err != nil {
		return 0, err
	}
	if _, err := t.exec(`INSERT INTO posting_nums (scope, num, key) VALUES (?, ?, ?)`, scope, num, key); err != nil {
		return 0, err
	}
	return num, nil
}

// changePosting adds num to, or removes it from, the head of k, and writes
// what changed. Adding a number the head holds, or removing one it does not,
// writes nothing.
func (t *sqliteTx) changePosting(scope int64, k []byte, num int64, add bool) error {
	h, err := t.loadPostingHead(scope, k)
	if err != nil {
		return err
	}
	if add {
		err = t.addPostingNum(h, num)
	} else {
		if !h.exists {
			return nil
		}
		err = t.removePostingNum(h, num)
	}
	if err != nil {
		return fmt.Errorf("sqlitestore: posting key %x: %w", k, err)
	}
	return t.savePostingHead(scope, h)
}

func (t *sqliteTx) loadPostingHead(scope int64, k []byte) (*postingHead, error) {
	h := &postingHead{k: k}
	var dir, tail []byte
	err := t.queryRow(
		`SELECT cnt, dir, tail FROM posting_heads WHERE scope = ? AND k = ?`, scope, k,
	).Scan(&h.cnt, &dir, &tail)
	if errors.Is(err, sql.ErrNoRows) {
		return h, nil
	}
	if err != nil {
		return nil, err
	}
	h.exists = true
	if err := fillHead(h, dir, tail); err != nil {
		return nil, fmt.Errorf("sqlitestore: posting key %x: %w", k, err)
	}
	return h, nil
}

// loadPostingChunk reads the numbers of c, once.
func (t *sqliteTx) loadPostingChunk(c *postingChunk) error {
	if c.nums != nil || c.id == 0 {
		return nil
	}
	var b []byte
	if err := t.queryRow(`SELECT nums FROM posting_chunks WHERE id = ?`, c.id).Scan(&b); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("chunk %d is missing", c.id)
		}
		return err
	}
	nums, err := decodeChunk(c, b, make([]int64, 0, c.n+1))
	if err != nil {
		return err
	}
	c.nums = nums
	return nil
}

// decodeChunk decodes the stored numbers of c and checks them against its
// directory entry.
func decodeChunk(c *postingChunk, b []byte, out []int64) ([]int64, error) {
	start := len(out)
	out, err := decodeNums(b, out)
	if err != nil {
		return nil, fmt.Errorf("chunk %d: %w", c.id, err)
	}
	got := out[start:]
	if len(got) != c.n {
		return nil, fmt.Errorf("chunk %d holds %d row numbers, its directory entry says %d", c.id, len(got), c.n)
	}
	if got[len(got)-1] != c.hi {
		return nil, fmt.Errorf("chunk %d ends at %d, its directory entry says %d", c.id, got[len(got)-1], c.hi)
	}
	return out, nil
}

// chunkFor returns the position of the chunk whose range holds num, or
// len(h.chunks) when num is above the last chunk.
func (h *postingHead) chunkFor(num int64) int {
	return sort.Search(len(h.chunks), func(i int) bool { return h.chunks[i].hi >= num })
}

func (t *sqliteTx) addPostingNum(h *postingHead, num int64) error {
	i := h.chunkFor(num)
	if i == len(h.chunks) {
		pos, found := slices.BinarySearch(h.tail, num)
		if found {
			return nil
		}
		h.tail = slices.Insert(h.tail, pos, num)
		h.cnt++
		h.dirty = true
		if len(h.tail) >= postingTailCap {
			return t.flushPostingTail(h)
		}
		return nil
	}
	c := h.chunks[i]
	if err := t.loadPostingChunk(c); err != nil {
		return err
	}
	pos, found := slices.BinarySearch(c.nums, num)
	if found {
		return nil
	}
	c.nums = slices.Insert(c.nums, pos, num)
	c.n++
	c.dirty = true
	h.cnt++
	h.dirty = true
	if c.n > postingChunkCap {
		half := c.n / 2
		right := &postingChunk{hi: c.hi, n: c.n - half, nums: slices.Clone(c.nums[half:]), dirty: true}
		c.nums = c.nums[:half:half]
		c.n = half
		c.hi = c.nums[half-1]
		h.chunks = slices.Insert(h.chunks, i+1, right)
	}
	return nil
}

// flushPostingTail moves a full tail into the last chunk when it fits, or into
// a new chunk.
func (t *sqliteTx) flushPostingTail(h *postingHead) error {
	if n := len(h.chunks); n > 0 && h.chunks[n-1].n+len(h.tail) <= postingChunkCap {
		last := h.chunks[n-1]
		if err := t.loadPostingChunk(last); err != nil {
			return err
		}
		last.nums = append(last.nums, h.tail...)
		last.n = len(last.nums)
		last.hi = h.tail[len(h.tail)-1]
		last.dirty = true
	} else {
		h.chunks = append(h.chunks, &postingChunk{
			hi: h.tail[len(h.tail)-1], n: len(h.tail), nums: h.tail, dirty: true,
		})
	}
	h.tail = nil
	return nil
}

func (t *sqliteTx) removePostingNum(h *postingHead, num int64) error {
	i := h.chunkFor(num)
	if i == len(h.chunks) {
		pos, found := slices.BinarySearch(h.tail, num)
		if !found {
			return nil
		}
		h.tail = slices.Delete(h.tail, pos, pos+1)
		h.cnt--
		h.dirty = true
		return nil
	}
	c := h.chunks[i]
	if err := t.loadPostingChunk(c); err != nil {
		return err
	}
	pos, found := slices.BinarySearch(c.nums, num)
	if !found {
		return nil
	}
	c.nums = slices.Delete(c.nums, pos, pos+1)
	c.n--
	c.dirty = true
	h.cnt--
	h.dirty = true
	switch {
	case c.n == 0:
		if c.id != 0 {
			h.freed = append(h.freed, c.id)
		}
		h.chunks = slices.Delete(h.chunks, i, i+1)
	case c.n < postingMergeBelow:
		return t.mergePostingChunk(h, i)
	default:
		c.hi = c.nums[c.n-1]
	}
	return nil
}

// mergePostingChunk merges the small chunk at position i with its right
// neighbour, or with its left one when it is the last chunk, when the two fit
// in one chunk. The left chunk keeps its record and the right one is freed.
func (t *sqliteTx) mergePostingChunk(h *postingHead, i int) error {
	c := h.chunks[i]
	c.hi = c.nums[c.n-1]
	j := i + 1
	if j == len(h.chunks) {
		j = i - 1
	}
	if j < 0 || h.chunks[j].n+c.n > postingChunkCap {
		return nil
	}
	left, right := h.chunks[min(i, j)], h.chunks[max(i, j)]
	if err := t.loadPostingChunk(left); err != nil {
		return err
	}
	if err := t.loadPostingChunk(right); err != nil {
		return err
	}
	left.nums = append(left.nums, right.nums...)
	left.n = len(left.nums)
	left.hi = right.hi
	left.dirty = true
	if right.id != 0 {
		h.freed = append(h.freed, right.id)
	}
	h.chunks = slices.Delete(h.chunks, max(i, j), max(i, j)+1)
	return nil
}

// savePostingHead writes the chunks of h that changed and then the head, or
// deletes the head when it holds nothing.
func (t *sqliteTx) savePostingHead(scope int64, h *postingHead) error {
	if !h.dirty {
		return nil
	}
	for _, id := range h.freed {
		if _, err := t.exec(`DELETE FROM posting_chunks WHERE id = ?`, id); err != nil {
			return err
		}
	}
	h.freed = nil
	if h.cnt == 0 {
		// Every chunk is freed by now: a chunk is removed when its last number
		// goes.
		if h.exists {
			_, err := t.exec(`DELETE FROM posting_heads WHERE scope = ? AND k = ?`, scope, h.k)
			return err
		}
		return nil
	}
	for _, c := range h.chunks {
		if !c.dirty {
			continue
		}
		blob := appendNums(make([]byte, 0, 2*len(c.nums)), c.nums)
		if c.id == 0 {
			res, err := t.exec(`INSERT INTO posting_chunks (scope, nums) VALUES (?, ?)`, scope, blob)
			if err != nil {
				return err
			}
			if c.id, err = res.LastInsertId(); err != nil {
				return err
			}
		} else if _, err := t.exec(`UPDATE posting_chunks SET nums = ? WHERE id = ?`, blob, c.id); err != nil {
			return err
		}
		c.dirty = false
	}
	dir := encodeDir(h.chunks)
	var tail []byte
	if len(h.tail) > 0 {
		tail = appendNums(make([]byte, 0, 2*len(h.tail)), h.tail)
	}
	var err error
	if h.exists {
		_, err = t.exec(`UPDATE posting_heads SET cnt = ?, dir = ?, tail = ? WHERE scope = ? AND k = ?`,
			h.cnt, dir, tail, scope, h.k)
	} else {
		_, err = t.exec(`INSERT INTO posting_heads (scope, k, cnt, dir, tail) VALUES (?, ?, ?, ?, ?)`,
			scope, h.k, h.cnt, dir, tail)
		h.exists = err == nil
	}
	h.dirty = false
	return err
}

func (t *sqliteTx) DeletePostings(entity, index, key string, held [][]byte) error {
	// No register(): the row is going away, and a workspace nothing has
	// written holds no row to delete.
	scope, err := t.postingScope(entity, index)
	if err != nil || scope == 0 {
		return err
	}
	num, err := t.postingRowNum(scope, key)
	if err != nil || num == 0 {
		return err
	}
	removes := make(map[string]struct{}, len(held))
	for _, k := range held {
		removes[string(k)] = struct{}{}
	}
	for _, k := range sortedKeys(removes) {
		if err := t.changePosting(scope, []byte(k), num, false); err != nil {
			return err
		}
	}
	// The number goes from both maps and from the Store's cache, so a number
	// that held does not name, and that stays in a head, resolves to no row
	// and is never yielded.
	if _, err := t.exec(`DELETE FROM posting_rows WHERE scope = ? AND key = ?`, scope, key); err != nil {
		return err
	}
	if _, err := t.exec(`UPDATE posting_scopes SET row_count = row_count - 1 WHERE id = ?`, scope); err != nil {
		return err
	}
	if t.forgotten == nil {
		t.forgotten = make(map[postingNum]struct{})
	}
	t.forgotten[postingNum{scope, num}] = struct{}{}
	_, err = t.exec(`DELETE FROM posting_nums WHERE scope = ? AND num = ?`, scope, num)
	return err
}

func (t *sqliteTx) ClearPostings(entity, index string) error {
	// No register(), for the reason DeletePostings gives.
	scope, err := t.postingScope(entity, index)
	if err != nil || scope == 0 {
		return err
	}
	return t.dropPostingScope(scope)
}

// dropPostingScope deletes one scope with everything it holds. The next add
// to the same index creates a scope with a new id, so no row number of the old
// scope can be read as one of the new.
func (t *sqliteTx) dropPostingScope(scope int64) error {
	// Nothing here decodes a record, so a corrupt record cannot stop the
	// clear that a rebuild starts with, or a workspace delete.
	for _, q := range []string{
		`DELETE FROM posting_chunks WHERE scope = ?`,
		`DELETE FROM posting_heads WHERE scope = ?`,
		`DELETE FROM posting_rows WHERE scope = ?`,
		`DELETE FROM posting_nums WHERE scope = ?`,
		`DELETE FROM posting_scopes WHERE id = ?`,
	} {
		if _, err := t.exec(q, scope); err != nil {
			return err
		}
	}
	return nil
}

// postingRangeSQL is the statement that reads the heads of one range of a
// scope, with the given columns. Its arguments are the scope id, then lo when
// hasLo, and hi when hasHi.
func postingRangeSQL(cols string, hasLo, hasHi bool) string {
	q := `SELECT ` + cols + ` FROM posting_heads WHERE scope = ?`
	if hasLo {
		q += ` AND k >= ?`
	}
	if hasHi {
		q += ` AND k < ?`
	}
	return q
}

func rangeArgs(scope int64, r storage.IndexRange) []any {
	args := []any{scope}
	if r.Lo != nil {
		args = append(args, r.Lo)
	}
	if r.Hi != nil {
		args = append(args, r.Hi)
	}
	return args
}

// emptyRange reports whether r holds no key at all.
func emptyRange(r storage.IndexRange) bool {
	return r.Lo != nil && r.Hi != nil && bytes.Compare(r.Lo, r.Hi) >= 0
}

func (t *sqliteTx) PostingCounts(entity, index string, ranges []storage.IndexRange, max int) ([]int, int, error) {
	out := make([]int, len(ranges))
	var scope int64
	var rows int
	err := t.queryRow(
		`SELECT id, row_count FROM posting_scopes WHERE workspace_id = ? AND entity = ? AND idx = ?`,
		t.ws, entity, index,
	).Scan(&scope, &rows)
	if errors.Is(err, sql.ErrNoRows) {
		return out, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	errStop := errors.New("stop")
	for i, r := range ranges {
		if emptyRange(r) {
			continue
		}
		sum := 0
		err := t.scanRows(postingRangeSQL(`cnt`, r.Lo != nil, r.Hi != nil), rangeArgs(scope, r), func(rows *sql.Rows) error {
			var cnt int
			if err := rows.Scan(&cnt); err != nil {
				return err
			}
			sum += cnt
			if max > 0 && sum > max {
				return errStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return nil, 0, err
		}
		out[i] = sum
	}
	return out, rows, nil
}

// storedHead is a head as the read path holds it: the count, the decoded
// directory and the stored tail.
type storedHead struct {
	cnt    int
	chunks []*postingChunk
	tail   []byte
}

// readHeads reads the heads of the keys in r and returns them with the sum of
// their counts.
func (t *sqliteTx) readHeads(scope int64, r storage.IndexRange) ([]storedHead, int, error) {
	if emptyRange(r) {
		return nil, 0, nil
	}
	var (
		heads []storedHead
		total int
	)
	err := t.scanRows(postingRangeSQL(`k, cnt, dir, tail`, r.Lo != nil, r.Hi != nil), rangeArgs(scope, r), func(rows *sql.Rows) error {
		var (
			k, dir, tail []byte
			h            storedHead
		)
		if err := rows.Scan(&k, &h.cnt, &dir, &tail); err != nil {
			return err
		}
		var err error
		if h.chunks, err = decodeDir(dir); err != nil {
			return fmt.Errorf("sqlitestore: posting key %x: %w", k, err)
		}
		// Scan reuses its buffers once the next row is read.
		h.tail = bytes.Clone(tail)
		heads = append(heads, h)
		total += h.cnt
		return nil
	})
	return heads, total, err
}

// readChunk appends the numbers of chunk c to out.
func (t *sqliteTx) readChunk(c *postingChunk, out []int64) ([]int64, error) {
	var b []byte
	if err := t.queryRow(`SELECT nums FROM posting_chunks WHERE id = ?`, c.id).Scan(&b); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("sqlitestore: posting chunk %d is missing", c.id)
		}
		return nil, err
	}
	out, err := decodeChunk(c, b, out)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: posting %w", err)
	}
	return out, nil
}

// allNums returns every number the heads hold, ascending and without repeats.
func (t *sqliteTx) allNums(heads []storedHead) ([]int64, error) {
	var out []int64
	for _, h := range heads {
		for _, c := range h.chunks {
			var err error
			if out, err = t.readChunk(c, out); err != nil {
				return nil, err
			}
		}
		var err error
		if out, err = decodeNums(h.tail, out); err != nil {
			return nil, fmt.Errorf("sqlitestore: posting tail: %w", err)
		}
	}
	if len(heads) > 1 {
		slices.Sort(out)
		out = slices.Compact(out)
	}
	return out, nil
}

// keepHeld returns the numbers of cand, which is ascending, that one of the
// heads holds. It reads only the chunks whose range holds a number of cand.
func (t *sqliteTx) keepHeld(cand []int64, heads []storedHead) ([]int64, error) {
	held := make([]bool, len(cand))
	mark := func(nums []int64) {
		i := 0
		for _, n := range nums {
			for i < len(cand) && cand[i] < n {
				i++
			}
			if i == len(cand) {
				return
			}
			if cand[i] == n {
				held[i] = true
			}
		}
	}
	var buf []int64
	for _, h := range heads {
		lo := int64(1)
		for _, c := range h.chunks {
			// The chunk holds numbers in [lo, c.hi]: read it only when a
			// candidate falls there.
			i, _ := slices.BinarySearch(cand, lo)
			lo = c.hi + 1
			if i == len(cand) || cand[i] > c.hi {
				continue
			}
			var err error
			if buf, err = t.readChunk(c, buf[:0]); err != nil {
				return nil, err
			}
			mark(buf)
		}
		var err error
		if buf, err = decodeNums(h.tail, buf[:0]); err != nil {
			return nil, fmt.Errorf("sqlitestore: posting tail: %w", err)
		}
		mark(buf)
	}
	out := cand[:0]
	for i, n := range cand {
		if held[i] {
			out = append(out, n)
		}
	}
	return out, nil
}

func (t *sqliteTx) ScanPostings(entity, index string, all, any []storage.IndexRange, limit int, fn func(rowKey string) error) error {
	if len(all) == 0 && len(any) == 0 {
		return nil
	}
	scope, err := t.postingScope(entity, index)
	if err != nil || scope == 0 {
		return err
	}
	var matched []int64
	if len(all) > 0 {
		type rangeHeads struct {
			heads []storedHead
			total int
		}
		rs := make([]rangeHeads, 0, len(all))
		empty := false
		for _, r := range all {
			heads, total, err := t.readHeads(scope, r)
			if err != nil {
				return err
			}
			if total == 0 {
				empty = true
				break
			}
			rs = append(rs, rangeHeads{heads, total})
		}
		if !empty {
			// The smallest range is read whole, and each other range only
			// where it can hold a candidate that is left.
			sort.SliceStable(rs, func(i, j int) bool { return rs[i].total < rs[j].total })
			if matched, err = t.allNums(rs[0].heads); err != nil {
				return err
			}
			for _, r := range rs[1:] {
				if len(matched) == 0 {
					break
				}
				if matched, err = t.keepHeld(matched, r.heads); err != nil {
					return err
				}
			}
		}
	}
	if len(any) > 0 {
		var heads []storedHead
		for _, r := range any {
			hs, _, err := t.readHeads(scope, r)
			if err != nil {
				return err
			}
			heads = append(heads, hs...)
		}
		nums, err := t.allNums(heads)
		if err != nil {
			return err
		}
		if len(nums) > 0 {
			matched = append(matched, nums...)
			slices.Sort(matched)
			matched = slices.Compact(matched)
		}
	}
	keys, err := t.resolvePostingNums(scope, matched, limit)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// resolvePostingNums returns the row keys of nums, which is ascending, in the
// same order, and stops at limit keys when limit > 0. A number that names no
// row (see DeletePostings) is skipped.
func (t *sqliteTx) resolvePostingNums(scope int64, nums []int64, limit int) ([]string, error) {
	want := len(nums)
	if limit > 0 && limit < want {
		want = limit
	}
	keys := make([]string, 0, want)
	if len(nums) == 0 {
		return keys, nil
	}
	// One range read of the map costs less than a point read per number when
	// the numbers are dense in their span. The factor is about what a point
	// read costs against one step of a range read.
	if span := nums[len(nums)-1] - nums[0] + 1; span <= int64(len(nums))*postingDenseSpan {
		i := 0
		errFull := errors.New("full")
		err := t.scanRows(
			`SELECT num, key FROM posting_nums WHERE scope = ? AND num >= ? AND num <= ? ORDER BY num`,
			[]any{scope, nums[0], nums[len(nums)-1]},
			func(rows *sql.Rows) error {
				var (
					num int64
					key string
				)
				if err := rows.Scan(&num, &key); err != nil {
					return err
				}
				for i < len(nums) && nums[i] < num {
					i++
				}
				if i == len(nums) {
					return errFull
				}
				if nums[i] == num {
					keys = append(keys, key)
					if len(keys) == want {
						return errFull
					}
				}
				return nil
			})
		if err != nil && !errors.Is(err, errFull) {
			return nil, err
		}
		return keys, nil
	}
	// A point read costs about 1.7 us on SQLite, which for the ten rows of a
	// rare term is a fifth of the search; a cache hit costs a map lookup.
	for _, num := range nums {
		// A row this transaction forgot is still in the cache until it
		// commits.
		_, forgot := t.forgotten[postingNum{scope, num}]
		if key, ok := t.postingKeys.get(postingNum{scope, num}); ok && !forgot {
			keys = append(keys, key)
			if len(keys) == want {
				break
			}
			continue
		}
		var key string
		err := t.queryRow(`SELECT key FROM posting_nums WHERE scope = ? AND num = ?`, scope, num).Scan(&key)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if t.learnRowKeys {
			t.postingKeys.addIf(postingNum{scope, num}, key, func() bool {
				n := t.postingForgets.Load()
				return n == t.postingGen && n%2 == 0
			})
		}
		keys = append(keys, key)
		if len(keys) == want {
			break
		}
	}
	return keys, nil
}

// postingDenseSpan is the span per wanted number up to which
// resolvePostingNums reads the map as one range.
const postingDenseSpan = 8
