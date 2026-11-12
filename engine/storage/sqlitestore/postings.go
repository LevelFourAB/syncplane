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
//	posting_pages   (scope, lowest key) -> the keys of one key range that few
//	                rows hold, each with its row numbers
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
// Each posting key with postings is in exactly one of two places. A key that
// few rows hold is in a page of posting_pages, together with the keys next to
// it in key order. A key that many rows hold has a head in posting_heads. A
// scope with few rows thus holds its keys in a few records and not in one
// record per key.
//
// One page record (scope, lo, body) holds every page key of the scope from lo
// up to the lo of the next page, in the format of postings_page.go. No page
// key is below the lo of the first page: a key below every page goes into a
// new page with the empty key as lo. lo is not always a key of the page: a
// removal does not change it. A page that a write makes larger than
// postingPageBudget bytes splits into parts of about equal size, each part
// after the first with its first key as lo; a page of one key may be larger.
// A page that a write makes smaller than a quarter of the budget merges into
// the page before it when the two fit in the budget, or else takes the page
// after it when those two fit. A page that loses its last key is deleted.
//
// A page key gets a head when it has more than postingPageMaxRows row
// numbers. A key with a head moves back to a page when a write leaves it with
// fewer than half of postingPageMaxRows. The gap between the two limits stops
// a key that gains and loses one row from moving at each write. A head can
// hold fewer numbers than half of the limit: a file that a version of this
// package without pages wrote has a head for every key. Such a head follows
// the same rule as every head: its key moves to a page when a write leaves
// it with fewer than half of postingPageMaxRows, and stays where it is with
// more.
//
// PutPostings and DeletePostings write the row numbers, the heads and the
// chunks at the call. They hold the page changes in memory (see
// postingPages), so that a transaction that changes one page in many calls
// reads it and writes it one time. flushPostings stores the changes before
// the transaction commits, before it reads postings, before a savepoint
// starts, and when the held pages pass postingHeldMax bytes. A transaction
// whose changes could not be stored does not commit (see
// sqliteTx.postingErr).
//
// The numbers of a key with a head are, in ascending order, the numbers of
// each chunk of its directory and then the numbers of its tail. Each directory
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
// it without the chunks, and counts a page key from its page without decoding
// a number. ScanPostings reads only the chunks of a large key whose range
// holds a candidate.
//
// There is no list of the keys each row holds: the caller names the keys it
// removes. DeletePostings removes the row's numbers from posting_rows and
// posting_nums, so a number that the caller did not remove from a page or a
// head names no row key any more, and ScanPostings skips it.

const (
	// postingTailCap is how many numbers a head holds before they move into a
	// chunk. A key with a head and few rows is then one record.
	postingTailCap = 256
	// postingChunkCap is how many numbers a chunk holds before it splits.
	postingChunkCap = 512
	// postingMergeBelow is the size below which a chunk merges with a
	// neighbour. It is far below half of postingChunkCap, so that a chunk
	// that a split made does not merge again on the next removal.
	postingMergeBelow = postingChunkCap / 4
)

// postingPageBudget is the most bytes a page body with two keys or more
// takes, and postingPageMaxRows the most row numbers a key in a page has.
// They are variables so that a spec can make pages small.
//
// posting_pages is a WITHOUT ROWID table. SQLite keeps a record of such a
// table on its b-tree page only while the record is below about a quarter of
// the page, which is 1 KB at the default page size, and moves the rest of a
// larger record to an overflow page. A budget of 512 bytes keeps a page
// record below that with its key, and so does a page of one key with 128 row
// numbers of up to six bytes each. A larger budget makes the tables larger
// and the writes slower, because a change to one key writes its page again.
var (
	postingPageBudget  = 512
	postingPageMaxRows = 128
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
	if err := t.postingFailed(); err != nil {
		return err
	}
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
	pc := t.pagesOf(scope)
	for _, k := range sortedKeys(removes) {
		if err := pc.change([]byte(k), num, false); err != nil {
			return err
		}
	}
	for _, k := range sortedKeys(adds) {
		if err := pc.change([]byte(k), num, true); err != nil {
			return err
		}
	}
	return t.boundPostingPages(pc)
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

// postingPage is a page that postingPages reads or makes.
type postingPage struct {
	lo []byte
	// next is the lo of the page after this one, or nil for the last page of
	// the scope. The page holds the page keys of the scope in [lo, next).
	next    []byte
	entries []pageEntry
	stored  bool
	dirty   bool
	// shrunk is set when a change took a key or a row number out of the
	// page, which can make it small enough to merge.
	shrunk bool
	// cut is set for the second part of a page that insert cut in two. Such
	// a page is in memory only, right after the page it came from, and flush
	// joins the two again before it writes.
	cut bool
}

// postingPages makes the changes of PutPostings and DeletePostings calls to
// the keys of one scope. It keeps each page it reads, so that the keys of one
// page cost one read, and flush writes each changed page one time, for all
// the calls since the flush before. Until flush, the tables do not hold the
// page changes, and nothing else may change the pages of the scope.
type postingPages struct {
	t     *sqliteTx
	scope int64
	// pages holds the pages read so far and the parts insert cut them into,
	// in lo order.
	pages []*postingPage
	// held is about the number of bytes the entries of pages take in memory
	// (see heldSize).
	held int
	// reads is the number of times pc went to posting_pages for a page.
	reads int
	// heads is whether the scope has a head, once headsRead is set.
	headsRead, heads bool
}

// postingHeldPageKeys is the most keys one held page has. A page that gets
// more is cut in two in memory (see postingPages.insert), so that the cost of
// one more key does not grow with the number of keys a transaction adds to
// one page of the table.
//
// postingHeldMax is about the most bytes the held pages of one scope take
// before the transaction stores their changes (see boundPostingPages). It
// bounds the memory of a transaction that changes many keys.
//
// Both are variables so that a spec can make them small.
var (
	postingHeldPageKeys = 128
	postingHeldMax      = 16 << 20
)

// heldSize is about the number of bytes e takes while a postingPages holds
// it: its key, its coded row numbers and the entry.
func heldSize(e pageEntry) int {
	return len(e.k) + len(e.coded) + 64
}

// hold adds the page pg to the pages pc holds.
func (pc *postingPages) hold(pg *postingPage) {
	i := sort.Search(len(pc.pages), func(i int) bool { return bytes.Compare(pc.pages[i].lo, pg.lo) > 0 })
	pc.pages = slices.Insert(pc.pages, i, pg)
	for _, e := range pg.entries {
		pc.held += heldSize(e)
	}
}

// insert puts the entry e, whose key pg does not hold, at position i of pg.
// When pg then has more than postingHeldPageKeys keys, insert cuts it in two:
// pg keeps the first half, and a page for the second half goes into pc.pages
// after it.
func (pc *postingPages) insert(pg *postingPage, i int, e pageEntry) {
	pg.entries = slices.Insert(pg.entries, i, e)
	pg.dirty = true
	pc.held += heldSize(e)
	if len(pg.entries) <= postingHeldPageKeys {
		return
	}
	half := len(pg.entries) / 2
	// The two halves share the array. The first half has no room left in
	// it, so an insert into it copies it and leaves the second half alone.
	rest := &postingPage{lo: pg.entries[half].k, next: pg.next, entries: pg.entries[half:], dirty: true, cut: true}
	pg.entries = pg.entries[:half:half]
	pg.next = rest.lo
	at := sort.Search(len(pc.pages), func(i int) bool { return bytes.Compare(pc.pages[i].lo, rest.lo) > 0 })
	pc.pages = slices.Insert(pc.pages, at, rest)
}

// replace makes e the entry at position i of pg, in place of the entry of
// the same key.
func (pc *postingPages) replace(pg *postingPage, i int, e pageEntry) {
	pc.held += len(e.coded) - len(pg.entries[i].coded)
	pg.entries[i] = e
	pg.dirty = true
}

// remove takes the entry at position i out of pg.
func (pc *postingPages) remove(pg *postingPage, i int) {
	pc.held -= heldSize(pg.entries[i])
	pg.entries = slices.Delete(pg.entries, i, i+1)
	pg.dirty, pg.shrunk = true, true
}

// join puts each page that insert cut in two together again, so that flush
// splits and merges the pages as if none was cut.
func (pc *postingPages) join() {
	joined := pc.pages[:0]
	for i := 0; i < len(pc.pages); {
		pg := pc.pages[i]
		j, n := i+1, len(pg.entries)
		for ; j < len(pc.pages) && pc.pages[j].cut; j++ {
			n += len(pc.pages[j].entries)
		}
		if j > i+1 {
			// One allocation for the run: appending part by part to the
			// first page would copy the entries of a long run many times.
			es := append(make([]pageEntry, 0, n), pg.entries...)
			for _, part := range pc.pages[i+1 : j] {
				es = append(es, part.entries...)
				pg.shrunk = pg.shrunk || part.shrunk
			}
			pg.entries, pg.next, pg.dirty = es, pc.pages[j-1].next, true
		}
		joined = append(joined, pg)
		i = j
	}
	clear(pc.pages[len(joined):])
	pc.pages = joined
}

// postingPageForSQL reads the page that holds or would hold key ?2 in scope
// ?1, with the lo of the page after it.
const postingPageForSQL = `SELECT lo, body,
		(SELECT lo FROM posting_pages WHERE scope = ?1 AND lo > ?2 ORDER BY lo LIMIT 1)
	FROM posting_pages WHERE scope = ?1 AND lo <= ?2 ORDER BY lo DESC LIMIT 1`

// cached returns the page that holds or would hold k when pc holds it, or
// nil.
func (pc *postingPages) cached(k []byte) *postingPage {
	i := sort.Search(len(pc.pages), func(i int) bool { return bytes.Compare(pc.pages[i].lo, k) > 0 }) - 1
	if i < 0 {
		return nil
	}
	if pg := pc.pages[i]; pg.next == nil || bytes.Compare(k, pg.next) < 0 {
		return pg
	}
	return nil
}

// pageFor returns the page that holds or would hold k, and reads it when pc
// does not hold it. When no page starts at or below k, a new page from the
// empty key on takes it.
func (pc *postingPages) pageFor(k []byte) (*postingPage, error) {
	if pg := pc.cached(k); pg != nil {
		return pg, nil
	}
	var (
		lo, body, next []byte
		pg             *postingPage
	)
	pc.reads++
	err := pc.t.queryRow(postingPageForSQL, pc.scope, notNilBytes(k)).Scan(&lo, &body, &next)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		err := pc.t.queryRow(
			`SELECT lo FROM posting_pages WHERE scope = ? AND lo > ? ORDER BY lo LIMIT 1`, pc.scope, notNilBytes(k),
		).Scan(&next)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		pg = &postingPage{lo: []byte{}, next: next}
	case err != nil:
		return nil, err
	default:
		es, err := decodePage(body)
		if err != nil {
			return nil, fmt.Errorf("sqlitestore: posting page at key %q of scope %d: %w", lo, pc.scope, err)
		}
		pg = &postingPage{lo: notNilBytes(lo), next: next, entries: es, stored: true}
	}
	pc.hold(pg)
	return pg, nil
}

// notNilBytes returns b, or an empty slice for nil, which the driver would
// bind as NULL.
func notNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// hasHeads reports whether the scope has a head.
func (pc *postingPages) hasHeads() (bool, error) {
	if !pc.headsRead {
		var one int
		err := pc.t.queryRow(`SELECT 1 FROM posting_heads WHERE scope = ? LIMIT 1`, pc.scope).Scan(&one)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		pc.headsRead, pc.heads = true, err == nil
	}
	return pc.heads, nil
}

// change adds num to the row numbers of k, or removes it from them, in the
// page or the head of k. Adding a number the key holds, or removing one it
// does not, changes nothing. The page changes are not stored until flush.
func (pc *postingPages) change(k []byte, num int64, add bool) error {
	pg := pc.cached(k)
	i, inPage := 0, false
	if pg != nil {
		i, inPage = findPageEntry(pg.entries, k)
	}
	if !inPage {
		// The key can have a head. The head is read before a page that pc
		// does not hold, because a key with a head then costs no page read.
		// A scope with few rows has no head at all, and one read finds that
		// for every key.
		heads, err := pc.hasHeads()
		if err != nil {
			return err
		}
		if heads {
			h, err := pc.t.loadPostingHead(pc.scope, k)
			if err != nil {
				return err
			}
			if h.exists {
				return pc.changeHead(h, num, add)
			}
		}
		if pg == nil {
			if pg, err = pc.pageFor(k); err != nil {
				return err
			}
			i, inPage = findPageEntry(pg.entries, k)
		}
	}
	var nums []int64
	if inPage {
		var err error
		if nums, err = pg.entries[i].nums(); err != nil {
			return fmt.Errorf("sqlitestore: posting page at key %q of scope %d: %w", pg.lo, pc.scope, err)
		}
	}
	pos, held := slices.BinarySearch(nums, num)
	if held == add {
		return nil
	}
	if !add {
		if len(nums) == 1 {
			pc.remove(pg, i)
		} else {
			pc.replace(pg, i, newPageEntry(pg.entries[i].k, slices.Delete(nums, pos, pos+1)))
			pg.shrunk = true
		}
		return nil
	}
	nums = slices.Insert(nums, pos, num)
	if len(nums) <= postingPageMaxRows {
		if inPage {
			pc.replace(pg, i, newPageEntry(pg.entries[i].k, nums))
		} else {
			pc.insert(pg, i, newPageEntry(k, nums))
		}
		return nil
	}
	// The key has too many rows for a page, so it gets a head. The head is
	// stored before the key leaves the page: a key must not be without both.
	h := &postingHead{k: k}
	for _, n := range nums {
		if err := pc.t.addPostingNum(h, n); err != nil {
			return fmt.Errorf("sqlitestore: posting key %x: %w", k, err)
		}
	}
	if err := pc.saveHead(h); err != nil {
		return err
	}
	pc.headsRead, pc.heads = true, true
	if inPage {
		pc.remove(pg, i)
	}
	return nil
}

// saveHead stores the head h and its chunks (see sqliteTx.savePostingHead).
// A failure can leave some of the records of the key stored and others not,
// so saveHead keeps the error in sqliteTx.postingErr, which stops the commit.
func (pc *postingPages) saveHead(h *postingHead) error {
	err := pc.t.savePostingHead(pc.scope, h)
	if err != nil {
		pc.t.postingErr = err
	}
	return err
}

// changeHead adds num to the head h, or removes it from h, and writes what
// changed. It moves the key to a page when the change leaves the head with
// fewer than half of postingPageMaxRows row numbers.
func (pc *postingPages) changeHead(h *postingHead, num int64, add bool) error {
	var err error
	if add {
		err = pc.t.addPostingNum(h, num)
	} else {
		err = pc.t.removePostingNum(h, num)
	}
	if err != nil {
		return fmt.Errorf("sqlitestore: posting key %x: %w", h.k, err)
	}
	if !h.dirty || h.cnt >= postingPageMaxRows/2 {
		return pc.saveHead(h)
	}
	// The key moves to its page. The page is read before the head goes: a
	// page that cannot be read must leave the head as it is stored.
	var (
		pg *postingPage
		i  int
	)
	if h.cnt > 0 {
		if pg, err = pc.pageFor(h.k); err != nil {
			return err
		}
		var inPage bool
		if i, inPage = findPageEntry(pg.entries, h.k); inPage {
			return fmt.Errorf("sqlitestore: posting key %x has a head and is in a page", h.k)
		}
	}
	nums := make([]int64, 0, h.cnt)
	for _, c := range h.chunks {
		if err := pc.t.loadPostingChunk(c); err != nil {
			return fmt.Errorf("sqlitestore: posting key %x: %w", h.k, err)
		}
		nums = append(nums, c.nums...)
		if c.id != 0 {
			h.freed = append(h.freed, c.id)
		}
	}
	nums = append(nums, h.tail...)
	// A head with a count of 0 is deleted with its freed chunks.
	h.chunks, h.tail, h.cnt = nil, nil, 0
	if err := pc.saveHead(h); err != nil {
		return err
	}
	if pg != nil {
		pc.insert(pg, i, newPageEntry(h.k, nums))
	}
	return nil
}

// before returns the page that will be before pc.pages[i] in the scope once
// the pages that hold no key are deleted, or nil when there is none. It reads
// the page when pc does not hold it, and puts it into pc.pages before position
// i, which moves the page that was at i to i+1. It must run before a merge
// changes a page below position i.
func (pc *postingPages) before(i int) (*postingPage, error) {
	for ; i > 0 && bytes.Equal(pc.pages[i-1].next, pc.pages[i].lo); i-- {
		if len(pc.pages[i-1].entries) > 0 {
			return pc.pages[i-1], nil
		}
	}
	// pc does not hold the page that ends where pc.pages[i] starts.
	at := pc.pages[i].lo
	var lo, body []byte
	pc.reads++
	err := pc.t.queryRow(
		`SELECT lo, body FROM posting_pages WHERE scope = ? AND lo < ? ORDER BY lo DESC LIMIT 1`, pc.scope, at,
	).Scan(&lo, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	es, err := decodePage(body)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: posting page at key %q of scope %d: %w", lo, pc.scope, err)
	}
	prev := &postingPage{lo: notNilBytes(lo), next: at, entries: es, stored: true}
	pc.hold(prev)
	return prev, nil
}

// after returns the page that will be after pg in the scope once the pages
// that hold no key are deleted, or nil when there is none. It reads the page
// when pc does not hold it, and puts it into pc.pages after pg.
func (pc *postingPages) after(pg *postingPage) (*postingPage, error) {
	for at := pg.next; at != nil; {
		// The page starts at the key at, so it is the page of that key.
		next, err := pc.pageFor(at)
		if err != nil {
			return nil, err
		}
		if len(next.entries) > 0 {
			return next, nil
		}
		at = next.next
	}
	return nil, nil
}

// pagesOf returns the page changes of scope that this transaction has not
// stored yet, which are none at first.
func (t *sqliteTx) pagesOf(scope int64) *postingPages {
	pc := t.postingPages[scope]
	if pc == nil {
		pc = &postingPages{t: t, scope: scope}
		if t.postingPages == nil {
			t.postingPages = make(map[int64]*postingPages)
		}
		t.postingPages[scope] = pc
	}
	return pc
}

// postingFailed returns the error of sqliteTx.postingErr for a call that
// comes after the failure, or nil when no write of page changes failed.
func (t *sqliteTx) postingFailed() error {
	if t.postingErr == nil {
		return nil
	}
	return fmt.Errorf("sqlitestore: posting page changes of this transaction were not stored: %w", t.postingErr)
}

// boundPostingPages stores the page changes of pc when its pages take more
// than postingHeldMax bytes. A failure is kept in t.postingErr.
func (t *sqliteTx) boundPostingPages(pc *postingPages) error {
	if pc.held <= postingHeldMax {
		return nil
	}
	delete(t.postingPages, pc.scope)
	if err := pc.flush(); err != nil {
		t.postingErr = err
		return err
	}
	return nil
}

// flushPostings stores the posting page changes of this transaction that are
// not stored yet. PutPostings and DeletePostings keep them in memory, so that
// a transaction that changes one page in many calls writes it one time.
// Everything that reads posting_pages without postingPages calls it first, and
// so does the commit.
//
// A failure leaves some changes stored and drops the others, so
// flushPostings keeps the error in t.postingErr and returns it. From then on
// it stores nothing and returns postingFailed, and so does every posting
// call, which stops the commit (see Store.WithWriteTx). Only the rewind of a
// savepoint that began before the failure clears the error (see InSavepoint).
func (t *sqliteTx) flushPostings() error {
	if err := t.postingFailed(); err != nil {
		return err
	}
	if len(t.postingPages) == 0 {
		return nil
	}
	pending := t.postingPages
	t.postingPages = nil
	// In scope order, so that the statements of a retried transaction run in
	// the order of its first attempt.
	scopes := make([]int64, 0, len(pending))
	for scope := range pending {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)
	for _, scope := range scopes {
		if err := pending[scope].flush(); err != nil {
			t.postingErr = err
			return err
		}
	}
	return nil
}

// flush merges each page that the changes made small with a page next to
// it, and writes each changed page: split when it is larger than the budget,
// deleted when it holds no key. It leaves pc empty.
func (pc *postingPages) flush() error {
	pc.join()
	// From the last page to the first, so that a page that took the page
	// after it is looked at again when its turn comes.
	for i := len(pc.pages) - 1; i >= 0; i-- {
		pg := pc.pages[i]
		if !pg.shrunk || len(pg.entries) == 0 {
			continue
		}
		size := pageLen(pg.entries)
		if size >= postingPageBudget/4 {
			continue
		}
		// A page that starts at the empty key has no page before it.
		if len(pg.lo) > 0 {
			was := len(pc.pages)
			prev, err := pc.before(i)
			if err != nil {
				return err
			}
			// A page that before read is below pg now, and pg one position
			// up. The loop goes on at the page below pg either way.
			i += len(pc.pages) - was
			if prev != nil && pageLen(prev.entries)+size <= postingPageBudget {
				prev.entries = append(prev.entries, pg.entries...)
				prev.next = pg.next
				// The two can still be small together.
				prev.dirty, prev.shrunk = true, true
				pg.entries = nil
				pg.dirty = true
				continue
			}
		}
		next, err := pc.after(pg)
		if err != nil {
			return err
		}
		if next != nil && size+pageLen(next.entries) <= postingPageBudget {
			pg.entries = append(pg.entries, next.entries...)
			pg.next = next.next
			pg.dirty = true
			next.entries = nil
			next.dirty = true
			// pg can still be small, so it is looked at again.
			i++
		}
	}
	for _, pg := range pc.pages {
		if !pg.dirty {
			continue
		}
		if err := pc.write(pg); err != nil {
			return err
		}
	}
	pc.pages, pc.held = nil, 0
	return nil
}

// write stores pg, as several pages when it is larger than the budget, or
// deletes it when it holds no key.
func (pc *postingPages) write(pg *postingPage) error {
	if len(pg.entries) == 0 {
		if !pg.stored {
			return nil
		}
		_, err := pc.t.exec(`DELETE FROM posting_pages WHERE scope = ? AND lo = ?`, pc.scope, pg.lo)
		return err
	}
	parts := [][]pageEntry{pg.entries}
	body := encodePage(pg.entries)
	if len(pg.entries) > 1 && len(body) > postingPageBudget {
		parts = splitPage(pg.entries, postingPageBudget)
	}
	for i, part := range parts {
		if len(parts) > 1 {
			body = encodePage(part)
		}
		if i == 0 && pg.stored {
			if _, err := pc.t.exec(`UPDATE posting_pages SET body = ? WHERE scope = ? AND lo = ?`, body, pc.scope, pg.lo); err != nil {
				return err
			}
			continue
		}
		// Each part after the first starts at its first key, which is above
		// every key of the part before it and below the lo of the next page.
		lo := pg.lo
		if i > 0 {
			lo = part[0].k
		}
		if _, err := pc.t.exec(`INSERT INTO posting_pages (scope, lo, body) VALUES (?, ?, ?)`, pc.scope, lo, body); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) DeletePostings(entity, index, key string, held [][]byte) error {
	if err := t.postingFailed(); err != nil {
		return err
	}
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
	pc := t.pagesOf(scope)
	for _, k := range sortedKeys(removes) {
		if err := pc.change([]byte(k), num, false); err != nil {
			return err
		}
	}
	if err := t.boundPostingPages(pc); err != nil {
		return err
	}
	// The number goes from both maps and from the Store's cache, so a number
	// that held does not name, and that stays in a page or a head, resolves
	// to no row and is never yielded.
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
	if err := t.postingFailed(); err != nil {
		return err
	}
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
	// The page changes that are not stored yet go with the pages.
	delete(t.postingPages, scope)
	// Nothing here decodes a record, so a corrupt record cannot stop the
	// clear that a rebuild starts with, or a workspace delete.
	for _, q := range []string{
		`DELETE FROM posting_chunks WHERE scope = ?`,
		`DELETE FROM posting_heads WHERE scope = ?`,
		`DELETE FROM posting_pages WHERE scope = ?`,
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

// The forms of the statement that reads one range of a scope.
const (
	rangeFrom   = iota // no high bound
	rangeFromTo        // a low and a high bound
	rangeOneKey        // the high bound is the key next after the low bound
	rangeForms
)

// postingRangeSQL makes the statement that reads one range of a scope: a row
// with headCols for each head of the range, and then a row with pageCols for
// each page that can hold a key of the range. Those are the page that holds
// or would hold the low bound, and the pages whose lo is in the range. A page
// row can hold keys outside the range, and no key in it.
//
// The arguments are the scope id, the low bound, which is the empty key for a
// range without one, and for rangeFromTo the high bound. One statement for
// both tables costs one call into the driver, and a call costs more than a
// b-tree descent. A range of one key has no page with a lo in it, so its
// statement leaves that read out.
func postingRangeSQL(headCols, pageCols string, form int) string {
	heads := `SELECT ` + headCols + ` FROM posting_heads WHERE scope = ?1 AND k `
	first := `
		UNION ALL
		SELECT ` + pageCols + ` FROM (
			SELECT body FROM posting_pages WHERE scope = ?1 AND lo <= ?2 ORDER BY lo DESC LIMIT 1)`
	rest := `
		UNION ALL
		SELECT ` + pageCols + ` FROM posting_pages WHERE scope = ?1 AND lo > ?2`
	switch form {
	case rangeOneKey:
		return heads + `= ?2` + first
	case rangeFromTo:
		return heads + `>= ?2 AND k < ?3` + first + rest + ` AND lo < ?3`
	}
	return heads + `>= ?2` + first + rest
}

// postingRangeSQLs makes postingRangeSQL in each form.
func postingRangeSQLs(headCols, pageCols string) (sqls [rangeForms]string) {
	for form := range sqls {
		sqls[form] = postingRangeSQL(headCols, pageCols, form)
	}
	return sqls
}

// The statements PostingCounts and ScanPostings read a range with. A page row
// has a count of -1.
var (
	postingCountsSQL = postingRangeSQLs(`cnt, NULL`, `-1, body`)
	postingHeadsSQL  = postingRangeSQLs(`k, cnt, dir, tail, NULL`, `NULL, -1, NULL, NULL, body`)
)

// rangeQuery returns the statement of sqls that reads r in scope, with its
// arguments.
func rangeQuery(sqls *[rangeForms]string, scope int64, r storage.IndexRange) (string, []any) {
	lo := notNilBytes(r.Lo)
	switch {
	case r.Hi == nil:
		return sqls[rangeFrom], []any{scope, lo}
	case len(r.Hi) == len(lo)+1 && r.Hi[len(lo)] == 0 && bytes.HasPrefix(r.Hi, lo):
		return sqls[rangeOneKey], []any{scope, lo}
	}
	return sqls[rangeFromTo], []any{scope, lo, r.Hi}
}

// inRangePage calls fn for each key of the page body b that is in r, with
// what walkPage gives for it.
func inRangePage(b []byte, r storage.IndexRange, fn func(count int, nums []byte)) error {
	return walkPage(b, func(k []byte, count int, nums []byte) bool {
		if r.Hi != nil && bytes.Compare(k, r.Hi) >= 0 {
			return false
		}
		if bytes.Compare(k, r.Lo) >= 0 {
			fn(count, nums)
		}
		return true
	})
}

// emptyRange reports whether r holds no key at all.
func emptyRange(r storage.IndexRange) bool {
	return r.Lo != nil && r.Hi != nil && bytes.Compare(r.Lo, r.Hi) >= 0
}

func (t *sqliteTx) PostingCounts(entity, index string, ranges []storage.IndexRange, max int) ([]int, int, error) {
	if err := t.flushPostings(); err != nil {
		return nil, 0, err
	}
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
		query, args := rangeQuery(&postingCountsSQL, scope, r)
		err := t.scanRows(query, args, func(rows *sql.Rows) error {
			var (
				cnt  int
				body sql.RawBytes
			)
			if err := rows.Scan(&cnt, &body); err != nil {
				return err
			}
			if cnt >= 0 {
				sum += cnt
			} else if err := inRangePage(body, r, func(count int, _ []byte) { sum += count }); err != nil {
				return fmt.Errorf("sqlitestore: posting page of scope %d: %w", scope, err)
			}
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

// storedHead is the row numbers of one key as the read path holds them: the
// count, the decoded directory and the stored tail. A page key is a
// storedHead with no directory and its numbers as the tail.
type storedHead struct {
	cnt    int
	chunks []*postingChunk
	tail   []byte
}

// readHeads reads the keys in r, from heads and from pages, and returns them
// with the sum of their counts.
func (t *sqliteTx) readHeads(scope int64, r storage.IndexRange) ([]storedHead, int, error) {
	if emptyRange(r) {
		return nil, 0, nil
	}
	var (
		heads []storedHead
		total int
	)
	query, args := rangeQuery(&postingHeadsSQL, scope, r)
	err := t.scanRows(query, args, func(rows *sql.Rows) error {
		var (
			k, dir, tail, body []byte
			h                  storedHead
		)
		if err := rows.Scan(&k, &h.cnt, &dir, &tail, &body); err != nil {
			return err
		}
		if h.cnt < 0 {
			// Scan copies the body, so the tails can stay parts of it.
			if err := inRangePage(body, r, func(count int, nums []byte) {
				heads = append(heads, storedHead{cnt: count, tail: nums})
				total += count
			}); err != nil {
				return fmt.Errorf("sqlitestore: posting page of scope %d: %w", scope, err)
			}
			return nil
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
	if err := t.flushPostings(); err != nil {
		return err
	}
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
