package sqlitestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// The page records of posting_pages (see postings.go). A page body is a
// sequence of entries, one per posting key, in bytewise key order. An entry
// is:
//
//   - the length of the prefix the key shares with the key of the entry
//     before it, as an unsigned varint (0 for the first entry),
//   - the length of the rest of the key, as an unsigned varint,
//   - the rest of the key,
//   - the number of row numbers of the key, as an unsigned varint, and
//   - the row numbers, as appendNums writes them.
//
// Every entry holds one row number or more, and a stored page holds one
// entry or more.

// pageEntry is one posting key of a page. n is the number of its row numbers
// and coded their bytes, as appendNums writes them: a write decodes the
// numbers of the keys it changes only.
type pageEntry struct {
	k     []byte
	n     int
	coded []byte
}

// newPageEntry returns the entry of k with the row numbers nums, which are
// ascending. It keeps k.
func newPageEntry(k []byte, nums []int64) pageEntry {
	return pageEntry{k: k, n: len(nums), coded: appendNums(make([]byte, 0, 2*len(nums)), nums)}
}

// nums returns the row numbers of e, in a slice with room for one more. It
// returns an error for bytes that appendNums cannot have made.
func (e pageEntry) nums() ([]int64, error) {
	nums, err := decodeNums(e.coded, make([]int64, 0, e.n+1))
	if err != nil {
		return nil, fmt.Errorf("key %x: %w", e.k, err)
	}
	return nums, nil
}

// sharedPrefix is the number of leading bytes a and b have in common.
func sharedPrefix(a, b []byte) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

// uvarintLen is len(binary.AppendUvarint(nil, v)).
func uvarintLen(v uint64) int {
	n := 1
	for v >= 1<<7 {
		v >>= 7
		n++
	}
	return n
}

// appendPageEntry appends e to out, with its key coded against prev, the key
// of the entry before it, or nil for the first entry.
func appendPageEntry(out, prev []byte, e pageEntry) []byte {
	shared := sharedPrefix(prev, e.k)
	out = binary.AppendUvarint(out, uint64(shared))
	out = binary.AppendUvarint(out, uint64(len(e.k)-shared))
	out = append(out, e.k[shared:]...)
	out = binary.AppendUvarint(out, uint64(e.n))
	return append(out, e.coded...)
}

// pageEntryLen is len(appendPageEntry(nil, prev, e)).
func pageEntryLen(prev []byte, e pageEntry) int {
	shared := sharedPrefix(prev, e.k)
	return uvarintLen(uint64(shared)) + uvarintLen(uint64(len(e.k)-shared)) + len(e.k) - shared +
		uvarintLen(uint64(e.n)) + len(e.coded)
}

// encodePage codes es, which are in key order, as a page body.
func encodePage(es []pageEntry) []byte {
	// Room for each key whole, which costs less to find than the exact size:
	// that takes the shared prefix of each pair of keys a second time.
	size := 0
	for _, e := range es {
		size += len(e.k) + len(e.coded) + 3*binary.MaxVarintLen16
	}
	out := make([]byte, 0, size)
	var prev []byte
	for _, e := range es {
		out = appendPageEntry(out, prev, e)
		prev = e.k
	}
	return out
}

// pageLen is len(encodePage(es)).
func pageLen(es []pageEntry) int {
	n := 0
	var prev []byte
	for _, e := range es {
		n += pageEntryLen(prev, e)
		prev = e.k
	}
	return n
}

// walkPage calls fn for each entry of the page body b, in key order, until fn
// returns false. count is the number of row numbers of the entry and nums
// their bytes, as appendNums wrote them; walkPage does not decode them. k is
// valid only during the call, and nums is a part of b. walkPage returns an
// error for an empty b and for bytes that encodePage cannot have made, also
// after calls to fn.
func walkPage(b []byte, fn func(k []byte, count int, nums []byte) bool) error {
	if len(b) == 0 {
		return errors.New("page with no entry")
	}
	var key []byte
	next := func(what string) (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, fmt.Errorf("truncated %s", what)
		}
		b = b[n:]
		return v, nil
	}
	for first := true; len(b) > 0; first = false {
		shared, err := next("key prefix length")
		if err != nil {
			return err
		}
		rest, err := next("key length")
		if err != nil {
			return err
		}
		if shared > uint64(len(key)) {
			return fmt.Errorf("key shares %d bytes with a key of %d", shared, len(key))
		}
		if rest > uint64(len(b)) {
			return fmt.Errorf("key of %d bytes in %d", rest, len(b))
		}
		// Keys are unique and in order. The two keys are equal up to shared,
		// so the bytes after it decide the order.
		if !first && bytes.Compare(b[:rest], key[shared:]) <= 0 {
			return fmt.Errorf("key %x is not above the key before it", append(key[:shared:shared], b[:rest]...))
		}
		key = append(key[:shared], b[:rest]...)
		b = b[rest:]
		count, err := next("row number count")
		if err != nil {
			return err
		}
		// A number takes one byte or more, so a count past the bytes is
		// corrupt.
		if count == 0 || count > uint64(len(b)) {
			return fmt.Errorf("key %x with %d row numbers in %d bytes", key, count, len(b))
		}
		// A varint ends at its first byte below 0x80.
		end := 0
		for left := count; left > 0; end++ {
			if end == len(b) {
				return fmt.Errorf("key %x: truncated row numbers", key)
			}
			if b[end] < 0x80 {
				left--
			}
		}
		nums := b[:end:end]
		b = b[end:]
		if !fn(key, int(count), nums) {
			return nil
		}
	}
	return nil
}

// decodePage returns the entries of the page body b. The entries keep parts
// of b, and their row numbers are not decoded or checked (see pageEntry.nums).
// It returns an error for an empty b and for bytes that encodePage cannot
// have made.
func decodePage(b []byte) ([]pageEntry, error) {
	// The first walk sizes one allocation for all keys and one for the
	// entries, with room for the key a write adds.
	entries, keyBytes := 0, 0
	if err := walkPage(b, func(k []byte, _ int, _ []byte) bool {
		entries++
		keyBytes += len(k)
		return true
	}); err != nil {
		return nil, err
	}
	keys := make([]byte, 0, keyBytes)
	out := make([]pageEntry, 0, entries+1)
	_ = walkPage(b, func(k []byte, count int, coded []byte) bool {
		start := len(keys)
		keys = append(keys, k...)
		out = append(out, pageEntry{k: keys[start:len(keys):len(keys)], n: count, coded: coded})
		return true
	})
	return out, nil
}

// splitPage cuts es, whose page codes to more than budget bytes, into runs
// that each code to about the same size and to at most budget, except a run
// of one entry, which may be larger.
func splitPage(es []pageEntry, budget int) [][]pageEntry {
	total := pageLen(es)
	parts := (total + budget - 1) / budget
	target := (total + parts - 1) / parts
	var out [][]pageEntry
	start, acc := 0, 0
	var prev []byte
	for i, e := range es {
		size := pageEntryLen(prev, e)
		if i > start && (acc+size > budget || acc >= target) {
			out = append(out, es[start:i:i])
			start, acc = i, 0
			// The first key of a run is coded whole.
			size = pageEntryLen(nil, e)
		}
		acc += size
		prev = e.k
	}
	return append(out, es[start:])
}

// findPageEntry returns the position of k in es, which is in key order, and
// whether es holds it.
func findPageEntry(es []pageEntry, k []byte) (int, bool) {
	return sort.Find(len(es), func(i int) int { return bytes.Compare(k, es[i].k) })
}
