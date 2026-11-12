package pgstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
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
//   - the sorted row numbers, as unsigned varints of the difference to the
//     number before, the first one to 0.
//
// Every entry holds one row number or more, and a stored page holds one
// entry or more.

// errCorruptPage is the error a page body that does not decode gives.
var errCorruptPage = errors.New("pgstore: corrupt posting page")

// pageEntry is one posting key of a page with its sorted row numbers.
type pageEntry struct {
	k    []byte
	nums []int64
}

// appendPageEntry appends e to out, front-coded against prev, the key of the
// entry before it, or nil for the first entry.
func appendPageEntry(out, prev []byte, e pageEntry) []byte {
	shared := 0
	for shared < len(prev) && shared < len(e.k) && prev[shared] == e.k[shared] {
		shared++
	}
	out = binary.AppendUvarint(out, uint64(shared))
	out = binary.AppendUvarint(out, uint64(len(e.k)-shared))
	out = append(out, e.k[shared:]...)
	out = binary.AppendUvarint(out, uint64(len(e.nums)))
	return append(out, encodeIDs(e.nums, 0)...)
}

// pageEntryLen is len(appendPageEntry(nil, prev, e)).
func pageEntryLen(prev []byte, e pageEntry) int {
	shared := 0
	for shared < len(prev) && shared < len(e.k) && prev[shared] == e.k[shared] {
		shared++
	}
	return uvarintLen(uint64(shared)) + uvarintLen(uint64(len(e.k)-shared)) + len(e.k) - shared +
		uvarintLen(uint64(len(e.nums))) + codedLen(e.nums, 0)
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 1<<7 {
		v >>= 7
		n++
	}
	return n
}

// encodePage codes es, which are in key order, as a page body.
func encodePage(es []pageEntry) []byte {
	var out, prev []byte
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
// returns false. k is valid only during the call. With wantNums false, the
// row numbers are not decoded and nums is nil. It returns errCorruptPage when
// b is empty or does not decode, also after calls to fn.
func walkPage(b []byte, wantNums bool, fn func(k []byte, count int, nums []int64) bool) error {
	if len(b) == 0 {
		return errCorruptPage
	}
	var key []byte
	next := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	first := true
	for len(b) > 0 {
		shared, ok := next()
		if !ok {
			return errCorruptPage
		}
		rest, ok := next()
		if !ok || shared > uint64(len(key)) || rest > uint64(len(b)) {
			return errCorruptPage
		}
		k := append(key[:shared:shared], b[:rest]...)
		b = b[rest:]
		// Keys are unique and in order, so each is above the key before it.
		if !first && bytes.Compare(k, key) <= 0 {
			return errCorruptPage
		}
		first = false
		key = k
		count, ok := next()
		// Each number takes one byte or more.
		if !ok || count == 0 || count > uint64(len(b)) {
			return errCorruptPage
		}
		var nums []int64
		if wantNums {
			nums = make([]int64, 0, count)
			prev := int64(0)
			for range count {
				d, ok := next()
				if !ok {
					return errCorruptPage
				}
				prev += int64(d)
				nums = append(nums, prev)
			}
		} else {
			// Skip count varints: each one ends at a byte below 0x80.
			for left := count; left > 0; {
				if len(b) == 0 {
					return errCorruptPage
				}
				if b[0] < 0x80 {
					left--
				}
				b = b[1:]
			}
		}
		if !fn(k, int(count), nums) {
			return nil
		}
	}
	return nil
}

// decodePage returns the entries of the page body b. It returns
// errCorruptPage when b does not decode or holds no entry.
func decodePage(b []byte) ([]pageEntry, error) {
	var out []pageEntry
	err := walkPage(b, true, func(k []byte, _ int, nums []int64) bool {
		out = append(out, pageEntry{k: slices.Clone(k), nums: nums})
		return true
	})
	return out, err
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
