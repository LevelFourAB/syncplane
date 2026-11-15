package connect

import "fmt"

// MatchKeys gives the parts of next the keys of the parts of prev they
// continue, for content whose parts carry no key in the store, such as the
// lines of a text file. prevKeys[i] is the key of prev[i].
//
// A part of next that is equal to a part of prev in a longest common
// subsequence of the two takes its key. Between two such pairs, the parts of
// next take the keys of the parts of prev in order, so that an edited part
// keeps its key. A part with no key gets "": give it a new key. When the keys
// in prevKeys are distinct, each key goes to one part at most.
//
// MatchKeys panics when prev and prevKeys differ in length. It takes memory
// in proportion to len(prev)+len(next), and time in proportion to that sum
// times the number of parts inserted and deleted.
func MatchKeys[P comparable](prev []P, prevKeys []string, next []P) []string {
	if len(prev) != len(prevKeys) {
		panic(fmt.Sprintf("connect: MatchKeys: %d parts and %d keys", len(prev), len(prevKeys)))
	}
	keys := make([]string, len(next))
	var gapP, gapQ []int
	pairGap := func() {
		for k := 0; k < len(gapP) && k < len(gapQ); k++ {
			keys[gapQ[k]] = prevKeys[gapP[k]]
		}
		gapP, gapQ = gapP[:0], gapQ[:0]
	}
	i, j := 0, 0
	for _, pr := range commonPairs(prev, next) {
		for ; i < pr[0]; i++ {
			gapP = append(gapP, i)
		}
		for ; j < pr[1]; j++ {
			gapQ = append(gapQ, j)
		}
		pairGap()
		keys[pr[1]] = prevKeys[pr[0]]
		i, j = pr[0]+1, pr[1]+1
	}
	for ; i < len(prev); i++ {
		gapP = append(gapP, i)
	}
	for ; j < len(next); j++ {
		gapQ = append(gapQ, j)
	}
	pairGap()
	return keys
}

// commonPairs returns the index pairs {i, j}, a[i] == b[j], of a longest
// common subsequence of a and b, in order. It is Myers' linear-space
// algorithm: memory in proportion to len(a)+len(b), and time in proportion to
// that sum times the edit distance.
func commonPairs[P comparable](a, b []P) [][2]int {
	var out [][2]int
	var diff func(aLo, aHi, bLo, bHi int)
	diff = func(aLo, aHi, bLo, bHi int) {
		for aLo < aHi && bLo < bHi && a[aLo] == b[bLo] {
			out = append(out, [2]int{aLo, bLo})
			aLo, bLo = aLo+1, bLo+1
		}
		tail := 0
		for aHi > aLo && bHi > bLo && a[aHi-1] == b[bHi-1] {
			aHi, bHi, tail = aHi-1, bHi-1, tail+1
		}
		if aLo < aHi && bLo < bHi {
			x, y, u, v := middleSnake(a[aLo:aHi], b[bLo:bHi])
			// Both halves are smaller: with no common prefix or suffix, the
			// snake lies at least one edit from either end.
			diff(aLo, aLo+x, bLo, bLo+y)
			for k := 0; k < u-x; k++ {
				out = append(out, [2]int{aLo + x + k, bLo + y + k})
			}
			diff(aLo+u, aHi, bLo+v, bHi)
		}
		for k := 0; k < tail; k++ {
			out = append(out, [2]int{aHi + k, bHi + k})
		}
	}
	diff(0, len(a), 0, len(b))
	return out
}

// middleSnake returns the start (x, y) and the end (u, v) of the middle
// snake of an edit path of a into b with the fewest edits: a run of equal
// parts that splits the path into two halves.
func middleSnake[P comparable](a, b []P) (x, y, u, v int) {
	n, m := len(a), len(b)
	delta := n - m
	odd := delta%2 != 0
	bound := (n + m + 1) / 2
	off := bound + 1
	// vf[off+k] is the furthest x of the forward search on diagonal k = x-y,
	// and vb[off+k] that of the backward search, which runs forward over the
	// reversed sequences: its diagonal k is the forward diagonal delta-k.
	vf := make([]int, 2*bound+3)
	vb := make([]int, 2*bound+3)
	for d := 0; d <= bound; d++ {
		for k := -d; k <= d; k += 2 {
			var px int
			if k == -d || (k != d && vf[off+k-1] < vf[off+k+1]) {
				px = vf[off+k+1]
			} else {
				px = vf[off+k-1] + 1
			}
			py := px - k
			sx, sy := px, py
			for px < n && py < m && a[px] == b[py] {
				px, py = px+1, py+1
			}
			vf[off+k] = px
			if kb := delta - k; odd && kb >= -(d-1) && kb <= d-1 && px+vb[off+kb] >= n {
				return sx, sy, px, py
			}
		}
		for k := -d; k <= d; k += 2 {
			var px int
			if k == -d || (k != d && vb[off+k-1] < vb[off+k+1]) {
				px = vb[off+k+1]
			} else {
				px = vb[off+k-1] + 1
			}
			py := px - k
			sx, sy := px, py
			for px < n && py < m && a[n-1-px] == b[m-1-py] {
				px, py = px+1, py+1
			}
			vb[off+k] = px
			if kf := delta - k; !odd && kf >= -d && kf <= d && px+vf[off+kf] >= n {
				return n - px, m - py, n - sx, m - sy
			}
		}
	}
	panic("connect: middleSnake found no overlap")
}

// editDistance returns the number of inserts and deletes that turn a into b,
// or limit+1 when it is more than limit. It takes time in proportion to
// (len(a)+len(b)) × the distance, and memory in proportion to the smaller of
// limit and len(a)+len(b) (Myers' greedy algorithm).
func editDistance[T comparable](a, b []T, limit int) int {
	for len(a) > 0 && len(b) > 0 && a[0] == b[0] {
		a, b = a[1:], b[1:]
	}
	for len(a) > 0 && len(b) > 0 && a[len(a)-1] == b[len(b)-1] {
		a, b = a[:len(a)-1], b[:len(b)-1]
	}
	n, m := len(a), len(b)
	bound := min(n+m, limit)
	if bound < 0 {
		return limit + 1
	}
	// v[off+k] is the furthest x reached on diagonal k = x - y.
	off := bound + 1
	v := make([]int, 2*bound+3)
	for d := 0; d <= bound; d++ {
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[off+k-1] < v[off+k+1]) {
				x = v[off+k+1]
			} else {
				x = v[off+k-1] + 1
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x, y = x+1, y+1
			}
			v[off+k] = x
			if x >= n && y >= m {
				return d
			}
		}
	}
	return limit + 1
}
