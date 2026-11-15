package connect

import "fmt"

// MatchKeys gives the parts of next the keys of the parts of prev they
// continue, for content whose parts carry no key in the store, such as the
// lines of a text file. prevKeys[i] is the key of prev[i].
//
// A part of next that is equal to a part of prev in a longest common
// subsequence of the two takes its key. Between two such pairs, the parts of
// next take the keys of the parts of prev in order, so that an edited part
// keeps its key. Each key goes to one part at most. A part with no key gets
// "": give it a new key.
//
// MatchKeys panics when prev and prevKeys differ in length. It takes time
// and memory in proportion to the product of the lengths of prev and next,
// less a common prefix and suffix.
func MatchKeys[P comparable](prev []P, prevKeys []string, next []P) []string {
	if len(prev) != len(prevKeys) {
		panic(fmt.Sprintf("connect: MatchKeys: %d parts and %d keys", len(prev), len(prevKeys)))
	}
	keys := make([]string, len(next))
	// Equal parts at the start and the end need no table.
	lo := 0
	for lo < len(prev) && lo < len(next) && prev[lo] == next[lo] {
		keys[lo] = prevKeys[lo]
		lo++
	}
	hiP, hiN := len(prev), len(next)
	for hiP > lo && hiN > lo && prev[hiP-1] == next[hiN-1] {
		hiP, hiN = hiP-1, hiN-1
		keys[hiN] = prevKeys[hiP]
	}
	p, q := prev[lo:hiP], next[lo:hiN]
	n, m := len(p), len(q)
	// lcs[i][j] is the length of a longest common subsequence of p[i:] and
	// q[j:].
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if p[i] == q[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var gapP, gapQ []int
	pairGap := func() {
		for k := 0; k < len(gapP) && k < len(gapQ); k++ {
			keys[lo+gapQ[k]] = prevKeys[lo+gapP[k]]
		}
		gapP, gapQ = gapP[:0], gapQ[:0]
	}
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case p[i] == q[j]:
			pairGap()
			keys[lo+j] = prevKeys[lo+i]
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			gapP = append(gapP, i)
			i++
		default:
			gapQ = append(gapQ, j)
			j++
		}
	}
	for ; i < n; i++ {
		gapP = append(gapP, i)
	}
	for ; j < m; j++ {
		gapQ = append(gapQ, j)
	}
	pairGap()
	return keys
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
