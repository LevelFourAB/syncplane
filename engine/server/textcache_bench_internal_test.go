package server

// The write closure's text cost, with and without the decoded-document cache.
//
// This is the number that matters for a workspace rather than for a row: the
// merge runs while the storage adapter holds write mutual exclusion for the
// whole workspace, so it bounds how fast EVERY other writer there can go while
// one person types. Run with: go test ./server/ -bench WriteClosureText
//
// The mirror of the stages either side of it lives in engine/text/bench_test.go
// (Go) and client/test/bench/text.bench.ts (TypeScript); extend all three when
// touching this path.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/levelfourab/syncplane/engine/text"
)

func benchTypedDoc(peer string, n int) *text.Doc {
	d := text.New(peer)
	const chunk = 500
	for w := 0; w < n; w += chunk {
		size := chunk
		if r := n - w; r < size {
			size = r
		}
		if err := d.Insert(d.Len(), strings.Repeat("a", size)); err != nil {
			panic(err)
		}
	}
	return d
}

// One keystroke arriving as a fragment, applied to the stored row, repeatedly —
// the shape of somebody typing.
func BenchmarkWriteClosureText(b *testing.B) {
	for _, n := range []int{5000, 20000, 50000} {
		for _, name := range []string{"cached", "uncached"} {
			b.Run(fmt.Sprintf("%dcp/%s", n, name), func(b *testing.B) {
				var c *textDocCache
				if name == "cached" {
					c = newTextDocCache(64 << 20)
				}
				stored := benchTypedDoc("w", n).Encode()
				key := textCacheKey("ws", "note", "n1", "body")
				writer, err := text.Decode(stored, "client")
				if err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					base, err := text.Decode(stored, "")
					if err != nil {
						b.Fatal(err)
					}
					if err := writer.Insert(writer.Len(), "x"); err != nil {
						b.Fatal(err)
					}
					frag, err := text.EncodeFragment(writer, base)
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()

					stored, err = c.merge(key, stored, frag, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
