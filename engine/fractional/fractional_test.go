package fractional_test

import (
	"math/rand/v2"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/fractional"
)

var _ = Describe("KeyBetween", func() {
	It("orders random inserts and keeps every key unique", func() {
		rnd := rand.New(rand.NewPCG(42, 0))
		list := []string{}
		for range 3000 {
			i := rnd.IntN(len(list) + 1)
			var a, b string
			if i > 0 {
				a = list[i-1]
			}
			if i < len(list) {
				b = list[i]
			}
			key, err := fractional.KeyBetween(a, b)
			Expect(err).NotTo(HaveOccurred())
			_, err = fractional.KeyBetween(key, "")
			Expect(err).NotTo(HaveOccurred(), "%q is not an order key", key)
			list = slices.Insert(list, i, key)
		}
		Expect(slices.IsSorted(list)).To(BeTrue())
		Expect(slices.Compact(slices.Clone(list))).To(HaveLen(len(list)))
	})

	It("accepts every key it returns as a bound", func() {
		rnd := rand.New(rand.NewPCG(7, 0))
		key := ""
		for range 1000 {
			var err error
			if rnd.IntN(2) == 0 {
				key, err = fractional.KeyBetween(key, "")
			} else {
				key, err = fractional.KeyBetween("", key)
			}
			Expect(err).NotTo(HaveOccurred())
			_, err = fractional.KeyBetween(key, "")
			Expect(err).NotTo(HaveOccurred())
			_, err = fractional.KeyBetween("", key)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("keeps keys short under repeated appends and prepends", func() {
		hi, lo := "", ""
		for range 500 {
			next, err := fractional.KeyBetween(hi, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(next > hi).To(BeTrue())
			hi = next

			prev, err := fractional.KeyBetween("", lo)
			Expect(err).NotTo(HaveOccurred())
			if lo != "" {
				Expect(prev < lo).To(BeTrue())
			}
			lo = prev
		}
		Expect(len(hi)).To(BeNumerically("<", 20))
		Expect(len(lo)).To(BeNumerically("<", 20))
	})

	It("keeps narrowing one gap without running out", func() {
		a, b := "a0", "a1"
		for i := range 200 {
			key, err := fractional.KeyBetween(a, b)
			Expect(err).NotTo(HaveOccurred())
			Expect(key > a && key < b).To(BeTrue())
			if i%2 == 0 {
				a = key
			} else {
				b = key
			}
		}
	})
})

var _ = Describe("NKeysBetween", func() {
	It("returns nil for a zero or negative count", func() {
		for _, n := range []int{0, -1, -5} {
			keys, err := fractional.NKeysBetween("a0", "a1", n)
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(BeNil())
		}
	})

	It("fits many keys between two adjacent keys", func() {
		keys, err := fractional.NKeysBetween("a0", "a0V", 100)
		Expect(err).NotTo(HaveOccurred())
		all := append(append([]string{"a0"}, keys...), "a0V")
		Expect(slices.IsSorted(all)).To(BeTrue())
		Expect(slices.Compact(slices.Clone(all))).To(HaveLen(102))
	})
})

var _ = Describe("KeyBetweenFor", func() {
	It("gives two actors in one gap distinct keys with room between them", func() {
		k1, err := fractional.KeyBetweenFor("a0", "a1", "actor-AAAA")
		Expect(err).NotTo(HaveOccurred())
		k2, err := fractional.KeyBetweenFor("a0", "a1", "actor-BBBB")
		Expect(err).NotTo(HaveOccurred())
		Expect(k1).NotTo(Equal(k2))
		lo, hi := min(k1, k2), max(k1, k2)
		mid, err := fractional.KeyBetweenFor(lo, hi, "actor-CCCC")
		Expect(err).NotTo(HaveOccurred())
		Expect(mid > lo && mid < hi).To(BeTrue())
	})

	It("orders random inserts by many actors", func() {
		rnd := rand.New(rand.NewPCG(9, 0))
		actors := []string{"client-1", "client-2", "client-3", ""}
		list := []string{}
		for range 2000 {
			i := rnd.IntN(len(list) + 1)
			var a, b string
			if i > 0 {
				a = list[i-1]
			}
			if i < len(list) {
				b = list[i]
			}
			key, err := fractional.KeyBetweenFor(a, b, actors[rnd.IntN(len(actors))])
			Expect(err).NotTo(HaveOccurred())
			Expect(key > a).To(BeTrue())
			if b != "" {
				Expect(key < b).To(BeTrue())
			}
			list = slices.Insert(list, i, key)
		}
		Expect(slices.IsSorted(list)).To(BeTrue())
	})
})
