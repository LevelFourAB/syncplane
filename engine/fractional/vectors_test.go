package fractional_test

import (
	"encoding/json"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/fractional"
)

// vectorsPath is the shared cross-language conformance file; the TS harness
// (client/test/unit/fractional-vectors.test.ts) runs the identical cases.
const vectorsPath = "../../testdata/fractional/vectors.json"

// bound is a JSON string or null; null reads as "", the open bound.
type bound string

func (b *bound) UnmarshalJSON(data []byte) error {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s != nil {
		*b = bound(*s)
	}
	return nil
}

type vectorFile struct {
	Between []struct {
		Name string `json:"name"`
		A    bound  `json:"a"`
		B    bound  `json:"b"`
		Want string `json:"want"`
	} `json:"between"`
	NBetween []struct {
		Name string   `json:"name"`
		A    bound    `json:"a"`
		B    bound    `json:"b"`
		N    int      `json:"n"`
		Want []string `json:"want"`
	} `json:"nBetween"`
	ActorBetween []struct {
		Name  string `json:"name"`
		A     bound  `json:"a"`
		B     bound  `json:"b"`
		Actor string `json:"actor"`
		Want  string `json:"want"`
	} `json:"actorBetween"`
	Rejects []struct {
		Name  string `json:"name"`
		A     bound  `json:"a"`
		B     bound  `json:"b"`
		Error string `json:"error"`
	} `json:"rejects"`
	Walks []struct {
		Name  string   `json:"name"`
		Actor *string  `json:"actor"`
		Gaps  []int    `json:"gaps"`
		Want  []string `json:"want"`
	} `json:"walks"`
}

// loadVectors reads the vector file at tree-construction time, so each case
// becomes its own spec.
func loadVectors() vectorFile {
	b, err := os.ReadFile(vectorsPath)
	if err != nil {
		panic(err)
	}
	var v vectorFile
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	return v
}

// expectStrictlyBetween fails unless key sorts after a and before b, where an
// empty bound is open.
func expectStrictlyBetween(key string, a, b bound) {
	GinkgoHelper()
	if a != "" {
		Expect(key > string(a)).To(BeTrue(), "%q is not above %q", key, a)
	}
	if b != "" {
		Expect(key < string(b)).To(BeTrue(), "%q is not below %q", key, b)
	}
}

var _ = Describe("conformance vectors", func() {
	v := loadVectors()

	It("has cases", func() {
		Expect(v.Between).NotTo(BeEmpty())
		Expect(v.Rejects).NotTo(BeEmpty())
		Expect(v.Walks).NotTo(BeEmpty())
	})

	Describe("between", func() {
		for _, c := range v.Between {
			It(c.Name, func() {
				got, err := fractional.KeyBetween(string(c.A), string(c.B))
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(c.Want))
				expectStrictlyBetween(got, c.A, c.B)
			})
		}
	})

	Describe("nBetween", func() {
		for _, c := range v.NBetween {
			It(c.Name, func() {
				got, err := fractional.NKeysBetween(string(c.A), string(c.B), c.N)
				Expect(err).NotTo(HaveOccurred())
				if len(c.Want) == 0 {
					Expect(got).To(BeEmpty())
				} else {
					Expect(got).To(Equal(c.Want))
				}
			})
		}
	})

	Describe("actorBetween", func() {
		for _, c := range v.ActorBetween {
			It(c.Name, func() {
				got, err := fractional.KeyBetweenFor(string(c.A), string(c.B), c.Actor)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(c.Want))
				expectStrictlyBetween(got, c.A, c.B)
			})
		}
	})

	Describe("rejects", func() {
		sentinels := map[string]error{
			"invalidKey": fractional.ErrInvalidKey,
			"outOfOrder": fractional.ErrOutOfOrder,
		}
		for _, c := range v.Rejects {
			It(c.Name, func() {
				want, ok := sentinels[c.Error]
				Expect(ok).To(BeTrue(), "unknown error kind %q", c.Error)
				_, err := fractional.KeyBetween(string(c.A), string(c.B))
				Expect(err).To(MatchError(want))
				_, err = fractional.KeyBetweenFor(string(c.A), string(c.B), "actor")
				Expect(err).To(MatchError(want))
				_, err = fractional.NKeysBetween(string(c.A), string(c.B), 2)
				Expect(err).To(MatchError(want))
			})
		}
	})

	Describe("walks", func() {
		for _, w := range v.Walks {
			It(w.Name, func() {
				var list []string
				for _, gap := range w.Gaps {
					i := min(gap, len(list))
					var a, b string
					if i > 0 {
						a = list[i-1]
					}
					if i < len(list) {
						b = list[i]
					}
					var key string
					var err error
					if w.Actor == nil {
						key, err = fractional.KeyBetween(a, b)
					} else {
						key, err = fractional.KeyBetweenFor(a, b, *w.Actor)
					}
					Expect(err).NotTo(HaveOccurred())
					list = append(list[:i], append([]string{key}, list[i:]...)...)
				}
				Expect(list).To(Equal(w.Want))
				for i := 1; i < len(list); i++ {
					Expect(list[i-1] < list[i]).To(BeTrue(), "%q is not below %q", list[i-1], list[i])
				}
			})
		}
	})
})
