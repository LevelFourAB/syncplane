package server

import (
	"encoding/json"
	"os"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// The within vectors are shared with the TS client
// (client/test/unit/access-within-vectors.test.ts): the client predicts with
// the walk the server makes here.
const accessWithinVectorsPath = "../../testdata/access/within.json"

type withinVectorCase struct {
	Name   string `json:"name"`
	Entity string `json:"entity"`
	Key    string `json:"key"`
	Within string `json:"within"`
	Graph  []struct {
		Entity  string          `json:"entity"`
		Key     string          `json:"key"`
		Parents json.RawMessage `json:"parents"`
	} `json:"graph"`
	Passes []struct {
		Entity string `json:"entity"`
		Key    string `json:"key"`
		Passes *bool  `json:"passes"`
	} `json:"passes"`
	Want string `json:"want"`
}

var _ = Describe("access within conformance vectors", func() {
	raw, err := os.ReadFile(accessWithinVectorsPath)
	if err != nil {
		panic(err)
	}
	var file struct {
		Cases []withinVectorCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		panic(err)
	}

	It("has cases", func() {
		Expect(file.Cases).NotTo(BeEmpty())
	})

	for _, c := range file.Cases {
		It(c.Name, func() {
			// The engine never lacks a row, so it decides only what the client
			// can decide too.
			edges := map[anchorRow]chainEdge{}
			entities := map[string]bool{c.Entity: true, c.Within: true}
			for _, g := range c.Graph {
				var parents []anchorRow
				var rows []struct{ Entity, Key string }
				if err := json.Unmarshal(g.Parents, &rows); err != nil {
					Skip("names a row the client does not hold")
				}
				for _, p := range rows {
					parents = append(parents, anchorRow{entity: p.Entity, key: p.Key})
					entities[p.Entity] = true
				}
				edges[anchorRow{entity: g.Entity, key: g.Key}] = edgeTo(parents...)
			}
			// A rule's when check stands for the entry's checks: a row passes
			// when it matches the spec "e".
			withins := map[anchorRow]rowWithin{}
			for _, p := range c.Passes {
				if p.Passes == nil {
					Skip("names a row the client does not hold")
				}
				st := rowWithin{live: true}
				if *p.Passes {
					st.matched = []string{"e"}
				}
				withins[anchorRow{entity: p.Entity, key: p.Key}] = st
			}
			if c.Want == "unknown" {
				Skip("only the client lacks an input")
			}
			entry := withinEntry{entity: c.Within, whenSpec: "e"}
			check := &withinCheck{spec: entry.spec(), target: &entityInfo{name: c.Within}, when: &query.Compiled{}, whenSpec: "e"}
			model := &accessModel{
				withins: map[string]*withinCheck{check.spec: check},
				below:   map[string][]string{},
			}
			for name := range entities {
				model.below[c.Within] = append(model.below[c.Within], name)
			}
			slices.Sort(model.below[c.Within])
			chains := newAccessChainsFrom(model, func(string, string) (storage.Row, error) {
				return storage.Row{}, storage.ErrNotFound
			}, 32, func(storage.Row, error) {})
			chains.edges, chains.withins = edges, withins
			ok, err := chains.withinAllAt(accessRule{within: []withinEntry{entry}}, nil, anchorRow{entity: c.Entity, key: c.Key})
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal(c.Want == "yes"))
		})
	}
})
