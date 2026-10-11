package server

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// The delta vectors are shared with the TS client
// (client/test/unit/access-delta.test.ts), which applies each delta this
// side finds.
const accessDeltaVectorsPath = "../../testdata/access/delta.json"

type deltaVectorSummary struct {
	Subjects []string            `json:"subjects"`
	Root     []string            `json:"root"`
	Rows     map[string][]string `json:"rows"`
}

type deltaVectorCase struct {
	Name  string             `json:"name"`
	Base  deltaVectorSummary `json:"base"`
	Want  deltaVectorSummary `json:"want"`
	Delta *struct {
		Set     map[string][]string `json:"set"`
		Removed []string            `json:"removed"`
	} `json:"delta"`
}

// groupedRows groups rows, which maps "entity/key" to permissions, as
// CallerAccess.anchor_groups orders them.
func groupedRows(rows map[string][]string) []*syncplanev1.AnchorGroup {
	byPerms := map[string][]string{}
	for row, perms := range rows {
		id := strings.Join(perms, "\x00")
		byPerms[id] = append(byPerms[id], row)
	}
	var groups []*syncplanev1.AnchorGroup
	for id, rows := range byPerms {
		groups = append(groups, &syncplanev1.AnchorGroup{
			Permissions: strings.Split(id, "\x00"),
			Anchors:     rowsByEntity(rows),
		})
	}
	slices.SortFunc(groups, func(a, b *syncplanev1.AnchorGroup) int {
		return slices.Compare(a.Permissions, b.Permissions)
	})
	return groups
}

// rowsByEntity groups rows, as "entity/key", per entity: the entities
// sorted, and each one's keys sorted.
func rowsByEntity(rows []string) []*syncplanev1.AnchorRows {
	sort.Strings(rows)
	var out []*syncplanev1.AnchorRows
	for _, row := range rows {
		entity, key, _ := strings.Cut(row, "/")
		if n := len(out); n > 0 && out[n-1].Entity == entity {
			out[n-1].Keys = append(out[n-1].Keys, key)
			continue
		}
		out = append(out, &syncplanev1.AnchorRows{Entity: entity, Keys: []string{key}})
	}
	return out
}

func summaryOf(s deltaVectorSummary) *syncplanev1.CallerAccess {
	return &syncplanev1.CallerAccess{Subjects: s.Subjects, RootPermissions: s.Root, AnchorGroups: groupedRows(s.Rows)}
}

// ApplyCallerAccessDelta is applyDelta, for the specs outside the package.
var ApplyCallerAccessDelta = applyDelta

// applyDelta applies delta to held as CallerAccessDelta documents it, and
// returns the result without seq and version.
func applyDelta(held *syncplanev1.CallerAccess, delta *syncplanev1.CallerAccessDelta) *syncplanev1.CallerAccess {
	rows := map[string][]string{}
	for _, g := range held.GetAnchorGroups() {
		for _, a := range g.GetAnchors() {
			for _, key := range a.GetKeys() {
				rows[a.GetEntity()+"/"+key] = g.GetPermissions()
			}
		}
	}
	for _, g := range delta.GetAnchorGroups() {
		for _, a := range g.GetAnchors() {
			for _, key := range a.GetKeys() {
				rows[a.GetEntity()+"/"+key] = g.GetPermissions()
			}
		}
	}
	for _, a := range delta.GetRemoved() {
		for _, key := range a.GetKeys() {
			delete(rows, a.GetEntity()+"/"+key)
		}
	}
	return &syncplanev1.CallerAccess{
		Subjects: held.Subjects, RootPermissions: held.RootPermissions, AnchorGroups: groupedRows(rows),
		MaxAccessDepth: held.MaxAccessDepth,
	}
}

var _ = Describe("CallerAccess delta conformance vectors", func() {
	raw, err := os.ReadFile(accessDeltaVectorsPath)
	if err != nil {
		panic(err)
	}
	var doc struct {
		Cases []deltaVectorCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(err)
	}
	for _, c := range doc.Cases {
		It(c.Name, func() {
			got := callerAccessDeltaOf(summaryOf(c.Base), summaryOf(c.Want))
			if c.Delta == nil {
				Expect(got).To(BeNil())
				return
			}
			var removed []string
			removed = append(removed, c.Delta.Removed...)
			want := &syncplanev1.CallerAccessDelta{AnchorGroups: groupedRows(c.Delta.Set), Removed: rowsByEntity(removed)}
			Expect(proto.Equal(got, want)).To(BeTrue(), "got %v\nwant %v", got, want)
		})
	}
})

var _ = Describe("callerAccessDeltaOf", func() {
	It("turns any summary into any other with the same subjects and root", func() {
		perms := [][]string{{"a"}, {"a", "b"}, {"b"}, {"a", "b", "c"}, {"c"}}
		random := func(r *rand.Rand) map[string][]string {
			rows := map[string][]string{}
			for i := range 12 {
				if r.IntN(3) > 0 {
					entity := []string{"project", "task"}[i%2]
					rows[fmt.Sprintf("%s/k%02d", entity, i)] = perms[r.IntN(len(perms))]
				}
			}
			return rows
		}
		for seed := range uint64(500) {
			r := rand.New(rand.NewPCG(seed, 0))
			base := summaryOf(deltaVectorSummary{Subjects: []string{"u"}, Rows: random(r)})
			want := summaryOf(deltaVectorSummary{Subjects: []string{"u"}, Rows: random(r)})
			delta := callerAccessDeltaOf(base, want)
			Expect(delta).NotTo(BeNil())
			Expect(proto.Equal(applyDelta(base, delta), want)).To(BeTrue(), "seed %d", seed)
			Expect(proto.Equal(applyDelta(want, delta), want)).To(BeTrue(), "seed %d: applying twice changed the result", seed)
		}
	})
})
