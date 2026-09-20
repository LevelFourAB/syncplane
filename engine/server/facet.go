package server

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// Facets on a row query — QueryRequest.facets — are grouped reads folded from
// the walk that answers the rows. Each axis compiles to the same key a grouped
// read uses (query.Group) and buckets the same way, so a facet and a COUNT
// aggregate over one predicate never disagree; what differs is that no walk
// of their own is taken.

// compileFacets resolves a request's facet axes, or returns nil for a request
// naming none. A grouped read already answers with groups, so a request
// combining the two is refused rather than resolved, and the count is capped
// by MaxQueryFacets: each axis costs a bucket per distinct value and a lookup
// per matching row, and nothing in the request's own size bounds it.
func (s *Server) compileFacets(
	desc protoreflect.MessageDescriptor,
	msg *syncplanev1.QueryRequest,
	grouped bool,
) ([]*query.Group, error) {
	specs := msg.GetFacets()
	if len(specs) == 0 {
		return nil, nil
	}
	if grouped {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("facets cannot be combined with group_by; a grouped read answers with groups already"))
	}
	if len(specs) > s.limits.maxQueryFacets() {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("%d facets", len(specs)), "MaxQueryFacets", s.limits.maxQueryFacets())))
	}
	out := make([]*query.Group, 0, len(specs))
	for i, spec := range specs {
		if len(spec.GetPath()) == 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("facets[%d]: a facet needs a path", i))
		}
		g, err := query.CompileGroup(desc, spec.GetPath())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("facets[%d]: %w", i, err))
		}
		out = append(out, g)
	}
	return out, nil
}

// facetFold accumulates every axis of a request while its rows are walked.
type facetFold struct {
	axes []*query.Group
	// One bucket table per axis: the group's index in out, by GroupValue.Key.
	byKey []map[string]int
	out   [][]query.GroupCount
}

func newFacetFold(axes []*query.Group) *facetFold {
	f := &facetFold{axes: axes}
	f.reset()
	return f
}

// reset drops every count, for the same reason topK.reset exists: an index
// walk that declines after offering rows hands them back, and the scan then
// offers them again.
func (f *facetFold) reset() {
	f.byKey = make([]map[string]int, len(f.axes))
	f.out = make([][]query.GroupCount, len(f.axes))
	for i := range f.axes {
		f.byKey[i] = map[string]int{}
	}
}

// add counts one matching row on every axis: once per distinct value it holds
// there, so a repeated path counts per element and a duplicated element once —
// the contribution a grouped read gives the row.
func (f *facetFold) add(msg protoreflect.Message) {
	for i, axis := range f.axes {
		seen := map[string]bool{}
		for _, gv := range axis.Values(msg) {
			key := gv.Key()
			if seen[key] {
				continue
			}
			seen[key] = true
			if at, ok := f.byKey[i][key]; ok {
				f.out[i][at].Count++
				continue
			}
			f.byKey[i][key] = len(f.out[i])
			f.out[i] = append(f.out[i], query.GroupCount{Value: gv, Count: 1})
		}
	}
}

// proto answers the request's facets in its order, each echoing its path with
// its groups in the order a grouped read promises.
func (f *facetFold) proto(specs []*syncplanev1.QueryFacet) []*syncplanev1.QueryFacet {
	out := make([]*syncplanev1.QueryFacet, 0, len(f.axes))
	for i := range f.axes {
		query.SortGroups(f.out[i])
		groups := make([]*syncplanev1.QueryGroup, 0, len(f.out[i]))
		for _, g := range f.out[i] {
			groups = append(groups, &syncplanev1.QueryGroup{Value: g.Value.Proto(), Count: g.Count})
		}
		out = append(out, &syncplanev1.QueryFacet{Path: specs[i].GetPath(), Groups: groups})
	}
	return out
}
