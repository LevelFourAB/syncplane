package server

import (
	"errors"
	"fmt"
	"math"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// relTargetInfo resolves the registered entity a rel node's target descriptor
// belongs to. A reference field always points at an entity message, but a
// server may not have that entity registered — a predicate crossing into one
// it does not serve is a caller error, not a server bug.
func (s *Server) relTargetInfo(node *query.RelNode) (*entityInfo, error) {
	target, ok := s.catalogPtr().byType[node.Target.FullName()]
	if !ok {
		return nil, fmt.Errorf("path %v: reference target %s is not a registered entity",
			node.Cond.GetPath(), node.Target.FullName())
	}
	return target, nil
}

// checkRelLimit rejects a predicate carrying more rel conditions than
// MaxRelConditions. Each one costs its own pass over its target entity, so the
// count multiplies the price of the query — and nothing in the predicate's own
// size bounds it.
func (s *Server) checkRelLimit(rels []*query.RelNode) error {
	if len(rels) > s.limits.maxRelConditions() {
		return connect.NewError(connect.CodeInvalidArgument, errors.New(limitExceeded(
			fmt.Sprintf("%d matches() conditions", len(rels)),
			"MaxRelConditions", s.limits.maxRelConditions())))
	}
	return nil
}

// resolveRels runs the pre-pass of the two-phase rel rewrite: for each rel
// node, evaluate its inner condition over the target entity's live rows in
// workspace — the one tx reads — and collapse the node to a membership test on
// the matching target keys (query.RelReplacement). A stub that names another
// workspace resolves to nothing here; the replacement says so.
//
// visible gates each TARGET row exactly as a direct read of that entity would
// — the rule follow already states: crossing a reference is never a
// permission-escalation path. On the RPC path the caller must have warmed the
// target entities' read filters before the snapshot opened (see
// Server.Query); a trusted read passes the all-visible gate.
func (s *Server) resolveRels(
	tx storage.ReadTx,
	workspace string,
	rels []*query.RelNode,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
) (map[*syncplanev1.RelCond]*syncplanev1.Condition, error) {
	out := make(map[*syncplanev1.RelCond]*syncplanev1.Condition, len(rels))
	// Nodes selecting the same target rows share one pass. A predicate that
	// asks two things about one relation — the same entity crossed with the
	// same inner condition, whether through the same ref field or a different
	// one — otherwise pays for that entity once per node.
	shared := make(map[string][][]*syncplanev1.QueryValue, len(rels))
	for _, node := range rels {
		info, err := s.relTargetInfo(node)
		if err != nil {
			return nil, err
		}
		shape, err := relShape(info.name, node)
		if err != nil {
			return nil, err
		}
		targets, done := shared[shape]
		if !done {
			targets, err = s.relTargets(tx, info, node, visible, emitter)
			if err != nil {
				return nil, err
			}
			shared[shape] = targets
		}
		out[node.Cond] = query.RelReplacement(node, targets, workspace)
	}
	return out, nil
}

// relShape identifies the pre-pass a node needs, as opposed to the replacement
// it builds from the result: the target entity and the inner condition decide
// which rows match, while the ref field and its arity only decide how the
// membership test is phrased. Two nodes sharing a shape share their pass.
//
// The key set a pass yields is read in the target's key-field order, which is
// a property of the target entity, so the entity name pins it.
func relShape(entity string, node *query.RelNode) (string, error) {
	// Deterministic so that two structurally identical conditions produce the
	// same bytes; equality of the encoding is stricter than equality of the
	// message, which only costs a missed sharing opportunity.
	cond, err := proto.MarshalOptions{Deterministic: true}.Marshal(node.Cond.GetCond())
	if err != nil {
		return "", err
	}
	return entity + "\x00" + string(cond), nil
}

// errRelKeys reports a pre-pass that matched more target rows than
// MaxRelKeys. It never escapes relTargets.
var errRelKeys = errors.New("rel key budget exceeded")

// relTargets is one rel node's pre-pass: the key tuple of every live target
// row this reader may see that the inner condition matches.
//
// An index narrows which target rows are read exactly as it does for any query
// — it never changes which of them match, and declines whenever it cannot
// promise that — so an inner condition a declared index serves costs
// O(matches) instead of O(target entity). That difference is most of the price
// of a rel query over a large target.
func (s *Server) relTargets(
	tx storage.ReadTx,
	info *entityInfo,
	node *query.RelNode,
	visible func(storage.Row) (bool, error),
	emitter *rowEmitter,
) ([][]*syncplanev1.QueryValue, error) {
	desc := info.msgType.Descriptor()
	inner, err := query.Compile(desc, node.Cond.GetCond())
	if err != nil {
		// ExtractRels compiled the same condition against the same descriptor,
		// so this cannot fail after it succeeded.
		return nil, err
	}
	// The pre-pass wants every match, in no particular order: an empty
	// ordering and an unbounded page are what say so to the planner.
	order, err := query.CompileOrder(desc, nil)
	if err != nil {
		return nil, err
	}
	budget := s.limits.maxRelKeys()

	var targets [][]*syncplanev1.QueryValue
	consider := func(row storage.Row, pred *query.Compiled) error {
		if visible != nil {
			ok, err := visible(row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		decoded, err := decodeMatchable(emitter, info, row)
		if err != nil || decoded == nil {
			return err
		}
		if !pred.Eval(decoded.ProtoReflect()) {
			return nil
		}
		if len(targets) >= budget {
			return errRelKeys
		}
		targets = append(targets, query.TargetKeyValues(node, decoded.ProtoReflect()))
		return nil
	}
	// want is unbounded and discard is the un-offer half of queryByIndex's
	// contract: a walk that declines after offering rows hands them back, and
	// the scan below offers them again.
	served, err := queryByIndex(tx, info, inner, order, page{}, s.limits, consider,
		func() int { return math.MaxInt },
		func() { targets = targets[:0] },
	)
	if err == nil && !served {
		err = tx.ScanEntity(info.name, func(row storage.Row) error {
			return consider(row, inner)
		})
	}
	if errors.Is(err, errRelKeys) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(limitExceeded(
			fmt.Sprintf("matches() on path %v selecting over %d target rows",
				node.Cond.GetPath(), budget),
			"MaxRelKeys", budget)))
	}
	if err != nil {
		return nil, err
	}
	return targets, nil
}
