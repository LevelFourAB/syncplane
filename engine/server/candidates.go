package server

import (
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Candidate narrowing: reading the rows a scope can admit instead of walking
// the entity to find them.
//
// A bootstrap and a delta resync both visit an entity to decide, row by row,
// which of its rows a client may see. Both have the same escape from that: when
// every rule deciding the entity names its rows — a predicate an index serves
// or a key it pins, a window's membership, a follow closure — the union of
// those names is a SUPERSET of the visible rows, so reading it and classifying
// each row produces exactly what the walk would, at the cost of the scope
// rather than the cost of the workspace.
//
// The two callers differ only in which rules they allow (see bootstrapNarrowable
// and resyncNarrowable) and which key sets they contribute. Everything past that
// decision is shared here, because the property that makes narrowing safe is the
// same one on both paths: what it collects must be a superset, and a caller that
// cannot prove it declines and scans.

// candidateKeys returns the row keys of one entity a caller must examine,
// sorted in key order. ok is false when the entity has to be scanned instead,
// which is always legal and is what every decline below means.
//
// Each scope contributes its key set as it stands and its predicates'
// candidates, from a secondary index or from the single key an equality on
// every key field pins (see keyRowCandidate). A predicate neither reaches
// abandons the whole set: a partial union would silently drop rows the caller
// owes its client.
//
// extras are key sets whose members are admitted whatever the predicates say —
// window membership, a follow closure — restricted here to this entity. Read
// filters do NOT narrow the candidates and must not: a filter only ever hides a
// row some predicate or membership set already admitted, and the caller applies
// it per row.
//
// Above maxCandidates it declines. Past that point the narrowing has stopped
// paying: a predicate matching most of an entity costs more through an index
// than through the scan, and the limit is also what bounds the keys held here
// at once.
func candidateKeys(
	tx storage.ReadTx,
	info *entityInfo,
	scopes []*entityScope,
	extras []closureRows,
	maxCandidates int,
	limits Limits,
) (keys []string, ok bool, err error) {
	found := make(map[string]struct{})
	over := false
	add := func(key string) {
		if _, dup := found[key]; dup {
			return
		}
		if len(found) >= maxCandidates {
			over = true
			return
		}
		found[key] = struct{}{}
	}

	for _, scope := range scopes {
		if scope == nil {
			continue
		}
		// A key set names its rows outright: the candidates are the set, and
		// the row read that follows is the only cost. This is what pinning N
		// rows buys over N per-key predicates — the same rows, without N
		// predicate walks to find them.
		for key := range scope.keys {
			add(key)
		}
		if over {
			return nil, false, nil
		}
		for _, pred := range scope.preds {
			// The pinned key first: it costs no read at all, and an entity
			// whose key fields carry no index has nothing else to narrow with.
			if key, pinned := keyRowCandidate(info, pred.cond); pinned {
				add(key)
				if over {
					return nil, false, nil
				}
				continue
			}
			// Keys, not rows: what the index names is a superset of the
			// predicate's matches, which is all a caller needs — the residual
			// is re-checked when the row is classified, against the right scope
			// and the right filter, and a key whose row is a tombstone or gone
			// is dropped there too. Reading the rows here would read every one
			// of them twice.
			served, err := candidateKeysByIndex(tx, info, pred.compiled, limits,
				func(rowKey string) error {
					add(rowKey)
					return nil
				})
			if err != nil {
				return nil, false, err
			}
			if !served || over {
				return nil, false, nil
			}
		}
	}
	for _, extra := range extras {
		for ck := range extra {
			if ck.entity == info.name {
				add(ck.key)
			}
		}
	}
	if over {
		return nil, false, nil
	}

	keys = make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	// Key order is what makes a resume position mean the same thing here as it
	// does on the scan path, so an interrupted walk can be continued by either.
	sort.Strings(keys)
	return keys, true, nil
}

// keyRowCandidate derives the one row key a predicate can match when it pins
// every key field of the entity to a literal, and reports false when it does
// not.
//
// Every row matching a conjunction matches each of its conjuncts, so a row
// matching such a predicate carries exactly those key-field values and
// therefore exactly this key. One candidate is then a superset of the
// predicate's matches — which is what serves "the row with id X" on an entity
// whose key fields carry no index of their own, and what would otherwise be a
// walk of the whole entity to find a single row.
//
// Only an equality against a literal of the field's OWN kind pins a field: the
// evaluator compares numbers across kinds while a key is rendered per kind (see
// naming.ScalarKeyString), so a cross-kind comparison could name a key that is
// not the matching row's. Everything else — a disjunction, a negation, a range,
// a key field left free — leaves the predicate unpinned.
func keyRowCandidate(info *entityInfo, cond *syncplanev1.Condition) (string, bool) {
	if cond == nil || len(info.keyFields) == 0 {
		return "", false
	}
	pinned := make(map[protoreflect.Name]string, len(info.keyFields))
	var walk func(c *syncplanev1.Condition)
	walk = func(c *syncplanev1.Condition) {
		switch kind := c.GetKind().(type) {
		case *syncplanev1.Condition_And:
			// Only into an AND: a row matching the whole predicate matches
			// every conjunct of it, at any depth. An OR or a NOT says nothing
			// about what its operands demand of the row.
			for _, sub := range kind.And.GetConds() {
				walk(sub)
			}
		case *syncplanev1.Condition_Cmp:
			cmp := kind.Cmp
			if cmp.GetOp() != syncplanev1.CmpOp_CMP_OP_EQ || len(cmp.GetPath()) != 1 {
				return
			}
			for _, fd := range info.keyFields {
				if string(fd.Name()) != cmp.GetPath()[0] {
					continue
				}
				if part, ok := literalKeyPart(fd, cmp.GetValue()); ok {
					pinned[fd.Name()] = part
				}
			}
		}
	}
	walk(cond)

	parts := make([]string, len(info.keyFields))
	for i, fd := range info.keyFields {
		part, ok := pinned[fd.Name()]
		if !ok {
			return "", false
		}
		parts[i] = part
	}
	return naming.RowKey(parts), true
}

// literalKeyPart renders a query literal as the key part a row holding that
// value would have. ok is false for a literal whose kind is not the field's,
// which is the conservative half of keyRowCandidate's contract.
func literalKeyPart(fd protoreflect.FieldDescriptor, qv *syncplanev1.QueryValue) (string, bool) {
	var v protoreflect.Value
	switch kind := qv.GetKind().(type) {
	case *syncplanev1.QueryValue_StringValue:
		if fd.Kind() != protoreflect.StringKind {
			return "", false
		}
		v = protoreflect.ValueOfString(kind.StringValue)
	case *syncplanev1.QueryValue_BoolValue:
		if fd.Kind() != protoreflect.BoolKind {
			return "", false
		}
		v = protoreflect.ValueOfBool(kind.BoolValue)
	case *syncplanev1.QueryValue_IntValue:
		switch fd.Kind() {
		case protoreflect.Int32Kind, protoreflect.Int64Kind,
			protoreflect.Sint32Kind, protoreflect.Sint64Kind,
			protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		default:
			return "", false
		}
		v = protoreflect.ValueOfInt64(kind.IntValue)
	case *syncplanev1.QueryValue_UintValue:
		switch fd.Kind() {
		case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
			protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		default:
			return "", false
		}
		v = protoreflect.ValueOfUint64(kind.UintValue)
	default:
		return "", false
	}
	return naming.ScalarKeyString(fd, v), true
}
