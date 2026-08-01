// Error text conventions for this package, and the helpers that hold the
// recurring messages to one wording.
//
// Which audience an error is for decides how it reads, and the prefix is what
// tells them apart:
//
//   - Errors an APPLICATION DEVELOPER gets back from a Go call — RegisterEntity,
//     RegisterMerger, RegisterDynamicEntity, EntityKey, the ViewTx/ApplyTx/
//     EntityTx methods, the options New applies — carry a "server: " prefix.
//     They surface far from here, wrapped into a startup error or logged beside
//     errors from unrelated libraries, so they must name their origin. They may
//     name Go identifiers, because the reader is holding the Go API.
//   - Errors a CLIENT gets back over the wire — the connect.Error values the RPC
//     handlers return — carry NO prefix. A Go package name is noise in an RPC
//     status, and the reader is on the far side of the wire, quite possibly not
//     writing Go at all.
//
// A helper whose error is always wrapped by its caller (resolveMergerField,
// kindToType) inherits the caller's prefix and must not add its own.
//
// Both audiences are owed a way out, which is the harder half of the rule: an
// error that reports a bound names the bound's current value AND the knob that
// moves it, and an error about an unregistered entity names the call that
// registers it. "exceeds MaxClosureSize" is a fact about the engine's internals
// that leaves the reader exactly where they started.
package server

import (
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// errNotRegistered is the single message for "the catalog has never seen this
// message type", shared by RegisterMerger, EntityKey and the ViewTx/EntityTx
// lookups. Reaching the engine before wiring the schema is the common newcomer
// mistake, so the message names the fix rather than only the fact.
func errNotRegistered(name protoreflect.FullName) error {
	return fmt.Errorf("server: %s is not a registered entity; register it first with "+
		"server.WithEntities(&%s{}) or Server.RegisterEntity", name, name.Name())
}

// limitExceeded formats every bound-exceeded message in the package: what was
// asked for, the Limits field it crossed with its current value, and the option
// that raises it. what carries its own number and unit ("push spans 70
// workspaces") so each site can say what it actually counted.
func limitExceeded(what, knob string, limit int) string {
	return fmt.Sprintf("%s, over %s (%d); raise it with server.WithLimits", what, knob, limit)
}

// errUnknownSegment reports a follow path naming something that is not a
// relation of the entity it was applied to, listing the relations that ARE
// available. Segment names are derived (forward refs by their JSON field name,
// reverse refs pluralized from the source entity), so a developer writing a
// path by hand cannot reliably guess them — showing the set is the difference
// between a typo fixed in seconds and one debugged against the ref-graph rules.
func errUnknownSegment(g *refGraph, entity, seg string) error {
	return connect.NewError(connect.CodeInvalidArgument,
		fmt.Errorf("follow segment %q is not a relation of entity %q (relations: %s)",
			seg, entity, relationList(g.relationsOf(entity))))
}

// relationList renders relation names for an error message, naming the empty
// case rather than trailing off after "relations: ".
func relationList(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// relationsOf returns every segment name valid on entity — forward refs and
// reverse refs together, sorted, which is exactly the namespace segmentTarget
// resolves a RefPath segment against. The two halves are disjoint by
// construction (buildRefGraph rejects a reverse name that clashes with either
// another reverse name or a forward ref field), so the concatenation cannot
// repeat a name.
func (g *refGraph) relationsOf(entity string) []string {
	names := make([]string, 0, len(g.forward[entity])+len(g.reverse[entity]))
	for name := range g.forward[entity] {
		names = append(names, name)
	}
	for name := range g.reverse[entity] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
