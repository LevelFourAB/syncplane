package rebase

import (
	"errors"
	"fmt"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// ErrTextDeltaUnusable reports a RowChange whose text delta state does not
// apply to the confirmed row it was folded onto. The confirmed state is not
// what the origin cut the fragments against, so no change of the page that
// carried it can be trusted; the recovery is a fresh bootstrap.
var ErrTextDeltaUnusable = errors.New("rebase: text delta state does not apply to the confirmed row")

// FoldTextDeltas returns the full row a RowChange stands for, given the row
// confirmed before it.
//
// A tombstone gives the absent row. A change without text deltas gives its
// value unchanged, bytes included. Otherwise each FieldDelta fills the
// document its instance path names: an empty fragment keeps the document
// confirmed holds there, and a non-empty fragment applies to that document,
// or to an empty one where confirmed holds none. The result is encoded
// deterministically.
//
// FoldTextDeltas returns an error wrapping ErrTextDeltaUnusable when the value
// or confirmed does not decode, when a delta names a position the value does
// not hold, when an empty fragment names a document confirmed does not hold,
// or when a fragment does not apply. Fields the change lists as hidden stay
// as the change has them: cleared.
func (m *Model) FoldTextDeltas(confirmed Row, change *syncplanev1.RowChange) (Row, error) {
	if change.GetTombstone() {
		return Row{}, nil
	}
	if len(change.GetTextDeltas()) == 0 {
		return Row{Present: true, Value: change.GetValue()}, nil
	}
	unusable := func(field string, cause error) error {
		if cause == nil {
			return fmt.Errorf("%w: %s %q field %q", ErrTextDeltaUnusable, change.GetEntity(), change.GetKey(), field)
		}
		return fmt.Errorf("%w: %s %q field %q: %v", ErrTextDeltaUnusable, change.GetEntity(), change.GetKey(), field, cause)
	}
	first := change.GetTextDeltas()[0].GetField()
	next, err := m.decode(change.GetValue())
	if err != nil {
		return Row{}, unusable(first, err)
	}
	var have map[string][]byte
	if confirmed.Present {
		base, err := m.decode(confirmed.Value)
		if err != nil {
			return Row{}, unusable(first, err)
		}
		have = textpath.Snapshot(m.text, base)
	}
	positions := make(map[string]textpath.Instance)
	for _, ti := range textpath.Instances(m.text, next) {
		if _, ok := positions[ti.Path]; !ok {
			positions[ti.Path] = ti
		}
	}
	for _, d := range change.GetTextDeltas() {
		ti, ok := positions[d.GetField()]
		if !ok {
			return Row{}, unusable(d.GetField(), errors.New("the row value holds no such position"))
		}
		base := have[d.GetField()]
		if len(d.GetFragment()) == 0 {
			// "Unchanged over the span" is only ever said of a document the
			// origin holds, so a confirmed row without one is not the row the
			// origin diffed against.
			if len(base) == 0 {
				return Row{}, unusable(d.GetField(), nil)
			}
			ti.Set(base)
			continue
		}
		// An absent base is legitimate for a document created inside the
		// span: its fragment is self-contained.
		full, err := text.ApplyFragmentBytes(base, d.GetFragment())
		if err != nil {
			return Row{}, unusable(d.GetField(), err)
		}
		ti.Set(full)
	}
	out, err := marshal(next)
	if err != nil {
		return Row{}, err
	}
	return Row{Present: true, Value: out}, nil
}
