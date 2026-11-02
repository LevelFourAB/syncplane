package rebase

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// Row is the state of one row. The zero value is an absent row, which is also
// how a tombstone reads.
type Row struct {
	// Present is false for an absent or deleted row.
	Present bool
	// Value is the encoded entity message. Ignored when Present is false.
	Value []byte
}

// Op is one local write to one row.
type Op struct {
	// Kind is PUT, PATCH or DELETE.
	Kind syncplanev1.CrudOp
	// Value is the entity message the write submitted, encoded. Ignored for
	// DELETE.
	Value []byte
	// Mask holds the field-mask paths of a PATCH. Ignored for PUT and DELETE.
	Mask []string
	// Text maps instance paths to full text documents (see TextDocuments). A
	// document here stands in for the bytes Value carries at that path, which
	// may be delta state or a plain-string value only the writer could
	// resolve. CaptureText builds it. Nil when the write carries no text.
	Text map[string][]byte
}

var (
	// ErrRefused reports an op the origin refuses whatever the row holds,
	// or refuses on the row it is applied to. Such an op leaves the row as it
	// was.
	ErrRefused = errors.New("rebase: op refused")

	// ErrNoTarget reports a PATCH of an absent row. A PATCH is not an
	// upsert, so the row stays absent. It wraps ErrRefused.
	ErrNoTarget = fmt.Errorf("%w: PATCH of an absent row", ErrRefused)

	// ErrUncapturedText reports a text document the op writes as delta state
	// or as a plain-string value with no full document in Op.Text to stand in
	// for it. The bytes only meant something to the replica that took the
	// write, so the op cannot be replayed or forwarded.
	ErrUncapturedText = errors.New("rebase: text document has no captured full state")
)

func refused(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrRefused}, args...)...)
}

// Replay returns the row that results from applying ops, in order, to
// confirmed. An op Apply refuses (ErrRefused) leaves the row as it was and
// replay continues with the next one. Replay returns any other error Apply
// returns, with the zero Row.
//
// Expected versions play no part: the origin decides compare-and-swap. With
// no ops, Replay returns confirmed unchanged, bytes included.
func (m *Model) Replay(confirmed Row, ops []Op) (Row, error) {
	row := confirmed
	for i, op := range ops {
		next, err := m.Apply(row, op)
		if errors.Is(err, ErrRefused) {
			continue
		}
		if err != nil {
			return Row{}, fmt.Errorf("rebase: op %d: %w", i, err)
		}
		row = next
	}
	return row, nil
}

// Apply returns the row that results from one op on row, with the semantics
// the origin applies a client write with:
//
//   - PUT replaces the row with Op.Value, except that each collaborative-text
//     document becomes the union of the document row holds at that position
//     and the one the op writes. A position the op leaves empty keeps row's
//     document; an element the op drops loses its document with it.
//   - PATCH merges the fields Op.Mask names onto row, per field, keyed list
//     and map items included, and runs the Model's mergers. A text document a
//     mask path names outright is the union of both sides; one a mask path
//     covers through an ancestor is replaced with the subtree. A document the
//     PATCH clears keeps its version vector, as an empty document. A PATCH of
//     an absent row returns ErrNoTarget.
//   - DELETE returns the absent row.
//
// Text in Value is taken from Op.Text where Op.Text holds a document for the
// path and Value carries non-empty bytes there; empty bytes stay empty, which
// is a clear for a PATCH. Reference stubs are stripped to their target's key
// fields. The result is encoded deterministically.
//
// An error wrapping ErrRefused means the origin refuses the op on row: a value
// or a stored row that does not decode, a PATCH without a mask or with a mask
// that does not fit the schema, text documents that do not union, or
// ErrNoTarget. ErrUncapturedText reports a text document Apply cannot use.
// Neither changes anything. Apply does not retain row, op or their slices.
func (m *Model) Apply(row Row, op Op) (Row, error) {
	switch op.Kind {
	case syncplanev1.CrudOp_CRUD_OP_DELETE:
		return Row{}, nil
	case syncplanev1.CrudOp_CRUD_OP_PUT:
		return m.applyPut(row, op)
	case syncplanev1.CrudOp_CRUD_OP_PATCH:
		return m.applyPatch(row, op)
	default:
		return row, refused("unknown op %v", op.Kind)
	}
}

func (m *Model) applyPut(row Row, op Op) (Row, error) {
	next, err := m.decode(op.Value)
	if err != nil {
		return row, refused("%v", err)
	}
	if err := m.resolveText(next, op.Text, nil); err != nil {
		return row, err
	}
	if row.Present && len(m.text) > 0 {
		// A stored row that does not decode is no base: the PUT replaces it,
		// as it repairs such a row on the origin.
		if base, err := m.decode(row.Value); err == nil {
			if err := m.unionText(base, next); err != nil {
				return row, refused("%v", err)
			}
		}
	}
	m.projectRefs(next)
	out, err := marshal(next)
	if err != nil {
		return row, err
	}
	return Row{Present: true, Value: out}, nil
}

func (m *Model) applyPatch(row Row, op Op) (Row, error) {
	if len(op.Mask) == 0 {
		return row, refused("PATCH without a field mask")
	}
	src, err := m.decode(op.Value)
	if err != nil {
		return row, refused("%v", err)
	}
	if err := m.resolveText(src, op.Text, op.Mask); err != nil {
		return row, err
	}
	if !row.Present {
		return row, ErrNoTarget
	}
	base, err := m.decode(row.Value)
	if err != nil {
		return row, refused("PATCH of a row that does not decode: %v", err)
	}
	prev := textpath.Snapshot(m.text, base)
	// base is this call's own decode, so a merge that fails part way leaves
	// nothing behind: row is returned as it came in.
	if err := lww.MergeWithMergers(base.Interface(), src.Interface(), op.Mask, m.mergers); err != nil {
		return row, refused("%v", err)
	}
	if err := textpath.KeepClearedVersions(m.text, prev, base); err != nil {
		return row, refused("%v", err)
	}
	m.projectRefs(base)
	out, err := marshal(base)
	if err != nil {
		return row, err
	}
	return Row{Present: true, Value: out}, nil
}

// resolveText replaces each document msg carries at a position the write
// reaches with the captured one, and re-encodes every other such document
// canonically, as the origin does on ingress. mask nil means a PUT, which
// reaches every position. Positions the mask does not reach are left alone:
// the merge never reads them.
func (m *Model) resolveText(msg protoreflect.Message, captured map[string][]byte, mask []string) error {
	for _, ti := range textpath.Instances(m.text, msg) {
		b := ti.Bytes()
		if len(b) == 0 || (mask != nil && !covers(mask, ti.Path)) {
			continue
		}
		if doc := captured[ti.Path]; len(doc) > 0 {
			b = doc
		} else if text.IsFragment(b) || text.IsPlainString(b) {
			return fmt.Errorf("%w: %q", ErrUncapturedText, ti.Path)
		}
		d, err := text.Decode(b, "")
		if err != nil {
			return refused("text field %q: %v", ti.Path, err)
		}
		ti.Set(d.Encode())
	}
	return nil
}

// unionText unions every document of base into next, pairing them by
// instance path.
func (m *Model) unionText(base, next protoreflect.Message) error {
	return textpath.Union(m.text, base, next, func(_ string, b, n []byte) ([]byte, error) {
		return text.Merge(b, n)
	})
}
