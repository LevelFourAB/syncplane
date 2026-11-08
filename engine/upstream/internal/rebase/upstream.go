package rebase

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/textpath"
	"github.com/levelfourab/syncplane/engine/text"
)

// CaptureText returns the full text documents to keep with a local write, for
// Op.Text: for every position where op.Value carries non-empty text bytes that
// the write reaches, the document rowAfter holds there. rowAfter is the
// encoded row as the replica stored it after the write. A PUT reaches every
// position; a PATCH reaches the positions a mask path names or sits above.
// op.Text is ignored.
//
// It returns nil for a DELETE, for an entity without text fields and for a
// write that carries no text. It returns an error when op.Value or rowAfter
// does not decode.
func (m *Model) CaptureText(op Op, rowAfter []byte) (map[string][]byte, error) {
	if len(m.text) == 0 || op.Kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return nil, nil
	}
	src, err := m.decode(op.Value)
	if err != nil {
		return nil, err
	}
	paths := m.writtenText(src, op)
	if len(paths) == 0 {
		return nil, nil
	}
	after, err := m.TextDocuments(rowAfter)
	if err != nil {
		return nil, err
	}
	var out map[string][]byte
	for _, path := range paths {
		if doc := after[path]; len(doc) > 0 {
			if out == nil {
				out = make(map[string][]byte, len(paths))
			}
			out[path] = doc
		}
	}
	return out, nil
}

// UpstreamValue returns the value to send the origin for op: op.Value with
// every text document the write reaches replaced by its document in op.Text.
// Text bytes the write leaves empty stay empty, so a PATCH that clears a
// document still clears it upstream.
//
// It returns op.Value unchanged when nothing is replaced, and a deterministic
// encoding otherwise. It returns ErrUncapturedText when a reached document is
// delta state or a plain-string value with no entry in op.Text, and an error
// when op.Value does not decode. For a DELETE it returns nil.
func (m *Model) UpstreamValue(op Op) ([]byte, error) {
	if op.Kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return nil, nil
	}
	return m.upstream(op, func(_ string, doc []byte) ([]byte, error) { return doc, nil })
}

// UpstreamFragments returns the value to send the origin for the PATCH op
// when the origin holds the encoded row confirmed: UpstreamValue's value
// with every text document the write reaches replaced by a fragment of the
// state it holds beyond the document confirmed holds at that position. A
// position where confirmed holds no document keeps the full document.
//
// The fragment applies at a receiver whose document covers confirmed's. It
// returns the errors UpstreamValue returns, an error for an op that is not a
// PATCH, and an error when confirmed does not decode or a document does not
// merge with confirmed's.
func (m *Model) UpstreamFragments(op Op, confirmed []byte) ([]byte, error) {
	if op.Kind != syncplanev1.CrudOp_CRUD_OP_PATCH {
		return nil, fmt.Errorf("rebase: a %v carries no text fragment", op.Kind)
	}
	bases, err := m.TextDocuments(confirmed)
	if err != nil {
		return nil, err
	}
	return m.upstream(op, func(path string, doc []byte) ([]byte, error) {
		base := bases[path]
		if len(base) == 0 {
			return doc, nil
		}
		// The union holds confirmed's document whole, so the fragment can be
		// cut against it also when the origin moved on after the write.
		merged, err := text.Merge(base, doc)
		if err != nil {
			return nil, err
		}
		d, err := text.Decode(merged, "")
		if err != nil {
			return nil, err
		}
		b, err := text.Decode(base, "")
		if err != nil {
			return nil, err
		}
		return text.EncodeFragment(d, b)
	})
}

// upstream returns op.Value with every text document the write reaches
// replaced by what form returns for its document in op.Text, or op.Value
// unchanged when nothing is replaced.
func (m *Model) upstream(op Op, form func(path string, doc []byte) ([]byte, error)) ([]byte, error) {
	if len(m.text) == 0 {
		return op.Value, nil
	}
	src, err := m.decode(op.Value)
	if err != nil {
		return nil, err
	}
	mask := reachMask(op)
	replaced := false
	for _, ti := range textpath.Instances(m.text, src) {
		b := ti.Bytes()
		if len(b) == 0 || (mask != nil && !covers(mask, ti.Path)) {
			continue
		}
		if doc := op.Text[ti.Path]; len(doc) > 0 {
			v, err := form(ti.Path, doc)
			if err != nil {
				return nil, err
			}
			ti.Set(v)
			replaced = true
			continue
		}
		if text.IsFragment(b) || text.IsPlainString(b) {
			return nil, fmt.Errorf("%w: %q", ErrUncapturedText, ti.Path)
		}
	}
	if !replaced {
		return op.Value, nil
	}
	return marshal(src)
}

// writtenText lists the instance paths where src carries text bytes the write
// reaches.
func (m *Model) writtenText(src protoreflect.Message, op Op) []string {
	mask := reachMask(op)
	var out []string
	for _, ti := range textpath.Instances(m.text, src) {
		if len(ti.Bytes()) > 0 && (mask == nil || covers(mask, ti.Path)) {
			out = append(out, ti.Path)
		}
	}
	return out
}

// reachMask returns the mask that limits which text positions op reaches, or
// nil for a PUT, which reaches every position.
func reachMask(op Op) []string {
	if op.Kind == syncplanev1.CrudOp_CRUD_OP_PATCH {
		return op.Mask
	}
	return nil
}
