package rebase

import (
	"maps"
	"slices"
	"strings"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/textpath"
)

// Requeue returns op as it is sent again behind later, the writes to the same
// row that were made after op and that the origin applies first. Applied after
// later, the result leaves the fields of the row as op followed by later
// leaves them. Collaborative text is an exception: text unions in any order,
// so op keeps its documents and the text that later writes is not folded in.
//
//   - A DELETE or PUT in later supersedes the fields op writes. When the last
//     of them is a DELETE, Requeue reports false: nothing of op is left to
//     send. When it is a PUT, the result is a PATCH of the documents in
//     op.Text, and Requeue reports false when op.Text is empty.
//   - A DELETE op is kept as it is when later holds only PATCHes.
//   - Otherwise later holds only PATCHes, and the result is op with each of
//     them applied to op.Value as Apply applies it, mask paths that name a
//     text document left out. A PATCH op keeps its mask.
//
// With later empty, Requeue returns op unchanged. It returns an error when
// op.Value does not decode, and any error Replay returns. Requeue does not
// retain or change op, later or their slices and maps; the result shares
// op.Text.
func (m *Model) Requeue(op Op, later []Op) (Op, bool, error) {
	if len(later) == 0 {
		return op, true, nil
	}
	last := -1
	for i, l := range later {
		if l.Kind != syncplanev1.CrudOp_CRUD_OP_PATCH {
			last = i
		}
	}
	if last >= 0 {
		if later[last].Kind == syncplanev1.CrudOp_CRUD_OP_DELETE || op.Kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
			return Op{}, false, nil
		}
		if len(op.Text) == 0 {
			return Op{}, false, nil
		}
		return Op{
			Kind:  syncplanev1.CrudOp_CRUD_OP_PATCH,
			Value: op.Value,
			Mask:  slices.Sorted(maps.Keys(op.Text)),
			Text:  op.Text,
		}, true, nil
	}
	if op.Kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return op, true, nil
	}
	if _, err := m.decode(op.Value); err != nil {
		return Op{}, false, err
	}
	patches := make([]Op, 0, len(later))
	for _, l := range later {
		mask := slices.DeleteFunc(slices.Clone(l.Mask), m.namesText)
		if len(mask) == 0 {
			continue
		}
		patches = append(patches, Op{Kind: l.Kind, Value: l.Value, Mask: mask, Text: l.Text})
	}
	row, err := m.Replay(Row{Present: true, Value: op.Value}, patches)
	if err != nil {
		return Op{}, false, err
	}
	out := op
	out.Value = row.Value
	if op.Mask != nil {
		// A PUT that names its fields replaces only those, so it names the
		// fields the folded patches wrote too.
		out.Mask = slices.Clone(op.Mask)
		for _, p := range patches {
			for _, path := range p.Mask {
				head, _, _ := strings.Cut(path, ".")
				if !slices.Contains(out.Mask, head) {
					out.Mask = append(out.Mask, head)
				}
			}
		}
	}
	return out, true, nil
}

// namesText reports whether path names a collaborative-text document.
func (m *Model) namesText(path string) bool {
	_, err := textpath.ParseRef(m.desc, path)
	return err == nil
}
