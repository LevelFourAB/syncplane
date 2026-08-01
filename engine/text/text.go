// Package text implements a convergent (CRDT) text document with
// Peritext-style marks and stable anchors, designed to live in a `bytes`
// entity field marked (syncplane.v1.field_behavior).text.
//
// Ordering is Fugue (https://arxiv.org/abs/2305.00583), the tree formulation:
// concurrent insertions at the same position never interleave. Replication is
// state-based: every write ships the whole encoded document, and Merge is a
// pure union — idempotent, commutative and associative — which is exactly the
// contract the engine's opaque-field merger seam requires (RowChange always
// ships merged snapshots, never deltas).
//
// The TypeScript client (client/src/text/) implements the identical model with
// a byte-identical canonical encoding; cross-language conformance vectors live
// in testdata/text/vectors.json.
//
// Model:
//   - An item is one Unicode code point with a permanent identity
//     (peer, counter). Counters are Lamport-style: a fresh id is allocated
//     past every counter the document has seen, so causally-later operations
//     order after what they saw.
//   - PEER UNIQUENESS is the precondition that makes those ids identities at
//     all, and this package cannot check it — see the ID doc below.
//   - Deletion sets a monotone tombstone flag; item content is immutable and
//     tombstones keep it, so merge can verify identity and anchors keep
//     working. Document state therefore grows with edit history until a
//     maintenance pass compacts it (compact.go), which drops tombstones nothing
//     can still reference and records a floor that stops replicas holding older
//     state from putting them back.
//   - Marks are operations over anchor ranges whose winner (per key, per
//     character) is the covering op with the highest id. Removal is a mark op
//     with a nil value, never a physical delete.
//
// All indices in this package count Unicode code points over the visible
// (non-deleted) text. The TypeScript client converts UTF-16 string indices at
// its API boundary.
//
// Because items are code points, the API is NOT byte-transparent: the write
// entry points (Insert, SetString) take text, not arbitrary bytes, and reject
// anything that is not valid UTF-8 rather than storing U+FFFD in its place.
// The TypeScript mirror rejects unpaired surrogates for the same reason, that
// being the one way a JS string can fail to encode.
package text

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

// ID is the permanent identity of an item or mark operation.
//
// Every caller that edits a document owes this package one invariant, which it
// has no way to verify: TWO DOCUMENTS THAT CAN DIVERGE MUST NEVER SHARE A PEER.
// Counters come from the document's own version vector, so two editable copies
// behind one peer allocate the same id for different work. Merge rejects that
// when the content or the tree position disagrees, permanently breaking the
// field for whichever writer holds the losing state; when they happen to agree
// it converges on a text neither writer typed.
//
// Reuse of a peer over TIME is fine, and is how writers avoid growing a
// document's peer table without bound: allocating past everything the document
// has seen is safe as long as every state that peer writes from already
// includes its own earlier writes. Concretely, a writer may keep one peer
// across edits when it re-reads current state before each edit and its edits
// are serialized (see the client's leases in client/src/text/peer.ts and the
// connector's per-instance peer); one that diffs against a stale ancestor, or
// runs concurrently with itself, needs a fresh peer per edit.
type ID struct {
	// Peer identifies the writer: a leased sync client identity, or a
	// server-side writer's own name. It is stored once per document in a peer
	// table, so peers are cheap but not free.
	Peer string
	// Counter is unique per peer within a document, allocated Lamport-style
	// (past every counter seen at allocation time). Never zero.
	Counter uint64
}

// less orders ids by (counter, peer) — the total order used for sibling
// ordering and mark-op wins. Counter-first makes causally-later operations
// (which allocated past everything they saw) order after them.
func (a ID) less(b ID) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Peer < b.Peer
}

// Expand controls whether text typed at a mark's boundary joins the mark.
type Expand uint8

const (
	// ExpandNone: boundary inserts on either side stay outside the mark.
	ExpandNone Expand = 0
	// ExpandBefore: text typed immediately before the range joins it.
	ExpandBefore Expand = 1
	// ExpandAfter: text typed immediately after the range joins it.
	ExpandAfter Expand = 2
	// ExpandBoth: text typed at either boundary joins the range.
	ExpandBoth Expand = 3
)

const sideL, sideR = 0, 1

// node is one item in the Fugue tree. The root is a sentinel node with a zero
// ID and no content.
type node struct {
	id      ID
	r       rune
	deleted bool
	parent  *node // nil only on the root sentinel
	side    int   // sideL or sideR relative to parent
	left    []*node
	right   []*node // children, each sorted ascending by id
}

// Doc is a mutable text document. It is not safe for concurrent use.
type Doc struct {
	peer  string // local writer identity; "" = read-only
	root  *node
	byID  map[ID]*node
	vv    map[string]uint64 // version vector: max counter per peer
	marks []*markOp

	order      []*node // cached in-order traversal (tombstones included)
	orderDirty bool
}

// markOp is one mark operation (apply or, with a nil value, remove).
type markOp struct {
	id     ID
	key    string
	value  any // nil | bool | string | float64
	start  Anchor
	end    Anchor
	expand Expand
}

// New returns an empty document owned by the given peer.
func New(peer string) *Doc {
	root := &node{}
	return &Doc{
		peer: peer,
		root: root,
		byID: make(map[ID]*node),
		vv:   make(map[string]uint64),
	}
}

// Peer returns the local writer identity ("" for a read-only decode).
func (d *Doc) Peer() string { return d.peer }

// SetPeer sets the writer identity used for subsequent edits.
func (d *Doc) SetPeer(peer string) { d.peer = peer }

// VersionVector returns a copy of the document's version vector: the highest
// counter seen from each peer.
func (d *Doc) VersionVector() map[string]uint64 {
	out := make(map[string]uint64, len(d.vv))
	for p, c := range d.vv {
		out[p] = c
	}
	return out
}

// ObserveVersion raises d's version vector to cover vv WITHOUT importing any
// items, so subsequent edits allocate counters past everything vv describes.
//
// It exists for the three-way writer: one that diffs against a stale ancestor
// document but will merge the result into a newer state. Editing the ancestor
// alone would reuse counters the newer state already spent, which is exactly
// the id collision the ID doc warns about — observing the newer version first
// is what lets such a writer keep ONE peer instead of minting a fresh one per
// edit. The observed peers appear in the encoded peer table; that costs
// nothing in practice, because the state they came from is merged in right
// after and carries them anyway.
func (d *Doc) ObserveVersion(vv map[string]uint64) {
	for p, c := range vv {
		d.bumpVV(p, c)
	}
}

// ensureOrder rebuilds the cached in-order traversal if edits invalidated it.
// Traversal is iterative — a document typed backwards is a deep chain of left
// children, and the TS mirror cannot recurse that deep.
func (d *Doc) ensureOrder() {
	if !d.orderDirty && d.order != nil {
		return
	}
	// The traversal visits every item, so size the result for all of them up
	// front: a rebuild runs on every read after an insert, and regrowing a
	// 50k-element slice from nothing costs more than the walk itself.
	if cap(d.order) < len(d.byID) {
		d.order = make([]*node, 0, len(d.byID))
	} else {
		d.order = d.order[:0]
	}
	type frame struct {
		n     *node
		stage int // 0 = walking left children, 1 = walking right children
		idx   int
	}
	// Frames are values, not pointers: one frame per item allocated
	// individually made rebuilds allocation-bound. `f` therefore points into
	// the stack's backing array and is invalidated by the appends below, which
	// is safe only because every mutation through it happens BEFORE the append
	// that pushes a child, and the loop re-takes it on the next iteration.
	stack := make([]frame, 1, 64)
	stack[0] = frame{n: d.root}
	for len(stack) > 0 {
		f := &stack[len(stack)-1]
		if f.stage == 0 {
			if f.idx < len(f.n.left) {
				c := f.n.left[f.idx]
				f.idx++
				stack = append(stack, frame{n: c})
				continue
			}
			if f.n != d.root {
				d.order = append(d.order, f.n)
			}
			f.stage, f.idx = 1, 0
			continue
		}
		if f.idx < len(f.n.right) {
			c := f.n.right[f.idx]
			f.idx++
			stack = append(stack, frame{n: c})
			continue
		}
		stack = stack[:len(stack)-1]
	}
	d.orderDirty = false
}

// Len returns the visible (non-deleted) length in code points.
func (d *Doc) Len() int {
	d.ensureOrder()
	n := 0
	for _, it := range d.order {
		if !it.deleted {
			n++
		}
	}
	return n
}

// PlainText returns the visible text.
func (d *Doc) PlainText() string {
	d.ensureOrder()
	runes := make([]rune, 0, len(d.order))
	for _, it := range d.order {
		if !it.deleted {
			runes = append(runes, it.r)
		}
	}
	return string(runes)
}

// PlainText decodes an encoded document and returns its visible text. It is
// the cheap read path for query evaluation, rendering and connectors.
func PlainText(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	d, err := Decode(data, "")
	if err != nil {
		return "", err
	}
	return d.PlainText(), nil
}

// visibleAt returns the node holding the visible code point at index, or nil.
func (d *Doc) visibleAt(index int) *node {
	d.ensureOrder()
	i := 0
	for _, it := range d.order {
		if it.deleted {
			continue
		}
		if i == index {
			return it
		}
		i++
	}
	return nil
}

// nextCounter allocates the next Lamport counter: one past everything seen.
func (d *Doc) nextCounter() uint64 {
	var max uint64
	for _, c := range d.vv {
		if c > max {
			max = c
		}
	}
	return max + 1
}

func (d *Doc) bumpVV(peer string, counter uint64) {
	if d.vv[peer] < counter {
		d.vv[peer] = counter
	}
}

// addChild inserts c into the side child list of parent, keeping it sorted
// ascending by id.
func addChild(parent *node, c *node) {
	list := &parent.right
	if c.side == sideL {
		list = &parent.left
	}
	i := sort.Search(len(*list), func(i int) bool { return !(*list)[i].id.less(c.id) })
	*list = append(*list, nil)
	copy((*list)[i+1:], (*list)[i:])
	(*list)[i] = c
}

// requireWellFormed rejects text this package cannot store faithfully. Items
// are code points, so writing a string means decoding it, and Go strings are
// arbitrary byte sequences: ranging over one maps an invalid byte to U+FFFD,
// and Encode writes that replacement character back out as if it had been
// typed. Nothing downstream can tell the difference — the document is
// well-formed and passes every check on the way to storage — so a writer
// handing over bytes it believes are text (a latin-1 file, a truncated
// multi-byte read) would get a document that silently differs from its source
// and never compares equal to it again. Callers that genuinely want the lossy
// behavior can ask for it explicitly with strings.ToValidUTF8.
//
// op names the operation for the message; both write entry points share this
// check, because SetString decodes to runes before Insert ever sees the bytes.
func requireWellFormed(op, s string) error {
	if off := InvalidUTF8Offset(s); off >= 0 {
		return fmt.Errorf("text: %s: invalid UTF-8 at byte %d", op, off)
	}
	return nil
}

// InvalidUTF8Offset reports the byte offset of the first invalid UTF-8
// sequence in s, or -1 when s is text the write entry points accept.
//
// It exists for callers that have to decide BEFORE writing: the connector
// checks the strings its source reported so it can skip one item and carry on,
// rather than discovering the problem inside a write and failing the whole
// sweep. Sharing this with requireWellFormed is what makes that precheck agree
// with the refusal by construction.
func InvalidUTF8Offset(s string) int {
	if utf8.ValidString(s) {
		return -1
	}
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			return i
		}
		i += size
	}
	return -1
}

// Insert inserts s at the given visible code-point index (0..Len). s must be
// valid UTF-8; the API is not byte-transparent (see requireWellFormed).
func (d *Doc) Insert(index int, s string) error {
	if d.peer == "" {
		return fmt.Errorf("text: document has no peer; SetPeer before editing")
	}
	if s == "" {
		return nil
	}
	if err := requireWellFormed("insert", s); err != nil {
		return err
	}
	if index < 0 || index > d.Len() {
		return fmt.Errorf("text: insert index %d out of range [0..%d]", index, d.Len())
	}

	// Fugue placement of the first new item: let a be the visible predecessor
	// (root sentinel at index 0) and b its successor in the full,
	// tombstone-inclusive order. If a has no right children the new item
	// becomes a's right child; otherwise b is the leftmost descendant of a's
	// right subtree (it has no left children yet from this replica's view) and
	// the new item becomes b's left child.
	var a *node
	if index == 0 {
		a = d.root
	} else {
		a = d.visibleAt(index - 1)
		if a == nil {
			return fmt.Errorf("text: internal: no node at index %d", index-1)
		}
	}

	counter := d.nextCounter()
	prev := a
	first := true
	for _, r := range s {
		n := &node{id: ID{Peer: d.peer, Counter: counter}, r: r}
		if first {
			if len(prev.right) == 0 {
				n.parent, n.side = prev, sideR
			} else {
				b := d.successor(prev)
				n.parent, n.side = b, sideL
			}
			first = false
		} else {
			// Continuing a typed run: the previous new item has no children.
			n.parent, n.side = prev, sideR
		}
		addChild(n.parent, n)
		d.byID[n.id] = n
		d.bumpVV(d.peer, counter)
		counter++
		prev = n
	}
	d.orderDirty = true
	return nil
}

// successor returns the node immediately after a in the full traversal order
// (the leftmost descendant of a's right subtree when it exists). a must have
// right children or a following node; callers only use it in that case.
func (d *Doc) successor(a *node) *node {
	if len(a.right) > 0 {
		n := a.right[0]
		for len(n.left) > 0 {
			n = n.left[0]
		}
		return n
	}
	// Walk the cached order (only reached when a has no right subtree).
	d.ensureOrder()
	for i, it := range d.order {
		if it == a && i+1 < len(d.order) {
			return d.order[i+1]
		}
	}
	return nil
}

// predecessor returns the node immediately before n in the full traversal
// order, or nil when n is first.
func (d *Doc) predecessor(n *node) *node {
	d.ensureOrder()
	var prev *node
	for _, it := range d.order {
		if it == n {
			return prev
		}
		prev = it
	}
	return nil
}

// Delete tombstones length visible code points starting at index.
func (d *Doc) Delete(index, length int) error {
	if d.peer == "" {
		return fmt.Errorf("text: document has no peer; SetPeer before editing")
	}
	if length == 0 {
		return nil
	}
	if index < 0 || length < 0 || index+length > d.Len() {
		return fmt.Errorf("text: delete range [%d,%d) out of range [0..%d]", index, index+length, d.Len())
	}
	d.ensureOrder()
	i, remaining := 0, length
	for _, it := range d.order {
		if it.deleted {
			continue
		}
		if i >= index {
			it.deleted = true
			remaining--
			if remaining == 0 {
				break
			}
		}
		i++
	}
	// Tombstoning removes nothing from the traversal, but visible indices
	// changed; the cached order itself is still valid.
	return nil
}
