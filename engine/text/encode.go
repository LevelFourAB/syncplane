package text

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"unicode/utf8"
)

// Binary format (version 1). All integers are unsigned LEB128 varints; the
// encoding is canonical — the same document state always produces the same
// bytes, in Go and TypeScript alike — because cross-language conformance
// vectors and merge idempotence compare raw bytes.
//
//	byte    version (0x01)
//	varint  peerCount
//	  per peer, sorted by UTF-8 bytes: varint len, bytes
//	varint  per peer (table order): version-vector counter
//	varint  runCount
//	  per run, sorted by (peerIdx, startCounter):
//	    varint peerIdx, varint startCounter, varint itemCount
//	    byte   flags: bit0 deleted, bit1 parent-is-root, bit2 side (1 = right)
//	    if !parent-is-root: varint parentPeerIdx, varint parentCounter
//	    varint contentLen, UTF-8 bytes of the run's code points
//	varint  markCount
//	  per mark, sorted by (peerIdx, counter):
//	    varint peerIdx, varint counter
//	    varint keyLen, key bytes
//	    value: byte tag 0 nil / 1 false / 2 true / 3 string (varint len,
//	           bytes) / 4 float64 (8 bytes big-endian IEEE 754)
//	    start anchor, end anchor: byte kind (AnchorKind); for
//	           AnchorBefore/AnchorAfter: varint peerIdx, varint counter
//	    byte   expand
//
// Canonical runs are maximal: an item extends the run before it iff it has
// the same peer, the consecutive counter, the previous item as parent on the
// right side, and the same deleted flag — exactly the shape linear typing
// produces.
//
// Two rules keep the set of decodable documents identical in both languages
// (see maxVarint and the tag-4 case in Decode): no varint may exceed 2^53−1,
// and a float mark value must be finite. Both are enforced at decode, not
// only at creation, so what the server stores is exactly what a browser can
// read back.
//
// Format version 2 (leading byte 0x02) is the delta-state FRAGMENT layout —
// the same sections and rules with relaxed reference resolution and the
// source document's full version vector; see fragment.go. Version 3 (0x03) is
// a PLAIN-STRING value: the tag followed by the text's UTF-8 bytes, the form
// a field carries when it says what it should read as without having been
// resolved into a document; see plain.go. Stored rows are always version 1,
// and Decode rejects the other two with distinct errors so neither partial
// state nor an unresolved value can be persisted as a document by accident.
const formatVersion = 1

// maxVarint is the largest value any varint in the format may carry: 2^53−1,
// the largest integer JavaScript represents exactly. Counters, counts and
// lengths all share the ceiling. Without it a document carrying a counter
// above 2^53 would be storable by the engine and undecodable by every
// JavaScript client — a row that bricks for the browser only, which reads
// like a client bug.
const maxVarint = 1<<53 - 1

// checkRunCeiling refuses a run whose items count past maxVarint. Only a run's
// first counter is a varint on the wire, but every later one becomes one when
// the document is encoded again — in the version vector, or as a run head or a
// parent reference — and every decoder refuses it then. A document must not
// decode when its own encoding would not.
func checkRunCeiling(peer string, start, itemCount uint64) error {
	if itemCount-1 > maxVarint-start {
		return fmt.Errorf("text: run %s:%d of %d items passes the counter ceiling", peer, start, itemCount)
	}
	return nil
}

type encoder struct{ buf []byte }

func (e *encoder) byte(b byte)      { e.buf = append(e.buf, b) }
func (e *encoder) uvarint(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *encoder) bytes(b []byte) {
	e.uvarint(uint64(len(b)))
	e.buf = append(e.buf, b...)
}
func (e *encoder) str(s string) { e.bytes([]byte(s)) }

// Encode serializes the document canonically. An empty document (no items, no
// marks, no version vector) still encodes to a small non-empty header;
// callers that want "absent" semantics should special-case it themselves.
func (d *Doc) Encode() []byte {
	// Peer table: every peer in the version vector, sorted. All item and mark
	// ids bump the vv, so the vv key set covers every referenced peer.
	peers := make([]string, 0, len(d.vv))
	for p := range d.vv {
		peers = append(peers, p)
	}
	sort.Strings(peers)
	peerIdx := make(map[string]uint64, len(peers))
	for i, p := range peers {
		peerIdx[p] = uint64(i)
	}

	e := &encoder{buf: make([]byte, 0, 64+8*len(d.byID))}
	e.byte(formatVersion)
	e.uvarint(uint64(len(peers)))
	for _, p := range peers {
		e.str(p)
	}
	for _, p := range peers {
		e.uvarint(d.vv[p])
	}

	// Group items into canonical runs: per peer in table order, per counter
	// ascending, splitting where the run invariant breaks.
	// Grouped from the in-order traversal, not from byID: map iteration order
	// is randomized, which handed the sort below a fully shuffled slice every
	// time. Document order is very nearly counter order for text that was
	// typed, so the same sort then hits its nearly-sorted path. The traversal
	// covers every item (Decode verifies exactly that), so the grouping is the
	// same either way.
	d.ensureOrder()
	nodesByPeer := make(map[string][]*node, len(peers))
	for _, n := range d.order {
		nodesByPeer[n.id.Peer] = append(nodesByPeer[n.id.Peer], n)
	}
	type run struct {
		peerIdx uint64
		start   uint64
		items   []*node
	}
	var runs []run
	for _, p := range peers {
		nodes := nodesByPeer[p]
		// slices.SortFunc, not sort.Slice: this sorts every item in the
		// document on every write, and sort.Slice's reflect-based swapping
		// dominated the encode. The order is the same.
		slices.SortFunc(nodes, func(a, b *node) int { return cmp.Compare(a.id.Counter, b.id.Counter) })
		for i := 0; i < len(nodes); i++ {
			n := nodes[i]
			if i > 0 {
				prev := nodes[i-1]
				last := &runs[len(runs)-1]
				if n.id.Counter == prev.id.Counter+1 &&
					n.parent == prev && n.side == sideR &&
					n.deleted == prev.deleted {
					last.items = append(last.items, n)
					continue
				}
			}
			runs = append(runs, run{peerIdx: peerIdx[p], start: n.id.Counter, items: []*node{n}})
		}
	}

	e.uvarint(uint64(len(runs)))
	for _, r := range runs {
		e.uvarint(r.peerIdx)
		e.uvarint(r.start)
		e.uvarint(uint64(len(r.items)))
		head := r.items[0]
		var flags byte
		if head.deleted {
			flags |= 1
		}
		if head.parent == d.root {
			flags |= 2
		}
		if head.side == sideR {
			flags |= 4
		}
		e.byte(flags)
		if flags&2 == 0 {
			e.uvarint(peerIdx[head.parent.id.Peer])
			e.uvarint(head.parent.id.Counter)
		}
		content := make([]byte, 0, len(r.items)*2)
		for _, it := range r.items {
			content = utf8.AppendRune(content, it.r)
		}
		e.bytes(content)
	}

	marks := make([]*markOp, len(d.marks))
	copy(marks, d.marks)
	sort.Slice(marks, func(i, j int) bool {
		a, b := marks[i], marks[j]
		if pa, pb := peerIdx[a.id.Peer], peerIdx[b.id.Peer]; pa != pb {
			return pa < pb
		}
		return a.id.Counter < b.id.Counter
	})
	e.uvarint(uint64(len(marks)))
	for _, m := range marks {
		e.uvarint(peerIdx[m.id.Peer])
		e.uvarint(m.id.Counter)
		e.str(m.key)
		switch v := m.value.(type) {
		case nil:
			e.byte(0)
		case bool:
			if v {
				e.byte(2)
			} else {
				e.byte(1)
			}
		case string:
			e.byte(3)
			e.str(v)
		case float64:
			e.byte(4)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
			e.buf = append(e.buf, b[:]...)
		}
		for _, a := range []Anchor{m.start, m.end} {
			e.byte(byte(a.Kind))
			if a.Kind == AnchorBefore || a.Kind == AnchorAfter {
				e.uvarint(peerIdx[a.Item.Peer])
				e.uvarint(a.Item.Counter)
			}
		}
		e.byte(byte(m.expand))
	}
	return e.buf
}

type decoder struct {
	buf []byte
	off int
}

func (r *decoder) byte() (byte, error) {
	if r.off >= len(r.buf) {
		return 0, fmt.Errorf("text: truncated document")
	}
	b := r.buf[r.off]
	r.off++
	return b, nil
}

// uvarint reads one LEB128 varint, rejecting anything above maxVarint. It is
// hand-rolled rather than binary.Uvarint so it fails in exactly the same place
// and with exactly the same message as the TypeScript reader, which
// accumulates in a float64 and cannot represent a larger value at all.
func (r *decoder) uvarint() (uint64, error) {
	var v uint64
	var shift uint
	// Overflow is reported at the terminating byte rather than the byte that
	// first exceeds the ceiling: TS only notices once it has the whole value,
	// and an input that runs past ten bytes must be "malformed" on both sides.
	overflow := false
	for i := 0; i < 10; i++ {
		b, err := r.byte()
		if err != nil {
			return 0, err
		}
		chunk := uint64(b & 0x7f)
		if chunk != 0 && (shift >= 53 || chunk > maxVarint>>shift) {
			overflow = true
		} else {
			v |= chunk << shift
		}
		if b&0x80 == 0 {
			if overflow {
				return 0, fmt.Errorf("text: varint overflows safe integer")
			}
			return v, nil
		}
		shift += 7
	}
	return 0, fmt.Errorf("text: malformed varint")
}

func (r *decoder) bytes() ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if uint64(len(r.buf)-r.off) < n {
		return nil, fmt.Errorf("text: truncated document")
	}
	b := r.buf[r.off : r.off+int(n)]
	r.off += int(n)
	return b, nil
}

// str reads a length-prefixed string. Go strings hold arbitrary bytes, but the
// TypeScript reader decodes with a fatal UTF-8 decoder and cannot accept
// anything else, so a peer name or mark key that is not valid UTF-8 is
// rejected here too rather than becoming a row only the engine can read.
func (r *decoder) str() (string, error) {
	b, err := r.bytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("text: invalid UTF-8 string")
	}
	return string(b), nil
}

// count reads an element count and rejects it when the bytes left in the
// buffer cannot possibly hold that many elements, given the smallest legal
// encoding of one (minBytes). Every count in the format is immediately
// followed by the elements it describes, so a larger count is always
// malformed and would fail on truncation anyway — checking it up front is what
// stops a five-byte body from sizing a multi-megabyte container first. Decode
// runs on client-supplied bytes (a PATCH to a text field merges into the
// stored document), so allocation must stay proportional to input length.
func (r *decoder) count(what string, minBytes int) (uint64, error) {
	n, err := r.uvarint()
	if err != nil {
		return 0, err
	}
	if remaining := len(r.buf) - r.off; n > uint64(remaining/minBytes) {
		return 0, fmt.Errorf("text: %s %d exceeds the %d bytes remaining", what, n, remaining)
	}
	return n, nil
}

// MaxCounter reports the highest counter in an encoded document's version
// vector, reading only the header. For bytes Encode produced that is the
// highest counter the document holds or has seen; for other bytes it is the
// highest the header declares, which Decode raises to the items' counters.
// Empty input reports 0, as Decode reads it as an empty document. It fails for
// anything but a format-version-1 document, and for a malformed header.
func MaxCounter(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	r := &decoder{buf: data}
	v, err := r.byte()
	if err != nil {
		return 0, err
	}
	if v != formatVersion {
		return 0, fmt.Errorf("text: unsupported format version %d", v)
	}
	peerCount, err := r.count("peer count", 2)
	if err != nil {
		return 0, err
	}
	for i := uint64(0); i < peerCount; i++ {
		if _, err := r.str(); err != nil {
			return 0, err
		}
	}
	var max uint64
	for i := uint64(0); i < peerCount; i++ {
		c, err := r.uvarint()
		if err != nil {
			return 0, err
		}
		if c > max {
			max = c
		}
	}
	return max, nil
}

// Decode parses an encoded document. peer becomes the writer identity for
// subsequent edits (pass "" for read-only use). Empty input decodes to an
// empty document — an absent bytes field is a document nobody wrote yet.
func Decode(data []byte, peer string) (*Doc, error) {
	d := New(peer)
	if len(data) == 0 {
		return d, nil
	}
	r := &decoder{buf: data}
	v, err := r.byte()
	if err != nil {
		return nil, err
	}
	if v == fragmentVersion {
		// Fragments (format version 2, see fragment.go) are replication
		// payloads only; a caller decoding one as a document is about to
		// persist partial state as a row, which must fail loudly instead.
		return nil, fmt.Errorf("text: fragment where full document expected")
	}
	if v == plainStringVersion {
		// A plain-string value (see plain.go) names text without saying who
		// wrote it, and building a document from it would have to mint items
		// under some peer — the choice this package refuses to make for a
		// caller. PlainText reads it; a resolver applies it.
		return nil, unresolved{fmt.Errorf(
			"text: plain-string value where full document expected; resolve it under a writer identity first")}
	}
	if v != formatVersion {
		return nil, fmt.Errorf("text: unsupported format version %d", v)
	}

	// A peer costs at least two bytes: a zero-length name plus its version
	// vector counter.
	peerCount, err := r.count("peer count", 2)
	if err != nil {
		return nil, err
	}
	peers := make([]string, peerCount)
	for i := range peers {
		if peers[i], err = r.str(); err != nil {
			return nil, err
		}
	}
	for i := range peers {
		c, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		d.vv[peers[i]] = c
	}
	peerAt := func(idx uint64) (string, error) {
		if idx >= peerCount {
			return "", fmt.Errorf("text: peer index %d out of range", idx)
		}
		return peers[idx], nil
	}

	// Pass 1: create all nodes; record each run head's parent reference.
	type pendingAttach struct {
		n      *node
		parent ID
		root   bool
	}
	// A run costs at least six bytes: peerIdx, startCounter, itemCount, flags,
	// contentLen, and — since itemCount must be non-zero — a byte of content.
	runCount, err := r.count("run count", 6)
	if err != nil {
		return nil, err
	}
	attaches := make([]pendingAttach, 0, runCount)
	for i := uint64(0); i < runCount; i++ {
		pIdx, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		p, err := peerAt(pIdx)
		if err != nil {
			return nil, err
		}
		start, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if start == 0 {
			// Counter 0 belongs to the root sentinel. An item holding it would
			// share the root's id, and bumpVV never records a peer whose
			// highest counter is 0, so Encode would leave the item, and every
			// item parented on it, out of the peer table. Only a run head can
			// hold it: later items count up from the head.
			return nil, fmt.Errorf("text: item %s:0 uses reserved counter 0", p)
		}
		itemCount, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if itemCount == 0 {
			return nil, fmt.Errorf("text: empty run")
		}
		if err := checkRunCeiling(p, start, itemCount); err != nil {
			return nil, err
		}
		flags, err := r.byte()
		if err != nil {
			return nil, err
		}
		deleted := flags&1 != 0
		rootParent := flags&2 != 0
		side := sideL
		if flags&4 != 0 {
			side = sideR
		}
		var parentID ID
		if !rootParent {
			ppIdx, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			pp, err := peerAt(ppIdx)
			if err != nil {
				return nil, err
			}
			pc, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			parentID = ID{Peer: pp, Counter: pc}
		}
		content, err := r.bytes()
		if err != nil {
			return nil, err
		}
		// Two slabs per run instead of two allocations per item. itemCount is
		// untrusted, so both are sized by the content actually present (a code
		// point is at least one byte) rather than by the claimed count; an
		// itemCount above that is still caught by the length check below, with
		// the same error it always produced.
		slabSize := itemCount
		if lim := uint64(len(content)); slabSize > lim {
			slabSize = lim
		}
		nodes := make([]node, slabSize)
		// Within a run every item but the last is its predecessor's sole right
		// child, so hand each one a one-element slice of a shared array with no
		// spare capacity. A later attach (a run head landing here) appends and
		// reallocates as usual — this only removes the allocation in the case
		// linear typing produces, which is nearly all of them.
		kids := make([]*node, slabSize)

		var prev *node
		var seen uint64
		for len(content) > 0 {
			ru, size := utf8.DecodeRune(content)
			if ru == utf8.RuneError && size <= 1 {
				return nil, fmt.Errorf("text: invalid UTF-8 in run content")
			}
			content = content[size:]
			if seen >= itemCount {
				return nil, fmt.Errorf("text: run content longer than item count")
			}
			n := &nodes[seen]
			n.id, n.r, n.deleted = ID{Peer: p, Counter: start + seen}, ru, deleted
			if _, dup := d.byID[n.id]; dup {
				return nil, fmt.Errorf("text: duplicate item %s:%d", n.id.Peer, n.id.Counter)
			}
			d.byID[n.id] = n
			d.bumpVV(p, n.id.Counter)
			if prev == nil {
				n.side = side
				attaches = append(attaches, pendingAttach{n: n, parent: parentID, root: rootParent})
			} else {
				// Attached here rather than deferred: the parent is the item
				// just decoded, so nothing needs resolving. Only run HEADS
				// reference an item that may not exist yet.
				n.parent, n.side = prev, sideR
				kids[seen-1] = n
				prev.right = kids[seen-1 : seen : seen]
			}
			prev = n
			seen++
		}
		if seen != itemCount {
			return nil, fmt.Errorf("text: run content shorter than item count")
		}
	}
	// Only run HEADS are still detached: every other item was attached to its
	// run predecessor as it was decoded. addChild inserts by id, so the child
	// lists come out the same whatever order this runs in — and resolving a
	// head whose parent is another head works regardless of which is attached
	// first, since attaching reads the parent's child list and not its parent
	// pointer.
	for _, a := range attaches {
		if a.root {
			a.n.parent = d.root
		} else {
			p, ok := d.byID[a.parent]
			if !ok {
				return nil, fmt.Errorf("text: item %s:%d references unknown parent %s:%d",
					a.n.id.Peer, a.n.id.Counter, a.parent.Peer, a.parent.Counter)
			}
			a.n.parent = p
		}
		addChild(a.n.parent, a.n)
	}
	d.orderDirty = true
	d.ensureOrder()
	if len(d.order) != len(d.byID) {
		return nil, fmt.Errorf("text: %d of %d items unreachable from the root (parent cycle)",
			len(d.byID)-len(d.order), len(d.byID))
	}

	// A mark costs at least seven bytes: peerIdx, counter, keyLen, the value
	// tag, both anchor kinds, and expand.
	markCount, err := r.count("mark count", 7)
	if err != nil {
		return nil, err
	}
	decodeAnchor := func() (Anchor, error) {
		k, err := r.byte()
		if err != nil {
			return Anchor{}, err
		}
		kind := AnchorKind(k)
		switch kind {
		case AnchorDocStart, AnchorDocEnd:
			return Anchor{Kind: kind}, nil
		case AnchorBefore, AnchorAfter:
			pIdx, err := r.uvarint()
			if err != nil {
				return Anchor{}, err
			}
			p, err := peerAt(pIdx)
			if err != nil {
				return Anchor{}, err
			}
			c, err := r.uvarint()
			if err != nil {
				return Anchor{}, err
			}
			id := ID{Peer: p, Counter: c}
			if _, ok := d.byID[id]; !ok {
				return Anchor{}, fmt.Errorf("text: anchor references unknown item %s:%d", p, c)
			}
			return Anchor{Kind: kind, Item: id}, nil
		default:
			return Anchor{}, fmt.Errorf("text: invalid anchor kind %d", k)
		}
	}
	seenMarks := make(map[ID]bool, markCount)
	for i := uint64(0); i < markCount; i++ {
		pIdx, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		p, err := peerAt(pIdx)
		if err != nil {
			return nil, err
		}
		c, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			// Reserved for the same reason as an item's counter 0 above.
			return nil, fmt.Errorf("text: mark op %s:0 uses reserved counter 0", p)
		}
		id := ID{Peer: p, Counter: c}
		if seenMarks[id] {
			return nil, fmt.Errorf("text: duplicate mark op %s:%d", p, c)
		}
		seenMarks[id] = true
		key, err := r.str()
		if err != nil {
			return nil, err
		}
		tag, err := r.byte()
		if err != nil {
			return nil, err
		}
		var value any
		switch tag {
		case 0:
			value = nil
		case 1:
			value = false
		case 2:
			value = true
		case 3:
			s, err := r.str()
			if err != nil {
				return nil, err
			}
			value = s
		case 4:
			if len(r.buf)-r.off < 8 {
				return nil, fmt.Errorf("text: truncated document")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(r.buf[r.off:]))
			r.off += 8
			// validateValue refuses these at creation; refusing them here too
			// keeps the rule at both ends of the pipe. A NaN would otherwise
			// compare unequal to itself and break Marks into one span per
			// character.
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, fmt.Errorf("text: non-finite mark value")
			}
			value = f
		default:
			return nil, fmt.Errorf("text: invalid mark value tag %d", tag)
		}
		start, err := decodeAnchor()
		if err != nil {
			return nil, err
		}
		end, err := decodeAnchor()
		if err != nil {
			return nil, err
		}
		ex, err := r.byte()
		if err != nil {
			return nil, err
		}
		if ex > byte(ExpandBoth) {
			return nil, fmt.Errorf("text: invalid expand %d", ex)
		}
		d.marks = append(d.marks, &markOp{
			id: id, key: key, value: value, start: start, end: end, expand: Expand(ex),
		})
		d.bumpVV(p, c)
	}
	if r.off != len(r.buf) {
		return nil, fmt.Errorf("text: %d trailing bytes", len(r.buf)-r.off)
	}
	return d, nil
}
