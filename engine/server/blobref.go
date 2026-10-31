package server

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Blob references.
//
// A field whose message type is syncplane.v1.BlobRef refers from a row to a
// blob. The engine finds every such field at registration, by type, wherever
// it sits in the entity, and answers one question about them: which live rows
// name a digest. Blob reads authorize against those rows (see blobservice.go)
// and blob GC keeps what they name (see blobgc.go).
//
// The answer comes from a derived sparse index per BlobRef path when one can
// serve it, and from reading the entity's rows otherwise. As for every other
// index, the two must give the same answer.

// blobRefName is the full name of the message type that marks a blob
// reference. Compared by name rather than by Go type, so a descriptor set
// registered at runtime carries blob references too.
var blobRefName = (&syncplanev1.BlobRef{}).ProtoReflect().Descriptor().FullName()

// blobIndexPrefix names the index the engine derives over one BlobRef path's
// digest. The rest of the name is the path's field names joined by dots.
const blobIndexPrefix = reservedIndexPrefix + "blob_"

// blobRefs is where an entity's rows can name a blob.
type blobRefs struct {
	// indexes are the names, in info.indexes, of the index over each BlobRef
	// path an index component can reach.
	indexes []string
	// unindexed is set when a BlobRef sits where no index component reaches:
	// below a second repeated field, or inside a message that contains
	// itself. A lookup then reads the entity's rows, because an index over
	// the other paths would miss the rows that name the digest there.
	unindexed bool
}

// buildBlobIndexes finds every BlobRef field of the entity and derives a
// sparse index over the digest of each one an index component can express.
// An entity without a BlobRef field is left as it was.
//
// A path the component compiler refuses is not a schema error: the lookup for
// that entity reads rows instead (see blobRefs.unindexed). Two shapes hit
// that, a list inside a list element and a recursive message, and refusing
// them would refuse a schema that worked before blob references existed.
func buildBlobIndexes(info *entityInfo) error {
	desc := info.msgType.Descriptor()
	var paths [][]string
	refs := &blobRefs{}
	collectBlobPaths(desc, nil, []protoreflect.FullName{desc.FullName()}, &paths, &refs.unindexed)
	if len(paths) == 0 && !refs.unindexed {
		return nil
	}
	for _, path := range paths {
		c, err := query.CompileIndexComponent(desc, path, false)
		if err != nil {
			refs.unindexed = true
			continue
		}
		enc, err := query.NewSparseIndexEncoder([]query.IndexComponent{c})
		if err != nil {
			refs.unindexed = true
			continue
		}
		name := blobIndexName(path)
		if info.indexByName(name) != nil {
			return fmt.Errorf("blob reference %q: index %q is already declared", strings.Join(path, "."), name)
		}
		info.indexes = append(info.indexes, compiledIndex{name: name, specHash: enc.SpecHash(), encoder: enc})
		refs.indexes = append(refs.indexes, name)
	}
	info.blobs = refs
	return nil
}

// blobIndexName names the index over the digest at path, an index component
// path that ends in "digest". The map fan segments are left out: a field is a
// map or a message, never both, so the field names alone keep two paths apart.
func blobIndexName(path []string) string {
	names := make([]string, 0, len(path))
	for i, seg := range path[:len(path)-1] {
		if seg == "*" || (seg == "value" && i > 0 && path[i-1] == "*") {
			continue
		}
		names = append(names, seg)
	}
	return blobIndexPrefix + strings.Join(names, ".")
}

// collectBlobPaths appends to out the index component path of the digest of
// every BlobRef below desc, and sets *unbounded when a BlobRef sits inside a
// message that contains itself. chain holds the message types on the descent
// path, so a recursive type ends the walk instead of repeating it.
//
// A reference field is not entered: a row stores only the target's key there,
// so the target's own BlobRef fields are never in the holder's rows.
func collectBlobPaths(
	desc protoreflect.MessageDescriptor,
	prefix []string,
	chain []protoreflect.FullName,
	out *[][]string,
	unbounded *bool,
) {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if lww.IsRef(fd) {
			continue
		}
		var sub protoreflect.MessageDescriptor
		seg := []string{string(fd.Name())}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			sub = fd.MapValue().Message()
			seg = append(seg, "*", "value")
		case fd.Kind() == protoreflect.MessageKind:
			sub = fd.Message()
		default:
			continue
		}
		path := slices.Concat(prefix, seg)
		if sub.FullName() == blobRefName {
			*out = append(*out, slices.Concat(path, []string{"digest"}))
			continue
		}
		if containsName(chain, sub.FullName()) {
			if reachesBlobRef(sub, map[protoreflect.FullName]bool{}) {
				*unbounded = true
			}
			continue
		}
		collectBlobPaths(sub, path, append(chain, sub.FullName()), out, unbounded)
	}
}

// reachesBlobRef reports whether a BlobRef sits anywhere below desc. seen
// holds the types already visited, so a cycle ends the walk.
func reachesBlobRef(desc protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if seen[desc.FullName()] {
		return false
	}
	seen[desc.FullName()] = true
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if lww.IsRef(fd) {
			continue
		}
		var sub protoreflect.MessageDescriptor
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				sub = fd.MapValue().Message()
			}
		case fd.Kind() == protoreflect.MessageKind:
			sub = fd.Message()
		}
		if sub == nil {
			continue
		}
		if sub.FullName() == blobRefName || reachesBlobRef(sub, seen) {
			return true
		}
	}
	return false
}

func containsName(names []protoreflect.FullName, name protoreflect.FullName) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// blobDigestsIn calls fn with the digest of every BlobRef msg holds, at any
// depth, except below a reference field (see collectBlobPaths). A BlobRef
// with an empty digest names nothing. One digest can reach fn more than once.
func blobDigestsIn(msg protoreflect.Message, fn func(blob.Digest)) {
	if msg.Descriptor().FullName() == blobRefName {
		if d := msg.Get(msg.Descriptor().Fields().ByName("digest")).String(); d != "" {
			fn(blob.Digest(d))
		}
		return
	}
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if lww.IsRef(fd) {
			return true
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					blobDigestsIn(mv.Message(), fn)
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					blobDigestsIn(l.Get(i).Message(), fn)
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			blobDigestsIn(v.Message(), fn)
		}
		return true
	})
}

// blobAdmit decides whether a live row that names a wanted digest counts as
// naming it. It may clear fields of msg, the row's decoded value; the digests
// msg names after the call are the ones that count. A nil blobAdmit counts
// every live row as it is stored.
type blobAdmit func(info *entityInfo, row storage.Row, msg proto.Message) (bool, error)

// errBlobsSettled stops a walk once every wanted digest is found. It never
// leaves referencedBlobs.
var errBlobsSettled = errors.New("every wanted digest is found")

// referencedBlobs returns the digests of want that a live row of some
// registered entity names in a BlobRef field, after admit. withServerOnly
// counts the rows of server-only entities too. The answer does not depend on
// whether an index serves it.
func (s *Server) referencedBlobs(tx storage.ReadTx, want []blob.Digest, admit blobAdmit, withServerOnly bool) (map[blob.Digest]bool, error) {
	found := make(map[blob.Digest]bool, len(want))
	remaining := make(map[blob.Digest]bool, len(want))
	for _, d := range want {
		remaining[d] = true
	}
	cat := s.catalogPtr()
	for _, name := range cat.entityNames() {
		info := cat.entities[name]
		if info.blobs == nil || (info.serverOnly && !withServerOnly) {
			continue
		}
		if len(remaining) == 0 {
			break
		}
		consider := func(row storage.Row) error {
			if row.Deleted {
				return nil
			}
			msg, err := info.decode(row.Value)
			if err != nil {
				return err
			}
			if !namesAny(msg, remaining) {
				return nil
			}
			if admit != nil {
				ok, err := admit(info, row, msg)
				if err != nil || !ok {
					return err
				}
			}
			blobDigestsIn(msg.ProtoReflect(), func(d blob.Digest) {
				if remaining[d] {
					delete(remaining, d)
					found[d] = true
				}
			})
			if len(remaining) == 0 {
				return errBlobsSettled
			}
			return nil
		}
		keys, served, err := blobCandidates(tx, info, remaining, s.limits)
		if err != nil {
			return nil, err
		}
		if served {
			batch := &rowBatch{tx: tx, entity: info.name, size: maxRowBatch, emit: consider}
			for _, k := range keys {
				if err = batch.add(k); err != nil {
					break
				}
			}
			if err == nil {
				err = batch.flush()
			}
		} else {
			err = tx.ScanEntity(info.name, consider)
		}
		if errors.Is(err, errBlobsSettled) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return found, nil
}

// namesAny reports whether msg names one of the digests in want.
func namesAny(msg proto.Message, want map[blob.Digest]bool) bool {
	hit := false
	blobDigestsIn(msg.ProtoReflect(), func(d blob.Digest) {
		if want[d] {
			hit = true
		}
	})
	return hit
}

// blobCandidates returns, in key order, the keys of the rows of info whose
// blob indexes hold one of the digests, and whether the indexes served the
// lookup. served is false when indexes are disabled, when a BlobRef of the
// entity has no index, when an index is not ready, or when the indexes name
// more rows than Limits.MaxIndexScanCandidates; the caller then reads every
// row of the entity.
func blobCandidates(tx storage.ReadTx, info *entityInfo, digests map[blob.Digest]bool, limits Limits) (keys []string, served bool, err error) {
	if limits.DisableIndexes || info.blobs.unindexed {
		return nil, false, nil
	}
	indexes := make([]*compiledIndex, 0, len(info.blobs.indexes))
	for _, name := range info.blobs.indexes {
		ci := info.indexByName(name)
		if ci == nil {
			return nil, false, nil
		}
		ready, err := ci.ready(tx, info.name)
		if err != nil || !ready {
			return nil, false, err
		}
		indexes = append(indexes, ci)
	}
	budget := limits.maxIndexScanCandidates()
	seen := map[string]bool{}
	for _, ci := range indexes {
		for d := range digests {
			lo, hi, err := ci.probeRefIndex([]protoreflect.Value{protoreflect.ValueOfString(string(d))})
			if err != nil {
				return nil, false, err
			}
			err = tx.ScanIndex(info.name, ci.name, lo, hi, 0, storage.ScanAsc, func(_ []byte, rowKey string) error {
				if !seen[rowKey] {
					seen[rowKey] = true
					keys = append(keys, rowKey)
				}
				if len(keys) > budget {
					return errIndexBudget
				}
				return nil
			})
			if errors.Is(err, errIndexBudget) {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
		}
	}
	sort.Strings(keys)
	return keys, true, nil
}
