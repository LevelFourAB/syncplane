package effects

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/internal/lww"
)

// Watch declares the fields whose before-state a handler needs, as field paths
// ("status", "settings.color") with the same meaning masks have everywhere else
// in the engine — including keyed collection items ("tags.red") and oneof groups
// named by the group.
//
// Declaring a field is what makes Event.Before and Event.Changed answer for it,
// and it is not free: the consumer keeps a shadow row per row it has seen,
// holding those fields' values, written on every delivery. Declaring nothing (the
// default) keeps no shadows at all and costs nothing, which is why after-state
// is the default and transitions are opt-in.
//
// The shadow records what THIS consumer last acted on, not the row's previous
// version. Writes that coalesced into one change are invisible to it, and so is
// anything that happened while the consumer was resetting past the tombstone
// horizon. It answers "has this changed since I last looked", which is the
// question a reaction actually has.
func Watch(fields ...string) Option {
	return func(r *registration) { r.watch = append(r.watch, fields...) }
}

// validateWatch rejects a path the entity does not have, at registration rather
// than on the first change that would have used it. Merging the prototype onto a
// fresh message of its own type exercises exactly the path resolution the
// shadows later run through, so the two cannot disagree.
func validateWatch(prototype proto.Message, paths []string) error {
	dst := prototype.ProtoReflect().New().Interface()
	if err := lww.Merge(dst, prototype, paths); err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	return nil
}

// project returns a new message holding only the watched fields of src. A nil
// src projects to an empty message, which is what a deleted row's "after" is:
// every watched field it held now reads as unset.
func project(prototype, src proto.Message, paths []string) (proto.Message, error) {
	dst := prototype.ProtoReflect().New().Interface()
	if src == nil {
		return dst, nil
	}
	if err := lww.Merge(dst, src, paths); err != nil {
		return nil, fmt.Errorf("effects: project watched fields: %w", err)
	}
	return dst, nil
}

// changedFields names the watched paths whose value differs between two
// projections. It compares path by path through the same merge machinery that
// built them, so a keyed item or a oneof group is compared as the one unit a
// mask treats it as, rather than by whole-message equality that could not say
// WHICH field moved.
func changedFields(before, after proto.Message, paths []string) ([]string, error) {
	var changed []string
	for _, path := range paths {
		b, err := project(before, before, []string{path})
		if err != nil {
			return nil, err
		}
		a, err := project(after, after, []string{path})
		if err != nil {
			return nil, err
		}
		if !proto.Equal(b, a) {
			changed = append(changed, path)
		}
	}
	return changed, nil
}
