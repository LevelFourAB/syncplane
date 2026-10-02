package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/internal/protoopts"
)

// RegisterResult reports the outcome of ingesting a FileDescriptorSet.
type RegisterResult struct {
	// Entities are the wire names registered (or already registered) by this
	// descriptor set, sorted.
	Entities []string
	// Revision is the stable schema revision derived from the canonical
	// descriptor-set bytes — the value clients see as SchemaRevision and the
	// Manager persists for idempotent re-registration.
	Revision string
	// Changed reports whether the catalog actually moved (false on an identical
	// re-registration).
	Changed bool
}

// RegisterDescriptorSet ingests a serialized google.protobuf.FileDescriptorSet
// — the wire form client/src/schema/builder.ts's defineSchema(...).toBinary()
// produces — and registers every message in it that carries (syncplane.v1.entity)
// options as a syncable entity, serving full CRUD through the same machinery
// as a compiled RegisterEntity. Messages without the option are skipped
// (mirroring the TS entitiesFromDescriptorSet). It is the Go counterpart of
// the client's "the server ships its schema" round-trip.
//
// The set must be self-contained: the TS builder bundles
// google/protobuf/descriptor.proto and syncplane/v1/options.proto ahead of the
// synthesized entity files, so the whole schema resolves from these bytes
// with no codegen.
//
// Re-registration is idempotent and additive (see registerDescriptorSet):
// an identical set is a no-op, appended fields/new entities upsert, and a
// conflicting change (a removed, renumbered or retyped field, or changed
// key_fields) is rejected without partially applying.
//
// A change to the access model of an entity that is already registered — its
// access, grant or access_parent options, or the enum values its grant role
// field matches — is rejected the same way unless opts include
// AllowAccessChange.
func (s *Server) RegisterDescriptorSet(set []byte, opts ...RegisterOption) ([]string, error) {
	r, err := s.registerDescriptorSet(set, opts...)
	if err != nil {
		return nil, err
	}
	if r.Changed {
		s.log().LogAttrs(context.Background(), slog.LevelInfo, "descriptor set registered",
			slog.String("component", "schema"),
			slog.Int("entities", len(r.Entities)),
			slog.String("revision", r.Revision))
	}
	return r.Entities, nil
}

// RegisterOption adjusts one call to RegisterDescriptorSet or
// Manager.Register.
type RegisterOption func(*registerConfig)

type registerConfig struct {
	allowAccessChange bool
}

// AllowAccessChange lets a registration change the access model of an entity
// that is already registered: its access, grant or access_parent options, or
// the names of the enum values its grant role field matches. Every client
// resyncs the rows the change affects.
func AllowAccessChange() RegisterOption {
	return func(c *registerConfig) { c.allowAccessChange = true }
}

// registerDescriptorSet is the richer internal form RegisterDescriptorSet and
// the Manager share: it also reports the derived revision and whether the
// catalog changed.
func (s *Server) registerDescriptorSet(set []byte, opts ...RegisterOption) (RegisterResult, error) {
	var cfg registerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(set, &fds); err != nil {
		return RegisterResult{}, fmt.Errorf("server: decode FileDescriptorSet: %w", err)
	}
	revision, err := descriptorSetRevision(&fds)
	if err != nil {
		return RegisterResult{}, err
	}

	// Build the bundled files into their own registry. The set is
	// self-contained, so NewFiles resolves descriptor.proto / options.proto and
	// the entity files' imports among the set itself, never touching the global
	// registry (two apps may both define sync.dynamic.Todo without colliding).
	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("server: build descriptor files: %w", err)
	}

	// Collect the entity message types in a stable order.
	var infos []*entityInfo
	var rangeErr error
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			md := msgs.Get(i)
			if protoopts.Entity(md).GetName() == "" {
				continue // not a syncable entity; skip
			}
			info, err := buildEntityInfo(dynamicpb.NewMessageType(md))
			if err != nil {
				rangeErr = err
				return false
			}
			infos = append(infos, info)
		}
		return true
	})
	if rangeErr != nil {
		return RegisterResult{}, rangeErr
	}
	if len(infos) == 0 {
		return RegisterResult{}, fmt.Errorf("server: descriptor set declares no (syncplane.v1.entity) messages")
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].name < infos[j].name })

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.name
	}

	// Idempotent fast path: an identical descriptor set (same canonical bytes)
	// produces the same revision; if the live catalog already advertises it,
	// nothing changed.
	if s.catalogPtr().schemaRev == revision {
		return RegisterResult{Entities: names, Revision: revision, Changed: false}, nil
	}

	err = s.swapCatalog(func(c *catalog) error {
		// Validate every entity that already exists against its incoming
		// descriptor before mutating, so a conflict rejects the whole set.
		for _, info := range infos {
			if cur, ok := c.entities[info.name]; ok {
				if err := validateEvolution(cur.msgType.Descriptor(), info.msgType.Descriptor()); err != nil {
					return err
				}
				if err := validateAccessEvolution(cur, info, cfg.allowAccessChange); err != nil {
					return err
				}
			}
		}
		for _, info := range infos {
			c.entities[info.name] = info
			c.byType[info.msgType.Descriptor().FullName()] = info
		}
		c.schemaRev = revision
		if err := ensureHistoryEntity(c); err != nil {
			return err
		}
		// A descriptor set is a whole schema, so unlike RegisterEntity it is
		// judged here and refused when it breaks a rule of the access model.
		c.recompileAccess()
		return c.accessErr
	})
	if err != nil {
		return RegisterResult{}, err
	}
	s.logAccessWarnings(s.catalogPtr())
	return RegisterResult{Entities: names, Revision: revision, Changed: true}, nil
}

// descriptorSetRevision derives a stable schema revision from a descriptor
// set: a hex SHA-256 over its deterministic marshaling, so two semantically
// identical sets that differ only in field/encoding order hash alike.
func descriptorSetRevision(fds *descriptorpb.FileDescriptorSet) (string, error) {
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(fds)
	if err != nil {
		return "", fmt.Errorf("server: canonicalize descriptor set: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// validateEvolution enforces the append-only evolution invariant when an
// already-registered entity is re-registered: key_fields must be unchanged and
// every previously-assigned field number must still carry a field of the same
// name, kind and cardinality. New fields with new numbers are additive and
// allowed. A violation is a conflict — the caller rejects the whole set.
func validateEvolution(old, next protoreflect.MessageDescriptor) error {
	name := entityNameOf(old)
	if !equalStrings(keyFieldNames(old), keyFieldNames(next)) {
		return fmt.Errorf("server: entity %q key_fields changed (%v -> %v); not an additive evolution",
			name, keyFieldNames(old), keyFieldNames(next))
	}
	nextFields := next.Fields()
	oldFields := old.Fields()
	for i := 0; i < oldFields.Len(); i++ {
		of := oldFields.Get(i)
		nf := nextFields.ByNumber(of.Number())
		if nf == nil {
			return fmt.Errorf("server: entity %q field %q (number %d) was removed; not an additive evolution",
				name, of.Name(), of.Number())
		}
		if of.Name() != nf.Name() {
			return fmt.Errorf("server: entity %q field number %d renamed %q -> %q; not an additive evolution",
				name, of.Number(), of.Name(), nf.Name())
		}
		if of.Kind() != nf.Kind() {
			return fmt.Errorf("server: entity %q field %q retyped %s -> %s; not an additive evolution",
				name, of.Name(), of.Kind(), nf.Kind())
		}
		// A message-typed field (embedded sub-message, typed reference, keyed
		// list, or map entry) must keep the SAME message type: the Kind check
		// alone passes a ref retargeted to a different entity, a swapped embedded
		// type, or a changed map entry, all of which silently break merge,
		// projection and ref-following.
		if of.Kind() == protoreflect.MessageKind && of.Message().FullName() != nf.Message().FullName() {
			return fmt.Errorf("server: entity %q field %q message type changed %s -> %s; not an additive evolution",
				name, of.Name(), of.Message().FullName(), nf.Message().FullName())
		}
		// The same holds for an enum: another enum type reads the stored
		// numbers as other values, and a grant role field picks its role by the
		// value's name.
		if of.Kind() == protoreflect.EnumKind && of.Enum().FullName() != nf.Enum().FullName() {
			return fmt.Errorf("server: entity %q field %q enum type changed %s -> %s; not an additive evolution",
				name, of.Name(), of.Enum().FullName(), nf.Enum().FullName())
		}
		if of.Cardinality() != nf.Cardinality() {
			return fmt.Errorf("server: entity %q field %q cardinality changed %s -> %s; not an additive evolution",
				name, of.Name(), of.Cardinality(), nf.Cardinality())
		}
	}
	return nil
}

// entityNameOf returns the (syncplane.v1.entity).name of a message descriptor, or
// its proto name if the option is somehow absent.
func entityNameOf(md protoreflect.MessageDescriptor) string {
	if eo := protoopts.Entity(md); eo.GetName() != "" {
		return eo.GetName()
	}
	return string(md.Name())
}

// keyFieldNames returns an entity descriptor's declared key field names in
// order.
func keyFieldNames(md protoreflect.MessageDescriptor) []string {
	return protoopts.Entity(md).GetKeyFields()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
