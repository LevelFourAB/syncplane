package server

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// DynamicEntity declares a syncable entity schema supplied at runtime, with no
// generated Go and no rebuild. Build it into a message type with
// BuildDynamicMessageType (or register it directly with
// Server.RegisterDynamicEntity); the resulting type is a dynamicpb message
// carrying the same (syncplane.v1.entity) options the compiled path reads off a
// generated descriptor, so the descriptor-driven engine treats it identically
// to a RegisterEntity'd type.
//
// This is the CRUD-only "dynamic tier": the schema is declarative, so it can
// be provisioned at runtime, but custom Go mutators/mergers cannot — those are
// funcs and stay on the compiled path.
type DynamicEntity struct {
	// Name is the wire/storage entity name (CrudMutation.entity), e.g. "todo".
	// It must be unique within a server, exactly like a RegisterEntity name.
	Name string
	// Package namespaces the synthesized proto message so distinct dynamic
	// entities get distinct proto full names. Defaults to "sync.dynamic". When
	// several entities are built into one file (BuildDynamicFile) they must
	// share a package.
	Package string
	// MessageName is the synthesized proto message name. Defaults to a
	// CamelCase of Name (e.g. "task_list" -> "TaskList").
	MessageName string
	// KeyFields name the fields (in order) whose values form the row key,
	// mirroring EntityOptions.key_fields. At least one is required, each must
	// name a declared field, and each must be a singular scalar.
	KeyFields []string
	// Fields are the entity's proto fields. Field numbers must be positive and
	// unique; names must be unique.
	Fields []DynamicField
	// Indexes are composite secondary indexes over this entity, mirroring
	// EntityOptions.index. Declare one when a query FILTERS on one field and
	// ORDERS by another — the shape a single DynamicField.Indexed mark can
	// only serve by re-checking every candidate.
	//
	// Names must be unique within the entity and must not start with "__",
	// which is reserved for indexes the engine derives for itself.
	Indexes []DynamicIndex
	// Access mirrors (syncplane.v1.entity).access: the rules that decide who
	// reads and writes the entity's rows in the access model. Nil declares
	// none. The entity keeps the pointer, so do not modify it after
	// registration.
	Access *syncplanev1.AccessOptions
	// Grant mirrors (syncplane.v1.entity).grant and declares the entity's rows
	// as grant rows. Nil declares none. A dynamic entity has no enum fields, so
	// its role field is a string field. The entity keeps the pointer, so do
	// not modify it after registration.
	Grant *syncplanev1.GrantOptions
	// Members mirrors (syncplane.v1.entity).members and declares the entity's
	// rows as teams, whose members are the rows of the entity it names. Nil
	// declares none. The entity keeps the pointer, so do not modify it after
	// registration.
	Members *syncplanev1.MembersOptions
}

// DynamicIndex is one declared secondary index of a dynamic entity, mirroring
// syncplane.v1.IndexSpec.
type DynamicIndex struct {
	// Name is unique within the entity.
	Name string
	// Key lists the index's components in sort order. At least one is required.
	Key []DynamicIndexKey
}

// DynamicIndexKey is one component of a DynamicIndex, mirroring
// syncplane.v1.IndexKey.
type DynamicIndexKey struct {
	// Path is a dotted proto field path, e.g. "settings.size".
	Path string
	// Desc sorts this component descending. Missing and NaN values sort last
	// whatever this says, exactly as in a query ordering.
	Desc bool
}

// DynamicField is one field of a DynamicEntity. Beyond scalars (and repeated
// scalars, opaque bytes) it covers the full compiled modeling power, in
// symmetry with the TS schema builder:
//
//   - embedded sub-messages: set Message to the element's fields (singular, or
//     a keyed message list when Repeated + KeyField are set);
//   - typed references: set Ref to another entity's Name in the same
//     registration (singular, or to-many when Repeated). Detection is
//     annotation-free — a message field whose target is a (syncplane.v1.entity) is a
//     reference;
//   - scalar-valued maps: set MapKey and MapValue.
//
// Exactly one shape applies per field; the zero value (only Kind set) is a
// scalar.
type DynamicField struct {
	// Name is the proto field name (snake_case by convention).
	Name string
	// Number is the proto field number (positive, unique within the entity).
	Number int32
	// Kind is the scalar proto kind (StringKind, Int64Kind, BoolKind, ...) for a
	// scalar field. Ignored for message/ref/map fields.
	Kind protoreflect.Kind
	// Repeated makes the field a list. For a scalar this is a scalar set; for an
	// embedded Message with KeyField it is a keyed message list; for a Ref it is
	// a to-many reference.
	Repeated bool
	// Opaque marks a field as replaced-whole during merges (the same
	// (syncplane.v1.field_behavior).opaque the compiled path honours). Valid on a
	// singular bytes field or a singular embedded Message.
	Opaque bool
	// Indexed maintains a single-component secondary index over this field (the
	// same (syncplane.v1.field_behavior).indexed the compiled path honours), so
	// filtering or ordering by it costs O(result) rather than a scan. Rejected
	// on a text field and on any leaf a comparison could never reach.
	Indexed bool
	// Text marks a field as holding an encoded collaborative text document (the
	// same (syncplane.v1.field_behavior).text the compiled path honours), so
	// registration installs the text merger and concurrent edits merge instead
	// of overwriting. Valid only on a singular bytes field, and mutually
	// exclusive with Opaque.
	Text bool
	// Message declares an embedded (non-entity) sub-message: the field's element
	// type is a nested message synthesized from these fields. Combined with
	// Repeated + KeyField it is a keyed message list.
	Message []DynamicField
	// Ref names another entity (by DynamicEntity.Name) in the same registration;
	// the field becomes a typed reference to it.
	Ref string
	// RefWorkspace stores a Ref as a reference message — `workspace_id` beside a
	// `value` of the target type, the shape (syncplane.v1.reference) declares —
	// so a stored value may name the workspace its target lives in. Ignored
	// without Ref.
	RefWorkspace bool
	// ReverseRelation names the reverse relation a Ref adds to its target
	// entity (the same (syncplane.v1.field_behavior).reverse_relation the
	// compiled path honours). Empty means <plural(entity)>By<Field>, such as
	// postsByAuthor. Valid only with Ref, on a field of the entity itself.
	ReverseRelation string
	// AccessParent places each row below the row this reference points at in
	// the access model (the same (syncplane.v1.field_behavior).access_parent
	// the compiled path honours). Valid only with a singular Ref without
	// RefWorkspace, on a field of the entity itself.
	AccessParent bool
	// Creator makes the field record who created the row: an enforced create
	// stores the caller's primary subject in it, and only a trusted write
	// changes it afterwards (the same (syncplane.v1.field_behavior).creator
	// the compiled path honours). Valid only on a singular string field.
	Creator bool
	// KeyField names the identity field(s) of a keyed message list (composite
	// allowed). OrderField names the element's fractional position field,
	// promoting it to a movable list. Both lower to (syncplane.v1.field_behavior).
	KeyField   []string
	OrderField string
	// MapKey / MapValue, when MapKey is non-zero, declare a scalar-valued map
	// (map<MapKey, MapValue>). Both must be scalar kinds; MapKey additionally
	// must be a valid proto map key kind.
	MapKey   protoreflect.Kind
	MapValue protoreflect.Kind
}

// isMessage reports whether the field declares an embedded sub-message.
func (f DynamicField) isMessage() bool { return len(f.Message) > 0 && f.Ref == "" && f.MapKey == 0 }

// isRef reports whether the field is a typed reference to another entity.
func (f DynamicField) isRef() bool { return f.Ref != "" }

// isMap reports whether the field declares a scalar-valued map.
func (f DynamicField) isMap() bool { return f.MapKey != 0 }

// RegisterDynamicEntity builds spec into a dynamicpb message type and registers
// it as a syncable entity, serving full CRUD through the existing sync
// machinery. It is the runtime-schema counterpart of RegisterEntity, and takes
// the same opts. Use RegisterDynamicEntities when entities reference one
// another, so they share a single file and the references resolve.
func (s *Server) RegisterDynamicEntity(spec DynamicEntity, opts ...RegisterOption) error {
	fd, err := BuildDynamicFile(spec)
	if err != nil {
		return err
	}
	return s.RegisterEntity(dynamicpb.NewMessageType(fd.Messages().Get(0)).New().Interface(), opts...)
}

// RegisterDynamicEntities builds several entities into one file (so intra-set
// references resolve) and registers each as a syncable entity, in order, as
// RegisterEntity without options does. It stops at the first entity that fails
// and keeps the ones registered before it. To declare the first role of a
// schema that serves callers, use RegisterDescriptorSet with
// AllowAccessChange.
func (s *Server) RegisterDynamicEntities(specs ...DynamicEntity) error {
	fd, err := BuildDynamicFile(specs...)
	if err != nil {
		return err
	}
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		mt := dynamicpb.NewMessageType(msgs.Get(i))
		if err := s.RegisterEntity(mt.New().Interface()); err != nil {
			return err
		}
	}
	return nil
}

// BuildDynamicMessageType synthesizes a proto message type from a single
// runtime schema and returns it as a dynamicpb message type. See
// BuildDynamicFile for the multi-entity form that resolves cross-entity
// references; a lone spec may still reference itself.
//
// The message carries (syncplane.v1.entity) options (and per-field
// (syncplane.v1.field_behavior) options) resolved against the process's extension
// registry, so proto.GetExtension on the resulting descriptor returns populated
// options. It does not touch the global registry.
func BuildDynamicMessageType(spec DynamicEntity) (protoreflect.MessageType, error) {
	fd, err := BuildDynamicFile(spec)
	if err != nil {
		return nil, err
	}
	// BuildDynamicFile appends entity messages in spec order, so the lone spec's
	// message is the first (and only) top-level message.
	return dynamicpb.NewMessageType(fd.Messages().Get(0)), nil
}

// BuildDynamicFile synthesizes one proto file holding every entity in specs as
// a top-level message (with its (syncplane.v1.entity) options and nested non-entity
// types) and returns the resolved file descriptor. Building all entities into a
// SINGLE file is what lets references resolve: intra-file type names are always
// legal, so self, forward, and mutual references work regardless of declaration
// order, with no circular file-import graph for protodesc to reject. The only
// dependency is syncplane/v1/options.proto, resolved against the global registry.
func BuildDynamicFile(specs ...DynamicEntity) (protoreflect.FileDescriptor, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("server: BuildDynamicFile needs at least one entity")
	}

	// Resolve the shared package and the entity-name -> message-name map up
	// front: references lower to a type_name that needs the target's message
	// name, and a ref to an unknown entity must fail with a clear error.
	pkg := ""
	msgNameByEntity := make(map[string]string, len(specs))
	seenEntity := make(map[string]bool, len(specs))
	seenMsg := make(map[string]bool, len(specs))
	for _, spec := range specs {
		if spec.Name == "" {
			return nil, fmt.Errorf("server: dynamic entity needs a name")
		}
		if seenEntity[spec.Name] {
			return nil, fmt.Errorf("server: dynamic entity %q declared twice", spec.Name)
		}
		seenEntity[spec.Name] = true
		p := spec.Package
		if p == "" {
			p = "sync.dynamic"
		}
		if pkg == "" {
			pkg = p
		} else if p != pkg {
			return nil, fmt.Errorf("server: dynamic entities must share a package (%q vs %q)", pkg, p)
		}
		mn := spec.MessageName
		if mn == "" {
			mn = camelCase(spec.Name)
		}
		if seenMsg[mn] {
			return nil, fmt.Errorf("server: dynamic entity message name %q is not unique", mn)
		}
		seenMsg[mn] = true
		msgNameByEntity[spec.Name] = mn
	}

	msgTypes := make([]*descriptorpb.DescriptorProto, 0, len(specs))
	for _, spec := range specs {
		dp, err := dynamicEntityProto(spec, pkg, msgNameByEntity)
		if err != nil {
			return nil, err
		}
		msgTypes = append(msgTypes, dp)
	}

	fdProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("sync/dynamic/schema.proto"),
		Package:     proto.String(pkg),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"syncplane/v1/options.proto"},
		MessageType: msgTypes,
	}
	fd, err := protodesc.NewFile(fdProto, protoregistry.GlobalFiles)
	if err != nil {
		return nil, fmt.Errorf("server: building dynamic schema: %w", err)
	}
	return fd, nil
}

// dynamicEntityProto lowers one entity spec to a top-level DescriptorProto with
// its (syncplane.v1.entity) options, validating key fields and field shapes.
func dynamicEntityProto(
	spec DynamicEntity,
	pkg string,
	msgNameByEntity map[string]string,
) (*descriptorpb.DescriptorProto, error) {
	if len(spec.KeyFields) == 0 {
		return nil, fmt.Errorf("server: dynamic entity %q declares no key_fields", spec.Name)
	}
	msgName := msgNameByEntity[spec.Name]
	fullName := pkg + "." + msgName

	dp, err := dynamicMessageProto(msgName, fullName, spec.Name, spec.Fields, pkg, msgNameByEntity)
	if err != nil {
		return nil, err
	}

	mopts := &descriptorpb.MessageOptions{}
	indexes, err := dynamicIndexSpecs(spec)
	if err != nil {
		return nil, err
	}
	proto.SetExtension(mopts, syncplanev1.E_Entity, &syncplanev1.EntityOptions{
		Name:      spec.Name,
		KeyFields: spec.KeyFields,
		Index:     indexes,
		Access:    spec.Access,
		Grant:     spec.Grant,
		Members:   spec.Members,
	})
	dp.Options = mopts

	// Key fields must reference declared singular scalar fields. The kind check
	// mirrors buildEntityInfo so the failure names the entity rather than
	// surfacing later as a useless row key.
	declared := make(map[string]DynamicField, len(spec.Fields))
	for _, f := range spec.Fields {
		declared[f.Name] = f
	}
	for _, kf := range spec.KeyFields {
		f, ok := declared[kf]
		if !ok {
			return nil, fmt.Errorf("server: dynamic entity %q key field %q is not a declared field", spec.Name, kf)
		}
		if f.isMessage() || f.isRef() || f.isMap() || f.Repeated {
			return nil, fmt.Errorf("server: dynamic entity %q key field %q must be a singular scalar", spec.Name, kf)
		}
	}
	return dp, nil
}

// dynamicMessageProto builds a DescriptorProto from a field list, recursively
// synthesizing the nested types (embedded messages, keyed-list elements, map
// entries) the fields reference. ctx names the owning entity for error
// messages; fullName qualifies nested type names.
func dynamicMessageProto(
	msgName, fullName, ctx string,
	fields []DynamicField,
	pkg string,
	msgNameByEntity map[string]string,
) (*descriptorpb.DescriptorProto, error) {
	dp := &descriptorpb.DescriptorProto{Name: proto.String(msgName)}
	seenName := make(map[string]bool, len(fields))
	seenNum := make(map[int32]bool, len(fields))
	for _, f := range fields {
		if f.Name == "" {
			return nil, fmt.Errorf("server: dynamic entity %q has a field with no name", ctx)
		}
		if f.Number <= 0 {
			return nil, fmt.Errorf("server: dynamic entity %q field %q has non-positive number %d", ctx, f.Name, f.Number)
		}
		if seenName[f.Name] {
			return nil, fmt.Errorf("server: dynamic entity %q has duplicate field name %q", ctx, f.Name)
		}
		if seenNum[f.Number] {
			return nil, fmt.Errorf("server: dynamic entity %q has duplicate field number %d", ctx, f.Number)
		}
		seenName[f.Name] = true
		seenNum[f.Number] = true

		fdp, nested, err := dynamicFieldProto(f, fullName, ctx, pkg, msgNameByEntity)
		if err != nil {
			return nil, err
		}
		dp.Field = append(dp.Field, fdp)
		dp.NestedType = append(dp.NestedType, nested...)
	}
	return dp, nil
}

// dynamicFieldProto lowers one DynamicField to its FieldDescriptorProto plus any
// nested types it introduces (an embedded/keyed-list element, or a map entry).
func dynamicFieldProto(
	f DynamicField,
	parentFullName, ctx, pkg string,
	msgNameByEntity map[string]string,
) (*descriptorpb.FieldDescriptorProto, []*descriptorpb.DescriptorProto, error) {
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	if f.Repeated {
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	}
	fdp := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(f.Name),
		Number: proto.Int32(f.Number),
		Label:  label.Enum(),
	}
	if f.Text && (f.Opaque || f.Kind != protoreflect.BytesKind || f.Repeated ||
		f.isMap() || f.isRef() || f.isMessage()) {
		return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: text is only valid on a singular non-opaque bytes field", ctx, f.Name)
	}
	if f.ReverseRelation != "" && !f.isRef() {
		return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: reverse_relation is only valid on a Ref field", ctx, f.Name)
	}
	if f.AccessParent && !f.isRef() {
		return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: access_parent is only valid on a Ref field", ctx, f.Name)
	}
	if f.Creator && (f.Kind != protoreflect.StringKind || f.Repeated || f.isMap() || f.isRef() || f.isMessage()) {
		return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: creator is only valid on a singular string field", ctx, f.Name)
	}

	switch {
	case f.isMap():
		entrySimple := camelCase(f.Name) + "Entry"
		entryFull := parentFullName + "." + entrySimple
		entry, err := mapEntryProto(entrySimple, f, ctx)
		if err != nil {
			return nil, nil, err
		}
		fdp.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fdp.TypeName = proto.String("." + entryFull)
		return fdp, []*descriptorpb.DescriptorProto{entry}, nil

	case f.isRef():
		targetMsg, ok := msgNameByEntity[f.Ref]
		if !ok {
			return nil, nil, fmt.Errorf("server: dynamic entity %q field %q references unknown entity %q", ctx, f.Name, f.Ref)
		}
		fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		if f.ReverseRelation != "" || f.AccessParent {
			b := &syncplanev1.FieldBehavior{ReverseRelation: f.ReverseRelation}
			if f.AccessParent {
				b.AccessParent = &syncplanev1.AccessParentOptions{}
			}
			fopts := &descriptorpb.FieldOptions{}
			proto.SetExtension(fopts, syncplanev1.E_FieldBehavior, b)
			fdp.Options = fopts
		}
		if f.RefWorkspace {
			// A workspace-qualified reference is stored as a reference message
			// synthesized per field, nested beside the map entries and embedded
			// messages of the same parent.
			simple := camelCase(f.Name) + "Ref"
			fdp.TypeName = proto.String("." + parentFullName + "." + simple)
			return fdp, []*descriptorpb.DescriptorProto{referenceMessageProto(simple, "."+pkg+"."+targetMsg)}, nil
		}
		fdp.TypeName = proto.String("." + pkg + "." + targetMsg)
		// Entity-typed reference detection is annotation-free, so the only mark
		// the field itself carries is its reverse relation's name.
		return fdp, nil, nil

	case f.isMessage():
		simple := camelCase(f.Name)
		full := parentFullName + "." + simple
		nested, err := dynamicMessageProto(simple, full, ctx, f.Message, pkg, msgNameByEntity)
		if err != nil {
			return nil, nil, err
		}
		fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fdp.TypeName = proto.String("." + full)
		if f.Opaque && f.Repeated {
			return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: opaque is only valid on a singular message", ctx, f.Name)
		}
		if err := setDynamicBehavior(fdp, f, ctx); err != nil {
			return nil, nil, err
		}
		return fdp, []*descriptorpb.DescriptorProto{nested}, nil

	default: // scalar
		ftype, err := kindToType(f.Kind)
		if err != nil {
			return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: %w", ctx, f.Name, err)
		}
		fdp.Type = ftype.Enum()
		if f.Opaque {
			if f.Kind != protoreflect.BytesKind || f.Repeated {
				return nil, nil, fmt.Errorf("server: dynamic entity %q field %q: opaque is only valid on a singular bytes field", ctx, f.Name)
			}
		}
		if f.Opaque || f.Text || f.Indexed || f.Creator {
			b := &syncplanev1.FieldBehavior{Opaque: f.Opaque, Text: f.Text, Indexed: f.Indexed}
			if f.Creator {
				b.Creator = &syncplanev1.CreatorOptions{}
			}
			fopts := &descriptorpb.FieldOptions{}
			proto.SetExtension(fopts, syncplanev1.E_FieldBehavior, b)
			fdp.Options = fopts
		}
		return fdp, nil, nil
	}
}

// setDynamicBehavior attaches (syncplane.v1.field_behavior) to a message-typed field
// for the keyed-list key_field/order_field identity and the opaque escape
// hatch. A no-op when none apply (a plain embedded sub-message).
func setDynamicBehavior(fdp *descriptorpb.FieldDescriptorProto, f DynamicField, ctx string) error {
	if f.OrderField != "" && len(f.KeyField) == 0 {
		return fmt.Errorf("server: dynamic entity %q field %q: order_field requires a keyed message list", ctx, f.Name)
	}
	if len(f.KeyField) > 0 && !f.Repeated {
		return fmt.Errorf("server: dynamic entity %q field %q: key_field requires a repeated message list", ctx, f.Name)
	}
	if !f.Opaque && !f.Indexed && len(f.KeyField) == 0 && f.OrderField == "" {
		return nil
	}
	behavior := &syncplanev1.FieldBehavior{
		Opaque:     f.Opaque,
		KeyField:   f.KeyField,
		OrderField: f.OrderField,
		Indexed:    f.Indexed,
	}
	fopts := &descriptorpb.FieldOptions{}
	proto.SetExtension(fopts, syncplanev1.E_FieldBehavior, behavior)
	fdp.Options = fopts
	return nil
}

// referenceMessageProto synthesizes the reference message a RefWorkspace field
// stores: `workspace_id` marked (syncplane.v1.field_behavior).workspace beside
// a `value` of the target type, with (syncplane.v1.reference) on the message.
func referenceMessageProto(simpleName, targetTypeName string) *descriptorpb.DescriptorProto {
	wsOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(wsOpts, syncplanev1.E_FieldBehavior, &syncplanev1.FieldBehavior{Workspace: true})
	msgOpts := &descriptorpb.MessageOptions{}
	proto.SetExtension(msgOpts, syncplanev1.E_Reference, &syncplanev1.ReferenceOptions{})
	return &descriptorpb.DescriptorProto{
		Name:    proto.String(simpleName),
		Options: msgOpts,
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:    proto.String("workspace_id"),
				Number:  proto.Int32(1),
				Label:   descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Options: wsOpts,
			},
			{
				Name:     proto.String("value"),
				Number:   proto.Int32(2),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(targetTypeName),
			},
		},
	}
}

// mapEntryProto synthesizes a map's entry message: a map_entry nested type with
// key=1 and value=2. protodesc (and the runtime) recognize exactly this shape
// as a map, so a repeated field referencing it merges per map key.
func mapEntryProto(simpleName string, f DynamicField, ctx string) (*descriptorpb.DescriptorProto, error) {
	keyType, err := kindToType(f.MapKey)
	if err != nil {
		return nil, fmt.Errorf("server: dynamic entity %q field %q map key: %w", ctx, f.Name, err)
	}
	switch f.MapKey {
	case protoreflect.MessageKind, protoreflect.GroupKind, protoreflect.EnumKind,
		protoreflect.BytesKind, protoreflect.FloatKind, protoreflect.DoubleKind:
		return nil, fmt.Errorf("server: dynamic entity %q field %q: %s is not a valid map key", ctx, f.Name, f.MapKey)
	}
	valType, err := kindToType(f.MapValue)
	if err != nil {
		return nil, fmt.Errorf("server: dynamic entity %q field %q map value: %w", ctx, f.Name, err)
	}
	return &descriptorpb.DescriptorProto{
		Name:    proto.String(simpleName),
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("key"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   keyType.Enum(),
			},
			{
				Name:   proto.String("value"),
				Number: proto.Int32(2),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   valType.Enum(),
			},
		},
	}, nil
}

// kindToType maps a scalar protoreflect.Kind to its FieldDescriptorProto type.
// Composite kinds (message/group/enum) are rejected here; message-typed fields
// are synthesized from DynamicField.Message/Ref/Map instead.
func kindToType(k protoreflect.Kind) (descriptorpb.FieldDescriptorProto_Type, error) {
	switch k {
	case protoreflect.BoolKind:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL, nil
	case protoreflect.Int32Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, nil
	case protoreflect.Sint32Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_SINT32, nil
	case protoreflect.Sfixed32Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_SFIXED32, nil
	case protoreflect.Int64Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, nil
	case protoreflect.Sint64Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_SINT64, nil
	case protoreflect.Sfixed64Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_SFIXED64, nil
	case protoreflect.Uint32Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT32, nil
	case protoreflect.Fixed32Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED32, nil
	case protoreflect.Uint64Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64, nil
	case protoreflect.Fixed64Kind:
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED64, nil
	case protoreflect.FloatKind:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT, nil
	case protoreflect.DoubleKind:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, nil
	case protoreflect.StringKind:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING, nil
	case protoreflect.BytesKind:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES, nil
	default:
		return 0, fmt.Errorf("unsupported kind %s (use Message/Ref/Map for composite fields)", k)
	}
}

// camelCase turns a snake_case entity/field name into a proto message name:
// "task_list" -> "TaskList", "todo" -> "Todo".
func camelCase(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	upper := true
	for _, r := range s {
		if r == '_' {
			upper = true
			continue
		}
		if upper {
			b.WriteString(strings.ToUpper(string(r)))
			upper = false
			continue
		}
		b.WriteRune(r)
	}
	if b.Len() == 0 {
		return "Entity"
	}
	return b.String()
}

// dynamicIndexSpecs converts the declared indexes to their proto form,
// validating the shape registration would otherwise reject with a message
// naming a synthesized descriptor rather than the spec the caller wrote.
//
// Whether each PATH is indexable is not checked here: buildEntityInfo resolves
// every path against the finished descriptor and rejects text fields and
// incomparable leaves there, which is the one place that knows the answer for
// both the compiled and the dynamic path.
func dynamicIndexSpecs(spec DynamicEntity) ([]*syncplanev1.IndexSpec, error) {
	if len(spec.Indexes) == 0 {
		return nil, nil
	}
	out := make([]*syncplanev1.IndexSpec, 0, len(spec.Indexes))
	for _, idx := range spec.Indexes {
		if idx.Name == "" {
			return nil, fmt.Errorf("server: dynamic entity %q: an index declares no name", spec.Name)
		}
		if len(idx.Key) == 0 {
			return nil, fmt.Errorf("server: dynamic entity %q index %q: declares no key components", spec.Name, idx.Name)
		}
		keys := make([]*syncplanev1.IndexKey, 0, len(idx.Key))
		for _, k := range idx.Key {
			if k.Path == "" {
				return nil, fmt.Errorf("server: dynamic entity %q index %q: a key component declares no path", spec.Name, idx.Name)
			}
			keys = append(keys, &syncplanev1.IndexKey{Path: k.Path, Desc: k.Desc})
		}
		out = append(out, &syncplanev1.IndexSpec{Name: idx.Name, Key: keys})
	}
	return out, nil
}
