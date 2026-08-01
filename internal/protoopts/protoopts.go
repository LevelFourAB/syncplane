// Package protoopts reads the sync custom options — (syncplane.v1.entity) and
// (syncplane.v1.field_behavior) — off proto descriptors.
//
// Reading them with proto.GetExtension alone ties the result to the Go type
// identity of the extension: it resolves only when the descriptor's options
// were parsed against this module's syncplane/v1/options.proto. An app that vendors
// options.proto and generates its own Go package for it produces descriptors
// whose options carry the same field numbers but a foreign extension type, and
// GetExtension reads nothing — the message looks unannotated even though the
// .proto is correct.
//
// The wire bytes are identical in both cases, so when the fast path finds
// nothing this package re-parses the descriptor's uninterpreted option bytes
// against a private registry holding only this module's extensions. That binds
// field 50100/50101 to the types the engine compiled against, whatever produced
// the descriptor.
//
// Recovery is a backstop, not the intended setup: an app whose generated code
// registers a second syncplane/v1/options.proto trips protobuf-go's global registry
// and panics at init long before it reaches here. See the README section
// "Defining entities in your own module" for the codegen configuration that
// keeps a vendored options.proto pointing at this module's Go package.
package protoopts

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// localTypes resolves the sync option field numbers to THIS module's extension
// types and nothing else. Deliberately not protoregistry.GlobalTypes: the whole
// point is to bypass whichever copy of the extension won global registration.
var localTypes = func() *protoregistry.Types {
	types := &protoregistry.Types{}
	for _, xt := range []protoreflect.ExtensionType{syncplanev1.E_Entity, syncplanev1.E_FieldBehavior} {
		if err := types.RegisterExtension(xt); err != nil {
			panic("protoopts: " + err.Error())
		}
	}
	return types
}()

// OptionsGoPackage is the Go import path a vendored syncplane/v1/options.proto must
// keep as its go_package so an app's generated code shares this module's
// extension types instead of registering a second copy. Derived from the
// compiled descriptor so it stays correct if the package ever moves.
var OptionsGoPackage = func() string {
	opts, _ := syncplanev1.File_syncplane_v1_options_proto.Options().(*descriptorpb.FileOptions)
	// go_package is "<import path>;<package name>"; callers want the path.
	path, _, _ := strings.Cut(opts.GetGoPackage(), ";")
	return path
}()

// Entity returns the (syncplane.v1.entity) options declared on md, or nil when md
// is not an entity.
func Entity(md protoreflect.MessageDescriptor) *syncplanev1.EntityOptions {
	opts, ok := md.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return nil
	}
	if proto.HasExtension(opts, syncplanev1.E_Entity) {
		eo, _ := proto.GetExtension(opts, syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
		return eo
	}
	var recovered descriptorpb.MessageOptions
	if !reparseUninterpreted(opts, &recovered) || !proto.HasExtension(&recovered, syncplanev1.E_Entity) {
		return nil
	}
	eo, _ := proto.GetExtension(&recovered, syncplanev1.E_Entity).(*syncplanev1.EntityOptions)
	return eo
}

// FieldBehavior returns the (syncplane.v1.field_behavior) options declared on fd, or
// nil when the field carries none. Callers can use the getters on a nil value.
func FieldBehavior(fd protoreflect.FieldDescriptor) *syncplanev1.FieldBehavior {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	if !ok || opts == nil {
		return nil
	}
	if proto.HasExtension(opts, syncplanev1.E_FieldBehavior) {
		fb, _ := proto.GetExtension(opts, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
		return fb
	}
	var recovered descriptorpb.FieldOptions
	if !reparseUninterpreted(opts, &recovered) || !proto.HasExtension(&recovered, syncplanev1.E_FieldBehavior) {
		return nil
	}
	fb, _ := proto.GetExtension(&recovered, syncplanev1.E_FieldBehavior).(*syncplanev1.FieldBehavior)
	return fb
}

// HasUninterpretedOptions reports whether md carries option bytes no extension
// in this process could interpret. On a message that should be an entity but
// reads as unannotated, that is the signature of a divergent
// syncplane/v1/options.proto — it distinguishes "never annotated" from "annotated
// against a different extension type", which Entity alone cannot.
//
// Unrelated third-party message options also land here, so this is a hint for
// an error message, not a verdict.
func HasUninterpretedOptions(md protoreflect.MessageDescriptor) bool {
	opts, ok := md.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return false
	}
	return len(opts.ProtoReflect().GetUnknown()) > 0
}

// reparseUninterpreted decodes the option bytes src could not interpret into
// dst, resolving extensions against localTypes. It reports whether there was
// anything to decode and the decode succeeded.
//
// The length check keeps the fast path free: descriptors built against this
// module leave no uninterpreted bytes, so a field with no behavior — the common
// case on every merge — returns immediately without re-parsing.
func reparseUninterpreted(src, dst proto.Message) bool {
	raw := src.ProtoReflect().GetUnknown()
	if len(raw) == 0 {
		return false
	}
	return proto.UnmarshalOptions{Resolver: localTypes}.Unmarshal(raw, dst) == nil
}
