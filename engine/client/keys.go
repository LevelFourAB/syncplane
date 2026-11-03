package client

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// entityShape is what the client needs to address rows of one entity message.
type entityShape struct {
	name      string
	keyFields []protoreflect.FieldDescriptor
}

// shapes caches entityShape by message full name. Descriptors of one full name
// can differ (a dynamic message built from another registry), but the entity
// options and key fields are a wire contract that cannot change without a
// breaking schema change, so caching by name is safe.
var shapes sync.Map // protoreflect.FullName -> *entityShape

// shapeOf reads the (syncplane.v1.entity) options of md the way server
// registration reads them, and refuses the same shapes registration refuses.
func shapeOf(md protoreflect.MessageDescriptor) (*entityShape, error) {
	if v, ok := shapes.Load(md.FullName()); ok {
		return v.(*entityShape), nil
	}
	eo := protoopts.Entity(md)
	if eo.GetName() == "" {
		return nil, fmt.Errorf("client: %s carries no (syncplane.v1.entity) option", md.FullName())
	}
	if err := naming.ValidateEntityName(eo.GetName()); err != nil {
		return nil, fmt.Errorf("client: %s: %w", md.FullName(), err)
	}
	if len(eo.GetKeyFields()) == 0 {
		return nil, fmt.Errorf("client: entity %q declares no key_fields", eo.GetName())
	}
	shape := &entityShape{name: eo.GetName()}
	for _, kf := range eo.GetKeyFields() {
		fd := md.Fields().ByName(protoreflect.Name(kf))
		if fd == nil {
			return nil, fmt.Errorf("client: entity %q key field %q not found", eo.GetName(), kf)
		}
		if err := naming.ValidateKeyFieldKind(fd); err != nil {
			return nil, fmt.Errorf("client: entity %q: %w", eo.GetName(), err)
		}
		shape.keyFields = append(shape.keyFields, fd)
	}
	v, _ := shapes.LoadOrStore(md.FullName(), shape)
	return v.(*entityShape), nil
}

// keyOf derives the row key of msg. It does not validate the result.
func (s *entityShape) keyOf(msg protoreflect.Message) string {
	parts := make([]string, len(s.keyFields))
	for i, fd := range s.keyFields {
		parts[i] = naming.ScalarKeyString(fd, msg.Get(fd))
	}
	return naming.RowKey(parts)
}

// EntityKey returns the entity name and row key that msg addresses, derived
// from its (syncplane.v1.entity) options exactly as the server derives them.
//
// It returns an error when msg carries no entity options, when the options
// name a key field msg does not have or of a kind that cannot form a key, and
// when the derived key breaks the row-key rules — for example an empty key
// from unset key fields.
func EntityKey(msg proto.Message) (entity, key string, err error) {
	shape, err := shapeOf(msg.ProtoReflect().Descriptor())
	if err != nil {
		return "", "", err
	}
	key = shape.keyOf(msg.ProtoReflect())
	if err := naming.ValidateRowKey(key); err != nil {
		return "", "", fmt.Errorf("client: entity %q: %w", shape.name, err)
	}
	return shape.name, key, nil
}

// EntityName returns the entity name of the message type of msg. msg may be
// a nil pointer of a generated message type.
func EntityName(msg proto.Message) (string, error) {
	shape, err := shapeOf(msg.ProtoReflect().Descriptor())
	if err != nil {
		return "", err
	}
	return shape.name, nil
}
