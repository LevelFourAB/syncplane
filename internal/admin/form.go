package admin

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/internal/admin/ui"
	"github.com/levelfourab/syncplane/internal/protoopts"
)

// form.go is the generic, reference-aware proto editor: it walks a
// protoreflect.MessageDescriptor to render a form (buildForm) and parses the
// submitted url.Values back into a dynamicpb message (bind), classifying each
// field — scalar, enum, opaque, embedded sub-message, typed reference,
// repeated scalar, or map — the same way the server does. Edits compute the
// changed-field set (changedMask) and submit it as the PATCH mask, so only the
// touched fields are written.
//
// Input naming is dotted/indexed so the bind is unambiguous:
//
//	title                  scalar
//	settings.color         embedded sub-message field
//	project.id             singular reference (target key field)
//	blocked_by[1].id       repeated reference element
//	tags[0]                repeated scalar element
//	members[0].name        keyed message-list element field
//	attrs[0].key           map entry key
//	attrs[0].value         map entry value

// buildForm renders the fields of md against msg. When edit is true the
// entity's key fields render read-only (they are identity). keyFields names the
// top-level key fields.
func buildForm(md protoreflect.MessageDescriptor, msg protoreflect.Message, keyFields []protoreflect.FieldDescriptor, edit bool) []ui.FieldView {
	return formBuilder{}.form(md, msg, keyFields, edit)
}

// formBuilder carries the workspace context a reference picker needs (the
// picker lists target rows in the same app + workspace). The zero value renders
// a form with non-functional picker URLs and unresolved ref labels — fine for
// unit tests that only check binding; handlers construct it with appID/ws and a
// label resolver.
type formBuilder struct {
	appID string
	ws    string
	// label resolves a selected reference (a message holding the target's key
	// fields) to the target row's display label. Nil falls back to the raw key.
	label func(target string, ref protoreflect.Message) string
}

func (b formBuilder) form(md protoreflect.MessageDescriptor, msg protoreflect.Message, keyFields []protoreflect.FieldDescriptor, edit bool) []ui.FieldView {
	keySet := map[protoreflect.Name]bool{}
	for _, kf := range keyFields {
		keySet[kf.Name()] = true
	}
	fields := md.Fields()
	views := make([]ui.FieldView, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		readOnly := edit && keySet[fd.Name()]
		views = append(views, b.field(fd, string(fd.Name()), msg, readOnly))
	}
	return views
}

// field renders one field at input-name base `name`. msg is the message that
// owns fd (its value is read from msg).
func (b formBuilder) field(fd protoreflect.FieldDescriptor, name string, msg protoreflect.Message, readOnly bool) ui.FieldView {
	fv := ui.FieldView{Label: humanLabel(fd.Name()), TypeLabel: typeLabel(fd), Name: name, ReadOnly: readOnly}
	switch {
	case server.RefKeyFields(fd) != nil:
		return b.ref(fd, name, msg, readOnly)
	case isText(fd):
		fv.Kind = ui.KindText
		fv.Value = textString(fd, msg)
		return fv
	case isOpaque(fd):
		fv.Kind = ui.KindOpaque
		fv.Value = opaqueString(fd, msg)
		return fv
	case fd.IsMap():
		fv.Kind = ui.KindMap
		fv.MapItems = buildMap(fd, name, msg, readOnly)
		return fv
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		if fd.IsList() {
			fv.Kind = ui.KindKeyedList
			fv.ListItems = b.listItems(fd, name, msg, readOnly)
			empty := dynamicpb.NewMessage(fd.Message())
			fv.Template = b.subFields(fd.Message(), fmt.Sprintf("%s[__i__]", name), empty, false)
			return fv
		}
		fv.Kind = ui.KindMessage
		sub := msg.Get(fd).Message()
		fv.Children = b.subFields(fd.Message(), name, sub, readOnly)
		return fv
	case fd.IsList(): // repeated scalar
		fv.Kind = ui.KindRepeatedScalar
		list := msg.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			fv.ScalarItems = append(fv.ScalarItems, scalarString(fd, list.Get(i)))
		}
		return fv
	case fd.Kind() == protoreflect.EnumKind:
		fv.Kind = ui.KindEnum
		fv.EnumOptions = buildEnum(fd, msg)
		return fv
	case fd.Kind() == protoreflect.BoolKind:
		fv.Kind = ui.KindBool
		fv.Value = strconv.FormatBool(msg.Get(fd).Bool())
		return fv
	default:
		fv.Kind = ui.KindScalar
		fv.Numeric = isNumeric(fd.Kind())
		if !isOptionalUnset(fd, msg) {
			fv.Value = scalarString(fd, msg.Get(fd))
		}
		return fv
	}
}

func (b formBuilder) subFields(md protoreflect.MessageDescriptor, prefix string, msg protoreflect.Message, readOnly bool) []ui.FieldView {
	fields := md.Fields()
	out := make([]ui.FieldView, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		out = append(out, b.field(fd, prefix+"."+string(fd.Name()), msg, readOnly))
	}
	return out
}

func (b formBuilder) ref(fd protoreflect.FieldDescriptor, name string, msg protoreflect.Message, readOnly bool) ui.FieldView {
	ref := server.RefOf(fd)
	keyNames := make([]string, 0, len(ref.KeyFields)+1)
	for _, kf := range ref.KeyFields {
		keyNames = append(keyNames, string(kf.Name()))
	}
	// The workspace rides along as one more hidden input per chip. A picked
	// row carries no value for it, so a fresh chip names the current
	// workspace, while a chip the row already held keeps where it pointed.
	var workspace string
	if ref.Workspace != nil {
		workspace = string(ref.Workspace.Name())
		keyNames = append(keyNames, workspace)
	}
	target := entityName(ref.Target)
	fv := ui.FieldView{
		Label: humanLabel(fd.Name()), TypeLabel: typeLabel(fd), Name: name, Kind: ui.KindRef, ReadOnly: readOnly,
		Ref: ui.RefControl{
			Name: name, Entity: target, Keys: keyNames, PickerURL: b.pickerURL(target),
			Workspace: workspace, Multi: fd.IsList(), ReadOnly: readOnly,
		},
	}
	if fd.IsList() {
		list := msg.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			fv.Ref.Items = append(fv.Ref.Items, b.refItemOf(fmt.Sprintf("%s[%d]", name, i), target, ref, list.Get(i).Message()))
		}
		return fv
	}
	if msg.Has(fd) {
		fv.Ref.Items = append(fv.Ref.Items, b.refItemOf(name, target, ref, msg.Get(fd).Message()))
	}
	return fv
}

// pickerURL is the picker endpoint for a reference target entity in this
// builder's app + workspace. Empty when the builder has no workspace context.
func (b formBuilder) pickerURL(target string) string {
	return refContext{appID: b.appID, ws: b.ws}.pickerURL(target)
}

func (b formBuilder) refItemOf(name, target string, ref *server.Reference, value protoreflect.Message) ui.RefItem {
	it := ui.RefItem{Name: name, Values: map[string]string{}}
	stub, ok := ref.Stub(value)
	if !ok {
		it.Label = "—"
		return it
	}
	for _, kf := range ref.KeyFields {
		it.Values[string(kf.Name())] = scalarString(kf, stub.Get(kf))
	}
	it.Label = joinKeyText(ref.KeyFields, stub)
	ws := ref.WorkspaceOf(value)
	if ref.Workspace != nil {
		it.Values[string(ref.Workspace.Name())] = ws
	}
	// A value naming another workspace resolves there; this form reads one
	// workspace, so it shows the key and where it points rather than a
	// same-key row from here.
	if ws != "" {
		it.Label += " @ " + ws
	} else if b.label != nil {
		if l := b.label(target, stub); l != "" {
			it.Label = l
		}
	}
	it.Color = colorSlot(it.Label)
	it.Initials = initials(it.Label)
	return it
}

func (b formBuilder) listItems(fd protoreflect.FieldDescriptor, name string, msg protoreflect.Message, readOnly bool) [][]ui.FieldView {
	list := msg.Get(fd).List()
	out := make([][]ui.FieldView, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		elemName := fmt.Sprintf("%s[%d]", name, i)
		out = append(out, b.subFields(fd.Message(), elemName, list.Get(i).Message(), readOnly))
	}
	return out
}

func buildMap(fd protoreflect.FieldDescriptor, name string, msg protoreflect.Message, readOnly bool) []ui.MapItem {
	m := msg.Get(fd).Map()
	keyFd := fd.MapKey()
	valFd := fd.MapValue()
	items := make([]ui.MapItem, 0, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		items = append(items, ui.MapItem{
			Key:   scalarString(keyFd, k.Value()),
			Value: scalarString(valFd, v),
		})
		return true
	})
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	for i := range items {
		items[i].Name = fmt.Sprintf("%s[%d]", name, i)
	}
	return items
}

func buildEnum(fd protoreflect.FieldDescriptor, msg protoreflect.Message) []ui.EnumOption {
	cur := msg.Get(fd).Enum()
	vals := fd.Enum().Values()
	out := make([]ui.EnumOption, 0, vals.Len())
	for i := 0; i < vals.Len(); i++ {
		v := vals.Get(i)
		out = append(out, ui.EnumOption{
			Value:    strconv.FormatInt(int64(v.Number()), 10),
			Label:    string(v.Name()),
			Selected: v.Number() == cur,
		})
	}
	return out
}

// bind parses the submitted form into a fresh dynamicpb message of md.
func bind(md protoreflect.MessageDescriptor, vals url.Values) (proto.Message, error) {
	msg := dynamicpb.NewMessage(md)
	if err := setMessage(msg, "", vals); err != nil {
		return nil, err
	}
	return msg, nil
}

// setMessage walks md's fields, reading each from vals under the dotted prefix.
func setMessage(m protoreflect.Message, prefix string, vals url.Values) error {
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := prefix + string(fd.Name())
		if err := setField(m, fd, name, vals); err != nil {
			return err
		}
	}
	return nil
}

func setField(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, vals url.Values) error {
	switch {
	case server.RefKeyFields(fd) != nil:
		return setRef(m, fd, name, vals)
	case isText(fd):
		// Bound separately with the stored document as base (bindTextFields):
		// the form carries a plain string, and merging it needs the prev row.
		return nil
	case isOpaque(fd):
		return setOpaque(m, fd, name, vals)
	case fd.IsMap():
		return setMap(m, fd, name, vals)
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		if fd.IsList() {
			return setKeyedList(m, fd, name, vals)
		}
		sub := m.NewField(fd)
		if err := setMessage(sub.Message(), name+".", vals); err != nil {
			return err
		}
		if hasAnyPopulated(sub.Message()) {
			m.Set(fd, sub)
		}
		return nil
	case fd.IsList(): // repeated scalar
		list := m.Mutable(fd).List()
		for _, idx := range indicesFor(vals, name) {
			s := vals.Get(fmt.Sprintf("%s[%d]", name, idx))
			v, ok, err := scalarValue(fd, s)
			if err != nil {
				return err
			}
			if ok {
				list.Append(v)
			}
		}
		return nil
	default: // scalar / enum / bool
		v, ok, err := scalarValue(fd, vals.Get(name))
		if err != nil {
			return err
		}
		if ok {
			m.Set(fd, v)
		}
		return nil
	}
}

func setRef(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, vals url.Values) error {
	ref := server.RefOf(fd)
	// build fills one stored reference value from the inputs under base,
	// reporting whether any key was supplied. The workspace input is the one
	// the form carried along (see formBuilder.ref); empty means this workspace.
	build := func(value protoreflect.Message, base string) bool {
		if !setKeys(ref.MutableStub(value), ref.KeyFields, base, vals) {
			return false
		}
		if ref.Workspace != nil {
			if s := vals.Get(base + "." + string(ref.Workspace.Name())); s != "" {
				value.Set(ref.Workspace, protoreflect.ValueOfString(s))
			}
		}
		return true
	}
	if fd.IsList() {
		list := m.Mutable(fd).List()
		for _, idx := range indicesFor(vals, name) {
			base := fmt.Sprintf("%s[%d]", name, idx)
			elem := list.NewElement()
			if build(elem.Message(), base) {
				list.Append(elem)
			}
		}
		return nil
	}
	val := m.NewField(fd)
	if build(val.Message(), name) {
		m.Set(fd, val)
	}
	return nil
}

// setKeys sets the key fields of a ref stub from vals named base.<keyfield>,
// reporting whether any key field was supplied.
func setKeys(stub protoreflect.Message, keyFields []protoreflect.FieldDescriptor, base string, vals url.Values) bool {
	any := false
	for _, kf := range keyFields {
		s := vals.Get(base + "." + string(kf.Name()))
		if s == "" {
			continue
		}
		v, ok, err := scalarValue(kf, s)
		if err == nil && ok {
			stub.Set(kf, v)
			any = true
		}
	}
	return any
}

func setKeyedList(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, vals url.Values) error {
	list := m.Mutable(fd).List()
	for _, idx := range indicesFor(vals, name) {
		base := fmt.Sprintf("%s[%d]", name, idx)
		elem := list.NewElement()
		if err := setMessage(elem.Message(), base+".", vals); err != nil {
			return err
		}
		if hasAnyPopulated(elem.Message()) {
			list.Append(elem)
		}
	}
	return nil
}

func setMap(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, vals url.Values) error {
	mp := m.Mutable(fd).Map()
	keyFd := fd.MapKey()
	valFd := fd.MapValue()
	for _, idx := range indicesFor(vals, name) {
		base := fmt.Sprintf("%s[%d]", name, idx)
		ks := vals.Get(base + ".key")
		if ks == "" {
			continue
		}
		kv, ok, err := scalarValue(keyFd, ks)
		if err != nil || !ok {
			continue
		}
		vv, ok, err := scalarValue(valFd, vals.Get(base+".value"))
		if err != nil {
			return err
		}
		if !ok {
			vv = valFd.Default()
		}
		mp.Set(kv.MapKey(), vv)
	}
	return nil
}

func setOpaque(m protoreflect.Message, fd protoreflect.FieldDescriptor, name string, vals url.Values) error {
	s := vals.Get(name)
	if s == "" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("admin: field %q: invalid base64: %w", name, err)
	}
	if fd.Kind() == protoreflect.BytesKind {
		m.Set(fd, protoreflect.ValueOfBytes(raw))
		return nil
	}
	// Opaque message: the base64 is the serialized sub-message, replaced whole.
	sub := m.NewField(fd)
	if err := proto.Unmarshal(raw, sub.Message().Interface()); err != nil {
		return fmt.Errorf("admin: field %q: %w", name, err)
	}
	m.Set(fd, sub)
	return nil
}

// indicesFor returns, sorted, every index i for which a key base[i] or
// base[i].something appears in vals.
func indicesFor(vals url.Values, base string) []int {
	prefix := base + "["
	set := map[int]bool{}
	for k := range vals {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		end := strings.IndexByte(rest, ']')
		if end <= 0 {
			continue
		}
		n, err := strconv.Atoi(rest[:end])
		if err != nil {
			continue
		}
		set[n] = true
	}
	out := make([]int, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

// changedMask compares the loaded row against the bound edit and returns the
// dotted field-mask paths that differ, excluding the top-level key fields
// (identity never changes). Singular non-opaque sub-messages recurse so the
// mask names only the leaf that changed; references, lists and maps are
// compared (and masked) whole.
func changedMask(loaded, bound proto.Message, keyFields []protoreflect.FieldDescriptor) []string {
	keySet := map[protoreflect.Name]bool{}
	for _, kf := range keyFields {
		keySet[kf.Name()] = true
	}
	var paths []string
	diffMessage("", loaded.ProtoReflect(), bound.ProtoReflect(), keySet, &paths)
	return paths
}

func diffMessage(prefix string, a, b protoreflect.Message, topKeys map[protoreflect.Name]bool, paths *[]string) {
	fields := b.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if prefix == "" && topKeys[fd.Name()] {
			continue
		}
		path := prefix + string(fd.Name())
		// Recurse into singular, non-ref, non-opaque sub-messages for leaf paths.
		if fd.Kind() == protoreflect.MessageKind && !fd.IsList() && !fd.IsMap() &&
			server.RefKeyFields(fd) == nil && !isOpaque(fd) {
			diffMessage(path+".", a.Get(fd).Message(), b.Get(fd).Message(), topKeys, paths)
			continue
		}
		if !fieldEqual(fd, a.Get(fd), b.Get(fd)) {
			*paths = append(*paths, path)
		}
	}
}

func fieldEqual(fd protoreflect.FieldDescriptor, a, b protoreflect.Value) bool {
	switch {
	case fd.IsList():
		la, lb := a.List(), b.List()
		if la.Len() != lb.Len() {
			return false
		}
		for i := 0; i < la.Len(); i++ {
			if !singularEqual(fd, la.Get(i), lb.Get(i)) {
				return false
			}
		}
		return true
	case fd.IsMap():
		ma, mb := a.Map(), b.Map()
		if ma.Len() != mb.Len() {
			return false
		}
		equal := true
		ma.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			if !mb.Has(k) || !singularEqual(fd.MapValue(), v, mb.Get(k)) {
				equal = false
				return false
			}
			return true
		})
		return equal
	default:
		return singularEqual(fd, a, b)
	}
}

func singularEqual(fd protoreflect.FieldDescriptor, a, b protoreflect.Value) bool {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return proto.Equal(a.Message().Interface(), b.Message().Interface())
	case protoreflect.BytesKind:
		return bytes.Equal(a.Bytes(), b.Bytes())
	default:
		return a.Interface() == b.Interface()
	}
}

// hasAnyPopulated reports whether any field of m is set, so an all-empty
// sub-message bound from a blank form is not persisted as present.
func hasAnyPopulated(m protoreflect.Message) bool {
	populated := false
	m.Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		populated = true
		return false
	})
	return populated
}

func isOptionalUnset(fd protoreflect.FieldDescriptor, msg protoreflect.Message) bool {
	return fd.HasPresence() && !msg.Has(fd)
}

// scalarValue converts a form string to a proto value for fd, reporting whether
// a value should be set (an empty string for a non-string field is "leave
// default").
func scalarValue(fd protoreflect.FieldDescriptor, s string) (protoreflect.Value, bool, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(s == "true" || s == "on" || s == "1"), true, nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(s), s != "", nil
	case protoreflect.BytesKind:
		if s == "" {
			return protoreflect.Value{}, false, nil
		}
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
		if err != nil {
			return protoreflect.Value{}, false, fmt.Errorf("admin: %q: invalid base64: %w", fd.Name(), err)
		}
		return protoreflect.ValueOfBytes(raw), true, nil
	case protoreflect.EnumKind:
		if s == "" {
			return protoreflect.Value{}, false, nil
		}
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), true, nil
		}
		if ev := fd.Enum().Values().ByName(protoreflect.Name(s)); ev != nil {
			return protoreflect.ValueOfEnum(ev.Number()), true, nil
		}
		return protoreflect.Value{}, false, fmt.Errorf("admin: %q: unknown enum value %q", fd.Name(), s)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return parseInt(s, 32, func(n int64) protoreflect.Value { return protoreflect.ValueOfInt32(int32(n)) })
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return parseInt(s, 64, func(n int64) protoreflect.Value { return protoreflect.ValueOfInt64(n) })
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return parseUint(s, 32, func(n uint64) protoreflect.Value { return protoreflect.ValueOfUint32(uint32(n)) })
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return parseUint(s, 64, func(n uint64) protoreflect.Value { return protoreflect.ValueOfUint64(n) })
	case protoreflect.FloatKind:
		if s == "" {
			return protoreflect.Value{}, false, nil
		}
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfFloat32(float32(f)), true, nil
	case protoreflect.DoubleKind:
		if s == "" {
			return protoreflect.Value{}, false, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfFloat64(f), true, nil
	default:
		return protoreflect.Value{}, false, fmt.Errorf("admin: %q: unsupported kind %s", fd.Name(), fd.Kind())
	}
}

func parseInt(s string, bits int, mk func(int64) protoreflect.Value) (protoreflect.Value, bool, error) {
	if s == "" {
		return protoreflect.Value{}, false, nil
	}
	n, err := strconv.ParseInt(s, 10, bits)
	if err != nil {
		return protoreflect.Value{}, false, err
	}
	return mk(n), true, nil
}

func parseUint(s string, bits int, mk func(uint64) protoreflect.Value) (protoreflect.Value, bool, error) {
	if s == "" {
		return protoreflect.Value{}, false, nil
	}
	n, err := strconv.ParseUint(s, 10, bits)
	if err != nil {
		return protoreflect.Value{}, false, err
	}
	return mk(n), true, nil
}

// scalarString renders a scalar/enum proto value as its form string.
func scalarString(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case protoreflect.EnumKind:
		return strconv.FormatInt(int64(v.Enum()), 10)
	default:
		return v.String()
	}
}

func isNumeric(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind:
		return true
	}
	return false
}

func opaqueString(fd protoreflect.FieldDescriptor, msg protoreflect.Message) string {
	if !msg.Has(fd) {
		return ""
	}
	if fd.Kind() == protoreflect.BytesKind {
		return base64.StdEncoding.EncodeToString(msg.Get(fd).Bytes())
	}
	raw, err := proto.Marshal(msg.Get(fd).Message().Interface())
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// isOpaque reports whether fd carries (syncplane.v1.field_behavior).opaque, so the
// admin form treats it as replaced-whole. Read straight off the descriptor's
// options — the same rule the server applies. Text fields are handled first
// (isText) so this covers only the raw base64 escape hatch.
func isOpaque(fd protoreflect.FieldDescriptor) bool {
	return protoopts.FieldBehavior(fd).GetOpaque()
}

// isText reports whether fd carries (syncplane.v1.field_behavior).text: a bytes
// field holding an encoded collaborative text document, rendered as a plain
// textarea (decoded for display, re-applied via SetString on submit).
func isText(fd protoreflect.FieldDescriptor) bool {
	return protoopts.FieldBehavior(fd).GetText()
}

// textString decodes a text field's document to its visible string (empty on
// absent or undecodable state).
func textString(fd protoreflect.FieldDescriptor, msg protoreflect.Message) string {
	s, err := text.PlainText(msg.Get(fd).Bytes())
	if err != nil {
		return ""
	}
	return s
}

// bindTextFields rewrites every text field of bound from its submitted
// plain-string form value: the stored document from prev (nil when the row does
// not exist) is decoded, SetString applies a minimal diff, and the re-encoded
// state lands on bound — so an admin edit merges with concurrent client edits
// instead of replacing the document. An unchanged value re-encodes to identical
// bytes, which keeps the field out of the update diff mask. The walk descends
// through singular, non-opaque sub-messages, mirroring the server's
// registration-time scan.
//
// peer is the writer identity new items are minted under, and it is the
// handler's — one per handler, not one per edit, so a document's peer table
// does not grow by an entry per admin write. Reusing it is only sound because
// counters are allocated out of prev's version vector: CALLERS MUST READ prev
// INSIDE THE WRITE TRANSACTION THAT STORES THE RESULT, so it already carries
// every earlier write under this peer. Binding against a document read before
// the transaction (or against an empty one when the row is live) would re-spend
// counters this peer holds, minting colliding item ids. The engine's per-
// workspace write exclusion serializes the calls, and re-running the closure on
// retry re-reads prev.
func bindTextFields(
	md protoreflect.MessageDescriptor,
	prev, bound protoreflect.Message,
	vals url.Values,
	peer string,
) error {
	var walk func(md protoreflect.MessageDescriptor, prefix string, chain []protoreflect.FieldDescriptor) error
	walk = func(md protoreflect.MessageDescriptor, prefix string, chain []protoreflect.FieldDescriptor) error {
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			name := prefix + string(fd.Name())
			if isText(fd) {
				if !vals.Has(name) {
					continue
				}
				var prevBytes []byte
				if prev != nil {
					prevBytes = chainBytes(prev, append(chain, fd))
				}
				doc, err := text.Decode(prevBytes, peer)
				if err != nil {
					return fmt.Errorf("admin: field %q: %w", name, err)
				}
				if err := doc.SetString(vals.Get(name)); err != nil {
					return fmt.Errorf("admin: field %q: %w", name, err)
				}
				chainSetBytes(bound, append(chain, fd), doc.Encode())
				continue
			}
			if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() ||
				isOpaque(fd) || server.RefKeyFields(fd) != nil {
				continue
			}
			if err := walk(fd.Message(), name+".", append(chain, fd)); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(md, "", nil)
}

// chainBytes walks a descriptor chain and returns the leaf bytes, or nil when
// an intermediate message is unset.
func chainBytes(msg protoreflect.Message, chain []protoreflect.FieldDescriptor) []byte {
	cur := msg
	for i, fd := range chain {
		if i == len(chain)-1 {
			return cur.Get(fd).Bytes()
		}
		if !cur.Has(fd) {
			return nil
		}
		cur = cur.Get(fd).Message()
	}
	return nil
}

// chainSetBytes writes the leaf bytes, materializing intermediate messages.
func chainSetBytes(msg protoreflect.Message, chain []protoreflect.FieldDescriptor, value []byte) {
	cur := msg
	for i, fd := range chain {
		if i == len(chain)-1 {
			cur.Set(fd, protoreflect.ValueOfBytes(value))
			return
		}
		cur = cur.Mutable(fd).Message()
	}
}

// typeLabel renders the field's type for the detail-panel type chip, e.g.
// "string", "enum Status", "repeated User", "ChecklistItem", "map<string,
// string>".
func typeLabel(fd protoreflect.FieldDescriptor) string {
	var base string
	switch {
	case fd.IsMap():
		return fmt.Sprintf("map<%s, %s>", fd.MapKey().Kind(), fd.MapValue().Kind())
	case server.RefKeyFields(fd) != nil:
		base = titleCase(entityName(server.RefOf(fd).Target))
	case fd.Kind() == protoreflect.EnumKind:
		base = "enum " + string(fd.Enum().Name())
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		base = string(fd.Message().Name())
	default:
		base = fd.Kind().String()
	}
	if fd.IsList() {
		return "repeated " + base
	}
	return base
}

// entityName returns the wire entity name carried by md's (syncplane.v1.entity)
// options (a reference target). Empty if md is not an entity.
func entityName(md protoreflect.MessageDescriptor) string {
	return protoopts.Entity(md).GetName()
}
