package connect

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// RowCodec is a Codec for records whose parts map to rows: Decode turns the
// content of a record into its root row and member rows, and Encode turns
// the rows back into content. The connector merges the decoded rows into the
// engine one row at a time, against the rows as the store held them when the
// two sides last agreed:
//
//   - A field the record changed since then takes the record's value. A field
//     the record did not change keeps the engine's value, also when a client
//     changed it.
//   - A collaborative-text field the record changed is merged with the
//     engine's document, so an edit in the store and an edit in the engine
//     both survive, also within one field.
//   - A member row that the record held and no longer holds is deleted. A
//     member row that a client added and that the record never held stays,
//     and the next write puts it in the store.
//   - A member row that Decode gives to another record by its key moves to
//     that record, with the fields that record does not own and with its
//     text history, when the store changes both records in one pass.
//   - With no agreed state, as on the first sync of a record, the rows take
//     the record's values for the fields in Fields, and the member rows the
//     record does not hold are deleted.
//
// Fields the record does not own, see Fields, always keep their engine
// value.
//
// The agreed state of each record holds a copy of its rows, with their
// collaborative-text documents, and each ingest and each write of the record
// writes that copy again: a record's bookkeeping costs about the size of its
// rows in storage, and that much again in writes on each change.
//
// Use a RowCodec as RecordType.Codec. New validates it against the record
// type; its Merge and Render methods return an error when called directly.
// Decode and Encode run inside the merge transaction, under the workspace
// write lock: keep them free of I/O. They can be called more than once for
// one change, and concurrently for different records, so they must give the
// same result for the same input and be safe for concurrent use.
type RowCodec struct {
	// Decode returns the rows a record holds. It is required, except for an
	// EgressOnly record type.
	//
	// An error refuses the record: no row changes, and the connector skips
	// the record as for any Codec error.
	Decode func(in DecodeInput) (Decoded, error)
	// Encode returns the content of a record from its rows. It is required,
	// except for an IngressOnly record type.
	//
	// The rows that Decode returns for the content must be equal to the rows
	// Encode got, in the fields the record owns: the connector takes them as
	// what the store holds after the write. An error refuses the record, as
	// for Decode.
	Encode func(in EncodeInput) (Rendered, error)
	// Fields names the fields the record owns, for each entity of the record
	// type, keyed by a prototype of the entity. The names follow
	// Binding.Fields: top-level field names, or a oneof name for a whole oneof
	// group. The key fields and the top-level field of Member.RecordField are
	// always owned. An entity with no entry owns every field.
	//
	// A field Decode cannot read back from the store must not be owned: on
	// the next change of the record, the connector would take its empty value
	// as an edit. New returns an error for a prototype of an entity outside
	// the record type, and for a name the entity does not declare.
	Fields map[proto.Message][]string
}

// RecordRows are the root row and the member rows of one record.
type RecordRows struct {
	// Root is the root row; nil when there is none.
	Root proto.Message
	// Members are the member rows. In DecodeInput.Rows and EncodeInput.Rows
	// they are in the order of RecordType.Members, and in key order within
	// one entity. In DecodeInput.Base they are in no fixed order.
	Members []proto.Message
}

// DecodeInput is the input of RowCodec.Decode. The rows are read in the
// merge transaction and are not retained after Decode returns; Decode must
// not change them. Their collaborative-text fields hold documents: read them
// with PlainTextField.
type DecodeInput struct {
	// Key is the record key. A root row with an empty key gets it.
	Key    string
	Record Record
	// Base holds the rows as the store held them when the store and the
	// engine last agreed. Base.Root is nil when there is no agreed state.
	// Match the parts of the content to these rows to give each part the key
	// of its row; see MatchKeys. When the connector cannot tell whether its
	// last store write was done (see MergeInput.Alternate), Base holds the
	// rows before or after that write, whichever Encode makes into content
	// nearer Record.Content.
	Base RecordRows
	// Rows holds the rows of the record in the engine now. A member row here
	// that Base does not hold is a row a client added, or a row an earlier
	// merge of this same content wrote: give a part the key of such a row
	// when they are equal, so that a repeated merge writes nothing.
	Rows RecordRows
	// Payload is the adopter's data of the agreed state, from
	// Decoded.Payload or from the Rendered.Payload of Encode; nil when there
	// is none.
	Payload []byte
}

// Decoded is the result of RowCodec.Decode. The connector can change the
// messages.
type Decoded struct {
	// Root is the root row. It is required. An empty key gets the record key
	// when the root entity has one string key field; any other key must be the
	// record key.
	Root proto.Message
	// Members are the member rows, with their keys set. A member whose
	// Member.RecordField is empty gets the record key; one that names another
	// record refuses the record.
	Members []proto.Message
	// Payload is the adopter's data for this content, given back as
	// DecodeInput.Payload and EncodeInput.Payload.
	Payload []byte

	texts map[proto.Message]map[string]string
}

// SetText gives the collaborative-text field of row, a message in Root or
// Members, the visible text value. field is the top-level proto field name.
// A text field the record owns and that SetText does not set is empty in the
// record. Leave the bytes of a text field unset on the message: the
// connector merges value into the engine's document.
func (d *Decoded) SetText(row proto.Message, field, value string) {
	if d.texts == nil {
		d.texts = make(map[proto.Message]map[string]string)
	}
	if d.texts[row] == nil {
		d.texts[row] = make(map[string]string, 1)
	}
	d.texts[row][field] = value
}

// EncodeInput is the input of RowCodec.Encode. The rows are read in the
// transaction and are not retained after Encode returns; Encode must not
// change them.
type EncodeInput struct {
	Key string
	// Locator is the locator of the agreed state; empty when the store does
	// not hold the record yet, and Encode must then return one.
	Locator string
	// Payload is the adopter's data of the agreed state, as in DecodeInput.
	Payload []byte
	// Rows are the rows of the record. Rows.Root is not nil. Render a
	// collaborative-text field with PlainTextField.
	Rows RecordRows
}

// errUnbound is the error of a RowCodec used outside a RecordType.
var errUnbound = errors.New("connect: RowCodec works only as RecordType.Codec")

// Merge returns an error: the connector calls the RowCodec that New binds to
// its record type instead.
func (RowCodec) Merge(*server.EntityTx, MergeInput) (Merged, error) {
	return Merged{}, errUnbound
}

// Render returns an error: the connector calls the RowCodec that New binds to
// its record type instead.
func (RowCodec) Render(*server.EntityTx, RenderInput) (Rendered, bool, error) {
	return Rendered{}, false, errUnbound
}

// rowCodec is a RowCodec bound to one record type.
type rowCodec struct {
	decode func(in DecodeInput) (Decoded, error)
	encode func(in EncodeInput) (Rendered, error)
	srv    *server.Server
	root   *rowEntity
	byName map[string]*rowEntity
}

// rowEntity is one entity of a bound RowCodec.
type rowEntity struct {
	binding
	name string
	// member is nil for the root entity.
	member *recordMember
	// order is the place of a member entity in RecordType.Members.
	order int
	// keyField is the key field of a root entity with one string key field,
	// which an empty root key is filled into; else nil.
	keyField protoreflect.FieldDescriptor
	// texts are the collaborative-text fields the record owns.
	texts []protoreflect.FieldDescriptor
}

func (c RowCodec) bindCodec(srv *server.Server, rt *recordType) (Codec, error) {
	if c.Decode == nil && rt.direction != EgressOnly {
		return nil, fmt.Errorf("RowCodec needs Decode")
	}
	if c.Encode == nil && rt.direction != IngressOnly {
		return nil, fmt.Errorf("RowCodec needs Encode")
	}
	fields := make(map[string][]string, len(c.Fields))
	for prototype, names := range c.Fields {
		entity, _, err := srv.EntityKey(prototype)
		if err != nil {
			return nil, fmt.Errorf("RowCodec Fields: %w", err)
		}
		if _, dup := fields[entity]; dup {
			return nil, fmt.Errorf("RowCodec Fields: entity %q named twice", entity)
		}
		fields[entity] = names
	}
	b := &rowCodec{decode: c.Decode, encode: c.Encode, srv: srv, byName: make(map[string]*rowEntity)}
	add := func(prototype proto.Message, name string, m *recordMember) (*rowEntity, error) {
		meta, ok := srv.Entity(name)
		if !ok {
			return nil, fmt.Errorf("entity %q is not registered", name)
		}
		owned, units, err := ownedFields(meta, fields[name])
		if err != nil {
			return nil, err
		}
		delete(fields, name)
		e := &rowEntity{
			binding: binding{prototype: prototype, owned: owned, ownedUnits: units},
			name:    name,
			member:  m,
			order:   len(b.byName),
		}
		if m != nil && owned != nil {
			// The record field says which record a row is in, which only
			// the record can say.
			top := m.path[0]
			unit := string(top.Name())
			if od := lww.OneofOf(top); od != nil {
				unit = string(od.Name())
			}
			if !owned[unit] {
				owned[unit] = true
				e.ownedUnits = append(e.ownedUnits, unit)
			}
		}
		if m == nil && len(meta.KeyFields) == 1 && meta.KeyFields[0].Kind() == protoreflect.StringKind {
			e.keyField = meta.KeyFields[0]
		}
		all := meta.Descriptor.Fields()
		for i := 0; i < all.Len(); i++ {
			if fd := all.Get(i); lww.IsText(fd) && (owned == nil || owned[string(fd.Name())]) {
				e.texts = append(e.texts, fd)
			}
		}
		b.byName[name] = e
		return e, nil
	}
	var err error
	if b.root, err = add(rt.root, rt.rootName, nil); err != nil {
		return nil, err
	}
	for _, m := range rt.members {
		if _, err := add(m.prototype, m.entity, m); err != nil {
			return nil, err
		}
	}
	for name := range fields {
		return nil, fmt.Errorf("RowCodec Fields: entity %q is not in the record type", name)
	}
	return b, nil
}

// rowID names a row across entities.
type rowID struct{ entity, key string }

// decodedRow is one row of a Decoded, checked.
type decodedRow struct {
	ent   *rowEntity
	key   string
	msg   proto.Message
	texts map[string]string
}

// rowPayload is the payload a RowCodec keeps in the agreed state: the
// adopter's payload and the rows as the store holds them, the root first.
// Text fields hold documents, the ancestors of the next three-way text
// merge.
type rowPayload struct {
	App  []byte    `json:"app,omitempty"`
	Rows []rowSnap `json:"rows,omitempty"`
}

type rowSnap struct {
	Entity string `json:"entity"`
	Key    string `json:"key"`
	Row    []byte `json:"row"`
}

// agreedRows is a decoded rowPayload.
type agreedRows struct {
	app   []byte
	rows  RecordRows
	byID  map[rowID]proto.Message
	order []rowID
}

func (c *rowCodec) readPayload(a *Agreed) (*agreedRows, error) {
	out := &agreedRows{byID: make(map[rowID]proto.Message)}
	if a == nil || len(a.Payload) == 0 {
		return out, nil
	}
	var p rowPayload
	if err := json.Unmarshal(a.Payload, &p); err != nil {
		return nil, fmt.Errorf("connect: agreed state is not from a RowCodec: %w", err)
	}
	out.app = p.App
	for _, s := range p.Rows {
		e := c.byName[s.Entity]
		if e == nil {
			// An entity the record type no longer holds.
			continue
		}
		msg := e.prototype.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(s.Row, msg); err != nil {
			return nil, fmt.Errorf("connect: agreed %s %q: %w", s.Entity, s.Key, err)
		}
		id := rowID{s.Entity, s.Key}
		out.byID[id] = msg
		if e.member == nil {
			out.rows.Root = msg
			continue
		}
		out.rows.Members = append(out.rows.Members, msg)
		out.order = append(out.order, id)
	}
	return out, nil
}

func (c *rowCodec) writePayload(app []byte, rows []proto.Message) ([]byte, error) {
	p := rowPayload{App: app, Rows: make([]rowSnap, 0, len(rows))}
	opts := proto.MarshalOptions{Deterministic: true}
	for _, msg := range rows {
		entity, key, err := c.srv.EntityKey(msg)
		if err != nil {
			return nil, err
		}
		raw, err := opts.Marshal(msg)
		if err != nil {
			return nil, err
		}
		p.Rows = append(p.Rows, rowSnap{Entity: entity, Key: key, Row: raw})
	}
	return json.Marshal(p)
}

func (c *rowCodec) Merge(tx *server.EntityTx, in MergeInput) (Merged, error) {
	if c.decode == nil {
		// An EgressOnly type never merges.
		return Merged{}, errors.New("connect: RowCodec has no Decode")
	}
	base, err := c.readPayload(in.Base)
	if err != nil {
		return Merged{}, err
	}
	var alt *agreedRows
	if in.Alternate != nil && base.rows.Root != nil {
		if alt, err = c.readPayload(in.Alternate); err != nil {
			return Merged{}, err
		}
		if alt.rows.Root == nil {
			alt = nil
		}
	}
	from := base
	if alt != nil {
		if from, err = c.nearer(in, base, alt); err != nil {
			return Merged{}, err
		}
	}
	dec, err := c.decode(DecodeInput{
		Key:     in.Key,
		Record:  in.Record,
		Base:    from.rows,
		Rows:    RecordRows{Root: in.Root, Members: in.Members},
		Payload: from.app,
	})
	if err != nil {
		return Merged{}, err
	}
	rows, err := c.check(in.Key, &dec)
	if err != nil {
		return Merged{}, err
	}
	cur := make(map[rowID]proto.Message, 1+len(in.Members))
	if in.Root != nil {
		cur[rowID{c.root.name, in.Key}] = in.Root
	}
	for _, msg := range in.Members {
		entity, key, err := c.srv.EntityKey(msg)
		if err != nil {
			return Merged{}, err
		}
		cur[rowID{entity, key}] = msg
	}
	held := base.rows.Root != nil
	decoded := make(map[rowID]bool, len(rows))
	views := make([]proto.Message, 0, len(rows))
	for _, r := range rows {
		id := rowID{r.ent.name, r.key}
		decoded[id] = true
		now, ok := cur[id]
		absent := false
		if !ok && r.ent.member != nil {
			// A row of another record that this record now holds.
			msg := r.ent.prototype.ProtoReflect().New().Interface()
			found, err := tx.Get(msg, r.key)
			if err != nil {
				return Merged{}, err
			}
			switch {
			case found:
				now = msg
			case in.dropped != nil:
				// An earlier transaction of the pass deleted it from the
				// record that held it: the store moved it.
				if was := in.dropped(id); was != nil {
					now, absent = proto.Clone(was), true
				}
			}
		}
		var altRow proto.Message
		if alt != nil {
			altRow = alt.byID[id]
		}
		merged, view, err := c.mergeRow(r, base.byID[id], altRow, now, in.TextPeer, in.TextClock)
		if err != nil {
			return Merged{}, fmt.Errorf("%s %q: %w", r.ent.name, r.key, err)
		}
		views = append(views, view)
		if merged != nil && (now == nil || absent || !proto.Equal(merged, now)) {
			if err := tx.Put(merged); err != nil {
				return Merged{}, err
			}
		}
	}
	// A member row of this record that the record does not hold: with an
	// agreed state, the store deleted it when the agreed state held it, and
	// a client added it when not; with none, the record wins.
	var gone []rowID
	if held {
		gone = base.order
	} else {
		for _, msg := range in.Members {
			entity, key, err := c.srv.EntityKey(msg)
			if err != nil {
				return Merged{}, err
			}
			gone = append(gone, rowID{entity, key})
		}
	}
	var drops []rowDrop
	for _, id := range gone {
		if _, ok := cur[id]; !ok || decoded[id] {
			continue
		}
		drops = append(drops, rowDrop{id: id, record: in.Key})
	}
	payload, err := c.writePayload(dec.Payload, views)
	if err != nil {
		return Merged{}, err
	}
	return Merged{Payload: payload, drops: drops}, nil
}

// check validates the rows of dec, fills in the root key and the record
// fields, and returns the rows with the text values the record owns.
func (c *rowCodec) check(key string, dec *Decoded) ([]decodedRow, error) {
	if dec.Root == nil {
		return nil, fmt.Errorf("connect: Decode returned no root row")
	}
	entityOf := func(msg proto.Message) (*rowEntity, error) {
		if msg == nil {
			return nil, fmt.Errorf("connect: Decode returned a nil member row")
		}
		entity, _, err := c.srv.EntityKey(msg)
		if err != nil {
			return nil, fmt.Errorf("connect: Decode: %w", err)
		}
		e := c.byName[entity]
		if e == nil {
			return nil, fmt.Errorf("connect: Decode returned a %s row, which is not an entity of the record type", entity)
		}
		return e, nil
	}
	root, err := entityOf(dec.Root)
	if err != nil {
		return nil, err
	}
	if root != c.root {
		return nil, fmt.Errorf("connect: Decode returned a %s root row, not a %s", root.name, c.root.name)
	}
	if fd := root.keyField; fd != nil && dec.Root.ProtoReflect().Get(fd).String() == "" {
		dec.Root.ProtoReflect().Set(fd, protoreflect.ValueOfString(key))
	}
	out := make([]decodedRow, 0, 1+len(dec.Members))
	seen := make(map[rowID]bool, 1+len(dec.Members))
	add := func(e *rowEntity, msg proto.Message) error {
		_, k, err := c.srv.EntityKey(msg)
		if err != nil {
			return err
		}
		id := rowID{e.name, k}
		if seen[id] {
			return fmt.Errorf("connect: Decode returned %s %q twice", e.name, k)
		}
		seen[id] = true
		texts, err := e.recordTexts(dec.texts[msg])
		if err != nil {
			return err
		}
		out = append(out, decodedRow{ent: e, key: k, msg: msg, texts: texts})
		return nil
	}
	if _, k, err := c.srv.EntityKey(dec.Root); err != nil {
		return nil, err
	} else if k != key {
		return nil, fmt.Errorf("connect: Decode returned root row %q for record %q", k, key)
	}
	if err := add(root, dec.Root); err != nil {
		return nil, err
	}
	for _, msg := range dec.Members {
		e, err := entityOf(msg)
		if err != nil {
			return nil, err
		}
		if e.member == nil {
			return nil, fmt.Errorf("connect: Decode returned a %s member row, which is the root entity", e.name)
		}
		switch rk := e.member.recordKeyOf(msg); rk {
		case "":
			e.member.setRecordKey(msg, key)
		case key:
		default:
			return nil, fmt.Errorf("connect: Decode returned %s row in record %q for record %q", e.name, rk, key)
		}
		if _, k, err := c.srv.EntityKey(msg); err != nil {
			return nil, err
		} else if k == "" {
			return nil, fmt.Errorf("connect: Decode returned a %s row with no key", e.name)
		}
		if err := add(e, msg); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recordTexts returns the values of the text fields e owns, "" for one
// SetText did not set. A name that is not a text field is an error.
func (e *rowEntity) recordTexts(set map[string]string) (map[string]string, error) {
	desc := e.prototype.ProtoReflect().Descriptor()
	for name := range set {
		if fd := desc.Fields().ByName(protoreflect.Name(name)); fd == nil || !lww.IsText(fd) {
			return nil, fmt.Errorf("connect: SetText: %s has no text field %q", desc.FullName(), name)
		}
	}
	if len(e.texts) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(e.texts))
	for _, fd := range e.texts {
		out[string(fd.Name())] = set[string(fd.Name())]
	}
	return out, nil
}

// setRecordKey sets the member's record field in msg to key.
func (m *recordMember) setRecordKey(msg proto.Message, key string) {
	rm := msg.ProtoReflect()
	for i, fd := range m.path {
		if i == len(m.path)-1 {
			rm.Set(fd, protoreflect.ValueOfString(key))
			return
		}
		rm = rm.Mutable(fd).Message()
	}
}

// mergeRow merges one decoded row into the engine row now, against base,
// the row as the store held it in the agreed state, and alt, the row as an
// alternate agreed state holds it; base, alt and now can be nil. It returns
// the row to write, nil to leave the row as it is, and the row as the store
// holds it now, for the next agreed state.
//
// With two ancestors, a field the store holds as in either of them is not a
// store edit, and a text field merges against the one whose text is nearest
// the store's.
func (c *rowCodec) mergeRow(r decodedRow, base, alt, now proto.Message, peer string, clock uint64) (proto.Message, proto.Message, error) {
	e := r.ent
	view := proto.Clone(r.msg)
	held := base != nil || alt != nil
	var paths []string
	if base != nil {
		paths = e.ownedPaths(fieldDiff(base, r.msg))
	}
	if alt != nil {
		changed := e.ownedPaths(fieldDiff(alt, r.msg))
		if base != nil {
			changed = slices.DeleteFunc(changed, func(p string) bool { return !slices.Contains(paths, p) })
		}
		paths = changed
	}
	type textEdit struct {
		fd             protoreflect.FieldDescriptor
		value          string
		base, now      []byte
		changed, agree bool
	}
	edits := make([]textEdit, 0, len(e.texts))
	changed := len(paths) > 0
	for _, fd := range e.texts {
		t := textEdit{fd: fd, value: r.texts[string(fd.Name())], changed: true}
		if held {
			doc, was, err := nearestText(fd, t.value, base, alt)
			if err != nil {
				return nil, nil, err
			}
			t.base, t.changed = doc, was != t.value
		}
		if now != nil {
			t.now = now.ProtoReflect().Get(fd).Bytes()
			if t.changed {
				s, err := text.PlainText(t.now)
				if err != nil {
					return nil, nil, err
				}
				// Both sides hold the value already: a merge would add the
				// edit a second time.
				t.agree = s == t.value
			}
		}
		changed = changed || t.changed
		edits = append(edits, t)
	}
	if now == nil && held && !changed {
		// A client deleted the row, and the store did not change it since.
		for _, t := range edits {
			setBytes(view, t.fd, t.base)
		}
		return nil, view, nil
	}
	var merged proto.Message
	switch {
	case now != nil && held:
		merged = proto.Clone(now)
		applyFields(merged, r.msg, paths)
	case e.owned != nil:
		if now != nil {
			merged = proto.Clone(now)
		} else {
			merged = e.prototype.ProtoReflect().New().Interface()
		}
		applyFields(merged, r.msg, e.ownedUnits)
	default:
		merged = proto.Clone(r.msg)
	}
	for _, t := range edits {
		switch {
		case !t.changed:
			setBytes(view, t.fd, t.base)
			if now != nil {
				setBytes(merged, t.fd, t.now)
			} else {
				setBytes(merged, t.fd, t.base)
			}
		case t.agree:
			setBytes(view, t.fd, t.now)
			setBytes(merged, t.fd, t.now)
		default:
			src, out, err := mergeText(t.base, t.now, held, t.value, peer, clock)
			if err != nil {
				return nil, nil, err
			}
			setBytes(view, t.fd, src)
			setBytes(merged, t.fd, out)
		}
	}
	return merged, view, nil
}

// nearestText returns the document of the text field fd in base or alt,
// whichever is not nil and has the plain text nearest value, and that text.
// A tie goes to base.
func nearestText(fd protoreflect.FieldDescriptor, value string, base, alt proto.Message) ([]byte, string, error) {
	var (
		doc  []byte
		was  string
		dist = -1
	)
	for _, row := range []proto.Message{base, alt} {
		if row == nil {
			continue
		}
		b := row.ProtoReflect().Get(fd).Bytes()
		s, err := text.PlainText(b)
		if err != nil {
			return nil, "", err
		}
		limit := len(s) + len(value)
		if dist >= 0 {
			limit = dist - 1
		}
		if d := editDistance([]byte(s), []byte(value), limit); dist < 0 || d < dist {
			doc, was, dist = b, s, d
		}
	}
	return doc, was, nil
}

// nearer returns base or alt, whichever Encode makes into content nearer
// in.Record.Content. A tie goes to base.
func (c *rowCodec) nearer(in MergeInput, base, alt *agreedRows) (*agreedRows, error) {
	if c.encode == nil {
		return base, nil
	}
	content := func(a *agreedRows) ([]byte, error) {
		out, err := c.encode(EncodeInput{Key: in.Key, Locator: in.Base.Locator, Payload: a.app, Rows: c.sorted(a.rows)})
		return out.Content, err
	}
	b, err := content(base)
	if err != nil {
		return nil, err
	}
	a, err := content(alt)
	if err != nil {
		return nil, err
	}
	db := editDistance(b, in.Record.Content, len(b)+len(in.Record.Content))
	if editDistance(a, in.Record.Content, db-1) < db {
		return alt, nil
	}
	return base, nil
}

// sorted returns rows with the members in the order of RecordType.Members,
// and in key order within one entity, as Encode gets them.
func (c *rowCodec) sorted(rows RecordRows) RecordRows {
	type keyed struct {
		order int
		key   string
		msg   proto.Message
	}
	all := make([]keyed, 0, len(rows.Members))
	for _, msg := range rows.Members {
		entity, key, _ := c.srv.EntityKey(msg)
		order := 0
		if e := c.byName[entity]; e != nil {
			order = e.order
		}
		all = append(all, keyed{order, key, msg})
	}
	slices.SortFunc(all, func(a, b keyed) int {
		if a.order != b.order {
			return a.order - b.order
		}
		return strings.Compare(a.key, b.key)
	})
	out := RecordRows{Root: rows.Root, Members: make([]proto.Message, len(all))}
	for i, k := range all {
		out.Members[i] = k.msg
	}
	return out
}

// mergeText applies the store's text value to a document. With an agreed
// state (held), the value is diffed against the agreed document base; else
// against the engine's document now, so the store wins the characters it
// differs in. It returns the document the store holds now and that document
// unioned with now, the engine's next document. The edit takes counters
// above clock (see MergeInput.TextClock).
//
// server.ApplyTextStrings does the same and returns the union alone. The
// document before the union is the next merge's ancestor: its text is the
// store's, and the union holds all its items, so an engine edit the store
// has not received yet is not taken for a deletion by the store.
func mergeText(base, now []byte, held bool, value, peer string, clock uint64) (src, merged []byte, err error) {
	if peer == "" {
		return nil, nil, fmt.Errorf("connect: text strings require a writer peer")
	}
	ancestor := now
	if held {
		ancestor = base
	}
	if len(ancestor) == 0 && len(now) == 0 && value == "" {
		return nil, nil, nil
	}
	doc, err := text.Decode(ancestor, peer)
	if err != nil {
		return nil, nil, err
	}
	var nowDoc *text.Doc
	if len(now) > 0 {
		if nowDoc, err = text.Decode(now, ""); err != nil {
			return nil, nil, err
		}
		// Edits must take counters past those now holds, which can be
		// this peer's own later edits.
		doc.ObserveVersion(nowDoc.VersionVector())
	}
	if doc.PlainText() != value {
		doc.ObserveVersion(map[string]uint64{peer: clock})
		if err := doc.SetString(value); err != nil {
			return nil, nil, err
		}
	}
	src = doc.Encode()
	if nowDoc == nil {
		return src, src, nil
	}
	if err := doc.MergeDocs(nowDoc); err != nil {
		return nil, nil, err
	}
	return src, doc.Encode(), nil
}

func setBytes(msg proto.Message, fd protoreflect.FieldDescriptor, b []byte) {
	if len(b) == 0 {
		msg.ProtoReflect().Clear(fd)
		return
	}
	msg.ProtoReflect().Set(fd, protoreflect.ValueOfBytes(b))
}

func (c *rowCodec) Render(_ *server.EntityTx, in RenderInput) (Rendered, bool, error) {
	if c.encode == nil {
		return Rendered{}, false, fmt.Errorf("connect: RowCodec has no Encode")
	}
	base, err := c.readPayload(in.Base)
	if err != nil {
		return Rendered{}, false, err
	}
	var locator string
	if in.Base != nil {
		locator = in.Base.Locator
	}
	rows := RecordRows{Root: in.Root, Members: in.Members}
	out, err := c.encode(EncodeInput{Key: in.Key, Locator: locator, Payload: base.app, Rows: rows})
	if err != nil {
		return Rendered{}, false, err
	}
	all := make([]proto.Message, 0, 1+len(in.Members))
	all = append(append(all, in.Root), in.Members...)
	if out.Payload, err = c.writePayload(out.Payload, all); err != nil {
		return Rendered{}, false, err
	}
	return out, true, nil
}
