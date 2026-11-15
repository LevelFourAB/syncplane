package connect

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/server"
)

// Records configures record mode, where one external record syncs with a
// group of rows: one root row and the member rows that name it. A record is
// what the external store reads and writes whole, such as a file, a JSON
// document or an API resource. Set it as Config.Records.
//
// The record key is the key of the root row and does not change. The locator
// is the record's address in the store, such as a path, and can change on
// either side.
//
// All bookkeeping is in the engine's store, under the consumer
// "connect:<WriterID>": the feed position, one shadow for each record, an
// index from member row to record, an index from locator to record, the
// records that wait out Quiet, and the records that wait out DeleteGrace. It
// is backed up and restored with the rows. Garbage collection does not remove
// it (see server.ConsumerUpdate.Retain); server.ResetConsumer does.
type Records struct {
	// Types are the record types. Each entity can be the root or a member of
	// one type only.
	Types []RecordType
	// FirstPass decides which side wins when a workspace has no record-mode
	// bookkeeping yet. The zero value is SourceWins.
	FirstPass FirstPass
	// ChooseFirstPass, when set, decides the policy for one workspace and
	// overrides FirstPass. It is called at the start of each first pass of
	// the workspace, also again after a first pass that failed, and never on
	// a later pass. It can be called for different workspaces at the same
	// time. An error, or a value that is not SourceWins or EngineWins, fails
	// the pass before it reads the store or writes a row. Nil uses FirstPass
	// for every workspace.
	ChooseFirstPass func(ctx context.Context, workspaceID string) (FirstPass, error)
	// Quiet is how long a record must have no engine change before egress
	// writes it to the store, so that a run of edits makes one write. 0 means
	// the default (1 second); negative writes at the next pass.
	Quiet time.Duration
	// DeleteGrace is how long a record must be missing from the store before
	// its rows are deleted. A store record that comes back in this time keeps
	// its rows, and a new record with the fingerprint of a missing one is a
	// rename of it. 0 means the default (2 seconds); negative deletes at the
	// end of the pass that found it missing.
	DeleteGrace time.Duration
	// BatchRows bounds the rows that one ingest transaction merges across
	// records, so that the import of many small records takes the workspace
	// write lock for a bounded time. A record with more rows is merged alone.
	// 0 means the default (1000). Config.IngestChunk and Config.IngestPause
	// count these transactions.
	BatchRows int
	// Overwrite is called before a first pass with EngineWins replaces a
	// store record with other content, so that the adopter can keep a copy
	// of it. An error stops the write and fails the pass. Nil writes without
	// a call.
	//
	// After the first pass, egress never replaces a store record the
	// connector does not know: it ingests it as a new record, and the engine
	// record whose locator it holds waits until the locator is free.
	Overwrite func(ctx context.Context, o Overwrite) error
}

// FirstPass is the policy of the first pass of a workspace in record mode.
// Records.FirstPass sets it for every workspace, and Records.ChooseFirstPass
// for each workspace.
//
// The first pass runs when the consumer "connect:<WriterID>" has no position
// in the workspace: on the first sync, and after server.ResetConsumer. A
// connector that did not run for longer than the tombstone retention (see
// server.WithGC) keeps its position and marks every record for egress; it
// does not run the first pass again.
type FirstPass int

const (
	// SourceWins imports the store: each record is merged with no base, and
	// root rows and member rows that no store record holds are deleted. When
	// the codec refuses a record, the pass fails and deletes no row.
	SourceWins FirstPass = iota
	// EngineWins rebuilds the store from the rows: each root row is rendered
	// and written to the store, and Records.Overwrite gets each store record
	// it replaces with other content. A store record with no rows is ingested
	// as a new record. No row is deleted. When the codec cannot render a root
	// row, the pass fails before it ingests a store record.
	EngineWins
)

// Overwrite describes a store record that an egress write is about to
// replace. Old and New are not retained after the call returns.
type Overwrite struct {
	WorkspaceID string
	// Entity is the root entity of the record type.
	Entity  string
	Key     string
	Locator string
	// Old is the content the store holds now, New the content that replaces
	// it.
	Old []byte
	New []byte
}

// RecordType binds one root entity and its member entities to a store and a
// codec.
type RecordType struct {
	// Root is a prototype of the root entity. Its row key is the record key.
	Root proto.Message
	// Members are the entities whose rows belong to a record. A type with no
	// members syncs one row as one record.
	Members []Member
	// Store reads and writes the records of every workspace.
	Store RecordStore
	// Codec maps records to rows and rows to records.
	Codec Codec
	// Direction is Bidirectional (the default) or IngressOnly. An IngressOnly
	// type never writes to the store: engine edits stay in the engine, and
	// ingest does not render the record.
	Direction Direction
}

// Member is one member entity of a record type.
type Member struct {
	// Prototype is a prototype of the member entity.
	Prototype proto.Message
	// RecordField is the dotted path of the singular string field that holds
	// the key of the record's root row, such as "page_id", or "page.id" for a
	// reference. The connector finds the members of a record with a query on
	// this field, so declare an index on it for records with many rows.
	RecordField string
}

// RecordRef addresses one record in a store.
type RecordRef struct {
	// Key is the record key, when the store knows it, such as an id the
	// content holds. The connector uses it for a locator it does not know
	// yet, so that a record the store moved keeps its key; a locator it
	// knows keeps the key of its record. Empty finds the key of a renamed
	// record by Fingerprint, or gives a new record a new key.
	Key     string
	Locator string
	// Fingerprint is a stamp of the content, such as a content hash. Equal
	// content must give an equal fingerprint, in List, Read and Write: it is
	// how the connector tells its own writes from edits. A content hash also
	// lets the connector find a renamed record by its content.
	Fingerprint string
}

// Record is one record and its content.
type Record struct {
	RecordRef
	Content []byte
}

// ErrRecordNotFound is the error a RecordStore wraps when a record does not
// exist.
var ErrRecordNotFound = errors.New("connect: record not found")

// RecordStore is the adopter's port over the records of one record type. The
// connector delivers at least once, so each method must be idempotent.
// Every method takes the workspace id, so one store serves all workspaces.
// The connector calls the methods of one workspace from one goroutine at a
// time, but calls for different workspaces can run at the same time.
type RecordStore interface {
	// List yields each record of the type in the workspace, in any order. It
	// must be cheap: the connector reads the content only of the records
	// whose fingerprint changed.
	List(ctx context.Context, workspaceID string, fn func(RecordRef) error) error
	// Read returns the record at locator. It returns an error that wraps
	// ErrRecordNotFound when there is none, also for a locator of another
	// record type.
	Read(ctx context.Context, workspaceID, locator string) (Record, error)
	// Write puts rec.Content at rec.Locator, replacing what is there, and
	// returns the fingerprint of the content as stored. from is the old
	// locator when the record moved, which the store removes; else from is
	// empty.
	Write(ctx context.Context, workspaceID string, rec Record, from string) (fingerprint string, err error)
	// Delete removes the record at locator. Removing a missing record is not
	// an error.
	Delete(ctx context.Context, workspaceID, locator string) error
}

// Codec maps the records of one record type to rows and back. Its methods run
// inside a trusted write transaction, under the workspace write lock, so keep
// them free of I/O. The transaction can be retried, so the methods must give
// the same result for the same input and rows. A Codec is shared by all
// workspaces and must be safe for concurrent use.
//
// An error from Merge or Render rolls back the transaction: no row and no
// shadow of the record changes. The connector logs the error, skips the
// record, and tries it again when it next sees a change of it.
type Codec interface {
	// Merge applies a store record to the rows of the record with key
	// in.Key, through tx. With a base, it applies the changes from the base
	// to the record, and keeps engine edits the record did not change. With
	// no base, the record is new to the connector, and Merge makes the rows
	// equal to it.
	//
	// A replay of a merge that committed, with the old base, must write
	// nothing. Write a row only when it changes.
	Merge(tx *server.EntityTx, in MergeInput) (Merged, error)
	// Render returns the record's native form from its rows. ok false means
	// the record has no native form, and the connector deletes it from the
	// store. Render can write rows, for example to give new rows a key the
	// native form holds.
	Render(tx *server.EntityTx, in RenderInput) (out Rendered, ok bool, err error)
}

// MergeInput is the input of Codec.Merge. The messages are rows read in the
// transaction; Merge can change them.
type MergeInput struct {
	// Key is the record key: the key of the root row the record merges into.
	Key    string
	Record Record
	// Base is the state the store and the engine last agreed on; nil when the
	// connector has no shadow of the record.
	Base *Agreed
	// Root is the root row; nil when there is none.
	Root proto.Message
	// Members are the member rows of the record, of each member entity in
	// the order of RecordType.Members, in key order.
	Members []proto.Message
	// TextPeer is the collaborative-text peer to write text under, for
	// example with server.ApplyTextStrings. See Config.TextPeer.
	TextPeer string
}

// Merged is the result of Codec.Merge.
type Merged struct {
	// Key is the key of the root row the record merged into, when Merge
	// matched a new record to an existing root row with another key. Empty
	// keeps MergeInput.Key. A record with a base cannot change its key.
	Key string
	// Payload is the codec's data for the agreed state, such as the row key
	// of each part of the record. The connector stores it with the shadow and
	// gives it back as Agreed.Payload.
	Payload []byte
}

// RenderInput is the input of Codec.Render.
type RenderInput struct {
	Key  string
	Base *Agreed
	// Root is the root row. Render is not called when there is none: the
	// record is then deleted from the store.
	Root proto.Message
	// Members are the member rows, as in MergeInput.
	Members []proto.Message
}

// Rendered is the native form of a record.
type Rendered struct {
	// Locator is where the record goes. Empty keeps the locator of the base;
	// a record with no base must have one.
	Locator string
	Content []byte
	// Payload is the codec's data for this content; see Merged.Payload.
	Payload []byte
}

// Agreed is the state of a record that the store and the engine last agreed
// on.
type Agreed struct {
	Locator     string
	Fingerprint string
	Payload     []byte
}

// recordType is one validated RecordType.
type recordType struct {
	root      proto.Message
	rootName  string
	members   []*recordMember
	store     RecordStore
	codec     Codec
	direction Direction
}

// recordMember is one validated Member.
type recordMember struct {
	prototype proto.Message
	entity    string
	field     string
	path      []protoreflect.FieldDescriptor
}

// records is the validated record-mode configuration.
type records struct {
	types     []*recordType
	byEntity  map[string]*recordType
	memberOf  map[string]*recordMember
	firstPass FirstPass
	choose    func(ctx context.Context, workspaceID string) (FirstPass, error)
	quiet     time.Duration
	grace     time.Duration
	batchRows int
	overwrite func(ctx context.Context, o Overwrite) error
}

// newRecords validates cfg.Records against cfg.
func newRecords(cfg Config) (*records, error) {
	r := cfg.Records
	if cfg.Source != nil || cfg.OpenSource != nil || len(cfg.Entities) > 0 || len(cfg.Bindings) > 0 {
		return nil, fmt.Errorf("connect: Records cannot be used with Source, OpenSource, Entities or Bindings")
	}
	if cfg.WriterID == "" {
		return nil, fmt.Errorf("connect: Records requires a WriterID")
	}
	if cfg.State != nil {
		return nil, fmt.Errorf("connect: Records keeps its bookkeeping in the engine's store and takes no State")
	}
	if len(r.Types) == 0 {
		return nil, fmt.Errorf("connect: Records needs at least one type")
	}
	out := &records{
		byEntity:  make(map[string]*recordType),
		memberOf:  make(map[string]*recordMember),
		firstPass: r.FirstPass,
		choose:    r.ChooseFirstPass,
		quiet:     r.Quiet,
		grace:     r.DeleteGrace,
		batchRows: r.BatchRows,
		overwrite: r.Overwrite,
	}
	if out.quiet == 0 {
		out.quiet = time.Second
	}
	if out.grace == 0 {
		out.grace = 2 * time.Second
	}
	if out.batchRows <= 0 {
		out.batchRows = 1000
	}
	if out.firstPass != SourceWins && out.firstPass != EngineWins {
		return nil, fmt.Errorf("connect: unknown FirstPass %d", out.firstPass)
	}
	claim := func(prototype proto.Message, rt *recordType) (string, error) {
		entity, _, err := cfg.Server.EntityKey(prototype)
		if err != nil {
			return "", fmt.Errorf("connect: record type %T: %w", prototype, err)
		}
		if _, dup := out.byEntity[entity]; dup {
			return "", fmt.Errorf("connect: entity %q bound twice", entity)
		}
		out.byEntity[entity] = rt
		return entity, nil
	}
	for i, t := range r.Types {
		if t.Root == nil || t.Store == nil || t.Codec == nil {
			return nil, fmt.Errorf("connect: record type %d needs Root, Store and Codec", i)
		}
		if t.Direction != Bidirectional && t.Direction != IngressOnly {
			return nil, fmt.Errorf("connect: record type %d: unknown Direction %d", i, t.Direction)
		}
		rt := &recordType{root: t.Root, store: t.Store, codec: t.Codec, direction: t.Direction}
		name, err := claim(t.Root, rt)
		if err != nil {
			return nil, err
		}
		rt.rootName = name
		for _, m := range t.Members {
			if m.Prototype == nil {
				return nil, fmt.Errorf("connect: record type %q has a member with no Prototype", name)
			}
			entity, err := claim(m.Prototype, rt)
			if err != nil {
				return nil, err
			}
			path, err := recordFieldPath(m.Prototype.ProtoReflect().Descriptor(), m.RecordField)
			if err != nil {
				return nil, fmt.Errorf("connect: member %q: %w", entity, err)
			}
			rm := &recordMember{prototype: m.Prototype, entity: entity, field: m.RecordField, path: path}
			rt.members = append(rt.members, rm)
			out.memberOf[entity] = rm
		}
		out.types = append(out.types, rt)
	}
	return out, nil
}

// recordFieldPath resolves a dotted path of singular fields that ends in a
// string field.
func recordFieldPath(md protoreflect.MessageDescriptor, path string) ([]protoreflect.FieldDescriptor, error) {
	if path == "" {
		return nil, fmt.Errorf("RecordField is required")
	}
	parts := strings.Split(path, ".")
	out := make([]protoreflect.FieldDescriptor, 0, len(parts))
	for i, part := range parts {
		fd := md.Fields().ByName(protoreflect.Name(part))
		if fd == nil || fd.Cardinality() == protoreflect.Repeated {
			return nil, fmt.Errorf("RecordField %q: %s has no singular field %q", path, md.FullName(), part)
		}
		out = append(out, fd)
		if i == len(parts)-1 {
			if fd.Kind() != protoreflect.StringKind {
				return nil, fmt.Errorf("RecordField %q is not a string field", path)
			}
			break
		}
		if fd.Kind() != protoreflect.MessageKind {
			return nil, fmt.Errorf("RecordField %q: %q is not a message field", path, part)
		}
		md = fd.Message()
	}
	return out, nil
}

// recordKeyOf returns the value of the member's RecordField in msg, or "" when
// a message on the path is not set.
func (m *recordMember) recordKeyOf(msg proto.Message) string {
	rm := msg.ProtoReflect()
	for i, fd := range m.path {
		if i == len(m.path)-1 {
			return rm.Get(fd).String()
		}
		if !rm.Has(fd) {
			return ""
		}
		rm = rm.Get(fd).Message()
	}
	return ""
}
