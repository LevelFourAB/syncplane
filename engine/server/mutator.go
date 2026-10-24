package server

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// MutatorFunc is a named server-authoritative mutation. It runs inside the
// push write transaction; every row it writes is stamped with the
// mutation's sequence. Return Reject(...) for a business "no".
//
// Any other error is read as infrastructure: the whole batch fails as
// CodeInternal, nothing in it is acked, and the client resends. Returning one
// for a verdict that cannot change — a validation failure, an unhandled case —
// therefore costs the client a retry loop that ends in the mutation being
// dropped to its dead-letter surface, so it reaches the app as a lost write
// rather than as the "no" it was.
//
// args follows the registration, not the caller: a mutator registered with a
// prototype always receives a non-nil message of that type — zeroed when the
// invocation carried none — and one registered without a prototype always
// receives nil. That holds on every entry point (a client push, Invoke,
// InvokeIdempotent), so a body may type-assert its prototype unconditionally,
// or be declared through Mutator, which does the assertion.
type MutatorFunc func(ctx context.Context, tx *EntityTx, args proto.Message) error

type mutatorInfo struct {
	name string
	args proto.Message // prototype; cloned per invocation
	fn   MutatorFunc
	// access is the MutatorAccess the declaration carried, nil when none.
	access *MutatorAccess
}

// pendingMutator is one WithMutator declaration, held until New has built the
// server it belongs to.
type pendingMutator struct {
	name string
	args proto.Message
	fn   MutatorFunc
	opts []MutatorOption
}

// MutatorOption adjusts one WithMutator declaration.
type MutatorOption interface {
	applyMutator(*mutatorInfo)
}

// MutatorAccess states which callers may invoke a mutator in the access model:
// every caller admitted to the workspace, or the callers that hold a
// permission where the access names.
//
// With Admitted set, every caller admitted to the workspace may invoke the
// mutator, and Permission, Entity and KeyField stay empty. With Entity and
// KeyField set, the caller must hold Permission at the row of
// Entity whose key the KeyField of the args holds: at the row itself, at the
// row its access parent points at, or at the workspace root. A row that does
// not exist refuses the invocation. With both empty, the caller must hold
// Permission at the workspace root.
//
// With TrustedOnly set, only a Trusted caller may invoke the mutator, and the
// other fields stay empty. This is also what a mutator without a MutatorAccess
// gets, but registration logs a warning for such a mutator and not for one
// that sets TrustedOnly.
//
// In a schema that uses the access model, an invocation of a mutator without a
// MutatorAccess is refused for every caller who is not Trusted. The check runs
// inside the write transaction before the body, and is refused with
// CodeAccessDenied. The rows the body writes are not checked unless it
// writes them through EntityTx.AsCaller; the body can also ask EntityTx.Can.
//
// Registration fails, with the rest of the access model, when Admitted or
// TrustedOnly is set together with another field, Permission is granted by no role, Entity is not
// in the model, or KeyField is not a singular string field of the args. In a
// schema without the access model, it has no effect.
//
//	server.Mutator("complete_task", completeTask,
//		server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "task_id"})
//	server.Mutator("send_feedback", sendFeedback,
//		server.MutatorAccess{Admitted: true})
//	server.WithMutator("rebuild_index", nil, rebuildIndex,
//		server.MutatorAccess{TrustedOnly: true})
type MutatorAccess struct {
	Permission  string
	Entity      string
	KeyField    string
	Admitted    bool
	TrustedOnly bool
}

func (ma MutatorAccess) applyMutator(mi *mutatorInfo) {
	mi.access = &ma
}

// WithMutator declares a custom mutation handler. argsPrototype defines the
// expected google.protobuf.Any payload type; pass nil for a mutator that takes
// no arguments. Mutator declares one whose fn receives the arguments typed.
//
// Mutators are construction-time only: they are funcs, so unlike a schema they
// cannot be provisioned at runtime (see DynamicEntity, the CRUD-only dynamic
// tier). That is also what keeps the mutator table lock-free on the push path
// — it never changes after New.
//
// opts adjust the declaration; see MutatorAccess.
func WithMutator(name string, argsPrototype proto.Message, fn MutatorFunc, opts ...MutatorOption) Option {
	return func(s *Server) error {
		s.pendingMutators = append(s.pendingMutators, pendingMutator{name: name, args: argsPrototype, fn: fn, opts: opts})
		return nil
	}
}

// registerMutator installs a WithMutator declaration. See WithMutator.
func (s *Server) registerMutator(name string, argsPrototype proto.Message, fn MutatorFunc, opts ...MutatorOption) error {
	if name == "" || fn == nil {
		return fmt.Errorf("server: mutator needs a name and a func")
	}
	if _, dup := s.mutators[name]; dup {
		return fmt.Errorf("server: mutator %q already registered", name)
	}
	mi := &mutatorInfo{name: name, args: argsPrototype, fn: fn}
	for _, opt := range opts {
		opt.applyMutator(mi)
	}
	s.mutators[name] = mi
	s.log().LogAttrs(context.Background(), slog.LevelInfo, "mutator registered",
		slog.String("component", "schema"), slog.String("mutator", name))
	return nil
}

// EntityTx is the typed storage facade mutators run against. Entity identity
// comes from the message type passed in; keys derive from key_fields.
type EntityTx struct {
	srv *Server
	// ctx is the caller's context, carried so the write path can log against
	// it (quarantine reports) without threading it through every method.
	ctx context.Context
	// ws is the workspace this transaction writes to. Carried because the
	// storage transaction does not expose it and per-row caching needs it to
	// tell two tenants' identically-keyed rows apart.
	ws string
	tx storage.WriteTx
	// actor attributes every row this tx writes (see storage.Actor). Resolved
	// once, where the transaction is opened, because the ctx cannot change
	// under it — and because a mutator body must not be able to write rows
	// attributed to someone other than the caller who invoked it.
	actor storage.Actor
	// seq stamps every row this tx writes. The push path allocates it
	// eagerly (one sequence per client mutation, applied or not is decided
	// by the savepoint); Server.Mutate leaves it 0 so the first write
	// allocates lazily and a no-write call consumes no sequence.
	seq uint64
	// wrote reports whether Put/Delete ran; Mutate notifies only if so.
	wrote bool
	// authz, when non-nil, authorizes each write before it is applied — the
	// per-write CanMutate that Server.Mutate runs for a caller who is not
	// Trusted. It is nil on every path that already authorized elsewhere: the
	// push path (which authorizes the whole envelope before the transaction
	// opens), Apply (which authorizes its buffered intents between the read and
	// write transactions), and mutator bodies (whose rows are covered by the
	// Write{Custom} verdict on the invocation, exactly as on push).
	//
	// It runs INSIDE the write transaction, which is the one place the engine
	// consults a permission hook under a lock — see Server.Mutate for why that
	// is confined to this path.
	authz func(Write) error
	// gate, when non-nil, is the request's write gate: Put takes the stored
	// value for every protected field rather than the one the caller submitted,
	// and every write to a live row passes the row check first. It is nil
	// wherever authz is, and additionally nil for a mutator body — whose rows
	// the Write{Custom} verdict covers — while Apply sets it with no authz,
	// having authorized its intents in an earlier pass.
	gate *writeGate
	// access is what the transaction's caller holds in the access model: the
	// enforced caller's, or for a mutator body the invoking caller's. Nil for
	// a Trusted transaction and in a schema without the model. Can answers
	// from it, and a write that can change it drops what it resolved.
	access *txAccess
	// caller, when non-nil, is the invoking caller's write gate, which the
	// writes of AsCaller's view pass. Set only on a mutator body invoked by a
	// caller who is not Trusted.
	caller *writeGate
	// root is the transaction an AsCaller view writes for, nil on the
	// transaction itself. The view stamps its writes with root's sequence and
	// marks root as written, so that the caller of the body sees every write.
	root *EntityTx
	// view is the AsCaller view, kept so that each call returns the same one.
	view *EntityTx
	// writes counts the writes of the transaction, on the transaction itself
	// and not on its view. Reads that apply the caller's read gate bind the
	// caller's access again after a write, which can change what it reads.
	writes uint64
	// reads is what the reads of a transaction with a write gate apply, nil
	// until its first read and on every transaction without one.
	reads *callerReads
}

// callerReads is the caller's read gate (see Server.readGate) that the reads
// of a transaction with a write gate apply. writes is the transaction's write
// count when the gate was bound.
type callerReads struct {
	visible func(storage.Row) (bool, error)
	project rowProjector
	refuse  specRefuser
	writes  uint64
}

// unfilteredReads is the callerReads of a transaction without a write gate:
// it filters and refuses nothing.
var unfilteredReads = &callerReads{}

// callerReadGate returns what this transaction's reads apply. A transaction
// with a write gate reads as its caller: an AsCaller view, and a
// Server.Mutate for a caller who is not Trusted. Its reads apply the caller's
// read rules, guards and ReadFilter, bound on the first read and again after
// the transaction writes. Every other transaction reads unfiltered.
func (e *EntityTx) callerReadGate() (*callerReads, error) {
	if e.gate == nil {
		return unfilteredReads, nil
	}
	owner := e
	if e.root != nil {
		owner = e.root
	}
	writes := owner.writes
	if e.reads != nil && e.reads.writes == writes {
		return e.reads, nil
	}
	adm := admission{principal: e.gate.p, creator: e.gate.creator}
	if a := e.gate.accessOf(); a != nil {
		// The transaction reached the gate's verdict before the body ran, so
		// none is left for the bind.
		adm.caller, adm.access = a.caller, a.admitted
	}
	visible, project, refuse, bind, err := e.srv.readGate(e.ctx, e.ws, adm, true)
	if err != nil {
		return nil, err
	}
	if err := bind(e.tx); err != nil {
		return nil, err
	}
	e.reads = &callerReads{visible: visible, project: project, refuse: refuse, writes: writes}
	return e.reads, nil
}

// AsCaller returns a view of this transaction whose writes are checked as the
// same writes sent by the invoking caller's client would be: by the access
// rules, the field write policies and the application's row write filters.
// A refused write returns a Rejection, with CodeAccessDenied for the access
// rules and CodePermissionDenied for the others, and writes nothing; the body
// decides whether to return it and fail the invocation.
// Permissions.CanMutate does not run for these writes, since the verdict on
// the invocation covers them.
//
// Reads through the view read as the invoking caller's client would: a row the
// caller may not read reads as absent from Get, GetMeta, Scan, ScanPage and
// Query, and each field a field guard hides from the caller reads as its zero
// value. A Query whose predicate or order names a field that a guard hides
// from the caller on some row fails with code permission_denied. The reads
// see the transaction's own earlier writes, grants included. Reads through a
// mutator body's own transaction are not filtered.
//
// Can and Seq answer as on this transaction, and writes through either one
// share its sequence. In a transaction whose writes are already checked, such
// as one Server.Mutate runs for a caller who is not Trusted, AsCaller returns
// this transaction itself, whose reads are filtered for that caller as above.
// In a Trusted transaction, AsCaller returns this transaction itself, whose
// reads are not filtered.
//
// A write or a read through the view can call the application's WriteFilter,
// ReadFilter and FieldPermissions hooks inside the transaction. The first read
// through the view, and the first after a write, calls ReadFilter once for
// each registered entity.
//
//	func completeTask(ctx context.Context, tx *server.EntityTx, args proto.Message) error {
//		id := args.(*taskv1.CompleteTaskArgs).GetTaskId()
//		return tx.AsCaller().Patch(&taskv1.Task{Id: id, Done: true}, []string{"done"}, nil)
//	}
func (e *EntityTx) AsCaller() *EntityTx {
	if e.caller == nil || e.gate != nil {
		return e
	}
	if e.view == nil {
		view := *e
		view.gate, view.caller, view.root, view.view = e.caller, nil, e, nil
		e.view = &view
	}
	return e.view
}

// authorizeCrud runs the per-write hook for one CRUD write, building the same
// CrudMutation the wire path would have carried so a policy cannot tell an
// in-process write from a client's. A nil authz (every already-authorized path)
// costs nothing; when it is set, PUT and PATCH pay one extra marshal of the
// submitted value, which is the price of handing the policy identical input.
//
// It authorizes what the caller SUBMITTED, before any merge — the value bytes
// and mask a client would have sent — not the merged result the row ends up
// holding. That is what push authorizes too.
func (e *EntityTx) authorizeCrud(
	info *entityInfo,
	key string,
	op syncplanev1.CrudOp,
	msg proto.Message,
	mask []string,
	expected *uint64,
) error {
	if e.authz == nil {
		return nil
	}
	w, err := writeOf(info, key, op, msg, mask, expected)
	if err != nil {
		return err
	}
	return e.authz(w)
}

// writeOf renders an in-process CRUD write as the Write a client push would
// have carried: the submitted value and mask, before any merge.
func writeOf(
	info *entityInfo,
	key string,
	op syncplanev1.CrudOp,
	msg proto.Message,
	mask []string,
	expected *uint64,
) (Write, error) {
	crud := &syncplanev1.CrudMutation{
		Entity:          info.name,
		Key:             key,
		Op:              op,
		ExpectedVersion: expected,
	}
	if msg != nil {
		value, err := proto.Marshal(msg)
		if err != nil {
			return Write{}, err
		}
		crud.Value = value
	}
	if len(mask) > 0 {
		crud.FieldMask = &fieldmaskpb.FieldMask{Paths: mask}
	}
	return Write{Crud: crud}, nil
}

// liveRow reads the stored row for key and reports whether it is live: absent
// and tombstoned read alike, as everywhere a write consults the row.
func (e *EntityTx) liveRow(info *entityInfo, key string) (storage.Row, bool, error) {
	row, err := e.tx.GetRow(info.name, key)
	if err != nil && err != storage.ErrNotFound {
		return storage.Row{}, false, err
	}
	return row, err == nil && !row.Deleted, nil
}

// Workspace returns the workspace this tx writes to. A mutator body that
// keeps state of its own outside the store — a cache, a counter, a permission
// epoch — uses it to partition that state the way every row is partitioned.
func (e *EntityTx) Workspace() string {
	return e.ws
}

// Seq returns the workspace sequence this tx's writes are stamped with, or
// 0 before the first write (Server.Mutate allocates lazily). Trusted callers
// use it to record which sequence their writes landed at — e.g. a connector
// stamping its bookkeeping so it can later recognize its own writes in the
// change feed.
func (e *EntityTx) Seq() uint64 {
	if e.root != nil {
		return e.root.Seq()
	}
	if !e.wrote {
		return 0
	}
	return e.seq
}

// writeSeq returns the sequence for a write, allocating it on first use
// when the tx was created without one (Server.Mutate).
func (e *EntityTx) writeSeq() (uint64, error) {
	if e.root != nil {
		return e.root.writeSeq()
	}
	if e.seq == 0 {
		seq, err := e.tx.NextSeq()
		if err != nil {
			return 0, err
		}
		e.seq = seq
	}
	e.wrote = true
	e.writes++
	return e.seq, nil
}

func (e *EntityTx) info(msg proto.Message) (*entityInfo, error) {
	return entityInfoFor(e.srv, msg)
}

// Get loads the row with the given key into dst. Returns false if the row
// does not exist (or is deleted). Through an AsCaller view, and in a
// Server.Mutate for a caller who is not Trusted, it also returns false for a
// row the caller may not read and clears the fields a guard hides from the
// caller (see AsCaller).
func (e *EntityTx) Get(dst proto.Message, key string) (bool, error) {
	info, err := e.info(dst)
	if err != nil {
		return false, err
	}
	g, err := e.callerReadGate()
	if err != nil {
		return false, err
	}
	found, _, err := readRowVisible(e.tx, info, dst, key, g.visible, g.project)
	return found, err
}

// GetMeta is Get, additionally reporting what the engine recorded about the
// row's last write (see RowMeta). A missing or deleted row returns
// (false, RowMeta{}, nil).
//
// It is the read a per-row ownership rule — "only the author may edit their own
// comment", which no write policy can express — is built on, because the actor
// comes from the engine's own attribution rather than a field the writer filled
// in:
//
//	found, meta, err := tx.GetMeta(&comment, key)
//	if err != nil {
//		return err
//	}
//	if !found {
//		return Reject(CodeConflict, "comment is gone")
//	}
//	caller, _ := PrincipalFrom(ctx)
//	if caller.IsAnonymous() || meta.Actor.Subject != caller.Subject {
//		return Reject(CodePermissionDenied, "not your comment")
//	}
//
// Refuse the anonymous caller before comparing, as above: an unattributed row
// and an unauthenticated caller both carry an empty Subject, so equality alone
// reads as a match.
//
// Attribution follows the row's last write, so a row its author created and a
// moderator later edited reports the moderator. Where the rule is about who
// created a row, the entity has to carry that itself.
func (e *EntityTx) GetMeta(dst proto.Message, key string) (found bool, meta RowMeta, err error) {
	info, err := e.info(dst)
	if err != nil {
		return false, RowMeta{}, err
	}
	g, err := e.callerReadGate()
	if err != nil {
		return false, RowMeta{}, err
	}
	return readRowMeta(e.tx, info, dst, key, g.visible, g.project)
}

// Put stores msg as a full-row snapshot; the key derives from key_fields.
//
// Exception to "PUT replaces whole rows": collaborative-text fields
// ((syncplane.v1.field_behavior).text) merge with the stored row instead. A Put
// value carries the writer's view of the document; replacing wholesale would
// silently drop every concurrent edit the writer had not seen, so Put reads
// the live row and unions each text field (when both sides carry one) before
// writing. Every other field keeps replace semantics.
//
// Text fields can also be written from plain strings — WithTextString plus
// WithTextPeer — which diffs the string against the stored document under the
// given writer identity, so a whole-value write turns into minimal edits that
// merge with concurrent work (see ApplyTextStrings).
//
// A text field of msg holding a plain-string value (text.EncodePlainString,
// which is what a field-history record's before-state carries) is resolved the
// same way, so restoring a record needs no option naming its fields. It still
// requires WithTextPeer: without one the write fails rather than writing under
// an invented identity.
//
// WithExpectedVersion makes the write a compare-and-swap and is also how it
// states create-or-update intent; without one the write is unguarded and
// follows arrival order like any other.
func (e *EntityTx) Put(msg proto.Message, opts ...WriteOption) error {
	o, err := collectWriteOpts(opts)
	if err != nil {
		return err
	}
	info, err := e.info(msg)
	if err != nil {
		return err
	}
	if msg, err = info.adopt(msg); err != nil {
		return err
	}
	key := info.keyOf(msg.ProtoReflect())
	if err := info.checkKey(key); err != nil {
		return err
	}
	// Authorize the submitted value, before the text merge below rewrites it —
	// the bytes a client would have put on the wire.
	if err := e.authorizeCrud(info, key, syncplanev1.CrudOp_CRUD_OP_PUT, msg, nil, o.expected); err != nil {
		return err
	}
	// Fields this caller may not write keep what the row already holds; the
	// carry-over below is what makes that true without refusing the rest of the
	// write. A WithTextString naming one is refused instead: unlike a value in a
	// submitted row, the option names the field outright, which is the same
	// thing a PATCH mask does.
	protected, err := e.gate.protected(info)
	if err != nil {
		return err
	}
	for path := range o.texts {
		if protectedPath(path, protected) {
			return Reject(CodePermissionDenied, fmt.Sprintf(
				"entity %q: field %q may not be written by this caller, so WithTextString(%q) is refused",
				info.name, protectedHead(path), path))
		}
	}
	var (
		prevText map[string][]byte
		prevRead readInputs
		// The stored row before this write, read at most once: the row check,
		// the merges and history capture all want the same one — and, like the
		// wire path's memo, must treat an absent row and a tombstone alike.
		row     storage.Row
		live    bool
		fetched bool
	)
	currentRow := func() (storage.Row, bool, error) {
		if !fetched {
			r, l, err := e.liveRow(info, key)
			if err != nil {
				return storage.Row{}, false, err
			}
			row, live, fetched = r, l, true
		}
		return row, live, nil
	}
	// Before the compare-and-swap below, so a caller refused here does not
	// learn the row's version from a conflict.
	if err := e.gate.guardRow(info, key, currentRow, func() (Write, error) {
		return writeOf(info, key, syncplanev1.CrudOp_CRUD_OP_PUT, msg, nil, o.expected)
	}); err != nil {
		return err
	}
	aw, err := e.gate.beginAccessWrite(e.tx, info, key, syncplanev1.CrudOp_CRUD_OP_PUT, nil, currentRow)
	if err != nil {
		return err
	}
	if len(info.textFields) > 0 || len(o.texts) > 0 || info.history != nil || o.expected != nil ||
		len(protected) > 0 || aw != nil {
		if _, _, err := currentRow(); err != nil {
			return err
		}
		// WithExpectedVersion turns the write into a compare-and-swap, checked
		// before any merge so a stale write conflicts deterministically. Mirrors
		// Patch: an absent or deleted row reads as version 0.
		if o.expected != nil {
			var current uint64
			if live {
				current = row.Version
			}
			if current != *o.expected {
				return Reject(CodeConflict, fmt.Sprintf(
					"entity %q: expected version %d but row %q is at version %d", info.name, *o.expected, key, current))
			}
		}
		var base proto.Message
		if live {
			// A stored row that will not decode (see poison.go) is no base: Put
			// replaces whole rows, so this write is the repair that clears the
			// quarantine. Mirrors the client PUT path.
			if decoded, decErr := info.decode(row.Value); decErr != nil {
				e.srv.reportPoison(e.ctx, e.ws, row, decErr)
			} else {
				base = decoded
				prevText = info.textSnapshot(base)
				prevRead = readInputsOf(e.srv.catalogPtr().access, info, base)
			}
		}
		// Clone so the caller's message is not mutated by the merges.
		msg = proto.Clone(msg)
		// Before the text handling, so a plain-string value in a protected text
		// field is replaced by the stored document rather than resolved into an
		// edit against it.
		if err := carryProtectedFields(info, protected, base, msg); err != nil {
			return err
		}
		if base == nil && e.gate != nil && len(info.creatorFields) > 0 {
			stampCreatorFields(info, msg, e.gate.creatorSubject())
		}
		named := make([]string, 0, len(o.texts))
		for path := range o.texts {
			named = append(named, protectedHead(path))
		}
		if err := aw.carryGuarded(e.tx, msg, named); err != nil {
			return err
		}
		texts, err := takePlainStrings(info, msg, nil, o)
		if err != nil {
			return err
		}
		if len(texts) > 0 {
			// Diff each string against the stored document (ancestor = cur):
			// the union below is then a no-op for these fields, but it still
			// covers text fields the caller set as raw bytes.
			if err := ApplyTextStrings(msg, nil, base, texts, o.peer); err != nil {
				return err
			}
		}
		if base != nil {
			tw, err := info.textWriteFor(e.srv, e.ws, e.tx, key)
			if err != nil {
				return err
			}
			if err := info.mergeTextFields(base.ProtoReflect(), msg.ProtoReflect(), tw); err != nil {
				return rejectTextWrite(err)
			}
		}
	}
	// A reference stores only its target's key fields, and the workspace a
	// reference message names has to be one a client can hold — the same
	// projection the push path runs.
	if err := info.projectRefs(msg.ProtoReflect()); err != nil {
		return err
	}
	if err := e.srv.checkGrantRow(info, key, syncplanev1.CrudOp_CRUD_OP_PUT, nil, msg); err != nil {
		return err
	}
	if err := aw.checkNewParentsRead(e.tx, msg); err != nil {
		return err
	}
	if err := e.srv.checkAccessChain(e.ctx, e.ws, e.tx, info, key, currentRow, msg); err != nil {
		return err
	}
	if err := aw.finish(e.tx, msg); err != nil {
		return err
	}
	value, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	if err := info.checkTextCounterSteps(prevText, msg.ProtoReflect()); err != nil {
		return err
	}
	if err := info.recordTextDeltas(e.tx, key, prevText, msg.ProtoReflect(), seq); err != nil {
		return err
	}
	if err := info.recordReadGaps(e.tx, e.srv.catalogPtr().access, key, prevRead, msg, seq); err != nil {
		return err
	}
	if err := info.dropRemovedTextInstances(e.tx, e.srv.textCache, e.ws, key,
		prevText, msg.ProtoReflect()); err != nil {
		return err
	}
	if err := info.updateRowIndexes(e.tx, e.srv.limits, key, msg); err != nil {
		return err
	}
	// Same contract as the wire PUT: no mask, so capture starts from the whole
	// capture set and narrows it to what `msg` actually moves against the row
	// read above.
	if err := e.srv.captureHistory(e.ctx, e.tx, e.ws, info, key, row, live, msg, nil, seq, e.actor); err != nil {
		return err
	}
	if err := e.srv.recordAccessMove(e.ctx, e.ws, e.tx, info, key, currentRow, msg, seq, e.actor); err != nil {
		return err
	}
	if err := e.tx.PutRow(info.name, key, value, seq, e.actor); err != nil {
		return err
	}
	e.access.noteWrite(info)
	return nil
}

// Patch merges only the masked fields of msg onto the stored row, honoring
// per-field LWW plus keyed-list / opaque merge semantics — the same merge the
// client PATCH path runs, lifted into a trusted server-side write. msg must
// carry the row's key fields (they identify the row); mask names the dotted
// proto paths to merge.
//
// PATCH is NOT an upsert: a missing or deleted target row is a conflict
// (delete-wins), so an edit of a row a concurrent writer removed is rejected
// rather than resurrecting a half-formed ghost. When expected is non-nil the
// row's current version must equal *expected or the patch is rejected as a
// conflict (optimistic compare-and-swap); an absent/deleted row reads as
// version 0. Conflicts and poison masks come back as *Rejection errors
// (AsRejection recovers them on the caller's side), mirroring the push path.
//
// Text fields can be written from plain strings — WithTextString plus
// WithTextPeer — which diffs the string against the stored document under the
// given writer identity and adds the field to the mask automatically (see
// ApplyTextStrings).
//
// A text field of msg holding a plain-string value (text.EncodePlainString,
// which is what a field-history record's before-state carries) is resolved the
// same way, so restoring a record needs no option naming its fields. It still
// requires WithTextPeer: without one the write fails rather than writing under
// an invented identity. Unlike WithTextString it does not widen mask: a value
// in a field mask does not name is ignored, like any other unmasked field.
func (e *EntityTx) Patch(msg proto.Message, mask []string, expected *uint64, opts ...WriteOption) error {
	o, err := collectWriteOpts(opts)
	if err != nil {
		return err
	}
	info, err := e.info(msg)
	if err != nil {
		return err
	}
	if msg, err = info.adopt(msg); err != nil {
		return err
	}
	for path := range o.texts {
		if !slices.Contains(mask, path) {
			mask = append(slices.Clone(mask), path)
		}
	}
	if len(mask) == 0 {
		return Reject(CodeInvalidArgument, fmt.Sprintf("entity %q: PATCH requires a field_mask", info.name))
	}
	key := info.keyOf(msg.ProtoReflect())
	if err := info.checkKey(key); err != nil {
		return err
	}
	// Authorize on the EFFECTIVE mask — text options have already added their
	// paths above, so the policy sees every field this write will merge.
	if err := e.authorizeCrud(info, key, syncplanev1.CrudOp_CRUD_OP_PATCH, msg, mask, expected); err != nil {
		return err
	}

	row, live, err := e.liveRow(info, key)
	if err != nil {
		return err
	}
	// Before the compare-and-swap below, so a caller refused here does not
	// learn the row's version from a conflict.
	if err := e.gate.guardRow(info, key, func() (storage.Row, bool, error) {
		return row, live, nil
	}, func() (Write, error) {
		return writeOf(info, key, syncplanev1.CrudOp_CRUD_OP_PATCH, msg, mask, expected)
	}); err != nil {
		return err
	}
	aw, err := e.gate.beginAccessWrite(e.tx, info, key, syncplanev1.CrudOp_CRUD_OP_PATCH, mask,
		func() (storage.Row, bool, error) { return row, live, nil })
	if err != nil {
		return err
	}

	// Optional compare-and-swap: an absent or deleted row reads as version 0
	// (so expected 0 means "row must not exist"). The check runs before the
	// merge so a stale edit conflicts deterministically regardless of mask.
	if expected != nil {
		var current uint64
		if live {
			current = row.Version
		}
		if current != *expected {
			return Reject(CodeConflict,
				fmt.Sprintf("entity %q: expected version %d but row %q is at version %d", info.name, *expected, key, current))
		}
	}

	// Merge onto the live row, or onto an empty base when the row is gone purely
	// to validate the mask paths first: a poison/version-skew mask must reject as
	// invalid_argument before the conflict check below.
	base := info.msgType.New().Interface()
	if live {
		if base, err = info.decode(row.Value); err != nil {
			// A mask can only be merged onto a base that decodes. Rejecting
			// rather than erroring mirrors the client PATCH path, and matters
			// most where this code is reached from one: a mutator invoked by a
			// custom mutation, whose plain error would abort the batch and wedge
			// the client's queue on a row retrying cannot fix. A Put of the same
			// key repairs it.
			e.srv.reportPoison(e.ctx, e.ws, row, err)
			return Reject(CodeConflict,
				fmt.Sprintf("entity %q: PATCH of %q targets a row that does not decode", info.name, key))
		}
	}
	// Snapshot before the merge: the delta log records the move it makes.
	prevText := info.textSnapshot(base)
	model := e.srv.catalogPtr().access
	prevRead := readInputsOf(model, info, base)
	if len(o.texts) > 0 || hasPlainString(info, msg) {
		// Clone before either rewrite so the caller's message is not mutated.
		msg = proto.Clone(msg)
		texts, err := takePlainStrings(info, msg, mask, o)
		if err != nil {
			return err
		}
		if len(texts) > 0 {
			// Turn each string into document bytes, diffed against the live
			// row's document; the masked merge below then unions them in (the
			// diffed doc already contains the base, so the union keeps exactly
			// the intended state).
			var cur proto.Message
			if live {
				cur = base
			}
			if err := ApplyTextStrings(msg, nil, cur, texts, o.peer); err != nil {
				return err
			}
		}
	}
	mergers := info.mergers
	if live && len(info.textFields) > 0 {
		tw, err := info.textWriteFor(e.srv, e.ws, e.tx, key)
		if err != nil {
			return err
		}
		mergers = info.mergersFor(tw)
	}
	if err := lww.MergeWithMergers(base, msg, mask, mergers); err != nil {
		return rejectTextWrite(err)
	}
	if !live {
		// Absent and deleted are treated alike: a GC'd tombstone reads as absent,
		// so the outcome must not depend on GC timing.
		return Reject(CodeConflict,
			fmt.Sprintf("entity %q: PATCH of %q targets a deleted or missing row", info.name, key))
	}
	// A row's identity lives in both its storage key and its embedded key fields;
	// a mask that named a key field could drift them apart. Re-derive and reject
	// any change — mirrors the PUT/push checks.
	if derived := info.keyOf(base.ProtoReflect()); derived != key {
		return Reject(CodeInvalidArgument,
			fmt.Sprintf("entity %q: PATCH would change key fields of %q (to %q)", info.name, key, derived))
	}
	// Project refs on the MERGED result so a fat stub in the patch value cannot
	// smuggle extra fields past the per-item merge.
	if err := info.projectRefs(base.ProtoReflect()); err != nil {
		return err
	}
	if err := e.srv.checkGrantRow(info, key, syncplanev1.CrudOp_CRUD_OP_PATCH, mask, base); err != nil {
		return err
	}
	if err := aw.checkNewParentsRead(e.tx, base); err != nil {
		return err
	}
	if err := e.srv.checkAccessChain(e.ctx, e.ws, e.tx, info, key,
		func() (storage.Row, bool, error) { return row, live, nil }, base); err != nil {
		return err
	}
	if err := aw.finish(e.tx, base); err != nil {
		return err
	}
	value, err := proto.Marshal(base)
	if err != nil {
		return err
	}
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	if err := info.checkTextCounterSteps(prevText, base.ProtoReflect()); err != nil {
		return err
	}
	if err := info.recordTextDeltas(e.tx, key, prevText, base.ProtoReflect(), seq); err != nil {
		return err
	}
	if err := info.recordReadGaps(e.tx, model, key, prevRead, base, seq); err != nil {
		return err
	}
	if err := info.dropRemovedTextInstances(e.tx, e.srv.textCache, e.ws, key,
		prevText, base.ProtoReflect()); err != nil {
		return err
	}
	// Same guard as the wire PATCH: a mask that reaches no index component
	// leaves every entry of the row exactly as it is.
	if info.maskTouchesIndexes(mask) {
		if err := info.updateRowIndexes(e.tx, e.srv.limits, key, base); err != nil {
			return err
		}
	}
	// The EFFECTIVE mask, which is the one this write merged: a text write
	// option added its paths to `mask` above, before the authorization that
	// also runs on the effective set. Recording the request's mask instead
	// would leave a text field's before-state unrecorded on exactly the writes
	// that overwrite one. `row` still carries the encoded before-state — `base`
	// was merged in place, and is therefore the after-state to diff against.
	if err := e.srv.captureHistory(e.ctx, e.tx, e.ws, info, key, row, live, base, mask, seq, e.actor); err != nil {
		return err
	}
	if err := e.srv.recordAccessMove(e.ctx, e.ws, e.tx, info, key,
		func() (storage.Row, bool, error) { return row, live, nil }, base, seq, e.actor); err != nil {
		return err
	}
	if err := e.tx.PutRow(info.name, key, value, seq, e.actor); err != nil {
		return err
	}
	e.access.noteWrite(info)
	return nil
}

// Delete writes a tombstone. prototype only identifies the entity type.
func (e *EntityTx) Delete(prototype proto.Message, key string) error {
	info, err := e.info(prototype)
	if err != nil {
		return err
	}
	if err := info.checkKey(key); err != nil {
		return err
	}
	// A DELETE carries no value, exactly as on the wire.
	if err := e.authorizeCrud(info, key, syncplanev1.CrudOp_CRUD_OP_DELETE, nil, nil, nil); err != nil {
		return err
	}
	if err := e.gate.guardRow(info, key, func() (storage.Row, bool, error) {
		return e.liveRow(info, key)
	}, func() (Write, error) {
		return writeOf(info, key, syncplanev1.CrudOp_CRUD_OP_DELETE, nil, nil, nil)
	}); err != nil {
		return err
	}
	if _, err := e.gate.beginAccessWrite(e.tx, info, key, syncplanev1.CrudOp_CRUD_OP_DELETE, nil,
		func() (storage.Row, bool, error) { return e.liveRow(info, key) }); err != nil {
		return err
	}
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	// Mirrors the wire DELETE: per-document side state is keyed by instance
	// path, so a row with element-borne text is decoded to name what it holds.
	textPaths := info.textStatePaths(nil)
	if info.hasNestedText() {
		if row, rowErr := e.tx.GetRow(info.name, key); rowErr == nil && !row.Deleted {
			if decoded, decErr := info.decode(row.Value); decErr == nil {
				textPaths = info.textStatePaths(decoded.ProtoReflect())
			}
		} else if rowErr != nil && rowErr != storage.ErrNotFound {
			return rowErr
		}
	}
	if err := info.recordTextDeltaGaps(e.tx, key, textPaths, seq); err != nil {
		return err
	}
	e.srv.textCache.forget(e.ws, info.name, key, textPaths)
	if err := info.clearTextFloors(e.tx, key, textPaths); err != nil {
		return err
	}
	if err := info.clearRowIndexes(e.tx, key); err != nil {
		return err
	}
	// Same as the wire DELETE: the row's history goes with the row.
	if err := e.srv.purgeHistory(e.tx, info, key); err != nil {
		return err
	}
	if err := e.srv.recordAccessMove(e.ctx, e.ws, e.tx, info, key,
		func() (storage.Row, bool, error) { return e.liveRow(info, key) }, nil, seq, e.actor); err != nil {
		return err
	}
	if err := e.tx.DeleteRow(info.name, key, seq, e.actor); err != nil {
		return err
	}
	e.access.noteWrite(info)
	return nil
}

// Scan iterates all live rows of the entity identified by prototype,
// decoding each into a fresh message. Through an AsCaller view, and in a
// Server.Mutate for a caller who is not Trusted, it skips the rows the caller
// may not read and clears the fields a guard hides from the caller (see
// AsCaller).
func (e *EntityTx) Scan(prototype proto.Message, fn func(msg proto.Message) error) error {
	info, err := e.info(prototype)
	if err != nil {
		return err
	}
	g, err := e.callerReadGate()
	if err != nil {
		return err
	}
	return e.tx.ScanEntity(info.name, func(r storage.Row) error {
		if g.visible != nil {
			ok, err := g.visible(r)
			if err != nil || !ok {
				return err
			}
		}
		msg, err := info.decode(r.Value)
		if err != nil {
			return err
		}
		if err := g.project.apply(info, r.Key, msg); err != nil {
			return err
		}
		return fn(msg)
	})
}

// Query iterates the live rows matching spec, in spec's order, decoding each
// into a fresh message — the read half of "find the rows matching X and update
// them", inside the same transaction as the writes so nothing moves underneath.
// It is the same compiled predicate ViewTx.Query and the Query RPC use.
//
// The cost to keep in mind here specifically is that it runs while the
// workspace's write lock is held, so a broad query inside a mutation blocks
// every other writer for its duration — and it is a full scan of the entity
// unless a declared secondary index covers its predicate and ordering. A spec
// with no Limit also retains every match in memory. See query.Spec.
//
// Through an AsCaller view, and in a Server.Mutate for a caller who is not
// Trusted, it answers as ViewTx.Query does for the caller (see AsCaller).
func (e *EntityTx) Query(prototype proto.Message, spec query.Spec, fn func(msg proto.Message) error) error {
	return e.queryMeta(prototype, spec, func(msg proto.Message, _ RowMeta) error {
		return fn(msg)
	})
}

func (e *EntityTx) queryMeta(prototype proto.Message, spec query.Spec, fn func(proto.Message, RowMeta) error) error {
	info, err := e.info(prototype)
	if err != nil {
		return err
	}
	g, err := e.callerReadGate()
	if err != nil {
		return err
	}
	if err := g.refuse.check(info, spec); err != nil {
		return err
	}
	rows, err := runQuerySpec(e.srv, e.tx, e.ws, info, spec, g.visible, g.project)
	if err != nil {
		return err
	}
	return eachQueryRow(rows, fn)
}

// ScanPage iterates up to limit live rows with key > afterKey, in key order.
// It returns the key of the last row visited and whether more rows follow;
// pass lastKey back as afterKey to continue. limit must be positive. Through
// an AsCaller view, and in a Server.Mutate for a caller who is not Trusted, it
// skips the rows the caller may not read (see AsCaller); a skipped row still
// counts toward limit.
func (e *EntityTx) ScanPage(prototype proto.Message, afterKey string, limit int, fn func(msg proto.Message) error) (lastKey string, more bool, err error) {
	return e.scanMeta(prototype, afterKey, limit, func(msg proto.Message, _ RowMeta) error {
		return fn(msg)
	})
}

func (e *EntityTx) scanMeta(prototype proto.Message, afterKey string, limit int, fn func(proto.Message, RowMeta) error) (string, bool, error) {
	info, err := e.info(prototype)
	if err != nil {
		return "", false, err
	}
	g, err := e.callerReadGate()
	if err != nil {
		return "", false, err
	}
	return scanPageRows(e.tx, info, afterKey, limit, g.visible, g.project, fn)
}

func (e *EntityTx) getMeta(dst proto.Message, key string) (bool, RowMeta, error) {
	return e.GetMeta(dst, key)
}
