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
// InvokeIdempotent), so a body may type-assert its prototype unconditionally.
type MutatorFunc func(ctx context.Context, tx *EntityTx, args proto.Message) error

type mutatorInfo struct {
	name string
	args proto.Message // prototype; cloned per invocation
	fn   MutatorFunc
}

// pendingMutator is one WithMutator declaration, held until New has built the
// server it belongs to.
type pendingMutator struct {
	name string
	args proto.Message
	fn   MutatorFunc
}

// WithMutator declares a custom mutation handler. argsPrototype defines the
// expected google.protobuf.Any payload type; pass nil for a mutator that takes
// no arguments.
//
// Mutators are construction-time only: they are funcs, so unlike a schema they
// cannot be provisioned at runtime (see DynamicEntity, the CRUD-only dynamic
// tier). That is also what keeps the mutator table lock-free on the push path
// — it never changes after New.
func WithMutator(name string, argsPrototype proto.Message, fn MutatorFunc) Option {
	return func(s *Server) error {
		s.pendingMutators = append(s.pendingMutators, pendingMutator{name: name, args: argsPrototype, fn: fn})
		return nil
	}
}

// registerMutator installs a WithMutator declaration. See WithMutator.
func (s *Server) registerMutator(name string, argsPrototype proto.Message, fn MutatorFunc) error {
	if name == "" || fn == nil {
		return fmt.Errorf("server: mutator needs a name and a func")
	}
	if _, dup := s.mutators[name]; dup {
		return fmt.Errorf("server: mutator %q already registered", name)
	}
	s.mutators[name] = &mutatorInfo{name: name, args: argsPrototype, fn: fn}
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
	crud := &syncplanev1.CrudMutation{
		Entity:          info.name,
		Key:             key,
		Op:              op,
		ExpectedVersion: expected,
	}
	if msg != nil {
		value, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		crud.Value = value
	}
	if len(mask) > 0 {
		crud.FieldMask = &fieldmaskpb.FieldMask{Paths: mask}
	}
	return e.authz(Write{Crud: crud})
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
	if !e.wrote {
		return 0
	}
	return e.seq
}

// writeSeq returns the sequence for a write, allocating it on first use
// when the tx was created without one (Server.Mutate).
func (e *EntityTx) writeSeq() (uint64, error) {
	if e.seq == 0 {
		seq, err := e.tx.NextSeq()
		if err != nil {
			return 0, err
		}
		e.seq = seq
	}
	e.wrote = true
	return e.seq, nil
}

func (e *EntityTx) info(msg proto.Message) (*entityInfo, error) {
	return entityInfoFor(e.srv, msg)
}

// Get loads the row with the given key into dst. Returns false if the row
// does not exist (or is deleted).
func (e *EntityTx) Get(dst proto.Message, key string) (bool, error) {
	info, err := e.info(dst)
	if err != nil {
		return false, err
	}
	found, _, err := readRowInto(e.tx, info, dst, key)
	return found, err
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
// states create-or-update intent to an entity restricting either (see
// CreatePolicy); without one the write is unguarded and follows arrival order
// like any other.
func (e *EntityTx) Put(msg proto.Message, opts ...WriteOption) error {
	o, err := collectWriteOpts(opts)
	if err != nil {
		return err
	}
	info, err := e.info(msg)
	if err != nil {
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
	var (
		prevText map[string][]byte
		// The stored row before this write, hoisted out of the text branch
		// because history capture wants the same one — and, like the wire path's
		// memo, must treat an absent row and a tombstone alike.
		row  storage.Row
		live bool
	)
	if len(info.textFields) > 0 || len(o.texts) > 0 || info.history != nil || o.expected != nil {
		var rowErr error
		row, rowErr = e.tx.GetRow(info.name, key)
		if rowErr != nil && rowErr != storage.ErrNotFound {
			return rowErr
		}
		live = rowErr == nil && !row.Deleted
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
			}
		}
		// Clone so the caller's message is not mutated by the merges.
		msg = proto.Clone(msg)
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
	value, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	if err := info.recordTextDeltas(e.tx, key, prevText, msg.ProtoReflect(), seq); err != nil {
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
	return e.tx.PutRow(info.name, key, value, seq, e.actor)
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

	row, rowErr := e.tx.GetRow(info.name, key)
	if rowErr != nil && rowErr != storage.ErrNotFound {
		return rowErr
	}
	live := rowErr == nil && !row.Deleted

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
	info.projectRefs(base.ProtoReflect())
	value, err := proto.Marshal(base)
	if err != nil {
		return err
	}
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	if err := info.recordTextDeltas(e.tx, key, prevText, base.ProtoReflect(), seq); err != nil {
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
	return e.tx.PutRow(info.name, key, value, seq, e.actor)
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
	seq, err := e.writeSeq()
	if err != nil {
		return err
	}
	if err := info.recordTextDeltaGaps(e.tx, key, seq); err != nil {
		return err
	}
	e.srv.textCache.forget(e.ws, info.name, key, info.textFields)
	if err := info.clearTextFloors(e.tx, key); err != nil {
		return err
	}
	if err := info.clearRowIndexes(e.tx, key); err != nil {
		return err
	}
	// Same as the wire DELETE: the row's history goes with the row.
	if err := e.srv.purgeHistory(e.tx, info, key); err != nil {
		return err
	}
	return e.tx.DeleteRow(info.name, key, seq, e.actor)
}

// Scan iterates all live rows of the entity identified by prototype,
// decoding each into a fresh message.
func (e *EntityTx) Scan(prototype proto.Message, fn func(msg proto.Message) error) error {
	info, err := e.info(prototype)
	if err != nil {
		return err
	}
	return e.tx.ScanEntity(info.name, func(r storage.Row) error {
		msg, err := info.decode(r.Value)
		if err != nil {
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
func (e *EntityTx) Query(prototype proto.Message, spec query.Spec, fn func(msg proto.Message) error) error {
	info, err := e.info(prototype)
	if err != nil {
		return err
	}
	rows, err := runQuerySpec(e.tx, info, spec, nil, e.srv.limits)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := fn(r.msg); err != nil {
			return err
		}
	}
	return nil
}

// ScanPage iterates up to limit live rows with key > afterKey, in key order.
// It returns the key of the last row visited and whether more rows follow;
// pass lastKey back as afterKey to continue. limit must be positive.
func (e *EntityTx) ScanPage(prototype proto.Message, afterKey string, limit int, fn func(msg proto.Message) error) (lastKey string, more bool, err error) {
	info, err := e.info(prototype)
	if err != nil {
		return "", false, err
	}
	return scanPageRows(e.tx, info, afterKey, limit, nil, func(msg proto.Message, _ uint64) error {
		return fn(msg)
	})
}
