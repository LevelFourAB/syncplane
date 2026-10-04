package server

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
	"github.com/levelfourab/syncplane/engine/storage"
)

// Apply is the in-process write path: fn reads from a consistent snapshot and
// records the writes it wants, and the engine authorizes them and applies them
// as one atomic mutation. It is what an app's own API handlers, agents and
// background jobs should be written against.
//
// It behaves like a client push, deliberately. The recorded writes go through
// the same EntityTx that a mutation off the wire does, so per-field LWW,
// keyed-list and collaborative-text merges, compare-and-swap and the delta log
// all mean here exactly what they mean to a sync client — and the CrudMutation
// handed to CanMutate is the one a client would have sent, so a policy cannot
// tell the two paths apart.
//
//	err := srv.Apply(ctx, ws, func(tx *server.ApplyTx) error {
//	    var issue issuev1.Issue
//	    found, version, err := tx.Get(&issue, key)
//	    if err != nil || !found {
//	        return server.Reject(server.CodeConflict, "no such issue")
//	    }
//	    issue.Status = "done"
//	    return tx.Patch(&issue, []string{"status"}, &version)
//	})
//
// # Three phases, and what runs where
//
//	read snapshot   fn runs: reads are live, writes are recorded. No write lock.
//	(no tx)         CanAccessWorkspace, then CanMutate per recorded write.
//	write tx        the recorded writes are applied at one sequence.
//
// No permission hook ever runs inside a transaction on this path, and none of
// your code runs while the workspace's write lock is held. A policy that needs
// to do I/O can therefore afford to, and a slow handler does not serialize
// every other writer to the workspace the way Mutate's does.
//
// # fn runs exactly once
//
// The read snapshot runs fn at most once (storage adapters never retry a read
// transaction), and the write transaction replays the recorded writes without
// re-entering your code — so unlike Mutate, there is no retried-closure
// contract here. Capture what you like; an object you build inside fn is still
// the object that was written.
//
// # What you give up relative to Mutate
//
//   - Reads see the snapshot, not your own recorded writes. Get on a key you
//     have already written returns an error rather than a stale row; scans and
//     queries silently do not include recorded writes.
//   - Reads and writes are not one serializable unit. The snapshot can be stale
//     by the time the writes apply, exactly as a client's optimistic state can
//     be. Per-field LWW is what resolves that, and Patch's expected version is
//     the opt-out when a concurrent edit must be noticed rather than merged.
//   - Writes are buffered in memory, so a bulk import belongs in chunks (or in
//     Mutate, which streams).
//
// # Permissions
//
// Authorized against the ctx principal; a ctx with no principal authorizes as
// anonymous, which a deny-by-default policy refuses. System work with no caller
// to authorize marks its ctx with Trusted. A write to an existing row also
// passes the row check (see RowWritePermissions) when the write transaction
// applies it. A rejection, from a hook or from the row check, is returned as an
// error and nothing is applied — recovered with AsRejection, or
// AsWorkspaceDenial for a settled workspace denial.
//
// A call that records no writes opens no write transaction at all: it consumes
// no sequence, notifies nobody, and leaves a workspace that did not exist
// unregistered.
//
// fn runs inside the read snapshot, so it must not start another transaction —
// no nested Apply, Mutate or View. Record the writes and let this call apply
// them.
func (s *Server) Apply(ctx context.Context, workspaceID string, fn func(tx *ApplyTx) error) error {
	_, err := s.apply(ctx, workspaceID, "", func(tx *ApplyTx) ([]byte, error) {
		return nil, fn(tx)
	})
	return err
}

// ApplyIdempotent is Apply guarded by a caller-supplied idempotency key: the
// first call for a key runs fn and records what it did, and every later call
// with the same key skips fn and replays that record. It is the write path for
// an HTTP handler behind a retrying client, or an at-least-once effects handler
// reacting to the change feed.
//
// The key semantics are MutateIdempotent's exactly — per workspace and per
// caller, claimed in the same transaction as the writes, not recorded for a
// failed call, collected on WithIdempotencyRetention. See MutateIdempotent for
// the full contract.
//
// The bytes fn returns are the recorded response, replayed verbatim on every
// later call with the same key:
//
//	out, err := srv.ApplyIdempotent(ctx, ws, r.Header.Get("Idempotency-Key"),
//	    func(tx *server.ApplyTx) ([]byte, error) {
//	        if err := tx.Put(&issue); err != nil {
//	            return nil, err
//	        }
//	        return json.Marshal(response{ID: issue.GetId()})
//	    })
//	if err != nil { … }
//	w.Write(out.Result) // the first attempt's response, however often it retries
func (s *Server) ApplyIdempotent(
	ctx context.Context,
	workspaceID, key string,
	fn func(tx *ApplyTx) ([]byte, error),
) (WriteOutcome, error) {
	if err := validateIdempotencyKey(key); err != nil {
		return WriteOutcome{}, err
	}
	return s.apply(ctx, workspaceID, key, fn)
}

// apply is the shared body of Apply and ApplyIdempotent. An empty key means no
// idempotency guard.
func (s *Server) apply(
	ctx context.Context,
	workspaceID, key string,
	fn func(tx *ApplyTx) ([]byte, error),
) (WriteOutcome, error) {
	if workspaceID == "" {
		return WriteOutcome{}, fmt.Errorf("server: workspace id is required")
	}
	if inWriteTx(ctx) {
		return WriteOutcome{}, fmt.Errorf("server: Apply called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock; perform writes through the enclosing EntityTx instead", workspaceID)
	}
	adm, enforced, err := s.gateServed(ctx, workspaceID)
	if err != nil {
		return WriteOutcome{}, err
	}
	p := adm.principal
	if key != "" {
		key = ledgerKey(p, enforced, key)
	}
	visible, bind, err := s.readGate(ctx, workspaceID, adm, enforced)
	if err != nil {
		return WriteOutcome{}, adm.verdict.settle(ctx, err)
	}

	// Phase 1: run fn over a read snapshot, recording its writes. No write lock
	// is held and fn runs at most once (WithReadTx never retries).
	atx := &ApplyTx{srv: s, ctx: ctx, ws: workspaceID, visible: visible}
	var (
		result []byte
		replay *storage.IdempotencyRecord
	)
	err = s.store.WithReadTx(ctx, workspaceID, func(tx storage.ReadTx) error {
		if err := bind(tx); err != nil {
			return err
		}
		atx.tx = tx
		if key != "" {
			// Cheap early-out for a retry whose key is already claimed: skip fn
			// entirely. The authoritative check still happens under the write
			// lock in phase 3 — this snapshot could be stale.
			rec, found, err := tx.IdempotencyRecord(key)
			if err != nil {
				return err
			}
			if found {
				replay = &rec
				return nil
			}
		}
		var fnErr error
		result, fnErr = fn(atx)
		return fnErr
	})
	// The snapshot reached the grant check first, so whatever follows runs for
	// an admitted caller.
	if err = adm.verdict.settle(ctx, err); err != nil {
		return WriteOutcome{}, err
	}
	if replay != nil {
		return WriteOutcome{Seq: replay.Seq, Result: replay.Result}, nil
	}
	if max := s.limits.MaxValueBytes; len(result) > max {
		return WriteOutcome{}, fmt.Errorf("server: idempotent write result is %d bytes, over the %d byte limit", len(result), max)
	}

	// Phase 2: authorize every recorded write, outside any transaction. A
	// rejection here means nothing is applied — the writes were never opened.
	// The gate outlives this pass: phase 3 reads the protected fields and row
	// checks it resolved here rather than calling the hooks under the write lock.
	gate := s.newWriteGate(ctx, adm, workspaceID, enforced)
	if enforced {
		for _, in := range atx.intents {
			if err := s.authorizeWrite(ctx, p, workspaceID, gate, Write{Crud: in.crud()}); err != nil {
				return WriteOutcome{}, err
			}
		}
	}

	// Nothing to do, and no key to claim: skip the write transaction entirely so
	// an unwritten workspace stays unregistered.
	if len(atx.intents) == 0 && key == "" {
		return WriteOutcome{}, nil
	}

	// Phase 3: apply. The closure is deterministic — it replays the recorded
	// intents and touches no caller code — so an adapter retry is free.
	var (
		out   WriteOutcome
		wrote bool
	)
	err = s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		out, wrote = WriteOutcome{}, false
		if key != "" {
			rec, found, err := tx.IdempotencyRecord(key)
			if err != nil {
				return err
			}
			if found {
				out = WriteOutcome{Seq: rec.Seq, Result: rec.Result}
				return nil
			}
		}
		actor, err := actorOf(ctx)
		if err != nil {
			return err
		}
		// authz stays nil: phase 2 already authorized every intent, and
		// re-running the hook here would put it back under the write lock. The
		// write gate does travel, warm: the PUT carry-over and the row check
		// need the stored row, which only this transaction reads.
		if err := gate.accessOf().begin(tx); err != nil {
			return err
		}
		etx := &EntityTx{srv: s, ctx: ctx, ws: workspaceID, tx: tx, gate: gate, actor: actor, access: gate.accessOf()}
		for _, in := range atx.intents {
			if err := in.apply(etx); err != nil {
				return err
			}
		}
		if key != "" {
			if err := tx.PutIdempotencyRecord(key, storage.IdempotencyRecord{Seq: etx.Seq(), Result: result}); err != nil {
				return err
			}
		}
		out = WriteOutcome{Applied: true, Seq: etx.Seq(), Result: result}
		wrote = etx.wrote
		return nil
	})
	if err != nil {
		return WriteOutcome{}, err
	}
	if wrote {
		s.notifier.Notify(workspaceID)
	}
	return out, nil
}

// intent is one recorded write, held until Apply authorizes and applies it. It
// keeps the caller's arguments rather than an encoded row so the apply replays
// through the ordinary EntityTx — there is no second implementation of PUT,
// PATCH or DELETE to drift from the one the wire path uses.
type intent struct {
	op       syncplanev1.CrudOp
	info     *entityInfo
	key      string
	msg      proto.Message // cloned at record time; nil for DELETE
	mask     []string
	expected *uint64
	opts     []WriteOption
}

// crud renders the intent as the CrudMutation a client would have sent, for
// CanMutate. Marshal failures surface as an empty value rather than an error:
// the same message is re-marshaled by the apply below, which reports it
// properly, and a policy seeing an empty value denies rather than admits.
func (in intent) crud() *syncplanev1.CrudMutation {
	c := &syncplanev1.CrudMutation{
		Entity:          in.info.name,
		Key:             in.key,
		Op:              in.op,
		ExpectedVersion: in.expected,
	}
	// A DELETE carries no value on the wire, and its msg is only a prototype
	// naming the entity — marshaling it would show a policy fields no client
	// could have sent.
	if in.msg != nil && in.op != syncplanev1.CrudOp_CRUD_OP_DELETE {
		if value, err := proto.Marshal(in.msg); err == nil {
			c.Value = value
		}
	}
	if len(in.mask) > 0 {
		c.FieldMask = &fieldmaskpb.FieldMask{Paths: in.mask}
	}
	return c
}

// apply replays the intent through a real EntityTx.
func (in intent) apply(etx *EntityTx) error {
	switch in.op {
	case syncplanev1.CrudOp_CRUD_OP_PUT:
		return etx.Put(in.msg, in.opts...)
	case syncplanev1.CrudOp_CRUD_OP_PATCH:
		return etx.Patch(in.msg, in.mask, in.expected, in.opts...)
	case syncplanev1.CrudOp_CRUD_OP_DELETE:
		return etx.Delete(in.msg, in.key)
	}
	return fmt.Errorf("server: unknown recorded write op %v", in.op)
}

// ApplyTx is the transaction facade Apply's closure runs against: reads are
// served from a consistent snapshot, writes are recorded and applied after the
// closure returns.
//
// The write methods mirror EntityTx exactly — same arguments, same merge
// semantics, same rejections — so code moves between a mutator and an Apply
// closure unchanged. What differs is when they take effect, and that reads do
// not see them (see Apply).
type ApplyTx struct {
	srv *Server
	ctx context.Context
	ws  string
	tx  storage.ReadTx
	// intents are the recorded writes, in the order they were made. They apply
	// in that order, so a later write observes an earlier one — a PATCH after a
	// PUT of the same key behaves as it does in a client batch.
	intents []intent
	// written is the set of keys with a recorded write, so a read of one can be
	// refused rather than silently answered from the snapshot.
	written map[rowRef]bool
	// visible gates each row read against the caller's read filter, or is nil
	// for a Trusted call. Resolved before the snapshot opened; see readGate.
	visible func(storage.Row) (bool, error)
}

// rowRef identifies a row within the workspace for read-after-write detection.
type rowRef struct{ entity, key string }

func (a *ApplyTx) info(msg proto.Message) (*entityInfo, error) {
	return entityInfoFor(a.srv, msg)
}

// record appends a write and marks its row.
func (a *ApplyTx) record(in intent) {
	if a.written == nil {
		a.written = map[rowRef]bool{}
	}
	a.written[rowRef{entity: in.info.name, key: in.key}] = true
	a.intents = append(a.intents, in)
}

// Get loads the row with the given key into dst and reports whether a live row
// was found and the sequence (version) it was last written at, so the result
// can be handed straight back to a compare-and-swap Patch. A missing or deleted
// row returns (false, 0, nil).
//
// Reading a key this closure has already written is an ERROR, not a stale read.
// Apply does not maintain a read-your-own-writes overlay, and silently
// answering with the pre-write row is the one outcome that would be wrong
// without saying so. Track the value you wrote yourself, or use Mutate if you
// genuinely need to read back mid-transaction.
func (a *ApplyTx) Get(dst proto.Message, key string) (found bool, version uint64, err error) {
	info, err := a.info(dst)
	if err != nil {
		return false, 0, err
	}
	if a.written[rowRef{entity: info.name, key: key}] {
		return false, 0, fmt.Errorf("server: Apply: Get of %s/%q after writing it — reads come from the snapshot taken before the closure ran and do not see recorded writes; keep the written value, or use Mutate for read-your-own-writes", info.name, key)
	}
	return readRowVisible(a.tx, info, dst, key, a.visible)
}

// GetMeta is Get, reporting the row's whole RowMeta instead of its version
// alone — the read that answers "who last wrote this". A missing or deleted row
// returns (false, RowMeta{}, nil), and reading a key this closure has already
// written is an error, exactly as in Get.
func (a *ApplyTx) GetMeta(dst proto.Message, key string) (found bool, meta RowMeta, err error) {
	info, err := a.info(dst)
	if err != nil {
		return false, RowMeta{}, err
	}
	if a.written[rowRef{entity: info.name, key: key}] {
		return false, RowMeta{}, fmt.Errorf("server: Apply: GetMeta of %s/%q after writing it — reads come from the snapshot taken before the closure ran and do not see recorded writes; keep the written value, or use Mutate for read-your-own-writes", info.name, key)
	}
	return readRowMeta(a.tx, info, dst, key, a.visible)
}

// ScanPage iterates up to limit live rows of the entity identified by prototype
// with key > afterKey, in key order, decoding each into a fresh message and
// passing it with its version. It returns the key of the last row visited and
// whether more rows follow; pass lastKey back as afterKey to continue. limit
// must be positive.
//
// Rows this closure has recorded writes for are returned as they were in the
// snapshot; recorded writes are not visible to any read (see Apply).
func (a *ApplyTx) ScanPage(prototype proto.Message, afterKey string, limit int, fn func(msg proto.Message, version uint64) error) (lastKey string, more bool, err error) {
	info, err := a.info(prototype)
	if err != nil {
		return "", false, err
	}
	return scanPageRows(a.tx, info, afterKey, limit, a.visible, fn)
}

// Query iterates the live rows matching spec, in spec's order, decoding each
// into a fresh message and passing it with its version — the "find everything
// matching X and write to it" read. It is the same compiled predicate the Query
// RPC and the TS client use.
//
// A declared secondary index serves it when one covers the predicate and
// ordering; otherwise it is a full scan of the entity. A spec with no Limit
// holds every match in memory either way; see query.Spec. Unlike EntityTx.Query
// it does NOT run under the workspace write lock, so a broad query here blocks
// no other writer.
//
// Recorded writes are not visible to it (see Apply).
func (a *ApplyTx) Query(prototype proto.Message, spec query.Spec, fn func(msg proto.Message, version uint64) error) error {
	info, err := a.info(prototype)
	if err != nil {
		return err
	}
	rows, err := runQuerySpec(a.srv, a.tx, a.ws, info, spec, a.visible)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := fn(r.msg, r.row.Version); err != nil {
			return err
		}
	}
	return nil
}

// Put records a full-row snapshot write; the key derives from key_fields. It
// takes effect after the closure returns, with EntityTx.Put's semantics —
// including the collaborative-text exception, where text fields union with the
// stored document rather than replacing it. That merge happens against the row
// as it stands at apply time, not as the snapshot saw it, which is what keeps a
// concurrent editor's work from being dropped.
//
// WithExpectedVersion makes it a compare-and-swap against the row at apply
// time, so the conflict surfaces from Apply rather than from this call. The
// version read through this same closure is the right thing to pass.
func (a *ApplyTx) Put(msg proto.Message, opts ...WriteOption) error {
	info, err := a.info(msg)
	if err != nil {
		return err
	}
	o, err := collectWriteOpts(opts)
	if err != nil {
		return err
	}
	key := info.keyOf(msg.ProtoReflect())
	if err := info.checkKey(key); err != nil {
		return err
	}
	// Clone: the caller may keep mutating msg after recording it, and the write
	// that lands must be the one that was authorized.
	a.record(intent{
		op:       syncplanev1.CrudOp_CRUD_OP_PUT,
		info:     info,
		key:      key,
		msg:      proto.Clone(msg),
		expected: o.expected,
		opts:     opts,
	})
	return nil
}

// Patch records a masked merge onto the stored row, with EntityTx.Patch's
// semantics: per-field LWW over the named paths, PATCH is not an upsert, and a
// non-nil expected makes it a compare-and-swap.
//
// The version you read through this same closure is the right thing to pass as
// expected — that is how a read-modify-write here notices a concurrent edit
// instead of silently merging with it. Without it, the merge follows arrival
// order like any other write.
//
// The conflict, mask and key-field rejections all surface when the write
// applies, after the closure returns, so they come back from Apply rather than
// from this call.
func (a *ApplyTx) Patch(msg proto.Message, mask []string, expected *uint64, opts ...WriteOption) error {
	info, err := a.info(msg)
	if err != nil {
		return err
	}
	o, err := collectWriteOpts(opts)
	if err != nil {
		return err
	}
	// Mirror EntityTx.Patch: text options contribute their paths to the mask, so
	// the recorded mask is the effective one and is what gets authorized.
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
	a.record(intent{
		op:       syncplanev1.CrudOp_CRUD_OP_PATCH,
		info:     info,
		key:      key,
		msg:      proto.Clone(msg),
		mask:     slices.Clone(mask),
		expected: expected,
		opts:     opts,
	})
	return nil
}

// Delete records a tombstone. prototype only identifies the entity type.
func (a *ApplyTx) Delete(prototype proto.Message, key string) error {
	info, err := a.info(prototype)
	if err != nil {
		return err
	}
	if err := info.checkKey(key); err != nil {
		return err
	}
	a.record(intent{op: syncplanev1.CrudOp_CRUD_OP_DELETE, info: info, key: key, msg: prototype})
	return nil
}
