package server

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/storage"
)

// MaxIdempotencyKeyBytes bounds a trusted write's idempotency key. Keys are
// stored per workspace and never truncated, so the bound is what keeps a
// caller-supplied string from becoming unbounded storage. It is generous
// against everything real keys are made of — a UUID, a change-feed sequence, a
// provider's event id.
const MaxIdempotencyKeyBytes = 256

// WriteOutcome reports what an idempotent trusted write did.
//
// The distinction that matters is Applied: false means the key had already been
// claimed, the closure did NOT run, and Seq/Result are the ORIGINAL write's —
// which is what lets a retried API request answer with the first attempt's
// response instead of an opaque "already applied".
type WriteOutcome struct {
	// Applied is true when this call ran the closure and committed it, false
	// when it replayed a record an earlier call left behind.
	Applied bool
	// Seq is the workspace sequence the write took, or 0 if it wrote nothing.
	Seq uint64
	// Result is the payload the closure recorded (nil if none). Replayed
	// verbatim on every retry of the key.
	Result []byte
}

// MutateIdempotent is Mutate guarded by a caller-supplied idempotency key: the
// first call for a key runs fn and records what it did, and every later call
// with the same key skips fn entirely and returns that record. It is the write
// path for callers that retry — an HTTP handler behind a load balancer, an
// at-least-once effects handler reacting to the change feed (which re-sees a
// page after a crash by design), any integration with its own backoff.
//
// The key's scope is the workspace, matching the partitioning of sequences,
// write exclusion and client watermarks, and the caller. Each principal has its
// own key space, so a key one principal claimed is a new key for another: that
// call runs fn under its own authorization, and never gets the first caller's
// Result. Trusted callers share one key space, apart from every principal's.
// Two trusted sources must not share keys by accident, so derive keys from
// something the caller owns — the feed cursor position being handled, the
// upstream event id — and prefix them per source when several sources write
// to one workspace.
//
// The check and the write commit together. The ledger read, fn's writes and the
// record all happen in ONE transaction under the workspace's write exclusion,
// so there is no window in which a concurrent retry sees "not applied" while
// the first call is mid-write, and no way to record an outcome the store rolled
// back:
//
//	out, err := srv.MutateIdempotent(ctx, ws, r.Header.Get("Idempotency-Key"),
//	    func(tx *server.EntityTx) ([]byte, error) {
//	        if err := tx.Put(&issue); err != nil {
//	            return nil, err
//	        }
//	        return json.Marshal(response{ID: issue.GetId()})
//	    })
//	if err != nil { ... }
//	w.Write(out.Result) // the first attempt's response, however often it is retried
//
// The bytes fn returns are opaque to the engine — marshal whatever the caller
// needs to reproduce its own answer, or return nil to record only "applied, at
// this sequence". They are bounded by the server's MaxValueBytes limit, since
// they are stored like any other value.
//
// Failure is NOT recorded. An error from fn (including Reject) rolls the whole
// transaction back, key included, so the write stays retryable — a retry after
// a rejection is a fresh attempt, not a replay of the rejection. Only committed
// outcomes are replayable.
//
// Two differences from Mutate worth knowing:
//
//   - A successful call always claims the key, even when fn wrote nothing, so
//     the "nothing to do" outcome is replayable too. Claiming registers the
//     workspace, so unlike Mutate this is not free to call against an arbitrary
//     workspace id.
//   - Keys are garbage-collected on the retention configured by
//     WithIdempotencyRetention (which needs WithGC to run at all). A retry that
//     arrives after its key expired applies again — set the retention above the
//     longest retry window your callers use.
//
// Everything else matches Mutate: each write fn makes is authorized for the ctx
// principal (CanMutate, the schema's write rules and the row check) unless ctx
// is Trusted, all writes share one lazily allocated sequence, clients are
// notified on commit, and fn may run more than once if the adapter retries the
// transaction, so keep side effects out of it until the call returns. A replay
// runs no hook: the record is the outcome of a call the same principal made.
func (s *Server) MutateIdempotent(
	ctx context.Context,
	workspaceID, key string,
	fn func(tx *EntityTx) ([]byte, error),
) (WriteOutcome, error) {
	if workspaceID == "" {
		return WriteOutcome{}, fmt.Errorf("server: workspace id is required")
	}
	if err := validateIdempotencyKey(key); err != nil {
		return WriteOutcome{}, err
	}
	if inWriteTx(ctx) {
		return WriteOutcome{}, fmt.Errorf("server: MutateIdempotent called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock; perform writes through the enclosing EntityTx instead", workspaceID)
	}
	// The ledger is read before txAccess.begin, so the gate keeps its own read:
	// a refused caller must not be answered from a record.
	adm, enforced, err := s.gate(ctx, workspaceID)
	if err != nil {
		return WriteOutcome{}, err
	}
	return s.mutateIdempotent(ctx, workspaceID, ledgerKey(adm.principal, enforced, key), adm, enforced, fn)
}

// mutateIdempotent is the body of MutateIdempotent once the call is validated
// and gated: key is the ledger key, already scoped to the caller.
func (s *Server) mutateIdempotent(
	ctx context.Context,
	workspaceID, key string,
	adm admission,
	enforced bool,
	fn func(tx *EntityTx) ([]byte, error),
) (WriteOutcome, error) {
	authz, gate := s.writeAuthorizer(ctx, adm, workspaceID, enforced)
	var (
		out   WriteOutcome
		wrote bool
	)
	err := s.store.WithWriteTx(ctx, workspaceID, func(tx storage.WriteTx) error {
		// Reset per attempt: the closure is re-runnable (see the storage.Adapter
		// contract), and a captured outcome from an abandoned attempt would
		// report a sequence that rolled back.
		out, wrote = WriteOutcome{}, false
		rec, found, err := tx.IdempotencyRecord(key)
		if err != nil {
			return err
		}
		if found {
			// Already applied: nothing runs, nothing is written, and the
			// transaction commits empty — so a workspace this call did not
			// otherwise touch stays as it was.
			out = WriteOutcome{Seq: rec.Seq, Result: rec.Result}
			return nil
		}
		actor, err := actorOf(ctx)
		if err != nil {
			return err
		}
		if err := gate.accessOf().begin(tx); err != nil {
			return err
		}
		etx := &EntityTx{srv: s, ctx: ctx, ws: workspaceID, tx: tx, authz: authz, gate: gate, actor: actor,
			access: gate.accessOf()}
		result, err := fn(etx)
		if err != nil {
			return err
		}
		if max := s.limits.MaxValueBytes; len(result) > max {
			return fmt.Errorf("server: idempotent write result is %d bytes, over the %d byte limit", len(result), max)
		}
		// Claim the key with the sequence the writes actually took (0 when fn
		// wrote nothing), in this same transaction.
		if err := tx.PutIdempotencyRecord(key, storage.IdempotencyRecord{Seq: etx.Seq(), Result: result}); err != nil {
			return err
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

// InvokeIdempotent runs a registered mutator by name through MutateIdempotent,
// so a trigger that may fire twice — a redelivered webhook, a re-read change
// feed page — applies the mutator once. args follows Invoke's contract: the
// registered prototype's message type, or nil for its zero value (and nil only,
// for a mutator registered without a prototype).
//
// Mutators have no result channel (they run on the client push path too, where
// nothing could receive one), so the recorded outcome carries only "applied, at
// this sequence". Callers that need a payload to replay should use
// MutateIdempotent directly.
func (s *Server) InvokeIdempotent(
	ctx context.Context,
	workspaceID, key, mutatorName string,
	args proto.Message,
) (WriteOutcome, error) {
	if inWriteTx(ctx) {
		return WriteOutcome{}, fmt.Errorf("server: InvokeIdempotent(%q) called re-entrantly inside a write transaction (workspace %q): a nested write transaction waits for the write lock the enclosing one holds and would deadlock; perform writes through the enclosing EntityTx instead", mutatorName, workspaceID)
	}
	mi, bodyArgs, err := s.mutatorFor(mutatorName, args)
	if err != nil {
		return WriteOutcome{}, err
	}
	// The ledger is read before the body checks the invocation, so the gate
	// keeps its own read: a refused caller must not be answered from a record.
	access, err := s.authorizeInvoke(ctx, workspaceID, mutatorName, args, false)
	if err != nil {
		return WriteOutcome{}, err
	}
	if workspaceID == "" {
		return WriteOutcome{}, fmt.Errorf("server: workspace id is required")
	}
	if err := validateIdempotencyKey(key); err != nil {
		return WriteOutcome{}, err
	}
	// Authorized on the verb, exactly as Invoke — the body writes as
	// server-authoritative logic, so the inner call must not re-gate. The key
	// stays the caller's, though: the body runs trusted, but the record must
	// not replay to another principal.
	key = ledgerKey(principalOf(ctx), !isTrusted(ctx), key)
	return s.mutateIdempotent(Trusted(ctx), workspaceID, key, admission{}, false,
		func(tx *EntityTx) ([]byte, error) {
			if err := s.beginInvocation(ctx, workspaceID, tx, access, mi, bodyArgs); err != nil {
				return nil, err
			}
			return nil, mi.fn(markWriteTx(ctx), tx, bodyArgs)
		})
}

// ledgerKey is the key the idempotency ledger stores for one caller's key.
//
// An enforced caller's key starts with callerPrefix, anonymous callers
// included, so two principals never share a record, and no principal shares
// one with a trusted caller. Anonymous callers share one key space: there is no
// identity to divide them by.
//
// A trusted caller's key is stored as given, so every trusted caller shares one
// key space. One that starts with U+001F gets a second U+001F: a principal's
// key has a digit after the first, so the two forms cannot meet.
func ledgerKey(p Principal, enforced bool, key string) string {
	if !enforced {
		if strings.HasPrefix(key, "\u001f") {
			return "\u001f" + key
		}
		return key
	}
	return callerPrefix(p) + key
}

// validateIdempotencyKey rejects keys the ledger cannot store meaningfully. An
// empty key is always a caller bug — it would collapse every write that forgot
// to supply one into a single claim, silently swallowing the second.
func validateIdempotencyKey(key string) error {
	if key == "" {
		return fmt.Errorf("server: idempotency key is required")
	}
	if len(key) > MaxIdempotencyKeyBytes {
		return fmt.Errorf("server: idempotency key is %d bytes, over the %d byte limit", len(key), MaxIdempotencyKeyBytes)
	}
	return nil
}
