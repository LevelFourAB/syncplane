package server

import (
	"context"

	"github.com/levelfourab/syncplane/engine/storage"
)

// actorKey is the unexported ctx key naming a trusted writer. Distinct type,
// like principalKey and trustedKey, so nothing outside this package can forge
// one.
type actorKey struct{}

// TrustedAs marks ctx as system work like Trusted does, and additionally names
// the machinery taking the bypass so its writes are attributable:
//
//	srv.Mutate(server.TrustedAs(ctx, "connect:markdown"), ws, func(…) error { … })
//
// The name is recorded on every row the call writes and surfaces on the change
// feed as Actor{System: true, Subject: name}. It is a label the operator
// chooses, never an identity: nothing verifies it and no policy runs against it
// (a trusted call skips the hooks entirely — that is what Trusted means). What
// it buys is a feed consumer that can recognize its own writes and not react to
// them, and an operator who can tell which pass wrote a row.
//
// Prefer it over bare Trusted for anything that writes on a schedule or in a
// loop. Trusted stays correct and is the right call for one-off work with
// nothing to distinguish; its rows record Actor{System: true} with no name.
func TrustedAs(ctx context.Context, name string) context.Context {
	return context.WithValue(Trusted(ctx), actorKey{}, name)
}

// trustedName returns the name a TrustedAs call gave itself, empty for bare
// Trusted.
func trustedName(ctx context.Context) string {
	n, _ := ctx.Value(actorKey{}).(string)
	return n
}

// actorOf resolves who a write is attributed to. clientID is the push's
// client_id, empty off the sync path.
//
// It reads the SAME ctx markers authorization does — the principal for a
// caller, the Trusted marker for machinery — so attribution and enforcement can
// never disagree about who is writing. A write authorized as some principal is
// recorded as that principal, and a write that skipped the hooks is recorded as
// having skipped them.
//
// Note the deliberate asymmetry with the hooks: Trusted wins here even when the
// ctx also carries a principal. A request ctx that opts one call out of
// enforcement is doing system work with a request's identity still attached
// (Trusted composes with a principal precisely so that is possible), and
// recording the principal would attribute a bypassed write to a caller who was
// never authorized for it.
func actorOf(ctx context.Context, clientID string) storage.Actor {
	if isTrusted(ctx) {
		return storage.Actor{System: true, Subject: trustedName(ctx), Client: clientID}
	}
	p := principalOf(ctx)
	return storage.Actor{Issuer: p.Issuer, Subject: p.Subject, Client: clientID}
}
