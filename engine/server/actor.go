package server

import (
	"context"
	"unicode/utf8"

	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/storage"
)

// actorKey is the unexported ctx key naming a trusted writer. Distinct type,
// like principalKey and trustedKey, so nothing outside this package can forge
// one.
type actorKey struct{}

// systemActorName is what a bare Trusted write records as its label. It gives
// every bypassed write a non-empty System, so "System is set" and "the hooks
// were skipped" are the same statement whether or not the caller named itself.
const systemActorName = "system"

// TrustedAs marks ctx as system work like Trusted does, and additionally names
// the machinery taking the bypass so its writes are attributable:
//
//	srv.Mutate(server.TrustedAs(ctx, "connect:markdown"), ws, func(…) error { … })
//
// The name is recorded on every row the call writes and surfaces on the change
// feed as Actor{System: name}. It is a label the operator chooses, never an
// identity: nothing verifies it and no policy runs against it (a trusted call
// skips the hooks entirely — that is what Trusted means). What it buys is a
// feed consumer that can recognize its own writes and not react to them, and an
// operator who can tell which pass wrote a row.
//
// A principal on the ctx is recorded alongside the name: a bypass taken inside
// a request records both the label and the caller whose request it ran under.
// Names longer than 64 bytes are truncated; one carrying a control character or
// invalid UTF-8 records "system".
//
// Prefer it over bare Trusted for anything that writes on a schedule or in a
// loop. Trusted stays correct and is the right call for one-off work with
// nothing to distinguish; its rows record Actor{System: "system"}.
func TrustedAs(ctx context.Context, name string) context.Context {
	return context.WithValue(Trusted(ctx), actorKey{}, name)
}

// trustedName returns the name a TrustedAs call gave itself, empty for bare
// Trusted.
func trustedName(ctx context.Context) string {
	n, _ := ctx.Value(actorKey{}).(string)
	return n
}

// actorOf resolves who a write is attributed to, and fails the write when the
// ctx principal carries a Subject that cannot be recorded.
//
// It reads the SAME ctx markers authorization does — the principal for a
// caller, the Trusted marker for machinery — so attribution and enforcement can
// never disagree about who is writing. Both are recorded when a ctx carries
// both, which is a bypass running inside a caller's request.
//
// The two halves fail differently on purpose. A subject that will not fit is
// rejected, because a truncated identity is a different identity and possibly a
// colliding one. A system label is truncated and sanitized, because it comes
// from the operator's own code and is not an authorization input — failing
// would turn a typo in a connector's name into a stalled connector.
func actorOf(ctx context.Context) (storage.Actor, error) {
	a := storage.Actor{Subject: principalOf(ctx).Subject}
	if err := naming.ValidateActorSubject(a.Subject); err != nil {
		return storage.Actor{}, Reject(CodeInvalidArgument, err.Error())
	}
	if isTrusted(ctx) {
		if a.System = sanitizeActorSystem(trustedName(ctx)); a.System == "" {
			a.System = systemActorName
		}
	}
	return a, nil
}

// sanitizeActorSystem cuts a trusted writer's label down to something storable:
// the longest prefix that fits MaxActorSystemBytes on a rune boundary, or empty
// if what remains still carries a control character or invalid UTF-8.
func sanitizeActorSystem(name string) string {
	if len(name) > naming.MaxActorSystemBytes {
		name = name[:naming.MaxActorSystemBytes]
		for len(name) > 0 && !utf8.ValidString(name) {
			name = name[:len(name)-1]
		}
	}
	if naming.ValidateActorSystem(name) != nil {
		return ""
	}
	return name
}
