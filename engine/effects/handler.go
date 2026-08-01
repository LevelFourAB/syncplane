package effects

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/storage"
)

// Event is one committed change, typed to the entity its handler registered
// for. It is the typed view of Change.
type Event[T proto.Message] struct {
	Workspace string
	Entity    string
	Key       string
	// Seq is the workspace sequence of the mutation that wrote this state.
	// Together with Workspace, Entity and Key it identifies this delivery
	// exactly, which is what a handler writing back keys its idempotency on.
	Seq uint64
	// Deleted reports a tombstone: the row is gone and After is the zero value.
	// A handler that dereferences After must check this first.
	Deleted bool
	// Actor is the row's last writer — see Change.Actor for what that does and
	// does not answer.
	Actor storage.Actor
	// After is the row's new state; the zero value when Deleted.
	After T
	// Before holds the watched fields as they stood when this consumer last
	// acted on this row. It is the zero value when the consumer declared no
	// Watch fields, and when it has never acted on this row before — a first
	// sighting and a row that just came into existence are indistinguishable
	// here, because the shadow is this consumer's memory rather than the row's
	// history.
	//
	// Only watched fields are populated. Everything else is zero, which is not
	// a claim that it was empty.
	Before T
	// Changed names the watched fields whose value differs between Before and
	// After. Empty when nothing watched changed, and empty when there is no
	// Before to compare against — so "did this become Done" is DidChange plus a
	// check on After, never DidChange alone.
	Changed []string
	// FirstSeen reports that this consumer had no shadow of the row, so Changed
	// is empty because nothing was known, not because nothing moved. See
	// Change.FirstSeen for which situations produce it and what a handler does
	// about them.
	FirstSeen bool
}

// DidChange reports whether the named watched field is among Changed. The name
// is a field path as declared to Watch.
func (e Event[T]) DidChange(field string) bool {
	return slices.Contains(e.Changed, field)
}

// Option configures a consumer registration.
type Option func(*registration)

// registration accumulates what the options say before the consumer is built.
type registration struct {
	watch []string
}

// On registers an in-process handler for one entity's changes, under a durable
// consumer name.
//
// The entity comes from T, and the handler runs post-commit on a background
// goroutine, serially per workspace, at-least-once — see the package comment for
// what that obliges a handler to do. A handler returning an error wedges this
// consumer on that workspace until the change goes through; returning nil is
// how a handler declines a change it does not care about.
//
//	effects.On(rt, "slack-status", func(ctx context.Context, ev effects.Event[*issuev1.Issue]) error {
//	    if ev.Deleted || !ev.DidChange("status") || ev.After.GetStatus() != done {
//	        return nil
//	    }
//	    return postToSlack(ctx, ev.After)
//	}, effects.Watch("status"))
//
// It is a package-level function rather than a method because Go has no generic
// methods; everything that does not need the type parameter is on Runtime.
func On[T proto.Message](rt *Runtime, name string, fn func(context.Context, Event[T]) error, opts ...Option) error {
	if fn == nil {
		return fmt.Errorf("effects: consumer %q needs a handler", name)
	}
	var reg registration
	for _, opt := range opts {
		opt(&reg)
	}
	// A typed nil carries the descriptor without an instance, which is all the
	// entity name needs; New gives the runtime a real message to work from.
	var zero T
	prototype := zero.ProtoReflect().New().Interface()
	entity, _, err := rt.srv.EntityKey(prototype)
	if err != nil {
		return fmt.Errorf("effects: consumer %q: %w", name, err)
	}
	var watch map[string][]string
	if len(reg.watch) > 0 {
		if err := validateWatch(prototype, reg.watch); err != nil {
			return fmt.Errorf("effects: consumer %q: %w", name, err)
		}
		watch = map[string][]string{entity: reg.watch}
	}
	return rt.register(name, handlerSink[T]{fn: fn}, []proto.Message{prototype}, watch)
}

// handlerSink adapts a typed handler to the untyped Sink the runtime drives.
type handlerSink[T proto.Message] struct {
	fn func(context.Context, Event[T]) error
}

func (h handlerSink[T]) Deliver(ctx context.Context, ch Change) error {
	ev := Event[T]{
		Workspace: ch.Workspace,
		Entity:    ch.Entity,
		Key:       ch.Key,
		Seq:       ch.Seq,
		Deleted:   ch.Deleted,
		Actor:     ch.Actor,
		Changed:   ch.Changed,
		FirstSeen: ch.FirstSeen,
	}
	// The consumer is bound to one entity, so the assertion holds for every
	// change that reaches it; a failure here means the catalog handed back a
	// different type for the entity than the handler registered with.
	if ch.After != nil {
		msg, ok := ch.After.(T)
		if !ok {
			return fmt.Errorf("effects: change for %s/%s carries %T, want %T", ch.Entity, ch.Key, ch.After, ev.After)
		}
		ev.After = msg
	}
	if ch.Before != nil {
		msg, ok := ch.Before.(T)
		if !ok {
			return fmt.Errorf("effects: shadow for %s/%s carries %T, want %T", ch.Entity, ch.Key, ch.Before, ev.Before)
		}
		ev.Before = msg
	}
	return h.fn(ctx, ev)
}
