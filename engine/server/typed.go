package server

import (
	"context"
	"fmt"
	"iter"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/query"
)

// Reader is a transaction handle the typed reads accept: *EntityTx, *ViewTx
// or *ApplyTx. No other type implements it.
//
// A typed read applies the same read rules as the handle's own methods — the
// caller's read filter and field guards on a ViewTx, an ApplyTx, an AsCaller
// view and a Server.Mutate for a caller who is not Trusted — and is valid only
// inside the closure or mutator body that received the handle.
type Reader interface {
	getMeta(dst proto.Message, key string) (bool, RowMeta, error)
	scanMeta(prototype proto.Message, afterKey string, limit int, fn func(proto.Message, RowMeta) error) (string, bool, error)
	queryMeta(prototype proto.Message, spec query.Spec, fn func(proto.Message, RowMeta) error) error
}

// Writer is a transaction handle Delete accepts: *EntityTx or *ApplyTx.
type Writer interface {
	Reader
	Delete(prototype proto.Message, key string) error
}

// Row is a decoded row together with what the engine recorded about it, as
// ScanRows and QueryRows yield it.
type Row[T proto.Message] struct {
	Msg T
	RowMeta
}

// scanPage is how many rows Scan and ScanRows read from storage per step.
// Each page is read in full before any of it is yielded, so the loop body
// never runs while a storage cursor is open and may write through the same
// transaction.
const scanPage = 256

// Get reads the row of T's entity with the given key, as the handle's own Get
// does. It returns the zero T and false for a row that does not exist, is
// deleted, or is hidden from the caller.
//
// T is a generated message type, such as *todov1.Todo. Get returns an error when
// T is an interface type or not a registered entity, and on an ApplyTx for a
// key the closure has already written.
func Get[T proto.Message](tx Reader, key string) (T, bool, error) {
	var zero T
	dst, err := prototypeOf[T]()
	if err != nil {
		return zero, false, err
	}
	found, _, err := tx.getMeta(dst, key)
	if err != nil || !found {
		return zero, false, err
	}
	return dst.(T), true, nil
}

// Scan yields every live row of T's entity in key order, each as a new T. An
// error ends the sequence: Scan yields it with the zero T and stops.
//
// Scan reads in pages of key order, and the loop body runs between pages, so
// the body may read and write through tx. Through an ApplyTx the writes are
// recorded and no later step yields them; through an EntityTx a later step may
// yield a row the body wrote, with its new value.
//
//	for todo, err := range server.Scan[*todov1.Todo](tx) {
//		if err != nil {
//			return err
//		}
//		…
//	}
//
// T is a generated message type, such as *todov1.Todo. Scan yields an error
// when T is an interface type or not a registered entity.
func Scan[T proto.Message](tx Reader) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for row, err := range ScanRows[T](tx) {
			if !yield(row.Msg, err) {
				return
			}
		}
	}
}

// ScanRows is Scan, yielding each row's RowMeta with it. Hand Version back
// through WithExpectedVersion, or Patch's expected argument, to make a write a
// compare-and-swap.
func ScanRows[T proto.Message](tx Reader) iter.Seq2[Row[T], error] {
	return func(yield func(Row[T], error) bool) {
		prototype, err := prototypeOf[T]()
		if err != nil {
			yield(Row[T]{}, err)
			return
		}
		var (
			after string
			page  []Row[T]
		)
		for {
			clear(page)
			page = page[:0]
			last, more, err := tx.scanMeta(prototype, after, scanPage, func(msg proto.Message, meta RowMeta) error {
				t, err := asMessage[T](msg)
				if err != nil {
					return err
				}
				page = append(page, Row[T]{Msg: t, RowMeta: meta})
				return nil
			})
			if err != nil {
				yield(Row[T]{}, err)
				return
			}
			for _, row := range page {
				if !yield(row, nil) {
					return
				}
			}
			if !more {
				return
			}
			after = last
		}
	}
}

// Query yields the live rows of T's entity that match spec, in spec's order,
// each as a new T. It answers as the handle's own Query does, with the same
// errors and the same cost; see ViewTx.Query. An error ends the sequence: Query
// yields it with the zero T and stops.
//
// The whole result is read before the first row is yielded, so the loop body
// may read and write through tx, and no write it makes changes which rows are
// yielded.
//
//	for todo, err := range server.Query[*todov1.Todo](tx, query.Spec{
//		Where: query.Eq("completed", false),
//	}) {
//		…
//	}
func Query[T proto.Message](tx Reader, spec query.Spec) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for row, err := range QueryRows[T](tx, spec) {
			if !yield(row.Msg, err) {
				return
			}
		}
	}
}

// QueryRows is Query, yielding each row's RowMeta with it.
func QueryRows[T proto.Message](tx Reader, spec query.Spec) iter.Seq2[Row[T], error] {
	return func(yield func(Row[T], error) bool) {
		prototype, err := prototypeOf[T]()
		if err != nil {
			yield(Row[T]{}, err)
			return
		}
		var rows []Row[T]
		err = tx.queryMeta(prototype, spec, func(msg proto.Message, meta RowMeta) error {
			t, err := asMessage[T](msg)
			if err != nil {
				return err
			}
			rows = append(rows, Row[T]{Msg: t, RowMeta: meta})
			return nil
		})
		if err != nil {
			yield(Row[T]{}, err)
			return
		}
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// Delete deletes the row of T's entity with the given key, as the handle's own
// Delete does. It returns an error when T is an interface type or not a
// registered entity.
func Delete[T proto.Message](tx Writer, key string) error {
	prototype, err := prototypeOf[T]()
	if err != nil {
		return err
	}
	return tx.Delete(prototype, key)
}

// Mutator declares a custom mutation handler that takes arguments of type A,
// as WithMutator does with a prototype of A. fn receives a non-nil A on every
// entry point, zeroed when the invocation carried no arguments. A mutator that
// takes no arguments is declared through WithMutator with a nil prototype.
//
//	server.Mutator("complete_task",
//		func(ctx context.Context, tx *server.EntityTx, args *taskv1.CompleteTaskArgs) error {
//			…
//		},
//		server.MutatorAccess{Permission: "edit_tasks", Entity: "task", KeyField: "task_id"})
//
// New fails when A is an interface type, and for every reason WithMutator's
// declaration fails.
func Mutator[A proto.Message](name string, fn func(ctx context.Context, tx *EntityTx, args A) error, opts ...MutatorOption) Option {
	return func(s *Server) error {
		prototype, err := prototypeOf[A]()
		if err != nil {
			return fmt.Errorf("server: mutator %q: %w", name, err)
		}
		var body MutatorFunc
		// A nil fn stays nil so that registration refuses it as it refuses a
		// nil MutatorFunc.
		if fn != nil {
			body = func(ctx context.Context, tx *EntityTx, args proto.Message) error {
				a, err := asMessage[A](args)
				if err != nil {
					return err
				}
				return fn(ctx, tx, a)
			}
		}
		return WithMutator(name, prototype, body, opts...)(s)
	}
}

// FilterRows returns a RowFilter that passes each row to fn as a T. It refuses
// a row of another message type.
//
//	func (p *policy) ReadFilter(ctx context.Context, pr server.Principal, ws, entity string) (server.RowFilter, error) {
//		if entity != "issue" {
//			return nil, nil
//		}
//		return server.FilterRows(func(_ string, issue *issuev1.Issue) bool {
//			return issue.GetOwner() == pr.Subject
//		}), nil
//	}
//
// On an entity registered from a descriptor set, each row reaches the filter
// as a dynamic message and is copied into a new T first, at the cost of one
// marshal and one unmarshal per row.
func FilterRows[T proto.Message](fn func(key string, row T) bool) RowFilter {
	return func(key string, row proto.Message) bool {
		t, err := asMessage[T](row)
		if err != nil {
			return false
		}
		return fn(key, t)
	}
}

// prototypeOf returns a new, empty message of type T.
func prototypeOf[T proto.Message]() (proto.Message, error) {
	var zero T
	if any(zero) == nil {
		return nil, fmt.Errorf("server: type argument %v is an interface; use a generated message type such as *todov1.Todo", reflect.TypeFor[T]())
	}
	return zero.ProtoReflect().New().Interface(), nil
}

// asMessage returns msg as a T. The engine decodes the rows of an entity
// registered from a descriptor set as dynamic messages, so a message of another
// Go type with T's full name is copied into a new T through its wire bytes, as
// the handles' Get does for a dst of a foreign type.
func asMessage[T proto.Message](msg proto.Message) (T, error) {
	if t, ok := msg.(T); ok {
		return t, nil
	}
	var zero T
	want := zero.ProtoReflect().Descriptor().FullName()
	if got := msg.ProtoReflect().Descriptor().FullName(); got != want {
		return zero, fmt.Errorf("server: got a %s where %s was expected", got, want)
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return zero, err
	}
	out := zero.ProtoReflect().New().Interface().(T)
	if err := proto.Unmarshal(raw, out); err != nil {
		return zero, err
	}
	return out, nil
}
