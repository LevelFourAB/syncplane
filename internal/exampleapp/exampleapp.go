// Package exampleapp holds the todo-app wiring shared by cmd/testserver and
// cmd/todoserver: entity registration, the complete_all mutator, and a
// small permission policy exercised by the test suites.
package exampleapp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// Permissions rejects todo writes whose title contains "forbidden" and hides
// todos whose title contains "secret" from reads. The filter epoch is a
// process-local counter per workspace that the bump_epoch mutator advances,
// standing in for out-of-band visibility changes (e.g. a membership edit). It
// also protects one field per caller (see ProtectedFields), which is the one
// hook here that does read the principal.
//
// It embeds server.AllowAll purely to show the partial-implementation pattern;
// every hook below is overridden. CanMutate authorizes on the write's content
// rather than on the caller, so it ignores the principal — a real policy would
// not.
//
// Safe for concurrent use.
type Permissions struct {
	server.AllowAll
	// Keyed per workspace so a bump cannot invalidate cursors it says nothing
	// about: the test suites share one server between workspaces, and a global
	// counter made every suite's cursors collateral of any other suite's bump.
	epochs sync.Map // workspace ID -> *atomic.Uint64
}

// NewPermissions returns the example permission policy (every workspace's
// epoch starts at 0).
func NewPermissions() *Permissions {
	return &Permissions{}
}

// epochOf returns the counter for a workspace, creating it on first use.
func (p *Permissions) epochOf(workspaceID string) *atomic.Uint64 {
	if v, ok := p.epochs.Load(workspaceID); ok {
		return v.(*atomic.Uint64)
	}
	v, _ := p.epochs.LoadOrStore(workspaceID, &atomic.Uint64{})
	return v.(*atomic.Uint64)
}

func (*Permissions) CanMutate(_ context.Context, _ server.Principal, _ string, w server.Write) error {
	crud := w.Crud
	if crud == nil || crud.GetEntity() != "todo" || crud.GetOp() == syncplanev1.CrudOp_CRUD_OP_DELETE {
		return nil
	}
	var todo todov1.Todo
	if err := proto.Unmarshal(crud.GetValue(), &todo); err != nil {
		return nil // push validation reports decode errors
	}
	if strings.Contains(todo.GetTitle(), "forbidden") {
		return server.Reject(server.CodePermissionDenied, "todo titles may not contain 'forbidden'")
	}
	return nil
}

func (*Permissions) ReadFilter(_ context.Context, _ server.Principal, _ string, entity string) (server.RowFilter, error) {
	if entity != "todo" {
		return nil, nil
	}
	return func(_ string, row proto.Message) bool {
		todo, ok := row.(*todov1.Todo)
		return !ok || !strings.Contains(todo.GetTitle(), "secret")
	}, nil
}

func (p *Permissions) FilterEpoch(_ context.Context, _ server.Principal, workspaceID string) (uint64, error) {
	return p.epochOf(workspaceID).Load(), nil
}

// ProtectedFields puts the "guarded" entity's `label` out of reach of the
// subject "viewer", on top of the fields that entity's guard already keeps
// from every client.
//
// It is the per-caller half of field protection, and the only half a client
// cannot predict: the suites assert that such a write enqueues and comes back as
// a rejection ack, where a field no guard rule admits writing is refused at the
// call site. The
// subject comes from the bearer token testserver takes at face value, so a spec
// chooses which side of the rule it is on by who it authenticates as.
func (*Permissions) ProtectedFields(
	_ context.Context, p server.Principal, _, entity string,
) ([]string, error) {
	if entity != "guarded" || p.Subject != "viewer" {
		return nil, nil
	}
	return []string{"label"}, nil
}

// BumpEpoch invalidates every outstanding cursor for one workspace, leaving
// the other workspaces' alone. This policy does not implement
// server.EpochReadFilters, so the next pull of every client in that workspace
// resets into a re-bootstrap (the delta-resync fallback path).
func (p *Permissions) BumpEpoch(workspaceID string) uint64 {
	return p.epochOf(workspaceID).Add(1)
}

// Options returns the server options for the whole example app: the todo,
// note and annotation entities plus every example mutator. perms may be nil
// when the caller does not install the example permission policy; the
// bump_epoch mutator is then a no-op.
//
//	srv, err := server.New(store, append(exampleapp.Options(perms),
//		server.WithPermissions(perms))...)
func Options(perms *Permissions) []server.Option {
	return append([]server.Option{
		// The collaborative-notes document (text field, auto-installed merger)
		// and its comments (ordinary rows referenced by "comment:<id>" marks).
		server.WithEntities(&todov1.Todo{}, &todov1.Note{}, &todov1.Annotation{}),
	}, MutatorOptions(perms)...)
}

// MutatorOptions returns just complete_all, write_then_reject, fail_internal,
// set_owner and bump_epoch, for servers that declare the todo entity
// themselves. Every caller admitted to the workspace may invoke each of them,
// in a schema that uses the access model too.
func MutatorOptions(perms *Permissions) []server.Option {
	open := server.MutatorAccess{Admitted: true}
	return []server.Option{
		server.WithMutator("complete_all", &todov1.CompleteAllArgs{},
			func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				var todos []*todov1.Todo
				if err := tx.Scan(&todov1.Todo{}, func(msg proto.Message) error {
					todos = append(todos, msg.(*todov1.Todo))
					return nil
				}); err != nil {
					return err
				}
				for _, todo := range todos {
					if todo.GetCompleted() {
						continue
					}
					todo.Completed = true
					if err := tx.Put(todo); err != nil {
						return err
					}
				}
				return nil
			}, open),
		// write_then_reject writes the given todo and then rejects. It exists so
		// the Go and TS test suites can verify that a rejection rolls back the
		// rows the mutator already wrote (push runs mutations in savepoints).
		server.WithMutator("write_then_reject", &todov1.Todo{},
			func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
				todo, _ := args.(*todov1.Todo)
				if todo.GetId() == "" {
					todo = &todov1.Todo{Id: "rejected", Title: "should never persist"}
				}
				if err := tx.Put(todo); err != nil {
					return err
				}
				return server.Reject(server.CodeConflict, "write_then_reject always rejects")
			}, open),
		// fail_internal returns a plain error where it should return a Reject,
		// which is the app-code mistake that fails a whole batch as "internal"
		// on every attempt. It exists so the TS suite can prove the client's
		// dead-letter backstop drains a queue no retry can.
		server.WithMutator("fail_internal", nil,
			func(context.Context, *server.EntityTx, proto.Message) error {
				return errors.New("fail_internal always fails")
			}, open),
		// set_owner is the escape hatch a guarded field is designed around: a
		// mutator's rows are covered by the verdict on its invocation, so this
		// writes "guarded".owner on behalf of a caller no ordinary write would
		// let near it. Integration tests use it to give a guarded field a
		// value, which is the only way one acquires one.
		server.WithMutator("set_owner", &testdatav1.Guarded{},
			func(_ context.Context, tx *server.EntityTx, args proto.Message) error {
				g := args.(*testdatav1.Guarded)
				return tx.Patch(g, []string{"owner"}, nil)
			}, open),
		// bump_epoch lets integration tests exercise filter-epoch revocation
		// end to end: push bump_epoch, the notifier fires, the next pull resets.
		server.WithMutator("bump_epoch", nil,
			func(_ context.Context, tx *server.EntityTx, _ proto.Message) error {
				if perms != nil {
					perms.BumpEpoch(tx.Workspace())
				}
				return nil
			}, open),
	}
}
