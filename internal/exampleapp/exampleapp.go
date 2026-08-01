// Package exampleapp holds the todo-app wiring shared by cmd/testserver and
// cmd/todoserver: entity registration, the complete_all mutator, and a
// small permission policy exercised by the test suites.
package exampleapp

import (
	"context"
	"strings"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// Permissions rejects todo writes whose title contains "forbidden" and hides
// todos whose title contains "secret" from reads. The filter epoch is a
// process-local counter the bump_epoch mutator advances, standing in for
// out-of-band visibility changes (e.g. a membership edit).
//
// It embeds server.AllowAll purely to show the partial-implementation pattern;
// every hook below is overridden. It authorizes on the write's content rather
// than on the caller, so it ignores the principal — a real policy would not.
type Permissions struct {
	server.AllowAll
	epoch atomic.Uint64
}

// NewPermissions returns the example permission policy (epoch starts at 0).
func NewPermissions() *Permissions {
	return &Permissions{}
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

func (p *Permissions) FilterEpoch(context.Context, server.Principal, string) (uint64, error) {
	return p.epoch.Load(), nil
}

// BumpEpoch invalidates all outstanding cursors. This policy does not
// implement server.EpochReadFilters, so the next pull of every client
// resets into a re-bootstrap (the delta-resync fallback path).
func (p *Permissions) BumpEpoch() uint64 {
	return p.epoch.Add(1)
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

// MutatorOptions returns just complete_all, write_then_reject and bump_epoch,
// for servers that declare the todo entity themselves.
func MutatorOptions(perms *Permissions) []server.Option {
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
			}),
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
			}),
		// bump_epoch lets integration tests exercise filter-epoch revocation
		// end to end: push bump_epoch, the notifier fires, the next pull resets.
		server.WithMutator("bump_epoch", nil,
			func(context.Context, *server.EntityTx, proto.Message) error {
				if perms != nil {
					perms.BumpEpoch()
				}
				return nil
			}),
	}
}
