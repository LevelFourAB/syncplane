package server_test

import (
	"log/slog"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	todov1 "github.com/levelfourab/syncplane/engine/gen/example/todo/v1"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// init quiets the engine's default-logger output for the whole test binary
// (init runs before any test, including the internal-package tests) so specs
// don't spam stderr; a spec exercising logging installs its own WithLogger.
func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Server Suite")
}

// testEntities are the entities every spec's server declares.
func testEntities() server.Option {
	return server.WithEntities(&todov1.Todo{}, &testdatav1.Widget{}, &testdatav1.Pair{},
		&testdatav1.Project{}, &testdatav1.User{}, &testdatav1.Issue{}, &testdatav1.Ledger{},
		// Both opt into field history, which also brings the engine's
		// server-only history entity into every spec's catalog — the state an
		// app that uses the feature runs in.
		&testdatav1.Historied{}, &testdatav1.HistoriedAll{})
}

// newTestServer builds a server on a fresh SQLite store with the test
// entities declared. Options let specs add permissions, mutators or mergers,
// and failing to build is a test failure — specs that assert on a bad
// declaration call server.New themselves (see newTestServerErr).
func newTestServer(opts ...server.Option) *server.Server {
	srv, _ := newTestServerStore(opts...)
	return srv
}

// newTestServerStore is newTestServer plus the store behind it, for specs that
// have to plant rows the engine's own write paths would never produce.
func newTestServerStore(opts ...server.Option) (*server.Server, storage.Adapter) {
	GinkgoHelper()
	store := newTestStore()
	return mustNewServer(store, append([]server.Option{testEntities()}, opts...)...), store
}

// mustNewServer builds a server, failing the spec if its options do not. For
// specs that bring their own store — a wrapped adapter, or one they plant rows
// in directly — rather than the one newTestServer opens.
func mustNewServer(store storage.Adapter, opts ...server.Option) *server.Server {
	GinkgoHelper()
	srv, err := server.New(store, opts...)
	Expect(err).NotTo(HaveOccurred())
	return srv
}

// newTestServerErr is newTestServer for specs asserting that a declaration is
// rejected: it returns New's error instead of failing the spec.
func newTestServerErr(opts ...server.Option) error {
	GinkgoHelper()
	_, err := server.New(newTestStore(), append([]server.Option{testEntities()}, opts...)...)
	return err
}

// newTestStore opens a SQLite store in the spec's temp dir, closed on cleanup.
func newTestStore() storage.Adapter {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	return store
}

func mustMarshal(m proto.Message) []byte {
	GinkgoHelper()
	raw, err := proto.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

// pushOf builds a single-workspace push — the shape almost every test wants
// now that a PushRequest carries one batch per workspace. Tests that exercise
// the multi-workspace path build the batches themselves.
func pushOf(ws string, muts []*syncplanev1.Mutation) *syncplanev1.PushRequest {
	return &syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: muts}}}
}

// pushResults returns the mutation results of a single-workspace push, failing
// the test if the workspace's batch was rejected as a whole (a gap, a revoked
// workspace). Tests asserting on that outcome read WorkspaceResults directly.
func pushResults(resp *syncplanev1.PushResponse) []*syncplanev1.MutationResult {
	GinkgoHelper()
	Expect(resp.GetResults()).To(HaveLen(1))
	ws := resp.GetResults()[0]
	Expect(ws.GetErrorCode()).To(BeEmpty(), "batch failed: %s", ws.GetErrorMessage())
	return ws.GetResults()
}

func putMutation(clientID string, mutationID uint64, entity, key string, value proto.Message) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
			Entity: entity,
			Key:    key,
			Op:     syncplanev1.CrudOp_CRUD_OP_PUT,
			Value:  mustMarshal(value),
		}},
	}
}

func patchMutation(clientID string, mutationID uint64, entity, key string, value proto.Message, paths ...string) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
			Entity:    entity,
			Key:       key,
			Op:        syncplanev1.CrudOp_CRUD_OP_PATCH,
			Value:     mustMarshal(value),
			FieldMask: &fieldmaskpb.FieldMask{Paths: paths},
		}},
	}
}

func deleteMutation(clientID string, mutationID uint64, entity, key string) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
			Entity: entity,
			Key:    key,
			Op:     syncplanev1.CrudOp_CRUD_OP_DELETE,
		}},
	}
}

// withExpectedVersion stamps a compare-and-swap guard onto a CRUD mutation's
// sub-payload and returns the same mutation for inline use.
func withExpectedVersion(m *syncplanev1.Mutation, version uint64) *syncplanev1.Mutation {
	m.GetCrud().ExpectedVersion = proto.Uint64(version)
	return m
}

func crudPut(entity, key string, value proto.Message) *syncplanev1.CrudMutation {
	return &syncplanev1.CrudMutation{Entity: entity, Key: key, Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: mustMarshal(value)}
}

func crudPatch(entity, key string, value proto.Message, paths ...string) *syncplanev1.CrudMutation {
	return &syncplanev1.CrudMutation{
		Entity: entity, Key: key, Op: syncplanev1.CrudOp_CRUD_OP_PATCH,
		Value: mustMarshal(value), FieldMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
}

func crudDelete(entity, key string) *syncplanev1.CrudMutation {
	return &syncplanev1.CrudMutation{Entity: entity, Key: key, Op: syncplanev1.CrudOp_CRUD_OP_DELETE}
}

func batchMutation(clientID string, mutationID uint64, cruds ...*syncplanev1.CrudMutation) *syncplanev1.Mutation {
	return &syncplanev1.Mutation{
		ClientId:   clientID,
		MutationId: mutationID,
		Payload:    &syncplanev1.Mutation_Batch{Batch: &syncplanev1.BatchMutation{Mutations: cruds}},
	}
}
