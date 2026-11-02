package rebase_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

func init() { slog.SetDefault(slog.New(slog.DiscardHandler)) }

func TestRebase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rebase Suite")
}

const ws = "ws1"

// node is one in-process engine on its own SQLite store, written to through
// the wire Push handler: the same path a client's writes take.
type node struct {
	srv *server.Server
	ids map[string]uint64
}

func newNode(opts ...server.Option) *node {
	GinkgoHelper()
	store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "node.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	srv, err := server.New(store, append([]server.Option{server.WithEntities(
		&testdatav1.Widget{}, &testdatav1.Pair{}, &testdatav1.KeyedTextEntity{},
		&testdatav1.Task{}, &testdatav1.User{})}, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	return &node{srv: srv, ids: map[string]uint64{}}
}

// push sends one crud as the next mutation of client and reports whether the
// engine applied it. A rejection is an outcome, not a failure.
func (n *node) push(client string, crud *syncplanev1.CrudMutation) bool {
	GinkgoHelper()
	return n.pushResult(client, crud).GetStatus() == syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED
}

// pushResult is push returning the engine's verdict.
func (n *node) pushResult(client string, crud *syncplanev1.CrudMutation) *syncplanev1.MutationResult {
	GinkgoHelper()
	n.ids[client]++
	resp, err := n.srv.Push(context.Background(), connect.NewRequest(&syncplanev1.PushRequest{
		Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: []*syncplanev1.Mutation{{
			ClientId:   client,
			MutationId: n.ids[client],
			Payload:    &syncplanev1.Mutation_Crud{Crud: crud},
		}}}},
	}))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Msg.GetResults()).To(HaveLen(1))
	batch := resp.Msg.GetResults()[0]
	Expect(batch.GetErrorCode()).To(BeEmpty(), "batch failed: %s", batch.GetErrorMessage())
	Expect(batch.GetResults()).To(HaveLen(1))
	return batch.GetResults()[0]
}

// row reads one row back, deterministically encoded.
func (n *node) row(prototype proto.Message, key string) rebase.Row {
	GinkgoHelper()
	dst := prototype.ProtoReflect().New().Interface()
	var found bool
	Expect(n.srv.View(context.Background(), ws, func(tx *server.ViewTx) error {
		var err error
		found, _, err = tx.Get(dst, key)
		return err
	})).To(Succeed())
	if !found {
		return rebase.Row{}
	}
	return rebase.Row{Present: true, Value: deterministic(dst)}
}

func (n *node) pull(cursor []byte) *syncplanev1.PullResponse {
	GinkgoHelper()
	resp, err := n.srv.Pull(context.Background(), connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: ws,
		Cursor:      cursor,
	}))
	Expect(err).NotTo(HaveOccurred())
	return resp.Msg
}

func crud(op syncplanev1.CrudOp, entity, key string, value proto.Message, mask ...string) *syncplanev1.CrudMutation {
	c := &syncplanev1.CrudMutation{Entity: entity, Key: key, Op: op}
	if value != nil {
		c.Value = deterministic(value)
	}
	if mask != nil {
		c.FieldMask = &fieldmaskpb.FieldMask{Paths: mask}
	}
	return c
}

func opOf(c *syncplanev1.CrudMutation) rebase.Op {
	return rebase.Op{Kind: c.GetOp(), Value: c.GetValue(), Mask: c.GetFieldMask().GetPaths()}
}

func deterministic(m proto.Message) []byte {
	GinkgoHelper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func mustModel(prototype proto.Message) *rebase.Model {
	GinkgoHelper()
	m, err := rebase.NewModel(prototype.ProtoReflect().Descriptor(), nil)
	Expect(err).NotTo(HaveOccurred())
	return m
}

// describe renders a row for a failure message.
func describe(prototype proto.Message, r rebase.Row) string {
	if !r.Present {
		return "<absent>"
	}
	dst := prototype.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(r.Value, dst); err != nil {
		return "<undecodable: " + err.Error() + ">"
	}
	out := prototext.Format(dst)
	// Text documents are binary; show what they read as.
	if w, ok := dst.(*testdatav1.Widget); ok && len(w.GetBody()) > 0 {
		s, _ := text.PlainText(w.GetBody())
		out += "\nbody text: " + s
	}
	return out
}

// doc returns a document holding s, written by peer.
func doc(peer, s string) []byte {
	GinkgoHelper()
	d := text.New(peer)
	Expect(d.Insert(0, s)).To(Succeed())
	return d.Encode()
}

// edited returns base with s appended by peer.
func edited(base []byte, peer, s string) []byte {
	GinkgoHelper()
	d, err := text.Decode(base, peer)
	Expect(err).NotTo(HaveOccurred())
	Expect(d.Insert(d.Len(), s)).To(Succeed())
	return d.Encode()
}

func plain(b []byte) string {
	GinkgoHelper()
	s, err := text.PlainText(b)
	Expect(err).NotTo(HaveOccurred())
	return s
}
