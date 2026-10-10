package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/cursorstate"
	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// In a schema that declares a role, the workspace gate refuses a caller who
// holds no permission. With the default policy it leaves that check to the
// transaction that serves the request, which makes it before it reads
// anything else. A refused caller must get the error a gate with a read of its
// own gives, and must leave no trace. The gate keeps a read of its own where
// the serving transaction cannot check first.
var _ = Describe("The grant check of the workspace gate", func() {
	const ws = "ws"

	ops := gateOps()
	entries := func() []TableEntry {
		var out []TableEntry
		for _, op := range ops {
			out = append(out, Entry(op.name, op))
		}
		return out
	}

	Context("with the default policy and no replica", func() {
		var f *gateFixture

		BeforeEach(func() {
			f = newGateFixture(false)
		})

		DescribeTable("serves an admitted caller from one transaction that checks the grants first",
			func(op gateOp) {
				op.run(f, "me", ws)
				txs := f.rec.take()
				Expect(txs).NotTo(BeEmpty())
				Expect(txs[0].kind).To(Equal(op.firstTx))
				Expect(txs[0].calls).NotTo(BeEmpty())
				Expect(txs[0].calls[0]).To(Equal(grantProbe), "the transaction read %v", txs[0].calls)
				for _, tx := range txs[1:] {
					Expect(tx.calls).NotTo(ContainElement(grantProbe), "the grants were read again")
				}
				if op.name != "Bootstrap" && op.name != "Apply" {
					Expect(txs).To(HaveLen(1))
				}
			},
			entries(),
		)

		DescribeTable("refuses a caller who holds no permission as a gate with a read of its own does",
			func(op gateOp) {
				apart := newGateFixture(false, WithPermissions(openPolicy{}))
				for _, user := range []string{"stranger", ""} {
					want := op.run(apart, user, ws)
					Expect(want).To(MatchRegexp(refusedOutcome))
					Expect(op.run(f, user, ws)).To(Equal(want))
					txs := f.rec.take()
					Expect(txs).To(HaveLen(1), "a refused request opened %d transactions", len(txs))
					Expect(txs[0].kind).To(Equal(op.firstTx))
					for _, call := range txs[0].calls {
						Expect(isGrantRead(call)).To(BeTrue(), "the transaction of a refused caller read %s", call)
					}
					Expect(f.rec.cursorStateCalls()).To(BeEmpty())
					f.expectUntouched(user)
				}
			},
			entries(),
		)

		DescribeTable("leaves no workspace behind for a refused caller",
			func(op gateOp) {
				op.run(f, "stranger", "ws-new")
				ids, err := f.srv.WorkspaceIDs(f.ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(ids).NotTo(ContainElement("ws-new"))
			},
			entries(),
		)

		DescribeTable("answers a malformed request of a refused caller with the gate's error",
			func(name string, run func(f *gateFixture, user string) string) {
				apart := newGateFixture(false, WithPermissions(openPolicy{}))
				want := run(apart, "stranger")
				Expect(want).To(ContainSubstring(CodeWorkspaceNoAccess))
				Expect(run(f, "stranger")).To(Equal(want))
				Expect(f.rec.cursorStateCalls()).To(BeEmpty())
				// The request is malformed, so an admitted caller is refused for
				// another reason, which a refused caller must not learn.
				Expect(run(f, "me")).NotTo(ContainSubstring(CodeWorkspaceNoAccess))
			},
			Entry("an undecodable pull cursor", "pull", func(f *gateFixture, user string) string {
				return f.pull(user, ws, []byte{0xff})
			}),
			Entry("a pull cursor that names a cursor-state entry", "pull", func(f *gateFixture, user string) string {
				return f.pull(user, ws, mustMarshalProto(&syncplanecursorv1.Cursor{
					SetsRef: &syncplanecursorv1.StateRef{Id: 7, Gen: 7},
				}))
			}),
			Entry("a query of an unknown entity", "query", func(f *gateFixture, user string) string {
				return f.query(user, ws, "nope")
			}),
			Entry("an undecodable bootstrap resume token", "bootstrap", func(f *gateFixture, user string) string {
				return f.bootstrap(user, ws, &syncplanev1.BootstrapResume{
					Cursor: mustMarshalProto(&syncplanecursorv1.Cursor{Seq: 1}), Token: []byte{0xff},
				})
			}),
			Entry("a push of a mutation without an id", "push", func(f *gateFixture, user string) string {
				return f.push(user, ws, 0)
			}),
		)

		It("reads the cursor-state store for an admitted caller's cursor", func() {
			// The spec above would pass vacuously if that cursor never reached
			// the store.
			f.pull("me", ws, mustMarshalProto(&syncplanecursorv1.Cursor{
				SetsRef: &syncplanecursorv1.StateRef{Id: 7, Gen: 7},
			}))
			Expect(f.rec.cursorStateCalls()).To(ContainElement("Get"))
		})

		DescribeTable("fails as a gate with a read of its own does when the grant read fails",
			func(op gateOp) {
				apart := newGateFixture(false, WithPermissions(openPolicy{}))
				apart.rec.failOn(grantProbe)
				f.rec.failOn(grantProbe)
				want := op.run(apart, "me", ws)
				Expect(want).To(ContainSubstring("resolving"))
				Expect(op.run(f, "me", ws)).To(Equal(want))
			},
			entries(),
		)
	})

	Context("with a policy of the application", func() {
		It("reads the grants in a transaction of its own and runs no hook of the policy for a refused caller", func() {
			policy := &hookCountingPolicy{}
			f := newGateFixture(false, WithPermissions(policy))
			for _, op := range ops {
				policy.hooks.Store(0)
				Expect(op.run(f, "stranger", ws)).To(MatchRegexp(refusedOutcome), op.name)
				Expect(policy.hooks.Load()).To(BeZero(), "%s ran a hook of the policy for a refused caller", op.name)
				txs := f.rec.take()
				Expect(txs).To(HaveLen(1), op.name)
				Expect(txs[0].kind).To(Equal("read"), op.name)
			}
			f.pull("me", ws, nil)
			txs := f.rec.take()
			Expect(txs).To(HaveLen(2))
			Expect(txs[0].calls).To(ContainElement(grantProbe))
			Expect(txs[1].calls).NotTo(ContainElement(grantProbe))
		})
	})

	Context("with a read replica", func() {
		It("reads the grants from the primary before a read that the replica can serve", func() {
			f := newGateFixture(true)
			for _, op := range ops {
				if !op.routed {
					continue
				}
				op.run(f, "me", ws)
				txs := f.rec.take()
				Expect(len(txs)).To(BeNumerically(">=", 2), op.name)
				Expect(txs[0].kind).To(Equal("read"), op.name)
				Expect(txs[0].calls).To(ContainElement(grantProbe), op.name)
				Expect(txs[1].kind).To(Equal("replica"), op.name)

				Expect(op.run(f, "stranger", ws)).To(MatchRegexp(refusedOutcome), op.name)
				txs = f.rec.take()
				Expect(txs).To(HaveLen(1), op.name)
				Expect(txs[0].kind).To(Equal("read"), op.name)
			}
		})

		It("checks the grants inside a transaction on the primary", func() {
			f := newGateFixture(true)
			for _, op := range ops {
				if op.routed {
					continue
				}
				op.run(f, "me", ws)
				txs := f.rec.take()
				Expect(txs).NotTo(BeEmpty(), op.name)
				Expect(txs[0].kind).To(Equal(op.firstTx), op.name)
				Expect(txs[0].calls[0]).To(Equal(grantProbe), op.name)
			}
		})
	})
})

const (
	// grantProbe is the first read of a resolution of the grants of "me": the
	// probe of the subject index of repo_member.
	grantProbe = "probe:repo_member/__field_user_id"
	// refusedOutcome matches the outcome of a request the gate refused for
	// want of a permission.
	refusedOutcome = "holds no permission in this workspace|" + CodeWorkspaceNoAccess
)

// gateOp is one request a caller can make of a workspace.
type gateOp struct {
	name string
	// firstTx is the kind of the transaction that serves the request first.
	firstTx string
	// routed marks a request whose read a replica can serve.
	routed bool
	// run makes the request as user in ws and returns its outcome, as
	// gateFixture's methods render it.
	run func(f *gateFixture, user, ws string) string
}

func gateOps() []gateOp {
	return []gateOp{
		{name: "Pull", firstTx: "read", routed: true, run: func(f *gateFixture, user, ws string) string {
			return f.pull(user, ws, nil)
		}},
		{name: "Query", firstTx: "read", routed: true, run: func(f *gateFixture, user, ws string) string {
			return f.query(user, ws, "issue")
		}},
		{name: "Bootstrap", firstTx: "read", routed: true, run: func(f *gateFixture, user, ws string) string {
			return f.bootstrap(user, ws, nil)
		}},
		{name: "Push", firstTx: "write", run: func(f *gateFixture, user, ws string) string {
			return f.push(user, ws, 1)
		}},
		{name: "View", firstTx: "read", run: func(f *gateFixture, user, ws string) string {
			return inProcess(f.srv.View(f.as(user), ws, func(tx *ViewTx) error {
				_, _, err := tx.Get(&accessv1.Issue{}, "i-0")
				return err
			}))
		}},
		{name: "Apply", firstTx: "read", run: func(f *gateFixture, user, ws string) string {
			return inProcess(f.srv.Apply(f.as(user), ws, func(tx *ApplyTx) error {
				return tx.Put(&accessv1.Issue{Id: "i-0", Repo: &accessv1.Repo{Id: "r0"}, Title: "changed"})
			}))
		}},
		{name: "Mutate", firstTx: "write", run: func(f *gateFixture, user, ws string) string {
			return inProcess(f.srv.Mutate(f.as(user), ws, func(tx *EntityTx) error {
				return tx.Put(&accessv1.Issue{Id: "i-0", Repo: &accessv1.Repo{Id: "r0"}, Title: "changed"})
			}))
		}},
		{name: "Invoke", firstTx: "write", run: func(f *gateFixture, user, ws string) string {
			return inProcess(f.srv.Invoke(f.as(user), ws, "touch", nil))
		}},
	}
}

// isGrantRead reports whether a call that gateRecorder recorded can belong to
// a resolution of grants: a probe of an index, or a read of rows of an entity
// other than the issue the requests read and write.
func isGrantRead(call string) bool {
	kind, target, _ := strings.Cut(call, ":")
	entity, _, _ := strings.Cut(target, "/")
	return (kind == "probe" || kind == "rows") && entity != "issue"
}

// hookCountingPolicy is a policy of the application that admits every caller
// and counts the calls of the hooks that run after CanAccessWorkspace.
type hookCountingPolicy struct {
	AllowAll
	hooks atomic.Int32
}

func (p *hookCountingPolicy) CanMutate(context.Context, Principal, string, Write) error {
	p.hooks.Add(1)
	return nil
}

func (p *hookCountingPolicy) ReadFilter(context.Context, Principal, string, string) (RowFilter, error) {
	p.hooks.Add(1)
	return nil, nil
}

func (p *hookCountingPolicy) FilterEpoch(context.Context, Principal, string) (uint64, error) {
	p.hooks.Add(1)
	return 0, nil
}

// gateFixture is a server over the team schema of the access test proto, on a
// recording store. The caller "me" writes in repo r0, which holds issue i-0;
// "stranger" holds nothing.
type gateFixture struct {
	ctx  context.Context
	rec  *gateRecorder
	srv  *Server
	sync syncplanev1connect.SyncServiceClient
}

// newGateFixture builds a gateFixture with opts. With replica set, the store
// offers a read replica, served by the same database.
func newGateFixture(replica bool, opts ...Option) *gateFixture {
	GinkgoHelper()
	ctx := context.Background()
	inner, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "gate.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(inner.Close()).To(Succeed()) })
	rec := &gateRecorder{Adapter: inner}
	var store storage.Adapter = rec
	if replica {
		store = replicaRecorder{rec}
	}
	srv, err := New(store, append([]Option{
		WithEntities(
			&accessv1.Squad{}, &accessv1.SquadMember{}, &accessv1.Repo{},
			&accessv1.RepoMember{}, &accessv1.RepoSquad{}, &accessv1.Issue{},
		),
		WithAuthenticator(AuthenticatorFunc(func(_ context.Context, h http.Header) (Principal, error) {
			return Principal{Subject: h.Get(resolveOnceUser)}, nil
		})),
		WithCursorStateStore(recordingCursorState{Store: cursorstate.NewMemory(1 << 20), rec: rec}),
		WithMutator("touch", nil, func(context.Context, *EntityTx, proto.Message) error { return nil },
			MutatorAccess{Permission: "view_issues"}),
	}, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	f := &gateFixture{ctx: ctx, rec: rec, srv: srv}
	Expect(srv.Mutate(Trusted(ctx), "ws", func(tx *EntityTx) error {
		for _, m := range []proto.Message{
			&accessv1.Repo{Id: "r0"},
			&accessv1.Issue{Id: "i-0", Repo: &accessv1.Repo{Id: "r0"}, Title: "issue"},
			&accessv1.RepoMember{
				Id: "rm-me-0", Repo: &accessv1.Repo{Id: "r0"},
				Member: &accessv1.RepoMember_UserId{UserId: "me"}, Role: "repo_writer",
			},
		} {
			if err := tx.Put(m); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
	Expect(srv.BuildIndexes(ctx, "ws")).To(Succeed())
	rec.take()

	ts := httptest.NewServer(srv.HTTPHandler())
	DeferCleanup(ts.Close)
	f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	return f
}

// as returns the fixture's context with user as its principal; an empty user
// is anonymous.
func (f *gateFixture) as(user string) context.Context {
	return WithPrincipal(f.ctx, Principal{Subject: user})
}

func (f *gateFixture) pull(user, ws string, cursor []byte) string {
	req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-" + user, Cursor: cursor})
	req.Header().Set(resolveOnceUser, user)
	_, err := f.sync.Pull(f.ctx, req)
	return onWire(err)
}

func (f *gateFixture) query(user, ws, entity string) string {
	req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: ws, Entity: entity})
	req.Header().Set(resolveOnceUser, user)
	_, err := f.sync.Query(f.ctx, req)
	return onWire(err)
}

func (f *gateFixture) bootstrap(user, ws string, resume *syncplanev1.BootstrapResume) string {
	req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user, Resume: resume})
	req.Header().Set(resolveOnceUser, user)
	stream, err := f.sync.Bootstrap(f.ctx, req)
	if err != nil {
		return onWire(err)
	}
	for stream.Receive() {
	}
	return onWire(stream.Err())
}

// push pushes a change to the title of issue i-0 as user, with the given
// mutation id, and renders the batch's outcome.
func (f *gateFixture) push(user, ws string, mutationID uint64) string {
	value := mustMarshalProto(&accessv1.Issue{Id: "i-0", Repo: &accessv1.Repo{Id: "r0"}, Title: "changed"})
	req := connect.NewRequest(&syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{
		WorkspaceId: ws,
		Mutations: []*syncplanev1.Mutation{{
			ClientId: "c-" + user, MutationId: mutationID,
			Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
				Entity: "issue", Key: "i-0", Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
			}},
		}},
	}}})
	req.Header().Set(resolveOnceUser, user)
	resp, err := f.sync.Push(f.ctx, req)
	if err != nil {
		return onWire(err)
	}
	batch := resp.Msg.GetResults()[0]
	if batch.GetErrorCode() != "" {
		return fmt.Sprintf("batch %s: %s", batch.GetErrorCode(), batch.GetErrorMessage())
	}
	return fmt.Sprintf("batch ok: %d results", len(batch.GetResults()))
}

// expectUntouched fails the spec when a request of user left the client
// watermark of its client, or a change to issue i-0, behind.
func (f *gateFixture) expectUntouched(user string) {
	GinkgoHelper()
	Expect(f.rec.Adapter.WithReadTx(f.ctx, "ws", func(tx storage.ReadTx) error {
		last, err := tx.ClientLastMutationID(watermarkKey(Principal{Subject: user}, "c-"+user))
		Expect(err).NotTo(HaveOccurred())
		Expect(last).To(BeZero(), "the request left a client watermark")
		row, err := tx.GetRow("issue", "i-0")
		Expect(err).NotTo(HaveOccurred())
		var issue accessv1.Issue
		Expect(proto.Unmarshal(row.Value, &issue)).To(Succeed())
		Expect(issue.GetTitle()).To(Equal("issue"), "the request changed the issue")
		return nil
	})).To(Succeed())
}

// onWire renders the outcome of an RPC: "ok", or the error's code, its error
// code metadata and its message.
func onWire(err error) string {
	if err == nil {
		return "ok"
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return fmt.Sprintf("%s [%s] %s", ce.Code(), ce.Meta().Get(ErrorCodeMetadataKey), ce.Message())
	}
	return err.Error()
}

// inProcess renders the outcome of an in-process call: "ok", or the error's
// message and whether it is a settled denial.
func inProcess(err error) string {
	if err == nil {
		return "ok"
	}
	_, denied := AsWorkspaceDenial(err)
	return fmt.Sprintf("%s (denial: %t)", err, denied)
}

func mustMarshalProto(m proto.Message) []byte {
	GinkgoHelper()
	b, err := proto.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return b
}

// errInjected is the failure gateRecorder injects.
var errInjected = errors.New("injected storage failure")

// gateRecorder records the transactions a server opens and, in order, the
// calls made in each that read rows, client state or the change feed, or
// write. A call is labelled with its name, with "probe:" and its entity and
// index for an index probe, or with "rows:" and its entity for a read of rows. It also records the calls of a
// recordingCursorState, and fails each call whose label failOn named.
type gateRecorder struct {
	storage.Adapter
	mu          sync.Mutex
	txs         []*recordedTx
	cursorCalls []string
	fail        string
}

type recordedTx struct {
	// kind is "read", "write" or "replica".
	kind  string
	calls []string
}

// Unwrap lets the server find the capabilities of the wrapped adapter.
func (r *gateRecorder) Unwrap() storage.Adapter { return r.Adapter }

// take returns the transactions recorded since the last call, and starts the
// record again.
func (r *gateRecorder) take() []recordedTx {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedTx, len(r.txs))
	for i, tx := range r.txs {
		out[i] = recordedTx{kind: tx.kind, calls: append([]string(nil), tx.calls...)}
	}
	r.txs = nil
	return out
}

func (r *gateRecorder) cursorStateCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.cursorCalls...)
}

func (r *gateRecorder) failOn(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = label
}

func (r *gateRecorder) begin(kind string) *recordedTx {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx := &recordedTx{kind: kind}
	r.txs = append(r.txs, tx)
	return tx
}

func (r *gateRecorder) note(tx *recordedTx, label string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx.calls = append(tx.calls, label)
	if label == r.fail {
		return errInjected
	}
	return nil
}

func (r *gateRecorder) readTx(ctx context.Context, ws, kind string, fn func(storage.ReadTx) error) error {
	tx := r.begin(kind)
	return r.Adapter.WithReadTx(ctx, ws, func(inner storage.ReadTx) error {
		return fn(recordingReadTx{ReadTx: inner, rec: r, tx: tx})
	})
}

func (r *gateRecorder) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	return r.readTx(ctx, ws, "read", fn)
}

func (r *gateRecorder) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	tx := r.begin("write")
	return r.Adapter.WithWriteTx(ctx, ws, func(inner storage.WriteTx) error {
		return fn(recordingWriteTx{WriteTx: inner, r: recordingReadTx{ReadTx: inner, rec: r, tx: tx}})
	})
}

// replicaRecorder is a gateRecorder whose adapter offers a read replica,
// served by the same database and recorded as "replica" transactions.
type replicaRecorder struct{ *gateRecorder }

func (r replicaRecorder) ReplicaReader() storage.ReplicaReader { return r }

func (r replicaRecorder) WithReadTxMinSeq(ctx context.Context, ws string, _ uint64, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return storage.ReadRouteReplica, r.readTx(ctx, ws, "replica", fn)
}

func (r replicaRecorder) WithReadTxAnchored(ctx context.Context, ws string, fn func(storage.ReadTx) error) (storage.ReadRoute, error) {
	return storage.ReadRouteReplica, r.readTx(ctx, ws, "replica", fn)
}

// recordingCursorState records the calls made of a cursor-state store.
type recordingCursorState struct {
	cursorstate.Store
	rec *gateRecorder
}

func (c recordingCursorState) Get(ctx context.Context, ws string, ref cursorstate.Ref) ([]byte, error) {
	c.rec.mu.Lock()
	c.rec.cursorCalls = append(c.rec.cursorCalls, "Get")
	c.rec.mu.Unlock()
	return c.Store.Get(ctx, ws, ref)
}

func (c recordingCursorState) Put(ctx context.Context, ws string, ref cursorstate.Ref, state []byte) error {
	c.rec.mu.Lock()
	c.rec.cursorCalls = append(c.rec.cursorCalls, "Put")
	c.rec.mu.Unlock()
	return c.Store.Put(ctx, ws, ref, state)
}

type recordingReadTx struct {
	storage.ReadTx
	rec *gateRecorder
	tx  *recordedTx
}

func (t recordingReadTx) note(label string) error { return t.rec.note(t.tx, label) }

func (t recordingReadTx) TombstoneHorizon() (uint64, error) {
	if err := t.note("TombstoneHorizon"); err != nil {
		return 0, err
	}
	return t.ReadTx.TombstoneHorizon()
}

func (t recordingReadTx) GetRow(entity, key string) (storage.Row, error) {
	if err := t.note("rows:" + entity); err != nil {
		return storage.Row{}, err
	}
	return t.ReadTx.GetRow(entity, key)
}

func (t recordingReadTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	if err := t.note("rows:" + entity); err != nil {
		return err
	}
	return t.ReadTx.GetRows(entity, keys, fn)
}

func (t recordingReadTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	if err := t.note("rows:" + entity); err != nil {
		return err
	}
	return t.ReadTx.ScanEntity(entity, fn)
}

func (t recordingReadTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	if err := t.note("rows:" + entity); err != nil {
		return err
	}
	return t.ReadTx.ScanEntityAfter(entity, afterKey, limit, fn)
}

func (t recordingReadTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	if err := t.note("ScanChanges"); err != nil {
		return err
	}
	return t.ReadTx.ScanChanges(afterSeq, afterEntity, afterKey, limit, fn)
}

func (t recordingReadTx) ClientLastMutationID(clientID string) (uint64, error) {
	if err := t.note("ClientLastMutationID"); err != nil {
		return 0, err
	}
	return t.ReadTx.ClientLastMutationID(clientID)
}

func (t recordingReadTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	if err := t.note("ClientRejections"); err != nil {
		return nil, err
	}
	return t.ReadTx.ClientRejections(clientID)
}

func (t recordingReadTx) IdempotencyRecord(key string) (storage.IdempotencyRecord, bool, error) {
	if err := t.note("IdempotencyRecord"); err != nil {
		return storage.IdempotencyRecord{}, false, err
	}
	return t.ReadTx.IdempotencyRecord(key)
}

func (t recordingReadTx) ScanIndex(entity, index string, lo, hi []byte, limit int, order storage.ScanOrder, fn func([]byte, string) error) error {
	if err := t.note("probe:" + entity + "/" + index); err != nil {
		return err
	}
	return t.ReadTx.ScanIndex(entity, index, lo, hi, limit, order, fn)
}

func (t recordingReadTx) ScanIndexRanges(entity, index string, ranges []storage.IndexRange, fn func(int, []byte, string) error) error {
	if err := t.note("probe:" + entity + "/" + index); err != nil {
		return err
	}
	return t.ReadTx.ScanIndexRanges(entity, index, ranges, fn)
}

// recordingWriteTx records the calls of a write transaction: the reads as
// recordingReadTx does, and the writes by name.
type recordingWriteTx struct {
	storage.WriteTx
	r recordingReadTx
}

func (t recordingWriteTx) TombstoneHorizon() (uint64, error) { return t.r.TombstoneHorizon() }
func (t recordingWriteTx) GetRow(entity, key string) (storage.Row, error) {
	return t.r.GetRow(entity, key)
}

func (t recordingWriteTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	return t.r.GetRows(entity, keys, fn)
}

func (t recordingWriteTx) ScanEntity(entity string, fn func(storage.Row) error) error {
	return t.r.ScanEntity(entity, fn)
}

func (t recordingWriteTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return t.r.ScanEntityAfter(entity, afterKey, limit, fn)
}

func (t recordingWriteTx) ScanChanges(afterSeq uint64, afterEntity, afterKey string, limit int, fn func(storage.Row) error) error {
	return t.r.ScanChanges(afterSeq, afterEntity, afterKey, limit, fn)
}

func (t recordingWriteTx) ClientLastMutationID(clientID string) (uint64, error) {
	return t.r.ClientLastMutationID(clientID)
}

func (t recordingWriteTx) ClientRejections(clientID string) ([]storage.ClientRejection, error) {
	return t.r.ClientRejections(clientID)
}

func (t recordingWriteTx) IdempotencyRecord(key string) (storage.IdempotencyRecord, bool, error) {
	return t.r.IdempotencyRecord(key)
}

func (t recordingWriteTx) ScanIndex(entity, index string, lo, hi []byte, limit int, order storage.ScanOrder, fn func([]byte, string) error) error {
	return t.r.ScanIndex(entity, index, lo, hi, limit, order, fn)
}

func (t recordingWriteTx) ScanIndexRanges(entity, index string, ranges []storage.IndexRange, fn func(int, []byte, string) error) error {
	return t.r.ScanIndexRanges(entity, index, ranges, fn)
}

func (t recordingWriteTx) NextSeq() (uint64, error) {
	if err := t.r.note("NextSeq"); err != nil {
		return 0, err
	}
	return t.WriteTx.NextSeq()
}

func (t recordingWriteTx) PutRow(entity, key string, value []byte, version uint64, actor storage.Actor) error {
	if err := t.r.note("PutRow:" + entity); err != nil {
		return err
	}
	return t.WriteTx.PutRow(entity, key, value, version, actor)
}

func (t recordingWriteTx) SetClientLastMutationID(clientID string, id uint64) error {
	if err := t.r.note("SetClientLastMutationID"); err != nil {
		return err
	}
	return t.WriteTx.SetClientLastMutationID(clientID, id)
}

func (t recordingWriteTx) SetClientRejections(clientID string, recs []storage.ClientRejection) error {
	if err := t.r.note("SetClientRejections"); err != nil {
		return err
	}
	return t.WriteTx.SetClientRejections(clientID, recs)
}
