package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// The workspace gate reads the caller's grants before the transaction that
// serves the request opens. That transaction uses the gate's grants only when
// it reads the workspace at the same position, so a request reads the grants
// once, and a write that lands between the two is never served under the
// grants from before it.
var _ = Describe("Access resolution per request", func() {
	var f *resolveOnceFixture

	BeforeEach(func() {
		f = newResolveOnceFixture()
	})

	Describe("reads the caller's grants once", func() {
		It("for a pull", func() {
			rows := f.pull(nil)
			Expect(f.resolutions()).To(Equal(1))
			Expect(rows).To(ContainElements("issue/i-0", "issue/i-1"))
			Expect(rows).NotTo(ContainElement("issue/i-2"))
		})

		It("for a pull from a current cursor", func() {
			cursor := f.cursor()
			f.pull(cursor)
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for a bootstrap", func() {
			Expect(f.bootstrap()).To(ContainElements("issue/i-0", "issue/i-1"))
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for a query", func() {
			Expect(f.query()).To(ConsistOf("issue/i-0", "issue/i-1"))
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for a push", func() {
			Expect(f.pushIssue("i-1")).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_APPLIED))
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for a Subscribe stream up to its catch-up", func() {
			cursor := f.cursor()
			req := connect.NewRequest(&syncplanev1.SubscribeRequest{
				ClientId: "c-me",
				Scopes:   []*syncplanev1.WorkspaceScope{{WorkspaceId: "ws", Cursor: cursor}},
			})
			req.Header().Set(resolveOnceUser, "me")
			ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			stream, err := f.sync.Subscribe(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			caughtUp := false
			for !caughtUp && stream.Receive() {
				Expect(stream.Msg().GetDropped()).To(BeNil(), "the stream dropped the scope")
				caughtUp = stream.Msg().GetCaughtUp()
			}
			Expect(caughtUp).To(BeTrue(), "the stream ended before it caught up: %v", stream.Err())
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for View", func() {
			Expect(f.srv.View(f.me, "ws", func(tx *ViewTx) error {
				found, _, err := tx.Get(&accessv1.Issue{}, "i-1")
				Expect(found).To(BeTrue())
				return err
			})).To(Succeed())
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for Mutate", func() {
			Expect(f.srv.Mutate(f.me, "ws", func(tx *EntityTx) error {
				return tx.Put(&accessv1.Issue{Id: "i-1", Repo: &accessv1.Repo{Id: "r1"}, Title: "changed"})
			})).To(Succeed())
			Expect(f.resolutions()).To(Equal(1))
		})

		It("for Apply", func() {
			Expect(f.srv.Apply(f.me, "ws", func(tx *ApplyTx) error {
				return tx.Put(&accessv1.Issue{Id: "i-1", Repo: &accessv1.Repo{Id: "r1"}, Title: "changed"})
			})).To(Succeed())
			Expect(f.resolutions()).To(Equal(1))
		})

		It("again when a write lands between the gate and the serving transaction", func() {
			f.afterGate(func() { f.seed(&accessv1.Issue{Id: "i-9", Repo: &accessv1.Repo{Id: "r0"}, Title: "new"}) })
			Expect(f.pull(nil)).To(ContainElement("issue/i-9"))
			Expect(f.resolutions()).To(Equal(2))
		})
	})

	// Each change runs after the gate has read the grants and before the
	// transaction that serves the request opens.
	revokeGrant := func(f *resolveOnceFixture) {
		f.trusted(func(tx *EntityTx) error { return tx.Delete(&accessv1.RepoMember{}, "rm-me-1") })
	}
	deleteAnchor := func(f *resolveOnceFixture) {
		f.trusted(func(tx *EntityTx) error { return tx.Delete(&accessv1.Repo{}, "r1") })
	}

	DescribeTable("serves no row below an anchor the caller lost after the gate",
		func(change func(*resolveOnceFixture), read func(*resolveOnceFixture) []string) {
			f.afterGate(func() { change(f) })
			rows := read(f)
			Expect(rows).To(ContainElement("issue/i-0"))
			Expect(rows).NotTo(ContainElement("issue/i-1"))
			Expect(rows).NotTo(ContainElement("repo/r1"))
		},
		Entry("a pull after a revoked grant", revokeGrant, func(f *resolveOnceFixture) []string { return f.pull(nil) }),
		Entry("a pull after a deleted anchor", deleteAnchor, func(f *resolveOnceFixture) []string { return f.pull(nil) }),
		Entry("a bootstrap after a revoked grant", revokeGrant, (*resolveOnceFixture).bootstrap),
		Entry("a bootstrap after a deleted anchor", deleteAnchor, (*resolveOnceFixture).bootstrap),
		Entry("a query after a revoked grant", revokeGrant, (*resolveOnceFixture).query),
		Entry("a query after a deleted anchor", deleteAnchor, (*resolveOnceFixture).query),
	)

	It("tombstones the rows below a grant revoked after the gate on a pull from a cursor", func() {
		cursor := f.cursor()
		f.afterGate(func() { revokeGrant(f) })
		var changes []*syncplanev1.RowChange
		for more := true; more; {
			resp := f.pullResp(cursor)
			changes = append(changes, resp.GetPatch().GetChanges()...)
			cursor, more = resp.GetNextCursor(), resp.GetHasMore()
		}
		var tombstoned []string
		for _, rc := range changes {
			Expect(rc.GetTombstone()).To(BeTrue(), "%s/%s came back live", rc.GetEntity(), rc.GetKey())
			tombstoned = append(tombstoned, rc.GetEntity()+"/"+rc.GetKey())
		}
		Expect(tombstoned).To(ContainElement("issue/i-1"))
	})

	DescribeTable("refuses a write below an anchor the caller lost after the gate",
		func(change func(*resolveOnceFixture)) {
			f.afterGate(func() { change(f) })
			Expect(f.pushIssue("i-1")).To(Equal(syncplanev1.MutationStatus_MUTATION_STATUS_REJECTED))
		},
		Entry("after a revoked grant", revokeGrant),
		Entry("after a deleted anchor", deleteAnchor),
	)
})

// resolveOnceUser names the caller of a request to the fixture's
// authenticator.
const resolveOnceUser = "X-Test-User"

// resolveOnceFixture is a server over the team schema of the access test
// proto. The caller "me" writes in repos r0 and r1, and "other" reads r2.
// Each repo holds one issue.
type resolveOnceFixture struct {
	ctx   context.Context
	store *grantProbeStore
	srv   *Server
	sync  syncplanev1connect.SyncServiceClient
	// me is ctx with the caller "me" as its principal, for the in-process
	// APIs.
	me context.Context
	// perResolution is the number of grant probes one resolution of the
	// grants of "me" makes.
	perResolution int
}

func newResolveOnceFixture() *resolveOnceFixture {
	GinkgoHelper()
	ctx := context.Background()
	inner, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "access.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(inner.Close()).To(Succeed()) })
	store := &grantProbeStore{Adapter: inner, indexes: map[[2]string]bool{}}
	srv, err := New(store,
		WithEntities(
			&accessv1.Squad{}, &accessv1.SquadMember{}, &accessv1.Repo{},
			&accessv1.RepoMember{}, &accessv1.RepoSquad{}, &accessv1.Issue{},
		),
		WithAuthenticator(AuthenticatorFunc(func(_ context.Context, h http.Header) (Principal, error) {
			return Principal{Subject: h.Get(resolveOnceUser)}, nil
		})),
	)
	Expect(err).NotTo(HaveOccurred())
	f := &resolveOnceFixture{ctx: ctx, store: store, srv: srv, me: WithPrincipal(ctx, Principal{Subject: "me"})}
	f.trusted(func(tx *EntityTx) error {
		for r, repo := range []string{"r0", "r1", "r2"} {
			if err := tx.Put(&accessv1.Repo{Id: repo}); err != nil {
				return err
			}
			if err := tx.Put(&accessv1.Issue{Id: "i-" + repo[1:], Repo: &accessv1.Repo{Id: repo}, Title: "issue"}); err != nil {
				return err
			}
			user, role := "me", "repo_writer"
			if r == 2 {
				user, role = "other", "repo_reader"
			}
			if err := tx.Put(&accessv1.RepoMember{
				Id: "rm-" + user + "-" + repo[1:], Repo: &accessv1.Repo{Id: repo},
				Member: &accessv1.RepoMember_UserId{UserId: user}, Role: role,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	Expect(srv.BuildIndexes(ctx, "ws")).To(Succeed())

	model := srv.catalogPtr().access
	for _, g := range model.grantEntities {
		for _, c := range g.grant.subjects {
			store.indexes[[2]string{g.name, c.indexName()}] = true
		}
	}
	caller, err := srv.resolveCaller(ctx, Principal{Subject: "me"}, model)
	Expect(err).NotTo(HaveOccurred())
	Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
		_, err := resolveAccess(tx, caller, srv.limits)
		return err
	})).To(Succeed())
	f.perResolution = store.take()
	// A resolution that scans its grant entities probes nothing, and every
	// count below would read as zero resolutions.
	Expect(f.perResolution).To(BeNumerically(">", 0), "the subject indexes of the grant entities are not ready")

	ts := httptest.NewServer(srv.HTTPHandler())
	DeferCleanup(ts.Close)
	f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	return f
}

// resolutions returns how many times the caller's grants were resolved since
// the last call, and starts the count again.
func (f *resolveOnceFixture) resolutions() int {
	GinkgoHelper()
	probes := f.store.take()
	Expect(probes%f.perResolution).To(BeZero(), "%d grant probes are not a whole number of resolutions of %d", probes, f.perResolution)
	return probes / f.perResolution
}

// afterGate runs fn once, after the next read transaction that reads grants
// has ended: for a request, after the workspace gate and before the
// transaction that serves the request opens.
func (f *resolveOnceFixture) afterGate(fn func()) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.after = fn
}

// trusted writes as a trusted caller.
func (f *resolveOnceFixture) trusted(fn func(tx *EntityTx) error) {
	GinkgoHelper()
	Expect(f.srv.Mutate(Trusted(f.ctx), "ws", fn)).To(Succeed())
}

// seed puts rows as a trusted caller.
func (f *resolveOnceFixture) seed(msgs ...proto.Message) {
	GinkgoHelper()
	f.trusted(func(tx *EntityTx) error {
		for _, m := range msgs {
			if err := tx.Put(m); err != nil {
				return err
			}
		}
		return nil
	})
}

func (f *resolveOnceFixture) pullResp(cursor []byte) *syncplanev1.PullResponse {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: "ws", ClientId: "c-me", Cursor: cursor})
	req.Header().Set(resolveOnceUser, "me")
	resp, err := f.sync.Pull(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	return resp.Msg
}

// pull returns the live rows one page of a pull from cursor shows "me", as
// "entity/key", sorted.
func (f *resolveOnceFixture) pull(cursor []byte) []string {
	GinkgoHelper()
	resp := f.pullResp(cursor)
	Expect(resp.GetHasMore()).To(BeFalse())
	return liveRowsOf(resp.GetPatch().GetChanges())
}

// cursor returns a current cursor of "me", and starts the count of
// resolutions again.
func (f *resolveOnceFixture) cursor() []byte {
	GinkgoHelper()
	resp := f.pullResp(nil)
	Expect(resp.GetHasMore()).To(BeFalse())
	f.store.take()
	return resp.GetNextCursor()
}

// bootstrap returns the rows a bootstrap shows "me".
func (f *resolveOnceFixture) bootstrap() []string {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: "ws", ClientId: "c-me"})
	req.Header().Set(resolveOnceUser, "me")
	stream, err := f.sync.Bootstrap(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	var changes []*syncplanev1.RowChange
	for stream.Receive() {
		changes = append(changes, stream.Msg().GetChunk().GetRows()...)
	}
	Expect(stream.Err()).NotTo(HaveOccurred())
	return liveRowsOf(changes)
}

// query returns the issues a query shows "me".
func (f *resolveOnceFixture) query() []string {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.QueryRequest{WorkspaceId: "ws", Entity: "issue"})
	req.Header().Set(resolveOnceUser, "me")
	resp, err := f.sync.Query(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	return liveRowsOf(resp.Msg.GetRows())
}

// pushIssue pushes a change to the title of an issue as "me", and returns its
// status.
func (f *resolveOnceFixture) pushIssue(key string) syncplanev1.MutationStatus {
	GinkgoHelper()
	value, err := proto.Marshal(&accessv1.Issue{Id: key, Repo: &accessv1.Repo{Id: "r" + key[2:]}, Title: "changed"})
	Expect(err).NotTo(HaveOccurred())
	req := connect.NewRequest(&syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{
		WorkspaceId: "ws",
		Mutations: []*syncplanev1.Mutation{{
			ClientId: "c-me", MutationId: 1,
			Payload: &syncplanev1.Mutation_Crud{Crud: &syncplanev1.CrudMutation{
				Entity: "issue", Key: key, Op: syncplanev1.CrudOp_CRUD_OP_PUT, Value: value,
			}},
		}},
	}}})
	req.Header().Set(resolveOnceUser, "me")
	resp, err := f.sync.Push(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.Msg.GetResults()).To(HaveLen(1))
	batch := resp.Msg.GetResults()[0]
	Expect(batch.GetErrorCode()).To(BeEmpty(), "batch failed: %s", batch.GetErrorMessage())
	Expect(batch.GetResults()).To(HaveLen(1))
	return batch.GetResults()[0].GetStatus()
}

// liveRowsOf returns the rows of changes that are not tombstones, as
// "entity/key", sorted.
func liveRowsOf(changes []*syncplanev1.RowChange) []string {
	var rows []string
	for _, rc := range changes {
		if !rc.GetTombstone() {
			rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
		}
	}
	sort.Strings(rows)
	return rows
}

// grantProbeStore counts the probes its transactions make of the subject
// indexes of grant entities, which is how a resolution of a caller's grants
// finds its grant rows. It can run a function once, after the first read
// transaction that makes such a probe has ended.
type grantProbeStore struct {
	storage.Adapter
	// indexes holds the (entity, index) pairs that count.
	indexes map[[2]string]bool

	mu     sync.Mutex
	probes int
	after  func()
}

// Unwrap lets the server find the capabilities of the wrapped adapter, such as
// its cursor-state store.
func (s *grantProbeStore) Unwrap() storage.Adapter { return s.Adapter }

// take returns the probes counted since the last call, and starts the count
// again.
func (s *grantProbeStore) take() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.probes
	s.probes = 0
	return n
}

func (s *grantProbeStore) probe(entity, index string) bool {
	if !s.indexes[[2]string{entity, index}] {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes++
	return true
}

func (s *grantProbeStore) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	tx := &grantProbeTx{store: s}
	err := s.Adapter.WithReadTx(ctx, ws, func(inner storage.ReadTx) error {
		tx.ReadTx = inner
		return fn(tx)
	})
	if tx.probed {
		s.mu.Lock()
		after := s.after
		s.after = nil
		s.mu.Unlock()
		if after != nil {
			after()
		}
	}
	return err
}

func (s *grantProbeStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	return s.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		return fn(&grantProbeWriteTx{WriteTx: tx, store: s})
	})
}

type grantProbeTx struct {
	storage.ReadTx
	store  *grantProbeStore
	probed bool
}

func (t *grantProbeTx) ScanIndex(entity, index string, lo, hi []byte, limit int, order storage.ScanOrder, fn func([]byte, string) error) error {
	if t.store.probe(entity, index) {
		t.probed = true
	}
	return t.ReadTx.ScanIndex(entity, index, lo, hi, limit, order, fn)
}

func (t *grantProbeTx) ScanIndexRanges(entity, index string, ranges []storage.IndexRange, fn func(int, []byte, string) error) error {
	if t.store.probe(entity, index) {
		t.probed = true
	}
	return t.ReadTx.ScanIndexRanges(entity, index, ranges, fn)
}

type grantProbeWriteTx struct {
	storage.WriteTx
	store *grantProbeStore
}

func (t *grantProbeWriteTx) ScanIndex(entity, index string, lo, hi []byte, limit int, order storage.ScanOrder, fn func([]byte, string) error) error {
	t.store.probe(entity, index)
	return t.WriteTx.ScanIndex(entity, index, lo, hi, limit, order, fn)
}

func (t *grantProbeWriteTx) ScanIndexRanges(entity, index string, ranges []storage.IndexRange, fn func(int, []byte, string) error) error {
	t.store.probe(entity, index)
	return t.WriteTx.ScanIndexRanges(entity, index, ranges, fn)
}
