package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	gosync "sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// accessEntities registers the org schema of the access test proto.
func accessEntities() server.Option {
	return server.WithEntities(
		&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
		&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{},
	)
}

// rootRolesBy gives each caller the root roles that roles returns, and keeps
// its Principal.Subject as its only subject.
func rootRolesBy(roles func(p server.Principal) []string) server.Option {
	return server.WithCallerAccess(func(_ context.Context, p server.Principal, _ string) (server.CallerAccess, error) {
		access := server.CallerAccess{RootRoles: roles(p)}
		if p.Subject != "" {
			access.Subjects = []string{p.Subject}
		}
		return access, nil
	})
}

// subjectsBy gives each caller the subjects that subjects returns, and no
// root role.
func subjectsBy(subjects func(p server.Principal) []string) server.Option {
	return server.WithCallerAccess(func(_ context.Context, p server.Principal, _ string) (server.CallerAccess, error) {
		return server.CallerAccess{Subjects: subjects(p)}, nil
	})
}

// userHeader names the caller of a request to the test authenticator. A
// request without it is anonymous; staffHeader adds a "staff" claim.
const (
	userHeader  = "X-Test-User"
	staffHeader = "X-Test-Staff"
)

func headerAuthenticator() server.Option {
	return server.WithAuthenticator(server.AuthenticatorFunc(
		func(_ context.Context, h http.Header) (server.Principal, error) {
			p := server.Principal{Subject: h.Get(userHeader)}
			if staff := h.Get(staffHeader); staff != "" {
				p.Claims = map[string]any{"staff": staff}
			}
			return p, nil
		}))
}

// as names the caller of a request; an empty user leaves it anonymous.
func as(user string, h http.Header) {
	if user != "" {
		h.Set(userHeader, user)
	}
}

// accessFixture is a server running the access test schema behind a header
// authenticator, with clients for its services.
type accessFixture struct {
	ctx      context.Context
	store    storage.Adapter
	srv      *server.Server
	sync     syncplanev1connect.SyncServiceClient
	presence syncplanev1connect.PresenceServiceClient
}

func newAccessFixture(opts ...server.Option) *accessFixture {
	GinkgoHelper()
	return newAccessFixtureOn(newTestStore(), opts...)
}

func newAccessFixtureOn(store storage.Adapter, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	f := &accessFixture{ctx: context.Background(), store: store}
	f.srv = mustNewServer(store, append([]server.Option{accessEntities(), headerAuthenticator()}, opts...)...)
	ts := httptest.NewServer(f.srv.HTTPHandler())
	DeferCleanup(ts.Close)
	f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	f.presence = syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL)
	return f
}

// newSchemaFixtureOn is a fixture over store running the schema that entities
// registers, behind the header authenticator, for a spec whose rows are not
// rows of the org schema.
func newSchemaFixtureOn(store storage.Adapter, entities server.Option, opts ...server.Option) *accessFixture {
	GinkgoHelper()
	f := &accessFixture{ctx: GinkgoT().Context(), store: store}
	f.srv = mustNewServer(store, append([]server.Option{entities, headerAuthenticator()}, opts...)...)
	ts := httptest.NewServer(f.srv.HTTPHandler())
	DeferCleanup(ts.Close)
	f.sync = syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL)
	f.presence = syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL)
	return f
}

// seed writes rows as a trusted caller.
func (f *accessFixture) seed(msgs ...proto.Message) {
	GinkgoHelper()
	Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
		for _, m := range msgs {
			if err := tx.Put(m); err != nil {
				return err
			}
		}
		return nil
	})).To(Succeed())
}

// plant writes a row below the engine, for rows every write path refuses
// but storage can still hold, such as a grant whose role a schema has since
// dropped.
func (f *accessFixture) plant(entity, key string, msg proto.Message) {
	GinkgoHelper()
	Expect(f.store.WithWriteTx(f.ctx, ws, func(tx storage.WriteTx) error {
		seq, err := tx.NextSeq()
		if err != nil {
			return err
		}
		return tx.PutRow(entity, key, mustMarshal(msg), seq, storage.Actor{})
	})).To(Succeed())
}

func project(id string) *accessv1.Project { return &accessv1.Project{Id: id} }

// heldAt flattens ca to the rows each permission is held at, as sorted
// "entity/key" strings, with "*" alone for a permission held at the root.
func heldAt(ca *syncplanev1.CallerAccess) map[string][]string {
	out := map[string][]string{}
	for _, perm := range ca.GetRootPermissions() {
		out[perm] = []string{"*"}
	}
	for _, g := range ca.GetAnchorGroups() {
		for _, perm := range g.GetPermissions() {
			for _, a := range g.GetAnchors() {
				for _, k := range a.GetKeys() {
					out[perm] = append(out[perm], a.GetEntity()+"/"+k)
				}
			}
		}
	}
	for _, rows := range out {
		sort.Strings(rows)
	}
	return out
}

// seedOrg writes the org every access spec starts from:
//
//   - admin and member hold org_admin and org_member at the root;
//   - guest reads project p1, writer writes in p2, padmin manages p1 and
//     owner owns p1;
//   - ghost's grant is anchored at a project that does not exist, and
//     badrole's names a role no entity declares, so neither holds anything;
//   - t1 is in p1, t2 in p2 and t3 in no project.
func (f *accessFixture) seedOrg() {
	GinkgoHelper()
	f.seed(
		&accessv1.OrgMember{Id: "om-admin", UserId: "admin", Role: "org_admin"},
		&accessv1.OrgMember{Id: "om-member", UserId: "member", Role: "org_member"},
		&accessv1.Project{Id: "p1", Name: "one"},
		&accessv1.Project{Id: "p2", Name: "two"},
		&accessv1.ProjectMember{Id: "pm-guest", Project: project("p1"), UserId: "guest", Role: "project_reader"},
		&accessv1.ProjectMember{Id: "pm-writer", Project: project("p2"), UserId: "writer", Role: "project_writer"},
		&accessv1.ProjectMember{Id: "pm-padmin", Project: project("p1"), UserId: "padmin", Role: "project_admin"},
		&accessv1.ProjectMember{Id: "pm-owner", Project: project("p1"), UserId: "owner", Role: "project_owner"},
		&accessv1.ProjectMember{Id: "pm-ghost", Project: project("p9"), UserId: "ghost", Role: "project_writer"},
		&accessv1.Task{Id: "t1", Project: project("p1"), Title: "in one"},
		&accessv1.Task{Id: "t2", Project: project("p2"), Title: "in two"},
		&accessv1.Task{Id: "t3", Title: "in none"},
		&accessv1.Label{Id: "l1", Name: "urgent"},
		&accessv1.Note{Id: "n1", Body: "for everyone"},
	)
	f.plant("project_member", "pm-badrole", &accessv1.ProjectMember{
		Id: "pm-badrole", Project: project("p1"), UserId: "badrole", Role: "superuser",
	})
}

func (f *accessFixture) pullErr(user string) ([]string, error) {
	req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-" + user})
	as(user, req.Header())
	resp, err := f.sync.Pull(f.ctx, req)
	if err != nil {
		return nil, err
	}
	var rows []string
	for _, rc := range resp.Msg.GetPatch().GetChanges() {
		if !rc.GetTombstone() {
			rows = append(rows, rc.GetEntity()+"/"+rc.GetKey())
		}
	}
	sort.Strings(rows)
	return rows, nil
}

// pull returns the rows a full pull from an empty cursor shows user, as
// "entity/key", sorted.
func (f *accessFixture) pull(user string) []string {
	GinkgoHelper()
	rows, err := f.pullErr(user)
	Expect(err).NotTo(HaveOccurred())
	return rows
}

// push sends mutations as user from one client, numbering them from 1 on
// every call, and returns their results.
func (f *accessFixture) push(user, client string, muts ...*syncplanev1.Mutation) []*syncplanev1.MutationResult {
	GinkgoHelper()
	req := connect.NewRequest(pushOf(ws, muts))
	as(user, req.Header())
	resp, err := f.sync.Push(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	return pushResults(resp.Msg)
}

// pageScanCountingStore counts, per entity, the rows its read transactions
// visit with ScanEntityAfter: the walk a read takes when it cannot name its
// rows.
type pageScanCountingStore struct {
	storage.Adapter
	mu      gosync.Mutex
	scanned map[string]int
}

func (c *pageScanCountingStore) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	return c.Adapter.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		return fn(&pageScanCountingTx{ReadTx: tx, store: c})
	})
}

// Unwrap lets the server find the wrapped adapter's capabilities, such as
// its cursor-state store.
func (c *pageScanCountingStore) Unwrap() storage.Adapter { return c.Adapter }

func (c *pageScanCountingStore) count(entity string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scanned[entity]
}

func (c *pageScanCountingStore) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.scanned)
}

type pageScanCountingTx struct {
	storage.ReadTx
	store *pageScanCountingStore
}

func (t *pageScanCountingTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	return t.ReadTx.ScanEntityAfter(entity, afterKey, limit, func(row storage.Row) error {
		t.store.mu.Lock()
		t.store.scanned[entity]++
		t.store.mu.Unlock()
		return fn(row)
	})
}
