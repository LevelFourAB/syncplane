package server_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	gosync "sync"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// convReplica is one client's copy of a workspace, kept by pulling as one
// caller. Unlike accessReplica it survives a reset the way a client does: it
// drops what it holds and pulls again from an empty cursor.
type convReplica struct {
	f      *accessFixture
	user   string
	cursor []byte
	rows   map[string]bool
	access *syncplanev1.CallerAccess
	resets int
	// log records what each pull did, for the failure message, after the
	// number of world changes mark returns.
	log  []string
	mark func() int
}

func newConvReplica(f *accessFixture, user string) *convReplica {
	return &convReplica{f: f, user: user, rows: map[string]bool{}}
}

// pullOnce pulls one page with limit and applies it. It reports whether the
// server has more to send.
func (r *convReplica) pullOnce(limit uint32) bool {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.PullRequest{
		WorkspaceId: ws, ClientId: "c-" + r.user, Cursor: r.cursor, Limit: limit,
		CallerAccessVersion: proto.Uint64(r.access.GetVersion()),
	})
	as(r.user, req.Header())
	resp, err := r.f.sync.Pull(r.f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	msg := resp.Msg
	if msg.GetReset_() {
		r.resets++
		r.log = append(r.log, "reset")
		r.rows, r.cursor, r.access = map[string]bool{}, nil, nil
		return true
	}
	r.access = applyCallerAccess(r.access, msg.GetCallerAccess(), msg.GetCallerAccessDelta())
	var applied []string
	for _, entity := range msg.GetPatch().GetDropEntities() {
		applied = append(applied, "drop "+entity)
		for k := range r.rows {
			if len(k) > len(entity) && k[:len(entity)+1] == entity+"/" {
				delete(r.rows, k)
			}
		}
	}
	for _, rc := range msg.GetPatch().GetChanges() {
		key := rc.GetEntity() + "/" + rc.GetKey()
		if rc.GetTombstone() {
			applied = append(applied, "-"+key)
			delete(r.rows, key)
		} else {
			applied = append(applied, "+"+key)
			r.rows[key] = true
		}
	}
	at := 0
	if r.mark != nil {
		at = r.mark()
	}
	r.log = append(r.log, fmt.Sprintf("@%d pull(limit=%d, more=%v): %v", at, limit, msg.GetHasMore(), applied))
	r.cursor = msg.GetNextCursor()
	return msg.GetHasMore()
}

// sync pulls with limit until the server has nothing more.
func (r *convReplica) sync(limit uint32) {
	GinkgoHelper()
	for range 500 {
		if !r.pullOnce(limit) {
			return
		}
	}
	Fail("the pulls did not settle")
}

func (r *convReplica) view() []string {
	var out []string
	for k := range r.rows {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// freshAccess returns the CallerAccess a pull from an empty cursor sends
// user.
func (f *accessFixture) freshAccess(user string) *syncplanev1.CallerAccess {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.PullRequest{WorkspaceId: ws, ClientId: "c-fresh-" + user})
	as(user, req.Header())
	resp, err := f.sync.Pull(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	return resp.Msg.GetCallerAccess()
}

// convWorld drives random trusted writes against the deep org schema
// (projects, tasks below projects, comments below tasks, and grants at the
// root and at projects) and remembers them for the failure message.
type convWorld struct {
	f   *accessFixture
	rnd *rand.Rand
	log []string
	// writeFn makes one random write; nil makes the org schema's.
	writeFn func(w *convWorld, tx *server.EntityTx) (string, error)
}

var (
	convUsers    = []string{"u1", "u2", "u3"}
	convProjects = []string{"p1", "p2", "p3"}
)

func (w *convWorld) pick(list []string) string { return list[w.rnd.IntN(len(list))] }

// step makes one random change, as one trusted transaction of one or more
// writes.
func (w *convWorld) step() {
	GinkgoHelper()
	n := 1
	if w.rnd.IntN(4) == 0 {
		n = 2 + w.rnd.IntN(2)
	}
	var did []string
	err := w.f.srv.Mutate(server.Trusted(w.f.ctx), ws, func(tx *server.EntityTx) error {
		did = did[:0]
		for range n {
			write := (*convWorld).write
			if w.writeFn != nil {
				write = w.writeFn
			}
			d, err := write(w, tx)
			if err != nil {
				return err
			}
			did = append(did, d)
		}
		return nil
	})
	if err != nil {
		// A trusted write the engine still refuses; the world moves on
		// without it.
		w.log = append(w.log, fmt.Sprintf("%d:refused", len(w.log)+1))
		return
	}
	w.log = append(w.log, fmt.Sprintf("%d:%v", len(w.log)+1, did))
}

// write makes one random write in tx and describes it.
func (w *convWorld) write(tx *server.EntityTx) (string, error) {
	switch w.rnd.IntN(10) {
	case 0, 1:
		u, p := w.pick(convUsers), w.pick(convProjects)
		id := "pm-" + u + "-" + p
		role := []string{"project_reader", "project_reader", "project_editor"}[w.rnd.IntN(3)]
		return "grant " + id + " " + role, tx.Put(&accessv1.ProjectMember{Id: id, Project: project(p), UserId: u, Role: role})
	case 2:
		u, p := w.pick(convUsers), w.pick(convProjects)
		id := "pm-" + u + "-" + p
		return "revoke " + id, tx.Delete(&accessv1.ProjectMember{}, id)
	case 3:
		u := w.pick(convUsers[1:])
		if w.rnd.IntN(2) == 0 {
			return "root grant " + u, tx.Put(&accessv1.OrgMember{Id: "om-" + u, UserId: u, Role: "org_member"})
		}
		return "root revoke " + u, tx.Delete(&accessv1.OrgMember{}, "om-"+u)
	case 4, 5:
		t := fmt.Sprintf("t%d", w.rnd.IntN(6))
		p := w.pick(append([]string{""}, convProjects...))
		var pr *accessv1.Project
		if p != "" {
			pr = project(p)
		}
		return fmt.Sprintf("task %s in %q", t, p), tx.Put(&accessv1.Task{Id: t, Project: pr, Title: t})
	case 6:
		t := fmt.Sprintf("t%d", w.rnd.IntN(6))
		return "delete task " + t, tx.Delete(&accessv1.Task{}, t)
	case 7, 8:
		c := fmt.Sprintf("c%d", w.rnd.IntN(8))
		t := fmt.Sprintf("t%d", w.rnd.IntN(6))
		return fmt.Sprintf("comment %s on %s", c, t), tx.Put(&accessv1.Comment{Id: c, Task: task(t), Body: c})
	default:
		p := w.pick(convProjects)
		if w.rnd.IntN(2) == 0 {
			return "delete project " + p, tx.Delete(&accessv1.Project{}, p)
		}
		return "project " + p, tx.Put(&accessv1.Project{Id: p, Name: p})
	}
}

// convergeByPulls lets w change the world at random while one replica per
// caller of convUsers pulls at random moments with small pages, then syncs
// each replica to the end and checks that it holds what a fresh pull shows
// its caller, and the CallerAccess a fresh pull sends.
func convergeByPulls(w *convWorld) {
	GinkgoHelper()
	f := w.f
	replicas := make([]*convReplica, len(convUsers))
	for i, u := range convUsers {
		replicas[i] = newConvReplica(f, u)
		replicas[i].mark = func() int { return len(w.log) }
		replicas[i].sync(0)
	}
	for range 60 {
		w.step()
		r := replicas[w.rnd.IntN(len(replicas))]
		switch w.rnd.IntN(3) {
		case 0:
			r.pullOnce(uint32(1 + w.rnd.IntN(3)))
		case 1:
			r.sync(uint32(1 + w.rnd.IntN(3)))
		}
	}
	for _, r := range replicas {
		r.sync(uint32(1 + w.rnd.IntN(3)))
		Expect(r.view()).To(Equal(f.pull(r.user)),
			"replica %s diverged\nworld:\n%v\npulls:\n%v", r.user, w.log, r.log)
		Expect(sameCallerAccess(r.access, f.freshAccess(r.user))).To(BeTrue(),
			"replica %s holds another CallerAccess\nworld:\n%v\npulls:\n%v", r.user, w.log, r.log)
	}
}

// seedConvOrg writes the projects the org world starts from, and a grant at
// p0 for each caller of convUsers that no change of the world touches.
func seedConvOrg(f *accessFixture) {
	GinkgoHelper()
	f.seed(
		&accessv1.Project{Id: "p0", Name: "home"},
		&accessv1.Project{Id: "p1", Name: "one"},
		&accessv1.Project{Id: "p2", Name: "two"},
	)
	for _, u := range convUsers {
		f.seed(&accessv1.ProjectMember{Id: "pm-" + u + "-home", Project: project("p0"), UserId: u, Role: "project_reader"})
	}
	Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
}

var _ = Describe("Access model sync under random changes", func() {
	// Each seed is one run; the failure message names the seed and the
	// changes and pulls that led to it.
	for seed := range uint64(40) {
		It(fmt.Sprintf("converges every replica on what a fresh pull shows (seed %d)", seed), func() {
			// Given the deep org schema, where every caller always reads p0
			// so that the workspace gate never refuses one
			f := newDeepFixtureOn(newTestStore())
			seedConvOrg(f)
			// When the world changes at random while each caller pulls, then
			// each replica converges on what a fresh pull shows its caller
			convergeByPulls(&convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 7))})
		})
	}
})

// convStream is one client's copy of a workspace kept by an open Subscribe
// stream. A scope the server drops with RESET starts over: the replica drops
// what it holds and opens a new stream from an empty cursor.
type convStream struct {
	f    *accessFixture
	user string

	mu     gosync.Mutex
	rows   map[string]bool
	access *syncplanev1.CallerAccess
	cursor []byte
	resets int
	// failure is a drop for another reason than RESET.
	failure string
	log     []string
	cancel  context.CancelFunc
}

func newConvStream(f *accessFixture, user string) *convStream {
	s := &convStream{f: f, user: user, rows: map[string]bool{}}
	s.open()
	DeferCleanup(func() { s.cancel() })
	return s
}

// open starts a stream from the cursor and summary the replica holds.
func (s *convStream) open() {
	s.mu.Lock()
	scope := &syncplanev1.WorkspaceScope{
		WorkspaceId: ws, Cursor: s.cursor,
		CallerAccessVersion: proto.Uint64(s.access.GetVersion()),
	}
	s.mu.Unlock()
	req := connect.NewRequest(&syncplanev1.SubscribeRequest{
		ClientId: "c-stream-" + s.user, Scopes: []*syncplanev1.WorkspaceScope{scope}, Limit: 2,
	})
	as(s.user, req.Header())
	ctx, cancel := context.WithCancel(s.f.ctx)
	s.cancel = cancel
	stream, err := s.f.sync.Subscribe(ctx, req)
	Expect(err).NotTo(HaveOccurred())
	go func() {
		defer GinkgoRecover()
		for stream.Receive() {
			if s.apply(stream.Msg()) {
				return
			}
		}
	}()
}

// apply applies one frame, and reports whether the stream is done.
func (s *convStream) apply(msg *syncplanev1.SubscribeResponse) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := msg.GetDropped(); d != nil {
		if d.GetReason() != syncplanev1.WorkspaceDropped_REASON_RESET {
			s.failure = fmt.Sprintf("dropped %v: %s", d.GetReason(), d.GetMessage())
			return true
		}
		s.resets++
		s.log = append(s.log, "reset")
		s.rows, s.cursor, s.access = map[string]bool{}, nil, nil
		go s.open()
		return true
	}
	page := msg.GetPage()
	if page == nil {
		return false
	}
	whole, delta := page.GetCallerAccess(), page.GetCallerAccessDelta()
	if delta != nil && (s.access == nil || s.access.GetVersion() != delta.GetBaseVersion()) {
		s.failure = "a delta for another summary"
		return true
	}
	if whole != nil {
		s.access = whole
	} else if delta != nil {
		out := server.ApplyCallerAccessDelta(s.access, delta)
		out.Seq, out.Version = delta.GetSeq(), delta.GetVersion()
		s.access = out
	}
	var applied []string
	for _, entity := range page.GetPatch().GetDropEntities() {
		applied = append(applied, "drop "+entity)
		for k := range s.rows {
			if len(k) > len(entity) && k[:len(entity)+1] == entity+"/" {
				delete(s.rows, k)
			}
		}
	}
	for _, rc := range page.GetPatch().GetChanges() {
		key := rc.GetEntity() + "/" + rc.GetKey()
		if rc.GetTombstone() {
			applied = append(applied, "-"+key)
			delete(s.rows, key)
		} else {
			applied = append(applied, "+"+key)
			s.rows[key] = true
		}
	}
	s.log = append(s.log, fmt.Sprintf("page(more=%v): %v", page.GetHasMore(), applied))
	if c := page.GetNextCursor(); len(c) > 0 {
		s.cursor = c
	}
	return false
}

func (s *convStream) view() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.rows {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ = Describe("Access model Subscribe under random changes", func() {
	for seed := range uint64(25) {
		It(fmt.Sprintf("converges every open stream on what a fresh pull shows (seed %d)", seed), func() {
			// Given the deep org schema, where every caller always reads p0
			f := newDeepFixtureOn(newTestStore(), server.WithHeartbeat(20*time.Millisecond))
			f.seed(
				&accessv1.Project{Id: "p0", Name: "home"},
				&accessv1.Project{Id: "p1", Name: "one"},
				&accessv1.Project{Id: "p2", Name: "two"},
			)
			for _, u := range convUsers {
				f.seed(&accessv1.ProjectMember{Id: "pm-" + u + "-home", Project: project("p0"), UserId: u, Role: "project_reader"})
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			w := &convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 11))}
			streams := make([]*convStream, len(convUsers))
			for i, u := range convUsers {
				streams[i] = newConvStream(f, u)
			}

			// When the world changes at random while each stream stays open
			for range 40 {
				w.step()
				if w.rnd.IntN(3) == 0 {
					time.Sleep(time.Duration(w.rnd.IntN(5)) * time.Millisecond)
				}
			}
			// And a last write every caller reads
			f.seed(&accessv1.Task{Id: "zz-last", Project: project("p0")})

			// Then each stream, once it has the last write, holds what a
			// fresh pull shows its caller, and the summary a fresh pull sends
			for _, s := range streams {
				Eventually(func() []string {
					s.mu.Lock()
					failure := s.failure
					s.mu.Unlock()
					Expect(failure).To(BeEmpty())
					return s.view()
				}).WithTimeout(10*time.Second).Should(ContainElement("task/zz-last"),
					"stream %s never caught up\nworld:\n%v", s.user, w.log)
				// A resync settles after the page that carries the last
				// write, so wait for the view to stop changing.
				Eventually(func() []string { return s.view() }).WithTimeout(5*time.Second).
					Should(Equal(f.pull(s.user)), func() string {
						s.mu.Lock()
						defer s.mu.Unlock()
						return fmt.Sprintf("stream %s diverged\nworld:\n%v\npages:\n%v", s.user, w.log, s.log)
					})
				s.mu.Lock()
				held := proto.CloneOf(s.access)
				s.mu.Unlock()
				Expect(sameCallerAccess(held, f.freshAccess(s.user))).To(BeTrue(),
					"stream %s holds another CallerAccess", s.user)
			}
		})
	}
})

// writeWithin makes one random write in the within schema: dossiers that
// turn private or change owner, matters that move between dossiers, memos
// below matters, and grants at dossiers and matters.
func writeWithin(w *convWorld, tx *server.EntityTx) (string, error) {
	dossiers := []string{"d1", "d2", "d3"}
	matters := []string{"m1", "m2", "m3", "m4"}
	switch w.rnd.IntN(12) {
	case 0, 1:
		d := w.pick(dossiers)
		priv := w.rnd.IntN(2) == 0
		return fmt.Sprintf("dossier %s private=%v", d, priv),
			tx.Patch(&accessv1.Dossier{Id: d, Private: priv}, []string{"private"}, nil)
	case 2:
		d, u := w.pick(dossiers), w.pick(append([]string{""}, convUsers...))
		return fmt.Sprintf("dossier %s owner=%q", d, u),
			tx.Patch(&accessv1.Dossier{Id: d, OwnerId: u}, []string{"owner_id"}, nil)
	case 3:
		d := w.pick(dossiers)
		if w.rnd.IntN(3) == 0 {
			return "delete dossier " + d, tx.Delete(&accessv1.Dossier{}, d)
		}
		return "dossier " + d, tx.Put(&accessv1.Dossier{Id: d, Title: d})
	case 4, 5:
		m, d := w.pick(matters), w.pick(append([]string{""}, dossiers...))
		var dr *accessv1.Dossier
		if d != "" {
			dr = dossier(d)
		}
		return fmt.Sprintf("matter %s in %q", m, d), tx.Put(&accessv1.Matter{Id: m, Dossier: dr, Title: m})
	case 6:
		m := w.pick(matters)
		return "delete matter " + m, tx.Delete(&accessv1.Matter{}, m)
	case 7, 8:
		memo, m := fmt.Sprintf("memo%d", w.rnd.IntN(6)), w.pick(matters)
		return fmt.Sprintf("memo %s on %s", memo, m), tx.Put(&accessv1.Memo{Id: memo, Matter: matter(m), Title: memo})
	case 9:
		u, d := w.pick(convUsers), w.pick(dossiers)
		id := "dm-" + u + "-" + d
		if w.rnd.IntN(2) == 0 {
			return "revoke " + id, tx.Delete(&accessv1.DossierMember{}, id)
		}
		return "grant " + id, tx.Put(&accessv1.DossierMember{Id: id, Dossier: dossier(d), UserId: u, Role: "dossier_member"})
	default:
		u, m := w.pick(convUsers), w.pick(matters)
		id := "mw-" + u + "-" + m
		if w.rnd.IntN(2) == 0 {
			return "unwatch " + id, tx.Delete(&accessv1.MatterWatcher{}, id)
		}
		return "watch " + id, tx.Put(&accessv1.MatterWatcher{Id: id, Matter: matter(m), UserId: u, Role: "matter_watcher"})
	}
}

var _ = Describe("Access model sync of within entries under random changes", func() {
	for seed := range uint64(40) {
		It(fmt.Sprintf("converges every replica on what a fresh pull shows (seed %d)", seed), func() {
			// Given the within schema, where every caller is a clerk and so
			// reads the matters of dossiers that are not private
			f := newSchemaFixtureOn(newTestStore(), withinEntities())
			for _, u := range convUsers {
				f.seed(&accessv1.Clerk{Id: "c-" + u, UserId: u, Role: "clerk"})
			}
			f.seed(&accessv1.Dossier{Id: "d1", Title: "d1"}, &accessv1.Dossier{Id: "d2", Title: "d2", Private: true})
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			// When the world changes at random while each caller pulls, then
			// each replica converges on what a fresh pull shows its caller
			convergeByPulls(&convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 13)), writeFn: writeWithin})
		})
	}
})

// writeTopics makes one random write in the multi-parent schema: topics
// that list other topics as parents, so that they form a graph, and grants
// at topics.
func writeTopics(w *convWorld, tx *server.EntityTx) (string, error) {
	topics := []string{"t0", "t1", "t2", "t3", "t4", "t5"}
	switch w.rnd.IntN(8) {
	case 0, 1, 2, 3:
		id := w.pick(topics)
		var parents []string
		for range w.rnd.IntN(3) {
			if p := w.pick(topics); p != id {
				parents = append(parents, p)
			}
		}
		return fmt.Sprintf("topic %s below %v", id, parents), tx.Put(topic(id, parents...))
	case 4:
		id := w.pick(topics)
		return "delete topic " + id, tx.Delete(&accessv1.Topic{}, id)
	default:
		u, t := w.pick(convUsers), w.pick(topics)
		id := "lm-" + u + "-" + t
		if w.rnd.IntN(2) == 0 {
			return "revoke " + id, tx.Delete(&accessv1.LaneMember{}, id)
		}
		return "grant " + id, tx.Put(laneOnTopic(id, t, u, "lane_viewer"))
	}
}

var _ = Describe("Access model sync of a graph of parents under random changes", func() {
	for seed := range uint64(40) {
		It(fmt.Sprintf("converges every replica on what a fresh pull shows (seed %d)", seed), func() {
			// Given topics that list other topics as parents, where every
			// caller views product p0 and so is always admitted
			f := newMultiFixtureOn(newTestStore())
			f.seed(&accessv1.Product{Id: "p0"})
			for _, u := range convUsers {
				f.seed(laneOnProduct("lm-"+u+"-home", "p0", u, "lane_viewer"))
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			// When the world changes at random while each caller pulls, then
			// each replica converges on what a fresh pull shows its caller
			convergeByPulls(&convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 17)), writeFn: writeTopics})
		})
	}
})

var _ = Describe("Access model sync under random changes on Postgres", func() {
	open := func() storage.Adapter {
		GinkgoHelper()
		store, err := pgstore.Open(GinkgoT().Context(), pgtest.DSN())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return store
	}

	for seed := range uint64(8) {
		It(fmt.Sprintf("converges every replica of the org schema on what a fresh pull shows (seed %d)", seed), func() {
			// Given the deep org schema on Postgres
			f := newDeepFixtureOn(open())
			seedConvOrg(f)
			// When the world changes at random while each caller pulls, then
			// each replica converges on what a fresh pull shows its caller
			convergeByPulls(&convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 7))})
		})

		It(fmt.Sprintf("converges every replica of a graph of parents on what a fresh pull shows (seed %d)", seed), func() {
			// Given topics that list other topics as parents, on Postgres
			f := newMultiFixtureOn(open())
			f.seed(&accessv1.Product{Id: "p0"})
			for _, u := range convUsers {
				f.seed(laneOnProduct("lm-"+u+"-home", "p0", u, "lane_viewer"))
			}
			Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
			// When the world changes at random while each caller pulls, then
			// each replica converges on what a fresh pull shows its caller
			convergeByPulls(&convWorld{f: f, rnd: rand.New(rand.NewPCG(seed, 17)), writeFn: writeTopics})
		})
	}
})
