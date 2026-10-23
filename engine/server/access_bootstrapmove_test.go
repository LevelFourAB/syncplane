package server_test

import (
	"context"
	"log/slog"
	gosync "sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// hookedReadStore runs a hook before a read transaction opens, chosen by
// what the transactions before it read: afterBegin before the first one
// after a transaction that read the store epoch (a Bootstrap's Begin), and
// afterEntity before the first one after a transaction that read rows of
// entity. Each hook runs once, outside any transaction of the store. It also
// records each row of the access move log that a read transaction scans.
type hookedReadStore struct {
	storage.Adapter
	mu          gosync.Mutex
	entity      string
	afterBegin  func()
	afterEntity func()
	sawBegin    bool
	sawEntity   bool
	firing      bool
	movesRead   []storage.Row
}

func (h *hookedReadStore) Unwrap() storage.Adapter { return h.Adapter }

func (h *hookedReadStore) WithReadTx(ctx context.Context, ws string, fn func(storage.ReadTx) error) error {
	h.mu.Lock()
	var hook func()
	if !h.firing {
		switch {
		case h.sawBegin && h.afterBegin != nil:
			hook, h.afterBegin = h.afterBegin, nil
		case h.sawEntity && h.afterEntity != nil:
			hook, h.afterEntity = h.afterEntity, nil
		}
		h.firing = hook != nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
		h.mu.Lock()
		h.firing = false
		h.mu.Unlock()
	}
	return h.Adapter.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		return fn(&hookedReadTx{ReadTx: tx, h: h})
	})
}

type hookedReadTx struct {
	storage.ReadTx
	h *hookedReadStore
}

func (t *hookedReadTx) StoreEpoch() (uint64, error) {
	t.h.mu.Lock()
	if !t.h.firing {
		t.h.sawBegin = true
	}
	t.h.mu.Unlock()
	return t.ReadTx.StoreEpoch()
}

func (t *hookedReadTx) saw(entity string) {
	t.h.mu.Lock()
	if !t.h.firing && entity == t.h.entity {
		t.h.sawEntity = true
	}
	t.h.mu.Unlock()
}

func (t *hookedReadTx) GetRows(entity string, keys []string, fn func(storage.Row) error) error {
	t.saw(entity)
	return t.ReadTx.GetRows(entity, keys, fn)
}

func (t *hookedReadTx) ScanEntityAfter(entity, afterKey string, limit int, fn func(storage.Row) error) error {
	t.saw(entity)
	return t.ReadTx.ScanEntityAfter(entity, afterKey, limit, fn)
}

func (t *hookedReadTx) ScanEntityByVersionDesc(entity string, limit int, fn func(storage.Row) error) error {
	if entity != accessMovesEntityName {
		return t.ReadTx.ScanEntityByVersionDesc(entity, limit, fn)
	}
	return t.ReadTx.ScanEntityByVersionDesc(entity, limit, func(row storage.Row) error {
		t.h.mu.Lock()
		t.h.movesRead = append(t.h.movesRead, row)
		t.h.mu.Unlock()
		return fn(row)
	})
}

var _ = Describe("Access model bootstrap across a move", func() {
	It("removes a comment the snapshot shipped while its task sat for a moment in a project the caller reads", func() {
		// Given comments below tasks below projects, where guest reads p1
		// and t2, with comment c2 below it, sits in p2
		hooked := &hookedReadStore{Adapter: newTestStore(), entity: "comment"}
		f := newDeepFixtureOn(hooked)
		f.seedOrg()
		f.seedComments()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
		moveTask := func(projectID string) {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&accessv1.Task{Id: "t2", Project: project(projectID)}, []string{"project"}, nil)
			})).To(Succeed())
		}
		// And t2 moves to p1 after the snapshot begins, and back to p2
		// after the snapshot reads the comments
		hooked.mu.Lock()
		hooked.sawBegin, hooked.sawEntity = false, false
		hooked.afterBegin = func() { moveTask("p1") }
		hooked.afterEntity = func() { moveTask("p2") }
		hooked.mu.Unlock()

		// When guest bootstraps, and pulls from the Begin cursor
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest"})
		as("guest", req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		guest := newAccessReplica(f, "guest")
		for stream.Receive() {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				guest.cursor = body.Begin.GetCursor()
				guest.access = body.Begin.GetCallerAccess()
			case *syncplanev1.BootstrapResponse_Chunk_:
				for _, rc := range body.Chunk.GetRows() {
					guest.rows[rc.GetEntity()+"/"+rc.GetKey()] = true
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		hooked.mu.Lock()
		Expect(hooked.afterBegin).To(BeNil(), "t2 did not move into p1 during the snapshot")
		Expect(hooked.afterEntity).To(BeNil(), "t2 did not move back during the snapshot")
		hooked.mu.Unlock()
		guest.sync()

		// Then guest holds what a fresh pull shows it: t2 and c2 are in
		// p2, which guest does not read
		Expect(guest.view()).NotTo(ContainElement("comment/c2"))
		Expect(guest.view()).To(Equal(f.pull("guest")))
	})
})

// resetReasons returns the reasons of the client resets that logs recorded,
// oldest first.
func resetReasons(logs *recordingHandler) []string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var out []string
	for _, r := range logs.records {
		if r.Message != "client reset; re-bootstrap required" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "reason" {
				out = append(out, a.Value.String())
			}
			return true
		})
	}
	return out
}

// bootstrapReplica bootstraps a replica of user, which holds the rows of the
// snapshot and the Begin cursor.
func bootstrapReplica(f *accessFixture, user string) *accessReplica {
	GinkgoHelper()
	req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-" + user})
	as(user, req.Header())
	stream, err := f.sync.Bootstrap(f.ctx, req)
	Expect(err).NotTo(HaveOccurred())
	r := newAccessReplica(f, user)
	for stream.Receive() {
		switch body := stream.Msg().GetBody().(type) {
		case *syncplanev1.BootstrapResponse_Begin_:
			r.cursor = body.Begin.GetCursor()
			r.access = body.Begin.GetCallerAccess()
		case *syncplanev1.BootstrapResponse_Chunk_:
			for _, rc := range body.Chunk.GetRows() {
				r.rows[rc.GetEntity()+"/"+rc.GetKey()] = true
			}
		}
	}
	Expect(stream.Err()).NotTo(HaveOccurred())
	return r
}

var _ = Describe("Access model move log overflow", func() {
	// scanLimit is Limits.MaxResyncScan, which also bounds how many access
	// moves one pull reads.
	const scanLimit = 8

	var (
		hooked *hookedReadStore
		logs   *recordingHandler
		f      *accessFixture
	)

	// fresh returns what a replica that syncs from no cursor holds now. A
	// single pull cannot show it: MaxPullChanges is as small as scanLimit.
	fresh := func() []string {
		GinkgoHelper()
		r := newAccessReplica(f, "guest")
		r.sync()
		return r.view()
	}

	// churn moves t2 n times between p2 and p9. guest reads neither, so each
	// move adds one record to the log and changes nothing guest can read.
	churn := func(n int) {
		GinkgoHelper()
		for i := range n {
			to := "p9"
			if i%2 == 1 {
				to = "p2"
			}
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Patch(&accessv1.Task{Id: "t2", Project: project(to)}, []string{"project"}, nil)
			})).To(Succeed())
		}
	}

	// snapshot runs a Bootstrap as guest from resume into r, and stops after
	// chunks chunks when chunks is not 0. It returns the Begin frame and the
	// resume token of the last chunk.
	snapshot := func(r *accessReplica, resume *syncplanev1.BootstrapResume, chunks int) (*syncplanev1.BootstrapResponse_Begin, []byte) {
		GinkgoHelper()
		req := connect.NewRequest(&syncplanev1.BootstrapRequest{WorkspaceId: ws, ClientId: "c-guest", Resume: resume})
		as("guest", req.Header())
		stream, err := f.sync.Bootstrap(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(stream.Close()).To(Succeed()) }()
		var (
			begin *syncplanev1.BootstrapResponse_Begin
			token []byte
		)
		for seen := 0; stream.Receive(); {
			switch body := stream.Msg().GetBody().(type) {
			case *syncplanev1.BootstrapResponse_Begin_:
				begin = body.Begin
				if !begin.GetResumed() {
					clear(r.rows)
				}
				r.cursor = begin.GetCursor()
				r.access = begin.GetCallerAccess()
			case *syncplanev1.BootstrapResponse_Chunk_:
				for _, rc := range body.Chunk.GetRows() {
					r.rows[rc.GetEntity()+"/"+rc.GetKey()] = true
				}
				token = body.Chunk.GetResumeToken()
				if seen++; seen == chunks {
					return begin, token
				}
			}
		}
		Expect(stream.Err()).NotTo(HaveOccurred())
		return begin, token
	}

	BeforeEach(func() {
		hooked = &hookedReadStore{Adapter: newTestStore()}
		logs = &recordingHandler{}
		f = newDeepFixtureOn(hooked,
			server.WithLimits(server.Limits{MaxResyncScan: scanLimit, MaxPullChanges: scanLimit}),
			server.WithChunkSize(1),
			server.WithLogger(slog.New(logs)))
		f.seedOrg()
		f.seedComments()
		Expect(f.srv.BuildIndexes(f.ctx, ws)).To(Succeed())
	})

	It("serves a pull whose cursor is behind as many moves as MaxResyncScan", func() {
		// Given guest has synced
		guest := newAccessReplica(f, "guest")
		guest.sync()

		// When t2 moves MaxResyncScan times, and guest pulls
		churn(scanLimit)
		guest.sync()

		// Then the pull does not reset guest
		Expect(resetReasons(logs)).To(BeEmpty())
		Expect(guest.view()).To(Equal(fresh()))
	})

	It("resets a pull whose cursor is behind more moves than MaxResyncScan", func() {
		// Given guest has synced
		guest := newAccessReplica(f, "guest")
		guest.sync()

		// When t2 moves one time more than MaxResyncScan, and guest pulls
		churn(scanLimit + 1)
		resp := guest.pullOnce(0)

		// Then the pull resets guest because of the move log
		Expect(resp.GetReset_()).To(BeTrue())
		Expect(resetReasons(logs)).To(Equal([]string{"access_moves_overflow"}))
	})

	It("resets the first pull after a Bootstrap during which more than MaxResyncScan moves were logged, and a new Bootstrap converges", func() {
		// Given t2 moves one time more than MaxResyncScan after the
		// snapshot of guest begins
		hooked.mu.Lock()
		hooked.sawBegin = false
		hooked.afterBegin = func() { churn(scanLimit + 1) }
		hooked.mu.Unlock()

		// When guest bootstraps, and pulls from the Begin cursor
		guest := bootstrapReplica(f, "guest")
		hooked.mu.Lock()
		Expect(hooked.afterBegin).To(BeNil(), "t2 did not move during the snapshot")
		hooked.mu.Unlock()
		resp := guest.pullOnce(0)

		// Then the pull resets guest because of the move log
		Expect(resp.GetReset_()).To(BeTrue())
		Expect(resetReasons(logs)).To(Equal([]string{"access_moves_overflow"}))

		// And when the moves stop, a new Bootstrap syncs without a reset
		again := bootstrapReplica(f, "guest")
		again.sync()
		Expect(resetReasons(logs)).To(HaveLen(1))
		Expect(again.view()).To(Equal(fresh()))
	})

	It("resumes a snapshot behind as many moves as MaxResyncScan", func() {
		// Given guest has received one chunk of a snapshot
		guest := newAccessReplica(f, "guest")
		first, token := snapshot(guest, nil, 1)
		Expect(token).NotTo(BeEmpty())

		// When t2 moves MaxResyncScan times, and guest resumes the snapshot
		churn(scanLimit)
		second, _ := snapshot(guest, &syncplanev1.BootstrapResume{Cursor: first.GetCursor(), Token: token}, 0)

		// Then the snapshot continues, and the pull after it does not reset
		// guest
		Expect(second.GetResumed()).To(BeTrue())
		guest.sync()
		Expect(resetReasons(logs)).To(BeEmpty())
		Expect(guest.view()).To(Equal(fresh()))
	})

	It("begins a new snapshot instead of resuming one behind more moves than MaxResyncScan", func() {
		// Given guest has received one chunk of a snapshot
		guest := newAccessReplica(f, "guest")
		first, token := snapshot(guest, nil, 1)
		Expect(token).NotTo(BeEmpty())

		// When t2 moves one time more than MaxResyncScan, and guest resumes
		// the snapshot
		churn(scanLimit + 1)
		second, _ := snapshot(guest, &syncplanev1.BootstrapResume{Cursor: first.GetCursor(), Token: token}, 0)

		// Then the server begins a new snapshot, and the pull after it does
		// not reset guest
		Expect(second.GetResumed()).To(BeFalse())
		guest.sync()
		Expect(resetReasons(logs)).To(BeEmpty())
		Expect(guest.view()).To(Equal(fresh()))
	})

	It("reads each move after the Begin position from the log one time during a snapshot", func() {
		// Given t2 moves MaxResyncScan times after the snapshot of guest
		// begins
		hooked.mu.Lock()
		hooked.sawBegin = false
		hooked.afterBegin = func() {
			churn(scanLimit)
			hooked.mu.Lock()
			hooked.movesRead = nil
			hooked.mu.Unlock()
		}
		hooked.mu.Unlock()

		// When guest bootstraps one row per chunk
		guest := newAccessReplica(f, "guest")
		begin, _ := snapshot(guest, nil, 0)

		// Then the pages of the snapshot read each of those moves one time,
		// except the newest, where each later page stops its scan
		hooked.mu.Lock()
		Expect(hooked.afterBegin).To(BeNil(), "t2 did not move during the snapshot")
		reads := map[string]int{}
		newest := ""
		for _, row := range hooked.movesRead {
			if row.Version > begin.GetSeq() {
				reads[row.Key]++
				newest = max(newest, row.Key)
			}
		}
		hooked.mu.Unlock()
		Expect(reads).To(HaveLen(scanLimit))
		for key, n := range reads {
			if key != newest {
				Expect(n).To(Equal(1), "move %q", key)
			}
		}

		// And the pull after it does not reset guest
		guest.sync()
		Expect(resetReasons(logs)).To(BeEmpty())
		Expect(guest.view()).To(Equal(fresh()))
	})
})
