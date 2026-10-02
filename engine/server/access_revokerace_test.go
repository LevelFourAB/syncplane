package server_test

import (
	"context"
	"errors"
	gosync "sync"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage"
)

// A grant can change while a long read runs. The snapshot ships rows under the
// grants it began with, and the pull from its Begin cursor must leave the
// client with what the grants after the change show.
var _ = Describe("Access model bootstrap across a revoked grant", func() {
	It("removes the rows the snapshot shipped below a grant revoked after the snapshot began", func() {
		// Given guest reads p1 and p2 through two project grants
		hooked := &hookedReadStore{Adapter: newTestStore(), entity: "task"}
		f := newAccessFixtureOn(hooked)
		f.seedOrg()
		f.seed(&accessv1.ProjectMember{Id: "pm-guest-p2", Project: project("p2"), UserId: "guest", Role: "project_reader"})

		// And the p1 grant is revoked after the snapshot begins, before it
		// reads a page
		hooked.mu.Lock()
		hooked.sawBegin, hooked.sawEntity = false, false
		hooked.afterBegin = func() {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.ProjectMember{}, "pm-guest")
			})).To(Succeed())
		}
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
		Expect(hooked.afterBegin).To(BeNil(), "the grant was not revoked during the snapshot")
		hooked.mu.Unlock()
		guest.sync()

		// Then guest holds what a fresh pull shows it: p2 and t2, and nothing
		// of p1
		Expect(guest.view()).NotTo(ContainElements("task/t1", "project/p1"))
		Expect(guest.view()).To(Equal(f.pull("guest")))
	})
})

// betweenWritesStore runs before once, before its write transaction number
// at, counted from 1. It lets a spec commit a write in the gap between two
// transactions of one request.
type betweenWritesStore struct {
	storage.Adapter
	mu     gosync.Mutex
	calls  int
	at     int
	before func()
}

func (s *betweenWritesStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	s.mu.Lock()
	s.calls++
	var hook func()
	if s.calls == s.at {
		hook, s.before = s.before, nil
	}
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return s.Adapter.WithWriteTx(ctx, ws, fn)
}

func (s *betweenWritesStore) Unwrap() storage.Adapter { return s.Adapter }

// arm makes the write transaction after the next one run before first.
func (s *betweenWritesStore) arm(before func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.at, s.before = s.calls+2, before
}

// ran reports whether the armed function ran.
func (s *betweenWritesStore) ran() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.before == nil
}

// errAttemptAgain rolls back the first attempt of a retryingStore.
var errAttemptAgain = errors.New("attempt again")

// retryingStore runs the closure of its next write transaction twice, as the
// storage.Adapter contract lets an adapter do: once in a transaction it rolls
// back, then before, then again in a transaction that commits.
type retryingStore struct {
	storage.Adapter
	mu     gosync.Mutex
	before func()
}

func (s *retryingStore) WithWriteTx(ctx context.Context, ws string, fn func(storage.WriteTx) error) error {
	s.mu.Lock()
	hook := s.before
	s.before = nil
	s.mu.Unlock()
	if hook == nil {
		return s.Adapter.WithWriteTx(ctx, ws, fn)
	}
	err := s.Adapter.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return errAttemptAgain
	})
	if !errors.Is(err, errAttemptAgain) {
		return err
	}
	hook()
	return s.Adapter.WithWriteTx(ctx, ws, fn)
}

func (s *retryingStore) Unwrap() storage.Adapter { return s.Adapter }

// A grant can also change between two transactions of one write: the first
// attempt of a push and its rerun under savepoints, two attempts of a closure
// that the adapter retries, or the gate's read of the grants and the write
// transaction. Member holds org_member at the root through om-member and no
// other grant, and notes admit every admitted caller, so no permission check
// of the write itself reads the grants again.
var _ = Describe("Access model writes across a revoked grant", func() {
	revokeMember := func(f *accessFixture) func() {
		return func() {
			Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
				return tx.Delete(&accessv1.OrgMember{}, "om-member")
			})).To(Succeed())
		}
	}

	noteStored := func(f *accessFixture, key string) bool {
		GinkgoHelper()
		var found bool
		Expect(f.srv.Mutate(server.Trusted(f.ctx), ws, func(tx *server.EntityTx) error {
			var err error
			found, err = tx.Get(&accessv1.Note{}, key)
			return err
		})).To(Succeed())
		return found
	}

	It("refuses a push whose rerun under savepoints starts after the caller's last grant went", func() {
		// Given om-member is revoked after the first attempt of the push and
		// before its rerun
		store := &betweenWritesStore{Adapter: newTestStore()}
		f := newAccessFixtureOn(store)
		f.seedOrg()
		store.arm(revokeMember(f))

		// When member pushes a task edit it may not make, which reruns the
		// push under savepoints, and a new note
		req := connect.NewRequest(pushOf(ws, []*syncplanev1.Mutation{
			patchMutation("c-member", 1, "task", "t1", &accessv1.Task{Id: "t1", Title: "x"}, "title"),
			putMutation("c-member", 2, "note", "n-revoked", &accessv1.Note{Id: "n-revoked", Body: "after revoke"}),
		}))
		as("member", req.Header())
		resp, err := f.sync.Push(f.ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.ran()).To(BeTrue(), "the revoke did not run between the attempts")

		// Then the gate refuses the batch, and the note is not stored
		Expect(resp.Msg.GetResults()).To(HaveLen(1))
		Expect(resp.Msg.GetResults()[0].GetErrorCode()).To(Equal(server.CodeWorkspaceNoAccess))
		Expect(noteStored(f, "n-revoked")).To(BeFalse())
	})

	It("refuses Server.Mutate when the caller's last grant goes before an adapter retry", func() {
		// Given an adapter that runs the next write closure twice, with
		// om-member revoked between the two runs
		store := &retryingStore{Adapter: newTestStore()}
		f := newAccessFixtureOn(store)
		f.seedOrg()
		store.mu.Lock()
		store.before = revokeMember(f)
		store.mu.Unlock()

		// When member writes a note through Server.Mutate
		ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: "member"})
		err := f.srv.Mutate(ctx, ws, func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.Note{Id: "n-retried", Body: "after revoke"})
		})

		// Then the gate refuses the call, and the note is not stored
		Expect(err).To(MatchError(server.ErrNoAccess))
		Expect(noteStored(f, "n-retried")).To(BeFalse())
	})

	It("refuses MutateIdempotent when the caller's last grant goes after the gate read it", func() {
		// Given om-member is revoked after the gate of MutateIdempotent reads
		// the grants, and before the write transaction begins
		store := &betweenWritesStore{Adapter: newTestStore()}
		f := newAccessFixtureOn(store)
		f.seedOrg()
		store.mu.Lock()
		store.at, store.before = store.calls+1, revokeMember(f)
		store.mu.Unlock()

		// When member writes a note through MutateIdempotent
		ctx := server.WithPrincipal(f.ctx, server.Principal{Subject: "member"})
		_, err := f.srv.MutateIdempotent(ctx, ws, "k1", func(tx *server.EntityTx) ([]byte, error) {
			return nil, tx.Put(&accessv1.Note{Id: "n-idem", Body: "after revoke"})
		})
		Expect(store.ran()).To(BeTrue(), "the revoke did not run")

		// Then the gate refuses the call, and the note is not stored
		Expect(err).To(MatchError(server.ErrNoAccess))
		Expect(noteStored(f, "n-idem")).To(BeFalse())
	})
})
