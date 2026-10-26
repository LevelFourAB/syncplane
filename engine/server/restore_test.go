package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// A store restored from a backup carries the store epoch it was backed up with,
// so the epoch cannot tell the restored history from the one the clients
// followed, and once the restored store has taken enough new writes the
// sequence check cannot either. These specs pin the check that can: the cursor
// names the write that issued its sequence, and the restored store wrote that
// sequence again under another name.
var _ = Describe("Cursor provenance across a restore from backup", func() {
	const wsID = "ws-restore"

	type node struct {
		srv    *server.Server
		client syncplanev1connect.SyncServiceClient
		stop   func()
	}

	start := func(path string) node {
		GinkgoHelper()
		store, err := sqlitestore.Open(path)
		Expect(err).NotTo(HaveOccurred())
		srv := mustNewServer(store, server.WithEntities(&testdatav1.Widget{}))
		p, h := srv.Handler()
		mux := http.NewServeMux()
		mux.Handle(p, h)
		ts := httptest.NewServer(mux)
		return node{
			srv:    srv,
			client: syncplanev1connect.NewSyncServiceClient(ts.Client(), ts.URL),
			stop: func() {
				ts.Close()
				Expect(srv.Close()).To(Succeed())
				Expect(store.Close()).To(Succeed())
			},
		}
	}

	copyFile := func(from, to string) {
		GinkgoHelper()
		src, err := os.Open(from)
		Expect(err).NotTo(HaveOccurred())
		defer src.Close()
		dst, err := os.Create(to)
		Expect(err).NotTo(HaveOccurred())
		defer dst.Close()
		_, err = io.Copy(dst, src)
		Expect(err).NotTo(HaveOccurred())
	}

	put := func(n node, client string, mid uint64, key, label string) {
		GinkgoHelper()
		resp, err := n.client.Push(context.Background(), connect.NewRequest(pushOf(wsID, []*syncplanev1.Mutation{
			putMutation(client, mid, "widget", key, &testdatav1.Widget{Id: key, Label: label}),
		})))
		Expect(err).NotTo(HaveOccurred())
		Expect(pushResults(resp.Msg)[0].GetErrorCode()).To(BeEmpty())
	}

	pull := func(n node, cursor []byte) *syncplanev1.PullResponse {
		GinkgoHelper()
		resp, err := n.client.Pull(context.Background(), connect.NewRequest(&syncplanev1.PullRequest{
			WorkspaceId: wsID, Cursor: cursor,
		}))
		Expect(err).NotTo(HaveOccurred())
		return resp.Msg
	}

	keysOf := func(resp *syncplanev1.PullResponse) []string {
		var keys []string
		for _, rc := range resp.GetPatch().GetChanges() {
			keys = append(keys, rc.GetKey())
		}
		return keys
	}

	// The history every spec starts from: a backup taken at seq 2, two more
	// writes on the live store, then the backup restored and three new writes
	// (seq 3..5) by another client. behind is a cursor at the backup point and
	// lost one at seq 4, from the history the restore threw away.
	var (
		restored     node
		behind, lost []byte
		lostFeed     server.Cursor
	)

	BeforeEach(func() {
		dir := GinkgoT().TempDir()
		live := filepath.Join(dir, "live.db")
		backup := filepath.Join(dir, "backup.db")

		a := start(live)
		put(a, "c1", 1, "w1", "kept")
		put(a, "c1", 2, "w2", "kept")
		behind = pull(a, nil).GetNextCursor()
		a.stop()
		copyFile(live, backup)

		a = start(live)
		put(a, "c1", 3, "w3", "lost")
		put(a, "c1", 4, "w4", "lost")
		resp := pull(a, nil)
		Expect(resp.GetSeq()).To(BeEquivalentTo(4))
		lost = resp.GetNextCursor()
		var err error
		lostFeed, _, err = a.srv.Changes(context.Background(), wsID, server.Cursor{}, 0, func(server.Change) error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(lostFeed.Seq).To(BeEquivalentTo(4))
		a.stop()

		restored = start(backup)
		DeferCleanup(restored.stop)
		put(restored, "c2", 1, "w5", "new")
		put(restored, "c2", 2, "w6", "new")
		put(restored, "c2", 3, "w7", "new")
	})

	It("resets a cursor from the lost history once the restored store has written past it", func() {
		resp := pull(restored, lost)
		Expect(resp.GetReset_()).To(BeTrue(),
			"the restored store wrote w5 and w6 at sequences the cursor claims to hold; got %v", keysOf(resp))
	})

	It("serves a cursor from before the backup point incrementally", func() {
		resp := pull(restored, behind)
		Expect(resp.GetReset_()).To(BeFalse())
		Expect(keysOf(resp)).To(ConsistOf("w5", "w6", "w7"))
	})

	It("answers a feed cursor from the lost history with ErrCursorTooOld", func() {
		var keys []string
		_, _, err := restored.srv.Changes(context.Background(), wsID, lostFeed, 0, func(ch server.Change) error {
			keys = append(keys, ch.Key)
			return nil
		})
		Expect(err).To(MatchError(server.ErrCursorTooOld), "the feed delivered %v", keys)
	})

	It("does not reset the cursor it issues after the restore", func() {
		first := pull(restored, behind)
		Expect(first.GetReset_()).To(BeFalse())
		put(restored, "c2", 4, "w8", "new")
		again := pull(restored, first.GetNextCursor())
		Expect(again.GetReset_()).To(BeFalse())
		Expect(keysOf(again)).To(ConsistOf("w8"))
	})
})
