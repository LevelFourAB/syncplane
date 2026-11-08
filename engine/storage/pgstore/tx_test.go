package pgstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

// The workspace's position scalars and index states are read once per
// transaction and kept, so every way the transaction can move them under
// itself has to drop what it kept. The conformance suite covers the forward
// moves; a savepoint rewind is the one that moves them BACKWARD, and only this
// adapter has it.
var _ = Describe("Postgres workspace position", func() {
	ctx := context.Background()
	const ws = "ws"

	It("forgets a store epoch a savepoint rolled back", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		boom := errors.New("boom")
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(tx.StoreEpoch()).To(BeZero())
			Expect(tx.InSavepoint(func() error {
				if err := tx.SetClientLastMutationID("c1", 1); err != nil {
					return err
				}
				Expect(tx.StoreEpoch()).NotTo(BeZero(), "the write registered the workspace")
				return boom
			})).To(MatchError(boom))
			// The rewind took the workspaces row with it, and with the row the
			// epoch that was minted on it.
			Expect(tx.StoreEpoch()).To(BeZero())
			return nil
		})).To(Succeed())

		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			Expect(tx.StoreEpoch()).To(BeZero())
			return nil
		})).To(Succeed())
	})

	It("forgets an index state a savepoint rolled back", func() {
		store := open()
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.SetIndexState("widget", "by_label", "old", true)
		})).To(Succeed())

		boom := errors.New("boom")
		state := func(tx storage.ReadTx) []any {
			GinkgoHelper()
			hash, ready, err := tx.IndexState("widget", "by_label")
			Expect(err).NotTo(HaveOccurred())
			return []any{hash, ready}
		}
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			Expect(state(tx)).To(Equal([]any{"old", true}))
			Expect(tx.InSavepoint(func() error {
				if err := tx.SetIndexState("widget", "by_label", "new", false); err != nil {
					return err
				}
				Expect(state(tx)).To(Equal([]any{"new", false}), "a write must see its own index state")
				return boom
			})).To(MatchError(boom))
			Expect(state(tx)).To(Equal([]any{"old", true}), "the rewind took the new state with it")
			return nil
		})).To(Succeed())
	})
})

// commitStall is a TCP proxy in front of Postgres that, once armed, withholds
// the first reply to a COMMIT (its CommandComplete tag) until released. The server has
// committed by the time it sends that reply, so a held one is the window a
// caller's context can end in after the commit happened.
type commitStall struct {
	ln       net.Listener
	upstream string

	mu       sync.Mutex
	armed    bool
	stalled  bool
	held     chan struct{} // closed when a COMMIT reply is held
	released chan struct{} // closed by release
}

func newCommitStall(upstream string) *commitStall {
	GinkgoHelper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	p := &commitStall{ln: ln, upstream: upstream, held: make(chan struct{}), released: make(chan struct{})}
	DeferCleanup(func() { _ = ln.Close() })
	go p.serve()
	return p
}

func (p *commitStall) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = io.Copy(server, client)
		}()
		go func() {
			defer func() { _ = client.Close() }()
			buf := make([]byte, 32<<10)
			for {
				n, err := server.Read(buf)
				if n > 0 {
					if bytes.Contains(buf[:n], []byte("COMMIT\x00")) && p.stall() {
						<-p.released
					}
					if _, werr := client.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
}

// stall reports whether this COMMIT reply is the one to hold.
func (p *commitStall) stall() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.armed || p.stalled {
		return false
	}
	p.stalled = true
	close(p.held)
	return true
}

func (p *commitStall) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
}

func (p *commitStall) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.released:
	default:
		close(p.released)
	}
}

// through returns dsn with its host replaced by the proxy's.
func (p *commitStall) through(dsn string) string {
	GinkgoHelper()
	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred())
	u.Host = p.ln.Addr().String()
	return u.String()
}

var _ = Describe("Postgres commit under cancellation", func() {
	ctx := context.Background()
	const ws = "ws"

	It("does not report failure for a write that committed while its context ended", func() {
		// Given a write whose COMMIT reached the server and committed, with
		// the reply still on its way
		dsn := pgtest.DSN()
		u, err := url.Parse(dsn)
		Expect(err).NotTo(HaveOccurred())
		direct, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(direct.Close()).To(Succeed()) })
		stall := newCommitStall(u.Host)
		DeferCleanup(stall.release)
		store, err := pgstore.Open(ctx, stall.through(dsn))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		stall.arm()
		writeCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			result <- store.WithWriteTx(writeCtx, ws, func(tx storage.WriteTx) error {
				seq, err := tx.NextSeq()
				if err != nil {
					return err
				}
				return tx.PutRow("widget", "k1", []byte("v"), seq, storage.Actor{})
			})
		}()
		Eventually(stall.held).Should(BeClosed())
		committed := func() bool {
			var found bool
			Expect(direct.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
				_, err := tx.GetRow("widget", "k1")
				if errors.Is(err, storage.ErrNotFound) {
					return nil
				}
				found = err == nil
				return err
			})).To(Succeed())
			return found
		}
		Eventually(committed).Should(BeTrue())

		// When the caller's context ends before the reply arrives
		cancel()
		// A call that waits out the reply gets it once the proxy lets it
		// through; one that gives up on the context returns before that.
		var callErr error
		select {
		case callErr = <-result:
		case <-time.After(500 * time.Millisecond):
			stall.release()
			callErr = <-result
		}

		// Then the call reports the commit that happened
		Expect(callErr).NotTo(HaveOccurred(), "the write committed, so the call must not report failure")
	})
})
