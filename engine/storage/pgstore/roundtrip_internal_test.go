package pgstore

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
)

// tripCounter is a TCP proxy in front of the test database that counts round
// trips: a client write that opens a connection, or that follows data from the
// server, starts one. A count read between two calls to the store is exact,
// because a call returns only after the server's answer has passed the proxy.
type tripCounter struct {
	ln    net.Listener
	trips atomic.Int64
}

// countTrips starts a tripCounter in front of the database behind dsn and
// returns it with dsn pointed at it. The proxy stops when the spec ends.
func countTrips(dsn string) (*tripCounter, string) {
	GinkgoHelper()
	u, err := url.Parse(dsn)
	Expect(err).NotTo(HaveOccurred(), "the suite's DSN must be a URI to put a proxy in front of")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(ln.Close)
	c := &tripCounter{ln: ln}
	go c.serve(u.Host)
	u.Host = ln.Addr().String()
	return c, u.String()
}

func (c *tripCounter) serve(target string) {
	for {
		client, err := c.ln.Accept()
		if err != nil {
			return
		}
		go c.pipe(client, target)
	}
}

func (c *tripCounter) pipe(client net.Conn, target string) {
	defer func() { _ = client.Close() }()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	var spoke atomic.Bool
	spoke.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64<<10)
		for {
			n, err := server.Read(buf)
			if n > 0 {
				// Before the bytes go on: the client's next write must find
				// the flag set.
				spoke.Store(true)
				if _, err := client.Write(buf[:n]); err != nil {
					return
				}
			}
			if err != nil {
				_ = client.Close()
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := client.Read(buf)
		if n > 0 {
			if spoke.Swap(false) {
				c.trips.Add(1)
			}
			if _, err := server.Write(buf[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = server.Close()
	<-done
}

// The statements that open and close a transaction carry nothing a caller
// waits for, so they travel with the statements around them. These specs pin
// what one transaction costs in round trips, which nothing a caller can see
// otherwise tells apart.
var _ = Describe("Postgres round trips", func() {
	ctx := context.Background()
	const ws = "ws"

	var (
		store *Store
		// trips runs fn twice and returns the round trips of the second run.
		// The first fills the connection's statement cache, which costs a
		// round trip per batch that holds a statement new to the connection.
		trips func(fn func()) int64
	)
	BeforeEach(func() {
		counter, dsn := countTrips(pgtest.DSN())
		var err error
		// One connection, so the warm-up and the measured run share a cache.
		store, err = Open(ctx, dsn, WithMaxConns(1))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		trips = func(fn func()) int64 {
			fn()
			before := counter.trips.Load()
			fn()
			return counter.trips.Load() - before
		}
	})

	read := func(fn func(tx storage.ReadTx) error) func() {
		return func() {
			GinkgoHelper()
			Expect(store.WithReadTx(ctx, ws, fn)).To(Succeed())
		}
	}
	write := func(fn func(tx storage.WriteTx) error) func() {
		return func() {
			GinkgoHelper()
			Expect(store.WithWriteTx(ctx, ws, fn)).To(Succeed())
		}
	}

	It("costs nothing for a transaction that issues no statement", func() {
		Expect(trips(read(func(storage.ReadTx) error { return nil }))).To(BeZero())
		Expect(trips(write(func(storage.WriteTx) error { return nil }))).To(BeZero())
	})

	It("opens a read transaction with its first read", func() {
		// The read with BEGIN, then the ROLLBACK.
		Expect(trips(read(func(tx storage.ReadTx) error {
			_, err := tx.GetRow("widget", "k")
			Expect(err).To(MatchError(storage.ErrNotFound))
			return nil
		}))).To(BeEquivalentTo(2))
	})

	It("reads the workspace position and every index state in one round trip", func() {
		write(func(tx storage.WriteTx) error {
			seq, err := tx.NextSeq()
			if err != nil {
				return err
			}
			if err := tx.SetIndexState("widget", "by_a", "ha", true); err != nil {
				return err
			}
			if err := tx.SetIndexState("widget", "by_b", "hb", false); err != nil {
				return err
			}
			return tx.PutRow("widget", "k", []byte("v"), seq, storage.Actor{})
		})()

		Expect(trips(read(func(tx storage.ReadTx) error {
			Expect(tx.CurrentSeq()).To(BeEquivalentTo(1))
			Expect(tx.StoreEpoch()).NotTo(BeZero())
			hash, ready, err := tx.IndexState("widget", "by_a")
			Expect(err).NotTo(HaveOccurred())
			Expect([]any{hash, ready}).To(Equal([]any{"ha", true}))
			hash, ready, err = tx.IndexState("widget", "by_b")
			Expect(err).NotTo(HaveOccurred())
			Expect([]any{hash, ready}).To(Equal([]any{"hb", false}))
			hash, ready, err = tx.IndexState("gadget", "by_a")
			Expect(err).NotTo(HaveOccurred())
			Expect([]any{hash, ready}).To(Equal([]any{"", false}))
			return nil
		}))).To(BeEquivalentTo(2), "the reads with BEGIN, then the ROLLBACK")
	})

	It("sends a write transaction that only writes as one batch", func() {
		Expect(trips(write(func(tx storage.WriteTx) error {
			if err := tx.PutRow("widget", "a", []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			return tx.PutRow("widget", "b", []byte("v"), 2, storage.Actor{})
		}))).To(BeEquivalentTo(1), "BEGIN, the lock, the writes and COMMIT")

		read(func(tx storage.ReadTx) error {
			for _, key := range []string{"a", "b"} {
				_, err := tx.GetRow("widget", key)
				Expect(err).NotTo(HaveOccurred())
			}
			return nil
		})()
	})

	It("sends a read in one batch with the writes queued before it", func() {
		Expect(trips(write(func(tx storage.WriteTx) error {
			if err := tx.PutRow("widget", "a", []byte("first"), 1, storage.Actor{}); err != nil {
				return err
			}
			row, err := tx.GetRow("widget", "a")
			Expect(err).NotTo(HaveOccurred())
			Expect(row.Value).To(Equal([]byte("first")), "the read must see the write it travelled with")
			return tx.PutRow("widget", "b", []byte("v"), 2, storage.Actor{})
		}))).To(BeEquivalentTo(2), "the opening, the first write and the read; then the second write and COMMIT")
	})
})

// A transaction runs on a connection it holds outside any pgx.Tx, so it is the
// store, not pgx, that has to hand the connection back with no transaction
// open on it — whichever way the transaction ends. Every store here holds one
// connection, so the next transaction gets the same one unless it was closed.
var _ = Describe("Postgres transaction cleanup", func() {
	ctx := context.Background()
	const ws = "ws"

	oneConn := func() *Store {
		GinkgoHelper()
		store, err := Open(ctx, pgtest.DSN(), WithMaxConns(1))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		return store
	}

	// next reports the backend and the transaction status of the connection
	// the store's pool hands out next.
	next := func(store *Store) (uint32, byte) {
		GinkgoHelper()
		conn, err := store.pool.Acquire(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Release()
		return conn.Conn().PgConn().PID(), conn.Conn().PgConn().TxStatus()
	}

	// usable runs a write and reads it back.
	usable := func(store *Store) {
		GinkgoHelper()
		Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
			return tx.PutRow("widget", "later", []byte("v"), 9, storage.Actor{})
		})).To(Succeed())
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			_, err := tx.GetRow("widget", "later")
			return err
		})).To(Succeed())
	}

	It("rolls back a first batch that fails and keeps the connection", func() {
		store := oneConn()
		var pid uint32
		err := store.WithWriteTx(ctx, ws, func(wtx storage.WriteTx) error {
			pid = wtx.(*pgTx).conn.Conn().PgConn().PID()
			if err := wtx.PutRow("widget", "bad\u0000key", []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			_, err := wtx.GetRow("widget", "anything")
			return err
		})
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue(), "the batch must fail in the server: got %v", err)

		backend, status := next(store)
		Expect(status).To(Equal(byte('I')))
		Expect(backend).To(Equal(pid), "a ROLLBACK, not a closed connection, must end the transaction")
		usable(store)
	})

	It("rolls back a closure that fails after its first read and keeps the connection", func() {
		store := oneConn()
		boom := errors.New("boom")
		var pid uint32
		Expect(store.WithWriteTx(ctx, ws, func(wtx storage.WriteTx) error {
			pid = wtx.(*pgTx).conn.Conn().PgConn().PID()
			if err := wtx.PutRow("widget", "doomed", []byte("v"), 1, storage.Actor{}); err != nil {
				return err
			}
			if _, err := wtx.GetRow("widget", "doomed"); err != nil {
				return err
			}
			return boom
		})).To(MatchError(boom))

		backend, status := next(store)
		Expect(status).To(Equal(byte('I')))
		Expect(backend).To(Equal(pid))
		Expect(store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
			_, err := tx.GetRow("widget", "doomed")
			Expect(err).To(MatchError(storage.ErrNotFound))
			return nil
		})).To(Succeed())
	})

	It("rolls back a read transaction whose closure panics", func() {
		store := oneConn()
		var pid uint32
		Expect(func() {
			_ = store.WithReadTx(ctx, ws, func(rtx storage.ReadTx) error {
				pid = rtx.(readOnlyTx).ReadTx.(*pgTx).conn.Conn().PgConn().PID()
				if _, err := rtx.CurrentSeq(); err != nil {
					return err
				}
				panic("boom")
			})
		}).To(PanicWith("boom"))

		backend, status := next(store)
		Expect(status).To(Equal(byte('I')))
		Expect(backend).To(Equal(pid))
		usable(store)
	})

	It("leaves no transaction open when the context ends during the first batch", func() {
		// The first batch waits on the workspace lock another node holds, and
		// the context ends while it waits.
		holder := oneConn()
		holding, release := make(chan struct{}), make(chan struct{})
		held := make(chan error, 1)
		go func() {
			held <- holder.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
				if _, err := tx.CurrentSeq(); err != nil {
					return err
				}
				close(holding)
				<-release
				return tx.PutRow("widget", "held", []byte("v"), 1, storage.Actor{})
			})
		}()
		<-holding

		store := oneConn()
		waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		err := store.WithWriteTx(waitCtx, ws, func(tx storage.WriteTx) error {
			_, err := tx.CurrentSeq()
			return err
		})
		Expect(err).To(HaveOccurred())
		Expect(waitCtx.Err()).To(HaveOccurred(), "the write must have waited for the lock until its context ended")

		_, status := next(store)
		Expect(status).To(Equal(byte('I')))
		close(release)
		Eventually(held, 5*time.Second).Should(Receive(BeNil()))
		usable(store)
	})
})
