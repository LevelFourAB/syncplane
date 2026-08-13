package pglisten_test

import (
	"context"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/pglisten"
)

func TestPglisten(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Listen Suite")
}

var _ = AfterSuite(pgtest.Shutdown)

// recorder is a Listener that parks what it is handed where a spec can wait on
// it. Both methods run on the transport's listener goroutine and must not
// block, so the sends are buffered and give up rather than wait.
type recorder struct {
	payloads   chan string
	reconnects chan struct{}
}

func newRecorder() *recorder {
	return &recorder{payloads: make(chan string, 64), reconnects: make(chan struct{}, 16)}
}

func (r *recorder) Notification(payload string) {
	select {
	case r.payloads <- payload:
	default:
	}
}

func (r *recorder) Reconnected() {
	select {
	case r.reconnects <- struct{}{}:
	default:
	}
}

var _ = Describe("pglisten", func() {
	ctx := context.Background()

	// conn builds a transport on the shared server and registers its teardown.
	conn := func(dsn string) *pglisten.Conn {
		GinkgoHelper()
		c, err := pglisten.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(c.Close()).To(Succeed()) })
		return c
	}

	// listenOn registers a channel and returns the recorder behind it.
	listenOn := func(c *pglisten.Conn, channel string) *recorder {
		GinkgoHelper()
		r := newRecorder()
		Expect(c.Listen(ctx, channel, r)).To(Succeed())
		return r
	}

	It("carries a payload from one process to another", func() {
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)
		got := listenOn(receiver, channel)

		Expect(sender.Publish(ctx, channel, "hello")).To(Succeed())
		Eventually(got.payloads, 5*time.Second).Should(Receive(Equal("hello")))
	})

	It("returns from Listen only once the subscription is live", func() {
		// The whole reason Listen blocks: a caller that publishes as soon as it
		// returns must not race the LISTEN reaching the database, because
		// nothing replays what is published before it lands.
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)

		got := listenOn(receiver, channel)
		Expect(sender.Publish(ctx, channel, "first")).To(Succeed())
		Eventually(got.payloads, 5*time.Second).Should(Receive(Equal("first")))
	})

	It("subscribes a channel registered before the listener starts waiting", func() {
		// Open-then-Listen is the racing sequence: the listener goroutine
		// subscribes what is registered, then arms the interrupt Listen uses to
		// break its wait. A registration landing between the two sends no
		// wakeup, and a wait nothing breaks never ends. Repeated because
		// whether the window is hit is a matter of scheduling.
		dsn := pgtest.ServerDSN()
		for range 20 {
			c, err := pglisten.Open(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())

			listenCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			err = c.Listen(listenCtx, pgtest.UniqueName("sp_listen"), newRecorder())
			cancel()
			Expect(err).NotTo(HaveOccurred(), "Listen must not park waiting for a wakeup nobody sends")
			Expect(c.Close()).To(Succeed())
		}
	})

	It("routes each channel to its own listener", func() {
		// One connection carries every channel the process listens to, which is
		// the point of sharing it — so the dispatch has to be the thing keeping
		// two packages' traffic apart.
		dsn := pgtest.ServerDSN()
		first := pgtest.UniqueName("sp_listen")
		second := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)

		toFirst := listenOn(receiver, first)
		toSecond := listenOn(receiver, second)

		Expect(sender.Publish(ctx, first, "for-first")).To(Succeed())
		Eventually(toFirst.payloads, 5*time.Second).Should(Receive(Equal("for-first")))
		Consistently(toSecond.payloads, "300ms").ShouldNot(Receive(),
			"a payload must not reach the listener of another channel")
	})

	It("picks up a channel registered after traffic is already flowing", func() {
		// Registering cycles the listener connection, so the channels already
		// subscribed have to come back with it.
		dsn := pgtest.ServerDSN()
		first := pgtest.UniqueName("sp_listen")
		second := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)

		toFirst := listenOn(receiver, first)
		Expect(sender.Publish(ctx, first, "before")).To(Succeed())
		Eventually(toFirst.payloads, 5*time.Second).Should(Receive())

		toSecond := listenOn(receiver, second)
		Expect(sender.Publish(ctx, second, "after")).To(Succeed())
		Eventually(toSecond.payloads, 5*time.Second).Should(Receive(Equal("after")))

		// And the first subscription survived the cycle.
		Eventually(func() bool {
			Expect(sender.Publish(ctx, first, "still-here")).To(Succeed())
			select {
			case <-toFirst.payloads:
				return true
			case <-time.After(200 * time.Millisecond):
				return false
			}
		}, 10*time.Second).Should(BeTrue(), "an existing subscription must survive a registration")
	})

	It("drops a payload on a channel nobody registered", func() {
		// Another process on the same database may listen to channels this one
		// does not, and its traffic must not reach this one's dispatch.
		dsn := pgtest.ServerDSN()
		mine := pgtest.UniqueName("sp_listen")
		theirs := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)
		got := listenOn(receiver, mine)

		Expect(sender.Publish(ctx, theirs, "not-for-me")).To(Succeed())
		Expect(sender.Publish(ctx, mine, "for-me")).To(Succeed())
		Eventually(got.payloads, 5*time.Second).Should(Receive(Equal("for-me")),
			"the unregistered channel's payload must not have been delivered first")
	})

	It("rejects a channel name LISTEN could not take unquoted", func() {
		c := conn(pgtest.ServerDSN())
		Expect(c.Listen(ctx, "bad name; DROP", newRecorder())).To(MatchError(ContainSubstring("must match")))
	})

	It("rejects a channel registered twice", func() {
		c := conn(pgtest.ServerDSN())
		channel := pgtest.UniqueName("sp_listen")
		Expect(c.Listen(ctx, channel, newRecorder())).To(Succeed())
		Expect(c.Listen(ctx, channel, newRecorder())).To(MatchError(ContainSubstring("already registered")))
	})

	It("refuses a payload over the NOTIFY limit", func() {
		// Refused rather than truncated or dropped: a caller sizes its own
		// payload against MaxPayload, and this is the backstop that keeps a
		// miscalculation from failing halfway through a drain.
		c := conn(pgtest.ServerDSN())
		channel := pgtest.UniqueName("sp_listen")
		Expect(c.Listen(ctx, channel, newRecorder())).To(Succeed())

		err := c.Publish(ctx, channel, strings.Repeat("x", pglisten.MaxPayload+1))
		Expect(err).To(MatchError(ContainSubstring("a NOTIFY can carry")))
	})

	It("fails to open against an unreachable database", func() {
		_, err := pglisten.Open(ctx, "postgres://nobody@127.0.0.1:1/none?sslmode=disable")
		Expect(err).To(HaveOccurred())
	})

	It("refuses to listen or publish once closed", func() {
		c, err := pglisten.Open(ctx, pgtest.ServerDSN())
		Expect(err).NotTo(HaveOccurred())
		channel := pgtest.UniqueName("sp_listen")
		Expect(c.Listen(ctx, channel, newRecorder())).To(Succeed())

		Expect(c.Close()).To(Succeed())
		Expect(c.Close()).To(Succeed(), "Close must be idempotent")

		Expect(c.Listen(ctx, pgtest.UniqueName("sp_listen"), newRecorder())).To(MatchError(pglisten.ErrClosed))
		Expect(c.Publish(ctx, channel, "gone")).To(MatchError(pglisten.ErrClosed))
	})

	It("announces the gap and resumes delivery after its listener connection is killed", func() {
		// Postgres buffers nothing across a dropped LISTEN, so a reconnect is
		// both a hole in the stream and the one moment this transport can
		// silently stop working. Terminating the backend is what a failover, a
		// restart or an idle-connection reaper does to it in production.
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)
		got := listenOn(receiver, channel)

		// Prove the pair works before breaking it, so a failure below is the
		// reconnect and not the setup.
		Expect(sender.Publish(ctx, channel, "before")).To(Succeed())
		Eventually(got.payloads, 5*time.Second).Should(Receive())

		killBackend(ctx, dsn, `query = $1`, "LISTEN "+channel)

		Eventually(got.reconnects, 10*time.Second).Should(Receive(),
			"a listener that reconnected must tell its listeners the stream had a gap")
		Eventually(func() bool {
			Expect(sender.Publish(ctx, channel, "after")).To(Succeed())
			select {
			case <-got.payloads:
				return true
			case <-time.After(200 * time.Millisecond):
				return false
			}
		}, 10*time.Second).Should(BeTrue(), "delivery must resume after the reconnect")
	})

	It("redials its publisher after that connection is killed", func() {
		// The publisher sits idle between bursts, which is exactly the
		// connection an idle reaper or a failover takes out. A pool absorbed
		// this; a dedicated connection has to retry for itself.
		dsn := pgtest.ServerDSN()
		channel := pgtest.UniqueName("sp_listen")
		sender, receiver := conn(dsn), conn(dsn)
		got := listenOn(receiver, channel)

		Expect(sender.Publish(ctx, channel, "before")).To(Succeed())
		Eventually(got.payloads, 5*time.Second).Should(Receive())

		// The publisher's last statement is its pg_notify, which is what makes
		// it identifiable — and pg_backend_pid excludes the killing session,
		// whose own statement contains the same text.
		killBackend(ctx, dsn, `query LIKE '%pg_notify%' AND pid <> pg_backend_pid()`)

		Expect(sender.Publish(ctx, channel, "after")).To(Succeed(),
			"a publish onto a dead connection must redial rather than fail")
		Eventually(got.payloads, 5*time.Second).Should(Receive(Equal("after")))
	})
})

// killBackend terminates every backend matching where, and fails the spec if it
// matched none — a kill that hit nothing would let the assertions that follow
// pass without ever testing the recovery they are named for.
func killBackend(ctx context.Context, dsn, where string, args ...any) {
	GinkgoHelper()
	admin, err := pgx.Connect(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())
	defer func() { Expect(admin.Close(ctx)).To(Succeed()) }()

	tag, err := admin.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE `+where, args...)
	Expect(err).NotTo(HaveOccurred())
	Expect(tag.RowsAffected()).To(BeNumerically(">", 0), "no backend was found to kill")
}
