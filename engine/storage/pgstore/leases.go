package pgstore

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/lease"
)

// Leases returns a lease.Locker backed by Postgres session advisory locks,
// making the adapter a lease.Provider: every node sharing the database leases
// out of one space, so a server constructed on this store dedups its
// background work across the deployment with no configuration.
//
// Every lease of a Store is held on one dedicated connection, dialled on the
// first TryAcquire. A node that crashes or loses
// that connection releases everything it held at once, with no TTL to wait
// out, and is never told its leases are gone — so leased work stays
// idempotent. TryAcquire returns the underlying error when the session cannot
// be dialled or the lock statement fails, and ctx's error, with no lease,
// when ctx is already done. A caller's cancellation never costs the Store
// the leases its other holders have.
//
// Two holders on one Store exclude each other, as two Stores do.
//
// Lock keys are 64-bit hashes of (schema, role, workspace). The schema keeps
// deployments that share one database server but live in separate schemas out
// of each other's lease space; a hash collision within one space makes two
// pairs exclude each other, which costs deferred work and never correctness.
func (s *Store) Leases() lease.Locker {
	return s.leases
}

// pgLeases owns the dedicated lock session of one Store. The connection is
// opened on first use and replaced on failure; locks acquired on a session that has since been replaced are gone
// server-side, which is why releases carry the generation they were acquired
// under.
type pgLeases struct {
	cfg *pgx.ConnConfig

	mu     sync.Mutex
	conn   *pgx.Conn
	gen    uint64
	schema string
	// held names the lock keys a holder on this Store has under the current
	// session. Advisory locks are re-entrant per session, so the server
	// grants a key the session already holds again; this set is what refuses
	// a second holder in the same process. It is emptied with the session,
	// whose locks the server has released.
	held map[int64]bool
}

func newPgLeases(cfg *pgx.ConnConfig) *pgLeases {
	return &pgLeases{cfg: cfg, held: map[int64]bool{}}
}

// releaseTimeout bounds the unlock statement of a release, which runs on a
// background context because the work's own context is often already
// cancelled by the time its lease is given back.
const releaseTimeout = 5 * time.Second

func (l *pgLeases) TryAcquire(ctx context.Context, role, ws string) (func(), bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureConnLocked(ctx); err != nil {
		return nil, false, err
	}
	key := leaseLockKey(l.schema, role, ws)
	if l.held[key] {
		return nil, false, nil
	}
	// Not ctx: pgx closes a connection whose query a cancellation
	// interrupts, and closing this one frees every lease the Store holds.
	// The lock call never waits, so the timeout only bounds a stalled
	// server.
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	var got bool
	if err := l.conn.QueryRow(qctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		// The session is not trustworthy anymore; close it so whatever it
		// held is released server-side and the next acquire reconnects.
		l.dropConnLocked()
		return nil, false, err
	}
	if !got {
		return nil, false, nil
	}
	l.held[key] = true
	gen := l.gen
	var once sync.Once
	return func() {
		once.Do(func() { l.release(key, gen) })
	}, true, nil
}

func (l *pgLeases) release(key int64, gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A lease acquired on a session that has since died was released with it;
	// unlocking the same key on the replacement would free a sibling's lease.
	if l.conn == nil || l.gen != gen {
		return
	}
	delete(l.held, key)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	var released bool
	if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); err != nil {
		// Unknown server state: close the session, which releases everything
		// it held, rather than leak this lock until process exit.
		l.dropConnLocked()
	}
}

// ensureConnLocked dials the dedicated session if there is none. Called with
// l.mu held.
func (l *pgLeases) ensureConnLocked(ctx context.Context) error {
	if l.conn != nil {
		return nil
	}
	conn, err := pgx.ConnectConfig(ctx, l.cfg)
	if err != nil {
		return err
	}
	var schema string
	if err := conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		_ = conn.Close(ctx)
		return err
	}
	l.conn = conn
	l.schema = schema
	l.gen++
	return nil
}

// dropConnLocked closes the session, releasing every advisory lock it held.
// Called with l.mu held.
func (l *pgLeases) dropConnLocked() {
	if l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	_ = l.conn.Close(ctx)
	l.conn = nil
	clear(l.held)
}

func (l *pgLeases) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropConnLocked()
}

// leaseLockKey hashes a lease identity into the advisory-lock keyspace. Same
// scheme as workspaceLockKey — FNV-64a, salted, NUL-separated — with the
// session's schema folded in, because advisory locks are server-global while
// everything else about a deployment is scoped by its search_path.
func leaseLockKey(schema, role, ws string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("syncplane:lease\x00"))
	_, _ = h.Write([]byte(schema))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(role))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(ws))
	return int64(h.Sum64())
}
