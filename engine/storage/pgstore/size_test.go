package pgstore_test

import (
	"context"
	"fmt"
	"math/rand/v2"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"

	"github.com/levelfourab/syncplane/engine/internal/pgtest"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/pgstore"
)

var _ = Describe("Table size", func() {
	ctx := context.Background()

	// The bytes of the heap and every index of a table, per live entry after
	// VACUUM FULL, for a load shaped like a real app database: workspace ids
	// and row keys are 36-character uuids, one consumer of 18 characters
	// keeps a 12-byte shadow of each row, and each row has three delta
	// entries in two text fields, almost all of them gap markers. The shape
	// with int ids takes about 170 bytes per shadow and 185 per delta entry;
	// the shape keyed by text ids took about 330 per shadow and 400 per delta
	// entry before VACUUM FULL. The ceilings sit about 10% above.
	It("keeps the delta log and the shadows small per entry", func() {
		const (
			workspaces = 3
			rowsPerWS  = 1500
			deltas     = 3
		)
		dsn := pgtest.DSN()
		store, err := pgstore.Open(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		rng := rand.New(rand.NewPCG(1, 2))
		uuid := func() string {
			return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xffff, rng.Uint32()&0xffff, rng.Uint64()&0xffffffffffff)
		}
		snapshot := make([]byte, 12)
		for range workspaces {
			ws := uuid()
			keys := make([]string, rowsPerWS)
			for i := range keys {
				keys[i] = uuid()
			}
			seq := uint64(0)
			for start := 0; start < rowsPerWS; start += 250 {
				Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					for _, key := range keys[start : start+250] {
						if err := tx.PutConsumerShadow("calendar-connector", "todos", key, snapshot); err != nil {
							return err
						}
					}
					return nil
				})).To(Succeed())
			}
			// Delta entries arrive in sequence order, for rows in random order.
			for start := 0; start < rowsPerWS*deltas; start += 500 {
				Expect(store.WithWriteTx(ctx, ws, func(tx storage.WriteTx) error {
					for range 500 {
						seq++
						field, frag := "title", []byte(nil)
						if rng.IntN(10) < 3 {
							field = "notes"
						}
						if rng.IntN(100) < 3 {
							frag = make([]byte, 20)
						}
						if err := tx.PutFieldDelta("todos", keys[rng.IntN(rowsPerWS)], field, seq, frag); err != nil {
							return err
						}
					}
					return nil
				})).To(Succeed())
			}
		}

		conn, err := pgx.Connect(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(conn.Close(ctx)).To(Succeed()) })
		perEntry := func(table string) float64 {
			GinkgoHelper()
			// A gap marker deletes the older entries of its field, and the
			// dead tuples would count against the entries that are left.
			// VACUUM FULL rewrites the table, so the size is the layout only.
			_, err := conn.Exec(ctx, `VACUUM FULL `+table)
			Expect(err).NotTo(HaveOccurred())
			var size, n int64
			Expect(conn.QueryRow(ctx,
				`SELECT pg_total_relation_size(to_regclass($1)), (SELECT count(*) FROM `+table+`)`,
				table).Scan(&size, &n)).To(Succeed())
			Expect(n).To(BeNumerically(">", 0))
			return float64(size) / float64(n)
		}
		shadows, fieldDeltas := perEntry("consumer_shadows"), perEntry("field_deltas")
		GinkgoWriter.Printf("consumer_shadows %.0f bytes/entry, field_deltas %.0f bytes/entry\n", shadows, fieldDeltas)
		Expect(shadows).To(BeNumerically("<", 190))
		Expect(fieldDeltas).To(BeNumerically("<", 205))
	})
})
