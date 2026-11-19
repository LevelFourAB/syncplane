package sqlitestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/storage"
)

// walFrameBytes is the size of one WAL frame at the default 4 KiB page: the
// page and its 24-byte frame header.
const walFrameBytes = 4096 + 24

var _ = Describe("SQLite checkpoints", func() {
	ctx := context.Background()
	// value fills most of a page, so each write transaction adds a few
	// frames to the WAL.
	value := bytes.Repeat([]byte("x"), 3000)

	var path string
	BeforeEach(func() {
		path = filepath.Join(GinkgoT().TempDir(), "checkpoint.db")
	})

	walSize := func() int64 {
		fi, err := os.Stat(path + "-wal")
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return fi.Size()
	}

	open := func(set func(*config)) *Store {
		store, err := Open(path, set)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return store
	}

	put := func(store *Store, i int) error {
		return store.WithWriteTx(ctx, "ws", func(tx storage.WriteTx) error {
			return tx.PutRow("widget", fmt.Sprintf("k%d", i), value, uint64(i+1), storage.Actor{})
		})
	}

	// burst writes n rows, one write transaction each, from row first, and
	// returns the largest WAL file size it saw after a commit.
	burst := func(store *Store, first, n int) int64 {
		var peak int64
		for i := first; i < first+n; i++ {
			ExpectWithOffset(1, put(store, i)).To(Succeed())
			peak = max(peak, walSize())
		}
		return peak
	}

	// rewinds reports whether one more write leaves the WAL file at the size
	// it had. A write appends frames to the end of the WAL unless it begins
	// with every frame copied to the database file, in which case it writes
	// from the start of the file again.
	rewinds := func(store *Store, i int) bool {
		before := walSize()
		ExpectWithOffset(1, put(store, i)).To(Succeed())
		return walSize() == before
	}

	// rows writes 3000 rows, which is about 9000 frames (more than 35 MiB) in
	// a WAL that nothing checkpoints. The limits below are 64 and 256 frames.
	const rows = 3000

	It("rewinds the WAL during a burst through the checkpoint loop alone", func() {
		// wal_autocheckpoint(0) turns off the writer's own checkpoint, so
		// only checkpointLoop can copy the frames and make the WAL rewind.
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 0
			c.checkpointDelay = time.Millisecond
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		// This counts writes after a rewind and does not check a peak size.
		// How far the WAL grows between two rewinds depends on how much CPU
		// time the loop gets next to the writer, and a loaded CI runner
		// gives it too little to stay under a fixed size.
		rewound := 0
		for i := range rows {
			if rewinds(store, i) {
				rewound++
			}
		}
		Expect(rewound).To(BeNumerically(">", 0))
	})

	It("keeps the WAL bounded through the writer's own checkpoint when the loop falls behind", func() {
		// The first pass runs at once and the next one only after an hour,
		// so the burst gets no help from the loop.
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 256
			c.checkpointDelay = time.Hour
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(pragma(store.write, "wal_autocheckpoint")).To(Equal("256"))

		Expect(burst(store, 0, rows)).To(BeNumerically("<", 1024*walFrameBytes))
	})

	It("copies the WAL in the background so that the next write rewinds it", func() {
		// Neither the writer nor a pass on the writer connection checkpoints
		// here; only the background passes do.
		store := open(func(c *config) {
			c.rewindPages = 1 << 30
			c.autocheckpointPages = 0
			c.checkpointDelay = time.Millisecond
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		burst(store, 0, 200)
		i := 200
		Eventually(func() bool { i++; return rewinds(store, i) }, 5*time.Second, 10*time.Millisecond).
			Should(BeTrue())
	})

	It("keeps writing while a read transaction holds an old snapshot, and rewinds the WAL after it ends", func() {
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 256
			c.checkpointDelay = time.Millisecond
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(put(store, 0)).To(Succeed())

		reading := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				// The first read takes the snapshot.
				if _, err := tx.GetRow("widget", "k0"); err != nil {
					return err
				}
				close(reading)
				<-release
				return nil
			})
		}()
		Eventually(reading).Should(BeClosed())

		// No checkpoint can copy a frame that the snapshot does not hold, so
		// the WAL grows past its limits for as long as the read lasts. The
		// writes must not fail or wait for the read.
		start := time.Now()
		peak := burst(store, 1, 1000)
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
		Expect(peak).To(BeNumerically(">", 1024*walFrameBytes))

		close(release)
		Expect(<-done).To(Succeed())
		i := 1001
		Eventually(func() bool { i++; return rewinds(store, i) }, 5*time.Second, 10*time.Millisecond).
			Should(BeTrue())
	})

	It("truncates the WAL to zero bytes when writes stop", func() {
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 256
			c.checkpointDelay = time.Millisecond
			c.truncateDelay = 20 * time.Millisecond
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })

		Expect(burst(store, 0, 500)).To(BeNumerically(">", 0))
		Eventually(walSize, 5*time.Second, 10*time.Millisecond).Should(BeZero())

		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			row, err := tx.GetRow("widget", "k499")
			Expect(row.Value).To(Equal(value))
			return err
		})).To(Succeed())
		// The WAL is in use again after the truncate.
		Expect(put(store, 500)).To(Succeed())
		Expect(walSize()).To(BeNumerically(">", 0))
	})

	It("does not make writes fail or wait when a read transaction stops the truncate", func() {
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 256
			c.checkpointDelay = time.Millisecond
			c.truncateDelay = 5 * time.Millisecond
		})
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		burst(store, 0, 100)

		reading := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
				if _, err := tx.GetRow("widget", "k0"); err != nil {
					return err
				}
				close(reading)
				<-release
				return nil
			})
		}()
		Eventually(reading).Should(BeClosed())

		// Each pause is longer than truncateDelay, so a truncate attempt
		// runs between the writes. busy_timeout on the writer is five
		// seconds, so a write that waited for a truncate that waits for the
		// reader would take that long.
		for i := 100; i < 120; i++ {
			start := time.Now()
			Expect(put(store, i)).To(Succeed())
			Expect(time.Since(start)).To(BeNumerically("<", time.Second))
			time.Sleep(20 * time.Millisecond)
		}
		Expect(walSize()).To(BeNumerically(">", 0), "the reader still uses the WAL")

		close(release)
		Expect(<-done).To(Succeed())
		// The failed attempt does not repeat until more writes come.
		Expect(put(store, 120)).To(Succeed())
		Eventually(walSize, 5*time.Second, 10*time.Millisecond).Should(BeZero())
	})

	It("leaves no WAL behind on Close and keeps every write", func() {
		store := open(func(c *config) {
			c.rewindPages = 64
			c.autocheckpointPages = 256
		})
		burst(store, 0, 500)
		Expect(store.Close()).To(Succeed())
		Expect(walSize()).To(BeZero())

		store, err := Open(path)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		Expect(store.WithReadTx(ctx, "ws", func(tx storage.ReadTx) error {
			for _, i := range []int{0, 250, 499} {
				row, err := tx.GetRow("widget", fmt.Sprintf("k%d", i))
				if err != nil {
					return err
				}
				Expect(row.Value).To(Equal(value))
			}
			return nil
		})).To(Succeed())
	})
})
