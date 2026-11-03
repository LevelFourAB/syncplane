package upstream

import (
	"context"
	"time"

	"github.com/levelfourab/syncplane/engine/storage"
)

// SetScanPage sets how many rows one transaction of a long pass handles, and
// returns a func that restores the previous value. For tests that need many
// pages from few rows.
func SetScanPage(n int) (restore func()) {
	old := scanPage
	scanPage = n
	return func() { scanPage = old }
}

// ConfirmedRecord reads the link's record of the row the origin confirmed for
// one local row. found is false when the link keeps no record; value is the
// row value the record holds, nil when it holds none.
func ConfirmedRecord(ctx context.Context, store storage.Adapter, ws, entity, key string) (found bool, value []byte, err error) {
	err = store.WithReadTx(ctx, ws, func(tx storage.ReadTx) error {
		b, err := tx.ConsumerShadow(consumerName, entity, key)
		if err != nil || b == nil {
			return err
		}
		c, err := decodeConfirmed(b)
		if err != nil {
			return err
		}
		found, value = true, c.value
		return nil
	})
	return found, value, err
}

// SetBootProgressInterval sets the shortest time between two reports of
// bootstrap progress alone, and returns a func that restores the previous
// value.
func SetBootProgressInterval(d time.Duration) (restore func()) {
	old := bootProgressInterval
	bootProgressInterval = d
	return func() { bootProgressInterval = old }
}
