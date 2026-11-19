package pgstore

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// rows holds the workspace number, wsn, in place of the workspace id: the
// number index_scopes gives the workspace, which index_rows, index_entries,
// field_deltas and consumer_shadows hold too. A workspace gets its number with
// its first row or entry, under the workspace lock, and keeps it until
// DeleteWorkspace deletes the number and every row under it in one
// transaction. A later write gives the workspace a new number, and no number
// is given twice. So in any snapshot, a row under a number is a row of the
// workspace that index_scopes gives that number to, and a workspace without a
// number holds no row.
//
// A statement on rows names the number in one of three ways (see rowsNumber):
//
//   - Bound as a parameter, from the transaction's memo of index_scopes (see
//     loadIndexScopes), which is exact for the reason the memo is.
//   - Bound as a parameter, from the store's cache of workspace numbers, by the
//     first read of rows in a transaction that has not loaded the memo. The
//     reads of loadState travel in the same batch in front of it, so they see
//     the snapshot of the read, or in a write transaction the state that the
//     workspace lock holds still. When the memo they load does not give the
//     number of the cache, the read is thrown away and sent again with the
//     number of the memo (see readRows).
//   - As a scalar subquery of index_scopes: by that first read when the cache
//     does not know the workspace and the layout is unpartitioned, and by a
//     write of a row when the memo is not loaded.
//
// A bound number lets a partitioned layout prune to one partition when it
// plans the statement. The number of a subquery is known only when the
// statement runs, so the planner plans a read for every partition and locks
// each of them and each of their indexes: 175 relations with 32 partitions,
// and about 5,000 with 1024, where the lock table of a server with the default
// settings holds 6,400 locks for all sessions. So on a partitioned layout the
// first read of a workspace that the cache does not know loads the memo first,
// which costs a round trip. An
// INSERT routes its row to a partition when it runs, so a write of a row loses
// nothing by the subquery. The statements on rows that delete or update take
// the number from the memo.

// rowsNumber names the workspace number in a statement on rows: the statement
// compares wsn to expr, and binds arg as $1.
type rowsNumber struct {
	expr string
	arg  any
}

// numberOf is the expr of a rowsNumber that reads the number of workspace $1
// from index_scopes. It is NULL for a workspace without a number, which no
// wsn equals.
const numberOf = `(SELECT scope FROM index_scopes WHERE workspace_id = $1 AND entity = '' AND idx = '')`

// makeNumberSQL gives workspace $1 a number when it has none. nextval runs
// only when the number is missing. The caller holds the workspace lock, so no
// other transaction gives the workspace a number at the same time.
const makeNumberSQL = `INSERT INTO index_scopes (workspace_id, entity, idx, scope)
SELECT $1, '', '', nextval('posting_scope_ids')::int4
WHERE NOT EXISTS (SELECT 1 FROM index_scopes WHERE workspace_id = $1 AND entity = '' AND idx = '')`

// boundNumber is the rowsNumber that binds wsn.
func boundNumber(wsn int32) rowsNumber {
	return rowsNumber{expr: "$1", arg: wsn}
}

// errStaleNumber is the error of a read that was sent with a number from the
// store's cache that the workspace does not have in the read's snapshot.
// readRows sends the read again; no caller outside it sees the error.
var errStaleNumber = errors.New("pgstore: workspace number in cache is stale")

// readRows calls read with the rowsNumber that its statement on rows must use,
// and does not call it when the workspace has no number. The statement must be
// the first that read sends, and read must return its error, as errStaleNumber
// can come back from it. read can be called a second time, so it must not
// keep what a call that failed gave it.
func (t *pgTx) readRows(read func(rowsNumber) error) error {
	for {
		if t.indexScopes != nil {
			wsn, ok := t.indexScopes[indexKey{}]
			if !ok {
				return nil
			}
			return read(boundNumber(wsn))
		}
		n := rowsNumber{expr: numberOf, arg: t.ws}
		cached, hit := t.numbers.get(t.ws, "", "")
		switch {
		case hit:
			n = boundNumber(cached)
		case t.partitioned:
			if err := t.loadIndexScopes(); err != nil {
				return err
			}
			continue
		}
		t.rideState, t.checkNumber = true, cached
		err := read(n)
		t.rideState, t.checkNumber = false, 0
		if !errors.Is(err, errStaleNumber) {
			return err
		}
		// sendRead has loaded the memo, so the next pass binds its number.
	}
}

// sendRead sends stmt, a read, in one batch with the queue, and returns the
// batch positioned at the result of stmt. When readRows asked for it, the reads
// of loadState go in front of stmt, and sendRead fails with errStaleNumber when
// the memo they load does not give the number that stmt binds.
func (t *pgTx) sendRead(stmt queuedStmt) (pgx.BatchResults, error) {
	if !t.rideState {
		return t.sendWith(stmt)
	}
	t.rideState = false
	br, err := t.sendWith(append(t.stateReads(), stmt)...)
	if err != nil {
		return nil, err
	}
	if err := t.decodeState(br); err != nil {
		_ = br.Close()
		return nil, err
	}
	if t.checkNumber != 0 && t.indexScopes[indexKey{}] != t.checkNumber {
		_ = br.Close()
		return nil, errStaleNumber
	}
	return br, nil
}

// writeNumber returns the rowsNumber of a write of rows, and gives the
// workspace a number when it has none. Only a write transaction may call it.
func (t *pgTx) writeNumber() (rowsNumber, error) {
	if t.indexScopes == nil {
		// A read of the memo would cost a round trip, and the write needs no
		// bound number.
		if !t.numbered {
			if err := t.queue(makeNumberSQL, t.ws); err != nil {
				return rowsNumber{}, err
			}
			t.numbered = true
		}
		return rowsNumber{expr: numberOf, arg: t.ws}, nil
	}
	wsn, ok := t.indexScopes[indexKey{}]
	if !ok {
		if err := t.makeIndexScopes([]indexKey{{}}); err != nil {
			return rowsNumber{}, err
		}
		wsn = t.indexScopes[indexKey{}]
	}
	return boundNumber(wsn), nil
}
