package server

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
)

// MutationOrigin identifies the client mutation a CapturedWrite applied, so
// that a later rejection of it can reach that client (see
// ReplicaTx.RejectLate). A WriteCapture may store it and pass it back in a
// later transaction.
type MutationOrigin struct {
	// Client identifies the pushing client and its caller to this server. It
	// is opaque: compare it, store it and pass it back, but do not build one.
	Client string
	// MutationID is the id the client pushed the mutation under.
	MutationID uint64
	// Mutator is the name of the custom mutation, "" for a CRUD or batch
	// mutation.
	Mutator string
	// Targets are the rows the mutation named, in payload order: one for a
	// CRUD mutation, one for each member of a batch, none for a custom
	// mutation.
	Targets []storage.RejectionTarget
}

// originOf returns the origin of a client mutation pushed under the
// watermark key client.
func originOf(client string, m *syncplanev1.Mutation) *MutationOrigin {
	o := &MutationOrigin{Client: client, MutationID: m.GetMutationId()}
	switch p := m.GetPayload().(type) {
	case *syncplanev1.Mutation_Crud:
		o.Targets = []storage.RejectionTarget{{Entity: p.Crud.GetEntity(), Key: p.Crud.GetKey()}}
	case *syncplanev1.Mutation_Batch:
		for _, c := range p.Batch.GetMutations() {
			o.Targets = append(o.Targets, storage.RejectionTarget{Entity: c.GetEntity(), Key: c.GetKey()})
		}
	case *syncplanev1.Mutation_Custom:
		o.Mutator = p.Custom.GetName()
	}
	return o
}

// RejectLate records that the mutation origin names, which this server
// acknowledged as applied, was rejected after the acknowledgement. The client
// that pushed it receives the rejection as a LateRejection on the first Pull
// or Subscribe page whose position reaches the transaction's sequence, under
// the same caller only. The rows the mutation wrote are not changed: revert
// them with Put and Delete in the same transaction.
//
// The record takes the transaction's sequence, and allocates it when the
// transaction has not written yet, so the call wakes the client's stream like
// a row write does. A second record for the same mutation replaces the first.
// The record is kept and expired with the client's other rejection records:
// a client that pulls after they expired, or bootstraps, does not receive it.
// The size bound on those records does not drop it. A message longer than
// 1 KiB is stored and delivered cut to 1 KiB.
//
// It returns an error when origin has no Client or no MutationID, or when
// code is empty.
func (r *ReplicaTx) RejectLate(origin MutationOrigin, code, message string) error {
	if origin.Client == "" || origin.MutationID == 0 || origin.MutationID > math.MaxInt64 {
		return fmt.Errorf("server: RejectLate: origin needs a client and a mutation id from 1 to 9223372036854775807")
	}
	if code == "" {
		return fmt.Errorf("server: RejectLate: code is required")
	}
	return r.atomically(func() error {
		seq, err := r.writeSeq()
		if err != nil {
			return err
		}
		recs, err := r.tx.ClientRejections(origin.Client)
		if err != nil {
			return err
		}
		rec := storage.ClientRejection{
			MutationID: origin.MutationID,
			Code:       code,
			Message:    clipMessage(message, maxLateRejectionMessageBytes),
			Seq:        seq,
			Targets:    slices.Clone(origin.Targets),
			Mutator:    origin.Mutator,
		}
		// The records stay ordered by mutation id, which a late rejection of
		// an older mutation would break if appended.
		i, found := slices.BinarySearchFunc(recs, origin.MutationID, func(c storage.ClientRejection, id uint64) int {
			return cmp.Compare(c.MutationID, id)
		})
		if found {
			recs[i] = rec
		} else {
			recs = slices.Insert(recs, i, rec)
		}
		return r.tx.SetClientRejections(origin.Client, trimRejections(recs, 0))
	})
}

// maxLateRejectionMessageBytes bounds the message of one late rejection. Late
// rejections are outside maxRejectionRecordBytes, so this is what bounds the
// record a mass refusal leaves for one client.
const maxLateRejectionMessageBytes = 1 << 10

// clipMessage cuts s to at most n bytes, at a rune boundary.
func clipMessage(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// deliveredThrough is the sequence through which a client holding cursor c
// has received every row change: a sub-position means rows at c.seq are still
// owed.
func deliveredThrough(c cursorState) uint64 {
	if c.partialEntity != "" && c.seq > 0 {
		return c.seq - 1
	}
	return c.seq
}

// attachLateRejections adds to page the late rejections of the client ackKey
// names that page moved the client past: those recorded at a sequence above
// what cur had delivered and at or below what next has. A page that leaves
// the delivered position where it was (a resync step, a reset) carries none,
// and a later page delivers them.
func attachLateRejections(tx storage.ReadTx, ackKey string, cur, next cursorState, page *syncplanev1.PullResponse) error {
	if ackKey == "" || page.GetReset_() {
		return nil
	}
	lo, hi := deliveredThrough(cur), deliveredThrough(next)
	if hi <= lo {
		return nil
	}
	recs, err := tx.ClientRejections(ackKey)
	if err != nil {
		return err
	}
	for _, r := range recs {
		if r.Seq <= lo || r.Seq > hi {
			continue
		}
		late := &syncplanev1.LateRejection{
			MutationId:   r.MutationID,
			ErrorCode:    r.Code,
			ErrorMessage: r.Message,
			Mutator:      r.Mutator,
		}
		for _, t := range r.Targets {
			late.Targets = append(late.Targets, &syncplanev1.MutationTarget{Entity: t.Entity, Key: t.Key})
		}
		page.LateRejections = append(page.LateRejections, late)
	}
	return nil
}
