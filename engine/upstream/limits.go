package upstream

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
	"github.com/levelfourab/syncplane/engine/server"
)

// originLimits fills the limits the link reads with the server's defaults.
func originLimits(l server.Limits) server.Limits {
	if l.MaxValueBytes <= 0 {
		l.MaxValueBytes = 1 << 20
	}
	if l.MaxFieldMaskPaths <= 0 {
		l.MaxFieldMaskPaths = 64
	}
	if l.MaxMutationsPerPush <= 0 {
		l.MaxMutationsPerPush = 256
	}
	if l.MaxRequestBytes <= 0 {
		l.MaxRequestBytes = 16 << 20
	}
	return l
}

// checkOriginLimits refuses an entry whose forwarded form the origin would
// refuse for its size. The capture calls it before the local write commits:
// the origin checks only what it receives, and a write it refuses after the
// local commit is reverted under every reader that saw it.
func (l *Link) checkOriginLimits(b *bound, st stateTx, ws string, e *entry) error {
	lim := l.cfg.OriginLimits
	if e.custom != nil {
		if n := len(e.custom.GetArgs().GetValue()); n > lim.MaxValueBytes {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("args of %d bytes exceed the origin's limit of %d", n, lim.MaxValueBytes))
		}
		return nil
	}
	// The origin takes one CRUD entry as one batch mutation, and counts the
	// batch's writes against its per-push limit.
	if n := len(b.sendableOps(e.ops)); n > lim.MaxMutationsPerPush {
		return server.Reject(server.CodeInvalidArgument,
			fmt.Sprintf("a write of %d rows is over the origin's limit of %d rows per mutation", n, lim.MaxMutationsPerPush))
	}
	// The size is measured with an unresolved compare-and-swap at its widest
	// and, below, with the longest client id, so it does not depend on what
	// the push is built with.
	m, _, err := mutationOf(b, st, &entry{id: e.id, ops: withoutCASBase(e.ops)}, nil)
	if err != nil {
		return err
	}
	cruds := m.GetBatch().GetMutations()
	if c := m.GetCrud(); c != nil {
		cruds = []*syncplanev1.CrudMutation{c}
	}
	for _, c := range cruds {
		if n := len(c.GetValue()); n > lim.MaxValueBytes {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("%s %q: value of %d bytes exceeds the origin's limit of %d", c.GetEntity(), c.GetKey(), n, lim.MaxValueBytes))
		}
		if n := len(c.GetFieldMask().GetPaths()); n > lim.MaxFieldMaskPaths {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("%s %q: %d field-mask paths exceed the origin's limit of %d", c.GetEntity(), c.GetKey(), n, lim.MaxFieldMaskPaths))
		}
	}
	// A push carries a large entry alone, so the entry must fit one request
	// by itself.
	m.MutationId = ^uint64(0)
	m.ClientId = strings.Repeat("x", naming.MaxClientIDBytes)
	req := &syncplanev1.PushRequest{Batches: []*syncplanev1.WorkspaceBatch{{WorkspaceId: ws, Mutations: []*syncplanev1.Mutation{m}}}}
	if n := proto.Size(req); n > lim.MaxRequestBytes {
		return server.Reject(server.CodeInvalidArgument,
			fmt.Sprintf("a push of %d bytes is over the origin's limit of %d", n, lim.MaxRequestBytes))
	}
	return nil
}

// withoutCASBase returns ops with each compare-and-swap on another entry
// replaced by one on the widest version, which sizes the same on the wire
// and needs no state to resolve.
func withoutCASBase(ops []entryOp) []entryOp {
	out := make([]entryOp, len(ops))
	for i, op := range ops {
		if op.cas.mode == casBase {
			op.cas = casRef{mode: casVersion, version: impossibleVersion}
		}
		out[i] = op
	}
	return out
}
