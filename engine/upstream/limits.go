package upstream

import (
	"fmt"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
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
	return l
}

// checkOriginLimits refuses an entry whose forwarded form the origin would
// refuse for its size. The capture calls it before the local write commits:
// the origin checks only what it receives, and a write it refuses after the
// local commit is reverted under every reader that saw it.
func (l *Link) checkOriginLimits(b *bound, e *entry) error {
	lim := l.cfg.OriginLimits
	if e.custom != nil {
		if n := len(e.custom.GetArgs().GetValue()); n > lim.MaxValueBytes {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("args of %d bytes exceed the origin's limit of %d", n, lim.MaxValueBytes))
		}
		return nil
	}
	for _, op := range e.ops {
		if op.kind == syncplanev1.CrudOp_CRUD_OP_DELETE {
			continue
		}
		if n := len(forwardedValue(b, op)); n > lim.MaxValueBytes {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("%s %q: value of %d bytes exceeds the origin's limit of %d", op.entity, op.key, n, lim.MaxValueBytes))
		}
		if n := len(op.mask); n > lim.MaxFieldMaskPaths {
			return server.Reject(server.CodeInvalidArgument,
				fmt.Sprintf("%s %q: %d field-mask paths exceed the origin's limit of %d", op.entity, op.key, n, lim.MaxFieldMaskPaths))
		}
	}
	return nil
}
