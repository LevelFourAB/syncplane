package connect_test

import (
	"context"
	"log/slog"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	"github.com/levelfourab/syncplane/engine/upstream"
)

// A replica hands the rows of each write the origin refused to its
// record-mode connector.
func ExampleConnector_Refused() {
	var conn *engineconnect.Connector // the record-mode connector of the replica

	cfg := upstream.Config{
		OnRejected: func(r upstream.Rejection) {
			rows := make([]engineconnect.RowRef, len(r.Rows))
			for i, row := range r.Rows {
				rows[i] = engineconnect.RowRef(row)
			}
			if err := conn.Refused(context.Background(), r.Workspace, rows); err != nil {
				slog.Error("mark refused records", "workspace", r.Workspace, "err", err)
			}
		},
	}
	_ = cfg
}
