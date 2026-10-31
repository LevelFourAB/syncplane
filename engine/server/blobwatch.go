package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/levelfourab/syncplane/engine/blob"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// WatchBlobs streams the state of blobs; see
// syncplanev1connect.BlobServiceHandler for the wire contract. Every message
// reports what StatBlobs would report at that moment, by the same read rule.
//
// It checks again each time the workspace is notified, which a commit and a
// completed upload both do, and sends an empty message at the heartbeat
// interval (WithHeartbeat). At each heartbeat it runs the workspace gate
// again, and ends the stream with the gate's error when the caller is no
// longer admitted. It holds no transaction between checks. A ctx marked with
// Trusted skips the gate.
func (s *Server) WatchBlobs(
	ctx context.Context,
	req *connect.Request[syncplanev1.WatchBlobsRequest],
	stream *connect.ServerStream[syncplanev1.WatchBlobsResponse],
) error {
	if _, err := s.blobStore(); err != nil {
		return err
	}
	msg := req.Msg
	ws := msg.GetWorkspaceId()
	if err := naming.ValidateWorkspaceID(ws); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	digests, err := parseDigests(msg.GetDigests(), false)
	if err != nil {
		return err
	}
	// Before the subscription, so a refused caller leaves no trace in the
	// workspace (see enterWorkspace).
	if err := s.regateBlobWatch(ctx, ws); err != nil {
		return err
	}
	// Subscribed before the first check, so a completion that lands between
	// the two still wakes the loop below.
	wake, cancel := s.notifier.Subscribe(ws)
	defer cancel()

	first, err := s.blobStatuses(ctx, ws, digests)
	if err != nil {
		return err
	}
	if err := stream.Send(&syncplanev1.WatchBlobsResponse{Blobs: first}); err != nil {
		return err
	}
	// The last state sent for each digest not yet reported complete, in first
	// request order.
	var pending []blob.Digest
	last := map[blob.Digest]*syncplanev1.BlobStatus{}
	for _, st := range first {
		d := blob.Digest(st.GetDigest())
		if _, seen := last[d]; seen || st.GetState() == syncplanev1.BlobState_BLOB_STATE_COMPLETE {
			continue
		}
		last[d] = st
		pending = append(pending, d)
	}

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()
	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeat.C:
			if err := s.regateBlobWatch(ctx, ws); err != nil {
				return err
			}
			if err := stream.Send(&syncplanev1.WatchBlobsResponse{}); err != nil {
				return err
			}
		case <-wake:
			// The notifier coalesces a burst of pokes, and the wake channel
			// holds one token, so pokes that land during this check cost one
			// more check, not one each.
			now, err := s.blobStatuses(ctx, ws, pending)
			if err != nil {
				return err
			}
			var changed []*syncplanev1.BlobStatus
			still := pending[:0]
			for i, st := range now {
				d := pending[i]
				if !proto.Equal(st, last[d]) {
					changed = append(changed, st)
					last[d] = st
				}
				if st.GetState() != syncplanev1.BlobState_BLOB_STATE_COMPLETE {
					still = append(still, d)
				}
			}
			pending = still
			if len(changed) > 0 {
				if err := stream.Send(&syncplanev1.WatchBlobsResponse{Blobs: changed}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// regateBlobWatch runs the workspace gate for a WatchBlobs stream, and does
// nothing for a trusted call.
func (s *Server) regateBlobWatch(ctx context.Context, workspaceID string) error {
	if isTrusted(ctx) {
		return nil
	}
	_, err := s.enterWorkspace(ctx, workspaceID)
	return err
}
