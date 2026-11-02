package presencebridge_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"

	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/presence/presencetest"
	"github.com/levelfourab/syncplane/engine/upstream/presencebridge"
)

func conformanceBridge(origin syncplanev1connect.PresenceServiceClient) presence.Bus {
	b := presencebridge.New(presence.NewLocal(), origin,
		presencebridge.WithAnnounce(announce),
		presencebridge.WithReconnectBackoff(10*time.Millisecond, 50*time.Millisecond),
		presencebridge.WithLogger(discard))
	DeferCleanup(b.Close)
	return b
}

var _ = presencetest.Conformance("presencebridge with the origin unreachable", func() presence.Bus {
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	return conformanceBridge(syncplanev1connect.NewPresenceServiceClient(http.DefaultClient, gone.URL))
})

var _ = presencetest.Conformance("presencebridge with a live origin", func() presence.Bus {
	_, ts := startServer("origin")
	return conformanceBridge(syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL,
		connect.WithInterceptors(asUser("bridge"))))
})
