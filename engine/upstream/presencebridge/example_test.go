package presencebridge_test

import (
	"log"
	"net/http"

	"github.com/levelfourab/syncplane/engine/client"
	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
	"github.com/levelfourab/syncplane/engine/upstream/presencebridge"
)

// A local engine whose rooms in workspace "acme" are joined to the origin's.
// The origin client carries the credentials the bridge uses on the origin.
func Example() {
	origin, err := client.New("https://sync.example.com",
		client.WithInterceptors( /* an interceptor that sets the origin credentials */ ))
	if err != nil {
		log.Fatal(err)
	}
	bridge := presencebridge.New(presence.NewLocal(), origin.PresenceService(),
		presencebridge.WithWorkspaces("acme"))
	defer bridge.Close()

	store, err := sqlitestore.Open("local.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	local, err := server.New(store, server.WithPresenceBus(bridge))
	if err != nil {
		log.Fatal(err)
	}
	// Close the engine before the bridge, so no local room outlives it.
	defer local.Close()

	log.Fatal(http.ListenAndServe("127.0.0.1:8787", local.HTTPHandler()))
}
