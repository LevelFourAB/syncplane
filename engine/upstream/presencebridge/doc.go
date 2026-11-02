// Package presencebridge joins the rooms of a local syncplane engine to the
// rooms of the origin server it syncs with, so that people connected to the
// origin and people connected to the local engine see each other's presence
// and broadcasts.
//
// A Bridge is a presence.Bus. Install it on the local engine with
// server.WithPresenceBus, wrapping the bus the engine would otherwise use:
//
//	origin, _ := client.New("https://origin.example", client.WithInterceptors(auth))
//	bridge := presencebridge.New(presence.NewLocal(), origin.PresenceService(),
//		presencebridge.WithWorkspaces("acme"))
//	defer bridge.Close()
//	srv, _ := server.New(store, server.WithPresenceBus(bridge))
//
// # What crosses the hop
//
// Upward, every presence update, departure and broadcast published on the
// local engine in a bridged workspace is published to the origin's
// PresenceService under the session id it carries locally. The origin stamps
// every one of them with the principal of the bridge's own connection, so
// participants from the local engine appear on the origin with the bridge's
// subject. A session id is claimed by the first local principal that
// publishes it; publishes naming that session from another local principal
// are not forwarded.
//
// Downward, the bridge watches the origin for every room in a bridged
// workspace that has a watcher on the local engine, and relays what it sees to
// the local engine only. Origin participants and broadcast senders appear
// locally with the subject the origin reports and with the issuer
// OriginIssuer. Sessions that the local engine published are left out, so a
// local participant never comes back as a second participant.
//
// The workspace sequence carried by a presence update is not translated
// between the two servers; it is cleared on both hops.
//
// # Liveness
//
// Presence stays soft state across the hop. Local records reach the origin
// again with every re-announcement of the local engine and lapse on the origin
// when those stop. Origin participants are re-announced locally for as long as
// the origin stream that reported them is up; when it is lost they lapse
// locally after the local expiry window, and the bridge reconnects with
// backoff and restores them from the origin's roster. Local presence keeps
// working while the origin is unreachable.
//
// Delivery is at most once on both hops, as on any presence.Bus. A broadcast
// sent while the origin is unreachable is lost.
//
// # Access on the origin
//
// The origin gates every bridged room on the bridge's principal, not on the
// local caller's. A room the origin refuses stays local and is retried with
// backoff, so a room anchored to a row the origin does not hold yet joins once
// the row arrives. The origin runs its room gate for every forwarded publish,
// re-announcements included, because the bridge holds no origin stream for the
// sessions it publishes as.
//
// # Limits
//
// A Bridge serves one engine built with server.New. A server.Manager prefixes
// the workspace ids it puts on its bus, and the bridge would forward them with
// the prefix. On a local deployment of several nodes over a shared inner bus,
// install a Bridge on every node: each relays the origin to its own node's
// rooms only.
package presencebridge
