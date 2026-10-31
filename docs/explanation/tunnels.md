# Tunnels: HTTP over a message channel

A browser client reaches the sync server over HTTP. Many hosts do not give a client that path. A webview in a desktop shell talks to its host process through a stream API. A page talks to a worker or an iframe through a `MessagePort`. A process talks to a sidecar through stdio. In each case, the client and the server can pass messages, but neither can open a socket to the other.

A tunnel connects the two halves over such a channel. This page explains what crosses it, why, and what it does not do. To set one up, see [Serve sync through a tunnel](../how-to/serve-sync-through-a-tunnel.md).

## The tunnel carries HTTP, not RPCs

On the server, the engine is an `http.Handler`. On the client, connect-web makes every call with `fetch`. The tunnel connects these two seams: the TypeScript half is a `fetch`, and the Go half calls an `http.Handler`. Between them, each HTTP exchange becomes a sequence of frames (`syncplane.tunnel.v1.Frame`).

Because the tunnel works below Connect, three things stay true:

- **The Connect protocol does not change.** Envelopes, error codes, timeouts and both encodings are the ones that connect-web and connect-go already speak. The tunnel does not know what a sync call is.
- **The handler is not restricted.** The engine's services, an app's own routes, or an `httputil.ReverseProxy` to a remote server all answer the tunnel the same way. This is why the remote mode needs no special code.
- **The HTTP transport stays as it is.** A client that uses `url` continues to use the network. The tunnel is a second way to carry the same calls.

The cost is that each call carries HTTP headers as frames. On a local channel, this is small compared with the work each call does.

## One channel per call, or one shared channel

The protocol tags each frame with a call id, so one channel can carry many calls. The client decides how many to put on a channel.

With **one channel per call**, a call maps one-to-one onto the channel. Closing the channel cancels the call, and a slow reader stalls only its own call. Use this when the host opens channels cheaply, such as a WebSocket or a Wails stream.

With **one shared channel**, every call goes over the same channel. Use this when the host gives only one channel, such as a `MessagePort`, or when opening a channel is expensive. The protocol has no flow control. A call whose reader stops reading stalls every call behind it. The client opens a new shared channel on the first call after the old one ends.

The server does not care which one the client chose. `tunnel.Serve` answers any number of concurrent calls on a channel.

## Failures

The tunnel puts every failure into a category that the sync client already handles:

| What happens                                    | What the client sees                                 |
| ----------------------------------------------- | ---------------------------------------------------- |
| The channel ends while a call is in flight      | `Unavailable`; the client goes `offline` and retries |
| The handler panics or aborts mid-response       | `Unavailable`, for that call only                    |
| The peer sends a frame that breaks the protocol | `Unavailable`, and the channel closes                |
| The caller's signal aborts                      | The abort reason; the handler's context ends         |

A handler that returns an HTTP or Connect error is not a tunnel failure. Its status and body cross the tunnel unchanged, so the client gets the same code it would get over HTTP.

## What the tunnel does not do

- **No authentication.** Anything that holds the client end of a channel can call the handler and set any header. In local mode, the host decides the identity before it calls the engine. In remote mode, the host is the client that the remote server authenticates, so the host holds the credential, and an allowlist keeps the admin service out of reach.
- **No browser network behaviour.** Cookies, CORS, redirects and the HTTP cache do not apply. A call gets what the handler wrote.
- **No compression on the channel.** The engine gzips streams only for a client that asks with `Accept-Encoding`, and the tunnel's client does not ask. In remote mode, the hop from the host to the server goes over Go's HTTP client, which asks for gzip and decompresses as it reads.

## Changing the protocol

The frame protocol is part of the published contract, as `syncplane.tunnel.v1`. A side ignores a frame kind that it does not know, so new kinds can be added without breaking older peers. In most deployments, both halves ship in one app, so they change together.
