// Package tunnel carries HTTP exchanges over a message channel, so an
// http.Handler can serve clients that have no socket to it: a webview behind a
// desktop shell, a process on the other end of stdio, a page across a
// WebSocket or a MessagePort.
//
// Serve answers the calls arriving on a Conn with an http.Handler. Client is
// the other end: an http.RoundTripper that sends requests through a Conn. The
// TypeScript client's syncplane/tunnel module speaks the same protocol, which
// is defined by syncplane.tunnel.v1.Frame.
//
// The handler can be anything. To serve the sync engine in process, pass a mux
// of server.Server.Handlers. To forward to a remote server, pass an
// httputil.ReverseProxy.
//
// A Conn is the only part a host supplies. Anything that sends and receives
// whole messages in order fits: NewStreamConn adapts a byte stream, and a
// WebSocket or a host's own stream API needs a wrapper of a few lines.
package tunnel
