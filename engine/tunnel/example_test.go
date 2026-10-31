package tunnel_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/levelfourab/syncplane/engine/tunnel"
)

// Example serves a handler over a byte stream and calls it through a Client.
// In a real app the two ends are two processes, joined by stdio or a socket;
// here net.Pipe stands in for that.
func Example() {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello, %s", r.URL.Query().Get("name"))
	})

	serverEnd, clientEnd := net.Pipe()
	go func() {
		_ = tunnel.Serve(context.Background(), tunnel.NewStreamConn(serverEnd), mux)
	}()

	c := tunnel.NewClient(tunnel.NewStreamConn(clientEnd))
	defer c.Close()
	// The host in the URL is ignored; only the path and query reach mux.
	resp, err := (&http.Client{Transport: c}).Get("http://tunnel/hello?name=world")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(body))
	// Output:
	// hello, world
}

// ExampleServe_forward answers a tunnel by forwarding to a remote sync server.
// The allowlist keeps everything but the client-facing services out of reach,
// and the proxy, not the tunnel's client, holds the credential.
func ExampleServe_forward() {
	remote, err := url.Parse("https://sync.example.com")
	if err != nil {
		log.Fatal(err)
	}
	token := os.Getenv("SYNC_TOKEN")
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(remote)
			// Set, not Add: an Authorization header from the tunnel's client
			// never reaches the remote.
			r.Out.Header.Set("Authorization", "Bearer "+token)
		},
		// Send each stream message on as it arrives.
		FlushInterval: -1,
	}
	allow := http.NewServeMux()
	allow.Handle("/syncplane.v1.SyncService/", proxy)
	allow.Handle("/syncplane.v1.PresenceService/", proxy)

	// One end of a stream from the host; stdio stands in for it here.
	conn := tunnel.NewStreamConn(struct {
		io.Reader
		io.Writer
		io.Closer
	}{os.Stdin, os.Stdout, os.Stdin})
	if err := tunnel.Serve(context.Background(), conn, allow); err != nil {
		log.Fatal(err)
	}
}
