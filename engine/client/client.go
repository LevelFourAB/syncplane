package client

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"connectrpc.com/connect"

	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/internal/naming"
)

// Client talks to one syncplane server. It holds no rows: every read goes to
// the server, and every write is pushed before the call returns.
//
// A Client is safe for concurrent use. Writes to one workspace are serialized:
// each waits until the previous one has its outcome. Writes to different
// workspaces run concurrently.
type Client struct {
	sync     syncplanev1connect.SyncServiceClient
	blob     syncplanev1connect.BlobServiceClient
	presence syncplanev1connect.PresenceServiceClient

	clientID    string
	pinned      bool
	maxAttempts int

	mu         sync.Mutex
	workspaces map[string]*wsState
}

type config struct {
	httpClient   connect.HTTPClient
	clientOpts   []connect.ClientOption
	interceptors []connect.Interceptor
	clientID     string
	maxAttempts  int
}

// Option configures New.
type Option func(*config)

// WithHTTPClient sets the HTTP client every call goes through. The default is
// http.DefaultClient. A client with a Timeout also ends Subscribe and presence
// streams when the timeout passes.
func WithHTTPClient(c connect.HTTPClient) Option {
	return func(cfg *config) { cfg.httpClient = c }
}

// WithInterceptors adds Connect interceptors to every call, in order, for
// example one that sets an Authorization header.
func WithInterceptors(interceptors ...connect.Interceptor) Option {
	return func(cfg *config) { cfg.interceptors = append(cfg.interceptors, interceptors...) }
}

// WithConnectOptions adds Connect client options to every call, for example
// connect.WithGRPC to speak gRPC instead of the Connect protocol, or
// connect.WithSendGzip.
func WithConnectOptions(opts ...connect.ClientOption) Option {
	return func(cfg *config) { cfg.clientOpts = append(cfg.clientOpts, opts...) }
}

// WithClientID sets the client id that mutations, pulls and streams carry. The
// default is a random id per Client.
//
// The server keeps one mutation watermark per workspace, caller and client
// id. With a set id, the Client reads that watermark from the server before
// its first write to a workspace and continues after it. Two live Clients
// must never share an id: their writes report ErrClientIDInUse or
// mutation-id gaps.
//
// The id must be 1 to 128 bytes with no control characters (\u0000 to
// \u001f); New returns an error otherwise.
func WithClientID(id string) Option {
	return func(cfg *config) { cfg.clientID = id }
}

// WithMaxAttempts sets how many times a write sends its push when it fails in
// transit or the server reports a transient failure. The default is 5. Values
// below 1 mean 1.
func WithMaxAttempts(n int) Option {
	return func(cfg *config) { cfg.maxAttempts = n }
}

// New returns a Client for the server at baseURL, for example
// "https://sync.example.com" or the URL of an httptest.Server. The services
// are expected at their standard paths below baseURL.
//
// New does not connect. It returns an error when baseURL is not an absolute
// URL or when the WithClientID value breaks the client-id rules.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("client: base URL: %w", err)
	}
	if !u.IsAbs() || u.Host == "" {
		return nil, fmt.Errorf("client: base URL %q is not absolute", baseURL)
	}
	cfg := config{httpClient: http.DefaultClient, maxAttempts: 5}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.maxAttempts = max(cfg.maxAttempts, 1)
	c := &Client{
		clientID:    cfg.clientID,
		pinned:      cfg.clientID != "",
		maxAttempts: cfg.maxAttempts,
		workspaces:  make(map[string]*wsState),
	}
	if c.pinned {
		if err := checkClientID(c.clientID); err != nil {
			return nil, err
		}
	} else {
		c.clientID = randomID()
	}
	copts := append([]connect.ClientOption{connect.WithInterceptors(cfg.interceptors...)}, cfg.clientOpts...)
	c.sync = syncplanev1connect.NewSyncServiceClient(cfg.httpClient, baseURL, copts...)
	c.blob = syncplanev1connect.NewBlobServiceClient(cfg.httpClient, baseURL, copts...)
	c.presence = syncplanev1connect.NewPresenceServiceClient(cfg.httpClient, baseURL, copts...)
	return c, nil
}

// ClientID returns the client id the Client sends.
func (c *Client) ClientID() string { return c.clientID }

// SyncService returns the generated SyncService client the Client calls,
// configured with its HTTP client and options. Mutations pushed through it
// bypass the Client's mutation-id counter, so do not push under the Client's
// client id.
func (c *Client) SyncService() syncplanev1connect.SyncServiceClient { return c.sync }

// BlobService returns the generated BlobService client the Client calls.
func (c *Client) BlobService() syncplanev1connect.BlobServiceClient { return c.blob }

// PresenceService returns the generated PresenceService client the Client
// calls.
func (c *Client) PresenceService() syncplanev1connect.PresenceServiceClient { return c.presence }

// Workspace returns a handle on one workspace. Handles are cheap; all handles
// on one id share the Client's per-workspace write state. The id is not
// checked until a call sends it.
func (c *Client) Workspace(id string) *Workspace {
	return &Workspace{c: c, id: id, st: c.state(id)}
}

func (c *Client) state(id string) *wsState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.workspaces[id]
	if !ok {
		st = &wsState{}
		c.workspaces[id] = st
	}
	return st
}

// Workspace addresses one workspace of a Client. It is safe for concurrent
// use.
type Workspace struct {
	c  *Client
	id string
	st *wsState
}

// ID returns the workspace id.
func (w *Workspace) ID() string { return w.id }

// Client returns the Client the handle belongs to.
func (w *Workspace) Client() *Client { return w.c }

func checkClientID(id string) error {
	if len(id) == 0 || len(id) > naming.MaxClientIDBytes {
		return fmt.Errorf("client: client id must be 1 to %d bytes", naming.MaxClientIDBytes)
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x20 {
			return errors.New("client: client id must not contain control characters")
		}
	}
	return nil
}

// randomID returns 128 random bits, URL-safe base64 without padding.
func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
