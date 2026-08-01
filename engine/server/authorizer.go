package server

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
)

// RegisterInfo is the context an authorizer sees for a registration attempt:
// the target app and the request headers (where a bearer token rides).
type RegisterInfo struct {
	AppID  string
	Header http.Header
}

// RegisterAuthorizer gates runtime schema registration on the admin service.
// CanRegister returns nil to admit the call and a non-nil error to deny it
// (mapped to permission_denied on the wire). It is deny-by-default: a Manager
// with no authorizer configured uses DenyAll, so the endpoint is safe by
// construction — registration is a trusted, server-side operation, never
// something the public sync surface exposes.
type RegisterAuthorizer interface {
	CanRegister(ctx context.Context, info RegisterInfo) error
}

// DenyAll rejects every registration. It is the default so a misconfigured
// deployment fails closed.
type DenyAll struct{}

func (DenyAll) CanRegister(context.Context, RegisterInfo) error {
	return fmt.Errorf("registration denied: no RegisterAuthorizer configured")
}

// TokenAuthorizer admits a registration iff the request carries
// "Authorization: Bearer <token>" matching token, compared in constant time.
// It is the simplest trusted-caller story: share one secret with the Node
// backend that owns the schema. An empty token denies everything (fail closed).
func TokenAuthorizer(token string) RegisterAuthorizer {
	return tokenAuthorizer{token: token}
}

type tokenAuthorizer struct{ token string }

func (t tokenAuthorizer) CanRegister(_ context.Context, info RegisterInfo) error {
	if t.token == "" {
		return fmt.Errorf("registration denied: empty registration token")
	}
	const prefix = "Bearer "
	auth := info.Header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return fmt.Errorf("registration denied: missing bearer token")
	}
	got := strings.TrimPrefix(auth, prefix)
	if subtle.ConstantTimeCompare([]byte(got), []byte(t.token)) != 1 {
		return fmt.Errorf("registration denied: invalid bearer token")
	}
	return nil
}
