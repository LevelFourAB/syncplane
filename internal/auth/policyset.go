package auth

import "sync"

// PolicySet hands out one Policy per app over a shared Store, for a host
// that serves many apps and learns of new ones at runtime. It is what a
// server.Manager's WithAppOptions closes over, and what the sign-in service
// tells about membership changes so the affected app's cache is dropped.
//
//	set := auth.NewPolicySet(store, auth.PolicyConfig{})
//	server.WithAppOptions(func(appID string) []server.Option {
//		return []server.Option{server.WithPermissions(set.For(appID))}
//	})
//
// PolicySet is safe for concurrent use.
type PolicySet struct {
	store *Store
	cfg   PolicyConfig

	mu       sync.Mutex
	policies map[string]*Policy
}

// NewPolicySet builds an empty set; policies are created on first For.
func NewPolicySet(store *Store, cfg PolicyConfig) *PolicySet {
	return &PolicySet{store: store, cfg: cfg, policies: map[string]*Policy{}}
}

// For returns the app's Policy, creating it on first use. Repeated calls with
// the same id return the same Policy.
func (ps *PolicySet) For(appID string) *Policy {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p, ok := ps.policies[appID]
	if !ok {
		p = NewPolicy(ps.store, appID, ps.cfg)
		ps.policies[appID] = p
	}
	return p
}

// Forget drops the cached membership answers of one app, so its next RPC
// asks the store. An app with no Policy yet has nothing to forget.
func (ps *PolicySet) Forget(appID string) {
	ps.mu.Lock()
	p := ps.policies[appID]
	ps.mu.Unlock()
	if p != nil {
		p.Forget()
	}
}
