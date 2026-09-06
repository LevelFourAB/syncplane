package server

import "github.com/levelfourab/syncplane/engine/internal/naming"

// ValidateWorkspaceID reports whether id is a workspace id the sync RPCs will
// accept: it must start with an ASCII letter or digit and continue with
// letters, digits, `_`, `-` or `:`, at most 128 bytes in total. Comparison is
// bytewise, so `Acme` and `acme` are two workspaces.
//
// The RPC handlers run this check themselves. Code that creates or records a
// workspace ahead of any RPC — a host that manages workspaces on the client's
// behalf — calls it so an id that can never be synced is refused at creation.
// The in-process API (Mutate, Invoke, View) does not check ids: those come
// from application code, at the same trust level as the schema.
func ValidateWorkspaceID(id string) error {
	return naming.ValidateWorkspaceID(id)
}
