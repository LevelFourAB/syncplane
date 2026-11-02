// Package rebase computes the rows a replica holds from the rows its origin
// confirmed and the local writes the origin has not confirmed yet.
//
// Everything here is pure: no I/O, no clock, no randomness. The same inputs
// give the same bytes, because every value this package encodes is marshaled
// with deterministic protobuf encoding. Inputs it returns unchanged keep the
// encoding they came with, so compare rows with Model.Equal rather than with
// bytes.Equal.
//
// A Model describes one entity: its collaborative-text fields and the
// positions they take in a row (one document per element when the field sits
// under a keyed list or a map), its references, and its merge hooks. With it:
//
//   - FoldTextDeltas turns a RowChange that carries text delta state into the
//     full row it stands for.
//   - Apply and Replay replay local writes on top of a confirmed row, with the
//     semantics the origin applies them with.
//   - CaptureText and UpstreamValue turn a local write into the value to send
//     to the origin, with every collaborative-text document as a full
//     document.
//   - Equal compares two row states.
package rebase
