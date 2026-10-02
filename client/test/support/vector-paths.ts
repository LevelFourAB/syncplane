// Path translation for the shared conformance vectors.
//
// testdata/queryeval and testdata/indexkey state paths as PROTO field names,
// because that is the form the two implementations have in common — it is what
// travels on the wire and what the Go compiler resolves against. This side
// addresses fields by localName, and the two differ for any name protobuf-es
// camel-cases (`blocked_by` / `blockedBy`).
//
// In the real read path prepareQuery has already produced localNames from the
// typed DSL; a vector harness feeding the shared file straight in translates
// them with the function an access rule's condition goes through.

export { condLocalNames as toLocalNames } from '../../src/query/wire.js';
