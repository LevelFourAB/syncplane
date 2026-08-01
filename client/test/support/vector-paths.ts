// Path translation for the shared conformance vectors.
//
// testdata/queryeval and testdata/indexkey state paths as PROTO field names,
// because that is the form the two implementations have in common — it is what
// travels on the wire and what the Go compiler resolves against. This side
// addresses fields by localName, and the two differ for any name protobuf-es
// camel-cases (`blocked_by` / `blockedBy`).
//
// In the real read path prepareQuery has already produced localNames from the
// typed DSL, so nothing needs this; a vector harness feeding the shared file
// straight in does.

import type { DescMessage } from '@bufbuild/protobuf';

import type { Cond } from '../../src/query/ast.js';
import { localPathOf, resolveLeafRef } from '../../src/query/wire.js';

/**
 * Rewrites a vector condition's proto-name paths as localNames.
 *
 * An exists sub-condition is scoped to the ELEMENT, so it translates against
 * the element type rather than the row's — the one place where "which schema
 * does this path belong to" has more than one answer.
 */
export function toLocalNames(schema: DescMessage, cond: Cond): Cond {
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'rep':
		case 'present':
			return { ...cond, path: localPathOf(schema, cond.path) };
		case 'exists': {
			const path = localPathOf(schema, cond.path);
			const { field } = resolveLeafRef(schema, path, false, true);
			if (field.fieldKind !== 'list' || field.listKind !== 'message') {
				throw new Error(`vector path ${cond.path.join('.')} is not a repeated message field`);
			}
			return { kind: 'exists', path, cond: toLocalNames(field.message, cond.cond) };
		}
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => toLocalNames(schema, c)) };
		case 'not':
			return { ...cond, cond: toLocalNames(schema, cond.cond) };
	}
}
