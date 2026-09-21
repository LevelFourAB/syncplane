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

import type { DescField, DescMessage } from '@bufbuild/protobuf';

import type { Cond, Path } from '../../src/query/ast.js';
import { localPathOf, resolveLeafRef, resolveRelLeaf } from '../../src/query/wire.js';

/**
 * Rewrites a vector condition's proto-name paths as localNames.
 *
 * An exists sub-condition is scoped to the ELEMENT, so it translates against
 * the element type rather than the row's — the one place where "which schema
 * does this path belong to" has more than one answer. Over a MAP the element is
 * the ENTRY: `key` and `value` are not field names and travel verbatim, and
 * anything below `value` translates against the map's value type.
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
			const { field } = resolveLeafRef(schema, path, false);
			if (field.fieldKind === 'map') {
				return { kind: 'exists', path, cond: entryLocalNames(field, cond.cond) };
			}
			if (field.fieldKind !== 'list' || field.listKind !== 'message') {
				throw new Error(`vector path ${cond.path.join('.')} is not a repeated message field or a map`);
			}
			return { kind: 'exists', path, cond: toLocalNames(field.message, cond.cond) };
		}
		case 'rel': {
			// The sub-condition is scoped to the TARGET row, so it translates
			// against the target schema — the same rule prepareQuery applies. A
			// rel path may name a case inside a repeated polymorphic reference
			// message, which localPathOf cannot descend into, so it translates
			// field by field here and resolves as a rel path.
			const path = relLocalPath(schema, cond.path);
			const { shape } = resolveRelLeaf(schema, path);
			return { kind: 'rel', path, cond: toLocalNames(shape.targetSchema, cond.cond) };
		}
		case 'match':
			// Entity-level: no path to translate.
			return cond;
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => toLocalNames(schema, c)) };
		case 'not':
			return { ...cond, cond: toLocalNames(schema, cond.cond) };
	}
}

/** A rel path's proto names as localNames, descending through message fields and message lists alike. */
function relLocalPath(schema: DescMessage, path: Path): Path {
	let desc: DescMessage | undefined = schema;
	return path.map((seg) => {
		const field = desc?.fields.find((f) => f.name === seg);
		if (!field) throw new Error(`vector path ${path.join('.')}: no field "${seg}"`);
		desc =
			field.fieldKind === 'message' || (field.fieldKind === 'list' && field.listKind === 'message')
				? field.message
				: undefined;
		return field.localName;
	});
}

/** toLocalNames inside a map ENTRY scope. */
function entryLocalNames(map: Extract<DescField, { fieldKind: 'map' }>, cond: Cond): Cond {
	const translate = (path: Path): Path => {
		const [ head, ...rest ] = path;
		if (head !== 'value' || rest.length === 0 || map.mapKind !== 'message') return path;
		return [ head, ...localPathOf(map.message, rest) ];
	};
	switch (cond.kind) {
		case 'cmp':
		case 'in':
		case 'str':
		case 'rep':
		case 'present':
			return { ...cond, path: translate(cond.path) };
		case 'match':
			return cond;
		case 'exists':
		case 'rel':
			// A nested any() inside an entry: its own path translates here, its
			// sub-condition against whatever that path resolves to. No vector needs
			// it yet — nor a rel, which prepare rejects inside an entry scope — so
			// it is left to the resolver to reject if one ever does.
			return { ...cond, path: translate(cond.path) };
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => entryLocalNames(map, c)) };
		case 'not':
			return { ...cond, cond: entryLocalNames(map, cond.cond) };
	}
}
