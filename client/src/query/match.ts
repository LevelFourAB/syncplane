// The entity search declaration behind full-text match: which fields a match
// condition reads, in importance order, and the analyzer options their text
// and every query against them are analyzed with. Mirror of the declaration
// half of engine/query/match.go; the evaluation half (the matcher and its
// rank) lives in evaluate.ts with the rest of the evaluator.

import { type DescField, type DescMessage, getOption, hasOption } from '@bufbuild/protobuf';

import {
	entity as entityExt,
	field_behavior as fieldBehaviorExt,
} from '../gen/syncplane/v1/options_pb.js';
import { isTextField } from '../mutations.js';
import type { AnalyzeOptions } from '../search/index.js';
import type { Path } from './ast.js';

/** One searchable field: the localName path the resolver reads, and the
 * dotted proto path it was declared as. */
export interface SearchFieldDecl {
	path: Path;
	proto: string;
}

/** An entity's compiled search declaration. */
export interface SearchDecl {
	/** Most important first: searchable-marked fields in field order, then the
	 * SearchSpec's declared fields in declaration order. */
	fields: SearchFieldDecl[];
	options: AnalyzeOptions;
	/**
	 * The declaration half of a matcher's cursor-shape fingerprint: analyzer
	 * options and proto field paths, stated identically to the Go side.
	 */
	spec: string;
}

const declCache = new WeakMap<DescMessage, SearchDecl | null>();

/**
 * The entity's search declaration, or undefined when it declares none.
 * Throws on a declaration that cannot search — an unresolvable path, a leaf
 * that is not a singular string or collaborative-text field, a duplicate
 * path, or a SearchSpec naming no fields with none marked either.
 */
export function searchDeclOf(schema: DescMessage): SearchDecl | undefined {
	const cached = declCache.get(schema);
	if (cached !== undefined) return cached ?? undefined;
	const decl = compileSearchDecl(schema);
	declCache.set(schema, decl ?? null);
	return decl;
}

function compileSearchDecl(schema: DescMessage): SearchDecl | undefined {
	const paths: string[] = [];
	for (const field of schema.fields) {
		if (hasOption(field, fieldBehaviorExt) && getOption(field, fieldBehaviorExt).searchable) {
			paths.push(field.name);
		}
	}
	const spec = hasOption(schema, entityExt) ? getOption(schema, entityExt).search : undefined;
	for (const f of spec?.field ?? []) paths.push(f.path);
	if (paths.length === 0) {
		if (spec) {
			throw new Error(
				`search on ${schema.typeName} names no fields: declare SearchSpec.field entries or mark fields searchable`,
			);
		}
		return undefined;
	}
	const fields: SearchFieldDecl[] = [];
	for (const proto of paths) {
		const path = resolveSearchPath(schema, proto);
		if (fields.some((f) => f.proto === proto || samePath(f.path, path))) {
			throw new Error(`search field "${proto}" is declared twice`);
		}
		fields.push({ path, proto });
	}
	const options: AnalyzeOptions = spec?.keepDiacritics ? { keepDiacritics: true } : {};
	return {
		fields,
		options,
		spec: `search:${spec?.keepDiacritics === true}${fields.map((f) => `|${f.proto}`).join('')}`,
	};
}

function samePath(a: Path, b: Path): boolean {
	return a.length === b.length && a.every((seg, i) => seg === b[i]);
}

/**
 * Resolves a dotted proto-name path to the localName path the evaluator
 * reads, mirroring the Go resolver: every segment but the last must be a
 * singular message field (or a map key addressing one entry), and the leaf
 * must read as a string — a string field, a collaborative-text field, or a
 * string map value.
 */
function resolveSearchPath(schema: DescMessage, proto: string): Path {
	const segs = proto.split('.');
	const path: Path = [];
	let desc: DescMessage | undefined = schema;
	let leaf: DescField | undefined;
	let mapValue: 'string' | 'message' | 'other' | undefined;
	for (let i = 0; i < segs.length; i++) {
		const seg = segs[i]!;
		if (mapValue !== undefined) {
			// The segment after a map field is the entry KEY, addressed verbatim.
			path.push(seg);
			leaf = undefined;
			if (mapValue === 'string') {
				mapValue = undefined;
				if (i === segs.length - 1) return path;
				throw new Error(`search field "${proto}": cannot descend into a string map value`);
			}
			throw new Error(`search field "${proto}": a map entry is not searchable here`);
		}
		if (!desc) {
			throw new Error(`search field "${proto}": "${segs[i - 1]}" cannot be descended into`);
		}
		const field: DescField | undefined = desc.fields.find((f) => f.name === seg);
		if (!field) throw new Error(`search field "${proto}": ${desc.typeName} has no field "${seg}"`);
		path.push(field.localName);
		leaf = field;
		if (field.fieldKind === 'map') {
			if (field.mapKey !== 9 /* ScalarType.STRING */) {
				throw new Error(`search field "${proto}": only string-keyed map entries are addressable`);
			}
			mapValue = field.mapKind === 'scalar' && field.scalar === 9
				? 'string'
				: field.mapKind === 'message'
				? 'message'
				: 'other';
			desc = undefined;
			continue;
		}
		desc = field.fieldKind === 'message' ? field.message : undefined;
	}
	if (mapValue !== undefined || !leaf) {
		throw new Error(`search field "${proto}": path stops at a map field; address one entry`);
	}
	if (leaf.fieldKind === 'scalar' && leaf.scalar === 9) return path;
	if (isTextField(leaf)) return path;
	throw new Error(
		`search field "${proto}": ${describe(leaf)} is not searchable; `
			+ `search reads singular string or collaborative-text fields`,
	);
}

function describe(field: DescField): string {
	switch (field.fieldKind) {
		case 'message':
			return `${field.message.typeName} field ${field.name}`;
		case 'list':
			return `repeated ${field.listKind} field ${field.name}`;
		default:
			return `${field.fieldKind} field ${field.name}`;
	}
}
