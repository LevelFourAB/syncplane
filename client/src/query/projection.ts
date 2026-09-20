// Decoding a candidate row as only the fields the query can look at.
//
// A local read that cannot stop at its page — a facet fold, a predicate no
// index brackets, a grouped read's bucketing — decodes every candidate row,
// and most of what it decodes is never read: a 400-byte string field and a
// nested message list cost string decoding and object construction for a query
// whose whole question is two scalars. The projection is a second descriptor
// for the same message, carrying only the fields the compiled query can
// observe; protobuf-es skips everything else with a length jump and no
// allocation.
//
// The rule the correctness of that rests on: a path the projection does not
// carry resolves as NOT FOUND on a projected row, which is a different answer
// from the one the whole row gives. So every path anything may resolve has to
// be in here, and a query holding a construct whose paths cannot be enumerated
// — a full-text match, an unrewritten reference hop — gets no projection at
// all. Enumeration is over the FIRST segment of each path and the field it
// names is kept whole, which is what makes a nested or repeated path safe
// without walking into it.
//
// Rows the caller hands back are decoded again, in full, from the bytes the
// candidate was decoded from (see decodeWholeRow in read.ts) — so the
// projection is never visible past the read that used it.

import {
	clone,
	createFileRegistry,
	type DescMessage,
	isFieldSet,
} from '@bufbuild/protobuf';
import {
	type DescriptorProto,
	type FieldDescriptorProto,
	FieldDescriptorProtoSchema,
	type FileDescriptorProto,
	FileDescriptorProtoSchema,
} from '@bufbuild/protobuf/wkt';

import type { CompiledQuery, Cond, OrderKey, Path } from './ast.js';
import type { CompiledGroupQuery, CompiledGroupRowsQuery } from './group.js';

/**
 * The descriptor a query's candidate rows may be decoded through: the entity's
 * own, with every field no part of the query reads removed.
 *
 * Undefined means "decode whole rows". That is the answer whenever the
 * projection cannot be shown to be safe or cannot pay:
 *
 * - the query reads something whose paths are not in the AST — a `match`
 *   condition or a relevance ordering (both read the entity's declared search
 *   fields), or a `rel` condition that was not rewritten to a row-local test;
 * - a path starts at a segment that names no field of `schema`;
 * - the query has no `limit`, so every matching row is returned and would be
 *   decoded in full anyway;
 * - the projection would keep every field.
 *
 * `extra` carries predicates the query itself does not hold — the per-workspace
 * ones of a read spanning several. Every row value the read resolves must come
 * from `compiled` or from `extra`; a caller that cannot enumerate its
 * predicates must not call this.
 *
 * The result is cached per (schema, field set), so repeated reads of one query
 * shape build one descriptor and the same call twice returns the same object.
 */
export function projectionFor(
	schema: DescMessage,
	compiled: CompiledQuery,
	extra?: Iterable<Cond | undefined>,
): DescMessage | undefined {
	if (compiled.limit === undefined) return undefined;
	return assemble(schema, (keep) => collectQuery(schema, compiled, keep), extra);
}

/**
 * The descriptor a GROUPED read's candidate rows may be decoded through: the
 * grouping path, the predicate's paths and, for a grouped ROW read, the
 * ordering's.
 *
 * Undefined means "decode whole rows", for {@link projectionFor}'s reasons
 * except the one about `limit`: a grouped read's limit counts GROUPS, and every
 * row behind them is read whatever it is. A grouped COUNT hands back no row at
 * all, so nothing it decodes is ever decoded twice; a grouped row read decodes
 * each group's page again, in full, from the bytes it came from.
 *
 * `extra` and the caching are {@link projectionFor}'s.
 */
export function projectionForGroup(
	schema: DescMessage,
	compiled: CompiledGroupQuery | CompiledGroupRowsQuery,
	extra?: Iterable<Cond | undefined>,
): DescMessage | undefined {
	return assemble(schema, (keep) => collectGroup(schema, compiled, keep), extra);
}

/** The shared tail of both entry points: the extra predicates, the oneof rule,
 * the pays-for-itself check and the cache. */
function assemble(
	schema: DescMessage,
	collect: (keep: Set<string>) => boolean,
	extra: Iterable<Cond | undefined> | undefined,
): DescMessage | undefined {
	const keep = new Set<string>();
	if (!collect(keep)) return undefined;
	for (const cond of extra ?? []) {
		if (cond !== undefined && !collectCond(schema, cond, keep)) return undefined;
	}
	// A oneof reaches a decoded message as one property holding the active
	// case, so a projection that kept some members and dropped others would
	// report the group unset for a row whose case it dropped. Keeping the group
	// whole removes the question.
	for (const field of schema.fields) {
		if (field.oneof !== undefined && keep.has(field.name)) {
			for (const sibling of field.oneof.fields) keep.add(sibling.name);
		}
	}
	if (keep.size >= schema.fields.length) return undefined;
	return cached(schema, keep);
}

/** Every field of `q` a row value can be resolved through; false when one of
 * them cannot be named. */
function collectQuery(schema: DescMessage, q: CompiledQuery, keep: Set<string>): boolean {
	if (q.where !== undefined && !collectCond(schema, q.where, keep)) return false;
	if (!collectOrder(schema, q.orderBy, keep)) return false;
	for (const facet of q.facets ?? []) {
		if (!collectPath(schema, facet.by, keep)) return false;
	}
	return true;
}

/** collectQuery for a grouped read: the grouping path in place of the facets,
 * and an ordering only a grouped ROW read carries. */
function collectGroup(
	schema: DescMessage,
	q: CompiledGroupQuery | CompiledGroupRowsQuery,
	keep: Set<string>,
): boolean {
	if (!collectPath(schema, q.by, keep)) return false;
	if (q.where !== undefined && !collectCond(schema, q.where, keep)) return false;
	return collectOrder(schema, 'orderBy' in q ? q.orderBy : undefined, keep);
}

function collectOrder(
	schema: DescMessage,
	orderBy: OrderKey[] | undefined,
	keep: Set<string>,
): boolean {
	for (const key of orderBy ?? []) {
		// A relevance rank is computed from the entity's search fields, which the
		// order key does not name.
		if (key.relevance) return false;
		if (!collectPath(schema, key.path, keep)) return false;
	}
	return true;
}

function collectCond(schema: DescMessage, cond: Cond, keep: Set<string>): boolean {
	switch (cond.kind) {
		case 'and':
		case 'or':
			return cond.conds.every((c) => collectCond(schema, c, keep));
		case 'not':
			return collectCond(schema, cond.cond, keep);
		case 'match':
		case 'rel':
			return false;
		// `exists` needs the head field alone: its sub-condition is scoped to an
		// ELEMENT of that field, and the field is kept whole.
		default:
			return collectPath(schema, cond.path, keep);
	}
}

/** Adds the field a path starts at, or reports that the path names none. */
function collectPath(schema: DescMessage, path: Path, keep: Set<string>): boolean {
	const head = path[0];
	if (head === undefined) return false;
	const field = schema.fields.find((f) => f.localName === head);
	if (field === undefined) return false;
	keep.add(field.name);
	return true;
}

/** Projections built so far, per entity descriptor and field set. `null` is a
 * build that failed, remembered so it is not retried on every read. */
const built = new WeakMap<DescMessage, Map<string, DescMessage | null>>();

function cached(schema: DescMessage, keep: ReadonlySet<string>): DescMessage | undefined {
	let perSchema = built.get(schema);
	if (perSchema === undefined) built.set(schema, perSchema = new Map());
	const cacheKey = [ ...keep ].sort().join(',');
	const held = perSchema.get(cacheKey);
	if (held !== undefined) return held ?? undefined;
	const made = build(schema, keep);
	perSchema.set(cacheKey, made ?? null);
	return made;
}

/**
 * Rebuilds the message's file with the unkept fields removed and resolves the
 * message out of it again.
 *
 * The file is rebuilt rather than the message alone because a descriptor is
 * only reachable through the registry its file belongs to. Imports resolve
 * against the original file's own dependencies, so nothing but this one message
 * changes shape.
 */
function build(schema: DescMessage, keep: ReadonlySet<string>): DescMessage | undefined {
	try {
		const file = clone(FileDescriptorProtoSchema, schema.file.proto);
		const target = messageIn(file, schema);
		if (target === undefined) return undefined;
		target.field = target.field.filter((f) => keep.has(f.name));
		pruneOneofs(target);
		const registry = createFileRegistry(file, (name) => {
			for (const dep of schema.file.dependencies) {
				if (dep.proto.name === name) return dep;
			}
			return undefined;
		});
		return registry.getMessage(schema.typeName);
	} catch {
		// A descriptor this build cannot produce is a decline, not a failure: the
		// read answers from whole rows instead.
		return undefined;
	}
}

/** The copy of `schema`'s declaration inside a fresh copy of its file. */
function messageIn(file: FileDescriptorProto, schema: DescMessage): DescriptorProto | undefined {
	const names: string[] = [];
	for (let at: DescMessage | undefined = schema; at !== undefined; at = at.parent) {
		names.unshift(at.name);
	}
	let found = file.messageType.find((m) => m.name === names[0]);
	for (let i = 1; i < names.length && found !== undefined; i++) {
		found = found.nestedType.find((m) => m.name === names[i]);
	}
	return found;
}

/**
 * Drops the oneof declarations no kept field belongs to and renumbers the rest.
 *
 * A declaration left behind with no members is not a valid message — which the
 * proto3 `optional` fields make routine, each one carrying a synthetic oneof of
 * its own.
 */
function pruneOneofs(target: DescriptorProto): void {
	const moved = new Map<number, number>();
	const decls = [];
	for (const field of target.field) {
		const at = oneofIndexOf(field);
		if (at === undefined || moved.has(at)) continue;
		moved.set(at, decls.length);
		decls.push(target.oneofDecl[at]!);
	}
	for (const field of target.field) {
		const at = oneofIndexOf(field);
		if (at !== undefined) field.oneofIndex = moved.get(at)!;
	}
	target.oneofDecl = decls;
}

/**
 * The oneof a field declares membership of, or undefined for one that declares
 * none. Zero is a legitimate index, so the answer turns on presence rather than
 * on the value.
 */
function oneofIndexOf(field: FieldDescriptorProto): number | undefined {
	return isFieldSet(field, FieldDescriptorProtoSchema.field.oneofIndex)
		? field.oneofIndex
		: undefined;
}
