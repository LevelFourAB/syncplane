// The two-phase rewrite behind rel conditions (`matches`, the one-hop
// semi-join). A rel node is not row-local — its verdict depends on the target
// entity's rows — so before anything evaluates, the execute pipeline runs the
// inner condition as an ordinary query over the target entity (the pre-pass,
// per workspace), and this module collapses each node to a membership test on
// the stored ref stubs. The rewritten query IS row-local, so everything
// downstream — evaluator, planner, index walk, cursors — is untouched.
//
// The scan-level definition the rewrite must be indistinguishable from: per
// row, per stub, derive the target key, look the target up in the same
// workspace, evaluate the inner condition against it; true if any target
// matches. The shared vectors in testdata/queryeval pin the two against each
// other.

import { type DescField, type DescMessage, fromBinary, type Message } from '@bufbuild/protobuf';

import type { KVRead } from '../kv/store.js';
import {
	type EntityDef,
	type EntityRegistry,
	refKeyPath,
	refQualified,
	type RefShape,
	refStub,
} from '../mutations.js';
import type { CompiledIndex } from '../optimistic/index-maintain.js';
import { sortOrderedFields } from '../ordering.js';
import {
	readMergedRowsWithVersion,
	type Row,
	scanDecoded,
	type WorkspaceScope,
} from '../read.js';
import type { Cond, Path, Scalar } from './ast.js';
import { evalCond, resolvePath, scalarKey } from './evaluate.js';
import { queryByIndex } from './execute.js';
import { resolveRelLeaf } from './wire.js';

/**
 * An entity's compiled secondary indexes, as the client's index cache answers
 * it. Absent from a pre-pass ⇒ every pass reads its target entity.
 */
export type IndexSource = (entity: string) => CompiledIndex[];

/** A rel condition, narrowed. */
export type RelCond = Extract<Cond, { kind: 'rel' }>;

/**
 * One rel node of a prepared query: the ref field it crosses and the target
 * shape the pre-pass queries. `cond` is the node object itself inside the
 * query tree — the identity a rewrite replaces.
 */
export interface RelNode {
	cond: RelCond;
	/** The forward reference field the path ends at — for a case inside a reference message, the message field. */
	field: DescField;
	/** The holder entity's schema, which `fieldPath` resolves against. */
	holderSchema: DescMessage;
	/** localName path to `field`: the condition's path, shortened by the case segment when it names a case. */
	fieldPath: Path;
	/** The target entity's message schema. */
	targetSchema: DescMessage;
	/** The target's key fields — the values a stored stub carries. */
	keyFields: DescField[];
	/** To-many (repeated ref) when true. */
	repeated: boolean;
	/** Where the key and, for a reference message, the workspace sit inside a stored value. */
	shape: RefShape;
}

/** Whether any rel condition appears anywhere in the tree. */
export function hasRel(cond: Cond | undefined): boolean {
	if (cond === undefined) return false;
	switch (cond.kind) {
		case 'rel':
			return true;
		case 'and':
		case 'or':
			return cond.conds.some(hasRel);
		case 'not':
			return hasRel(cond.cond);
		case 'exists':
			return hasRel(cond.cond);
		default:
			return false;
	}
}

/**
 * Finds every rel node of a prepared query (localName paths), resolved against
 * the schema. prepareQuery has already validated each — the path ends at a
 * reference field, nothing nests — so this only recovers the descriptors the
 * pre-pass and the rewrite need.
 */
export function collectRels(schema: DescMessage, cond: Cond | undefined): RelNode[] {
	const out: RelNode[] = [];
	const walk = (c: Cond): void => {
		switch (c.kind) {
			case 'rel': {
				const leaf = resolveRelLeaf(schema, c.path);
				out.push({
					cond: c,
					field: leaf.field,
					holderSchema: schema,
					fieldPath: leaf.fieldPath,
					targetSchema: leaf.shape.targetSchema,
					keyFields: leaf.shape.keyFields,
					repeated: leaf.field.fieldKind === 'list',
					shape: leaf.shape,
				});
				return;
			}
			case 'and':
			case 'or':
				c.conds.forEach(walk);
				return;
			case 'not':
				walk(c.cond);
				return;
			default:
				return;
		}
	};
	if (cond) walk(cond);
	return out;
}

/**
 * The row-local condition a rel node collapses to once the pre-pass has
 * selected its matching targets. `targets` holds one key-value tuple per
 * matching target row, in the node's keyFields order (see targetKeyTuple); no
 * matches means the node matches no row, which the empty membership test
 * already says.
 *
 * A single-key ref becomes in(path.key, K) — for a repeated ref, an exists
 * over the element's key field — the exact shape the reverse-relation
 * push-down produces, so the per-ref-field index serves it. A composite-key
 * ref becomes a disjunction of key-tuple equalities: comparing whole tuples is
 * what keeps a matching id under the wrong ns from matching.
 */
export function relReplacement(node: RelNode, targets: readonly Scalar[][]): Cond {
	return wrapStub(node, membershipOf(node, targets));
}

/**
 * The membership test over one node's stubs, before the exists a repeated ref
 * wraps it in: stub-relative for a repeated ref (the element is the scope),
 * hung off the field path for a singular one.
 */
function membershipOf(node: RelNode, targets: readonly Scalar[][]): Cond {
	const at = stubPathOf(node);
	if (node.keyFields.length === 1) {
		return {
			kind: 'in',
			path: at(refKeyPath(node.shape, node.keyFields[0]!)),
			values: targets.map((t) => t[0]!),
		};
	}
	return {
		kind: 'or',
		conds: targets.map((tuple): Cond => ({
			kind: 'and',
			conds: node.keyFields.map((kf, i): Cond => ({
				kind: 'cmp',
				path: at(refKeyPath(node.shape, kf)),
				op: 'eq',
				value: tuple[i]!,
			})),
		})),
	};
}

/** Prefixes a stub-relative path for the node's arity; see membershipOf. */
function stubPathOf(node: RelNode): (sub: Path) => Path {
	return (sub) => node.repeated ? sub : [ ...node.fieldPath, ...sub ];
}

/** Wraps a stub-relative condition in the exists a repeated ref needs. */
function wrapStub(node: RelNode, cond: Cond): Cond {
	return node.repeated ? { kind: 'exists', path: node.fieldPath, cond } : cond;
}

/**
 * The row-local condition one node collapses to for rows of `workspaceId`,
 * from the pre-pass result grouped per workspace.
 *
 * A reference that resolves in the holder's workspace tests the stubs against
 * that workspace's set alone. A workspace-qualified reference (see
 * ReferenceOptions) tests each stub against the set of the workspace it names:
 * one leg per workspace with matches, each pairing that workspace's membership
 * with the stub naming it — where naming none means the holder's own. No
 * matching target anywhere collapses to the empty membership test.
 */
export function relReplacementIn(
	node: RelNode,
	perWs: ReadonlyMap<string, RelWorkspaceKeys> | undefined,
	workspaceId: string,
): Cond {
	if (!refQualified(node.shape)) {
		const keyed = perWs?.get(workspaceId);
		return relReplacement(node, keyed ? [ ...keyed.values() ] : []);
	}
	const at = stubPathOf(node);
	const wsPath = at([ node.shape.workspaceField!.localName ]);
	const legs: Cond[] = [];
	for (const [ ws, keyed ] of perWs ?? []) {
		if (keyed.size === 0) continue;
		const names: Cond = ws === workspaceId
			? { kind: 'in', path: wsPath, values: [ '', ws ] }
			: { kind: 'cmp', path: wsPath, op: 'eq', value: ws };
		legs.push({ kind: 'and', conds: [ membershipOf(node, [ ...keyed.values() ]), names ] });
	}
	if (legs.length === 0) return relReplacement(node, []);
	return wrapStub(node, legs.length === 1 ? legs[0]! : { kind: 'or', conds: legs });
}

/**
 * One matching target row's key values in the node's keyFields order — what
 * relReplacement compares the stored stubs with.
 */
export function targetKeyTuple(node: RelNode, target: unknown): Scalar[] {
	const rec = target as Record<string, unknown>;
	return node.keyFields.map((kf) => rec[kf.localName] as Scalar);
}

/**
 * Returns cond with every rel node replaced by the condition `replacements`
 * maps it to, keyed by node identity as collectRels reported it. Subtrees
 * without a rel are shared, not copied; the input is never mutated. A rel
 * absent from the map is left in place, so evaluation fails loudly rather than
 * silently matching nothing.
 */
export function rewriteRels(cond: Cond, replacements: ReadonlyMap<RelCond, Cond>): Cond {
	switch (cond.kind) {
		case 'rel':
			return replacements.get(cond) ?? cond;
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => rewriteRels(c, replacements)) };
		case 'not':
			return { ...cond, cond: rewriteRels(cond.cond, replacements) };
		default:
			return cond;
	}
}

/**
 * One workspace's matching targets, by the target row's own key. Keyed rather
 * than listed so a caller holding the previous result can retire exactly the
 * rows a commit named; the values are what the membership test compares.
 */
export type RelWorkspaceKeys = ReadonlyMap<string, Scalar[]>;

/**
 * The pre-pass result: each rel node's matching targets, per workspace. A
 * workspace absent from a node's map simply had no matching targets there.
 */
export type RelKeys = ReadonlyMap<RelNode, ReadonlyMap<string, RelWorkspaceKeys>>;

/**
 * Pre-pass results held across calls, so passes selecting the same target rows
 * over the same scope run once between them all. Its lifetime is the caller's
 * to choose and must not outlive one consistent read: entries are answers, not
 * a cache with an invalidation rule.
 *
 * Keys are opaque. Two entries that could have shared and did not only cost a
 * repeated pass.
 */
export type RelPassCache = Map<string, ReadonlyMap<string, RelWorkspaceKeys>>;

/**
 * Runs the pre-pass for every rel node over the local merged view: each node's
 * target entity read under the inner condition across `scope`, with the
 * matching rows' key tuples grouped by workspace — references never cross one,
 * so each workspace's membership set is its own.
 *
 * The pre-pass is an ordinary query, so `indexes` — the target entity's
 * compiled indexes — lets a declared index covering the inner condition serve
 * it at O(matches) instead of a decode of every target row `scope` holds.
 * Without it, and whenever the walk declines, the pass falls back to the read
 * that IS the definition of what a query returns.
 *
 * Nodes selecting the same target rows — the same entity crossed with the same
 * inner condition, whether through the same ref field or a different one —
 * share one pass, so a predicate asking two things about one relation reads
 * that entity once. Passing `shared` extends that sharing beyond this call, to
 * every query whose passes the caller knows are answering over the same view.
 *
 * Throws when a node's target message is not a registered entity.
 */
export async function computeRelKeys(
	tx: KVRead,
	registry: EntityRegistry,
	nodes: readonly RelNode[],
	scope: WorkspaceScope | undefined,
	indexes?: IndexSource,
	shared: RelPassCache = new Map(),
): Promise<RelKeys> {
	const out = new Map<RelNode, ReadonlyMap<string, RelWorkspaceKeys>>();
	for (const node of nodes) {
		const def = registry.bySchema(node.targetSchema);
		// A workspace-qualified reference points wherever its stubs say, so its
		// pass reads the target entity in every workspace the store holds,
		// whatever the query's own scope is.
		const nodeScope = refQualified(node.shape) ? undefined : scope;
		const shape = passKey(nodeScope, def.name, node);
		const done = shared.get(shape);
		if (done !== undefined) {
			out.set(node, done);
			continue;
		}
		const rows = await relTargets(tx, registry, def, node, nodeScope, indexes);
		const perWs = new Map<string, Map<string, Scalar[]>>();
		for (const row of rows) {
			let keyed = perWs.get(row.workspaceId);
			if (keyed === undefined) perWs.set(row.workspaceId, keyed = new Map());
			keyed.set(row.key, targetKeyTuple(node, row.value));
		}
		shared.set(shape, perWs);
		out.set(node, perWs);
	}
	return out;
}

/** One target row a commit named, as the incremental update consumes it. */
export interface TouchedTarget {
	workspaceId: string;
	key: string;
}

/**
 * Brings `prev` up to date from the rows a commit named rather than asking the
 * target entity again: each named row is read back, tested against the inner
 * condition, and added to or retired from its workspace's set. That is the
 * difference between a commit costing one pass over the target entity per
 * watcher and costing a point read per row it actually moved.
 *
 * `touchedOf` reports the rows named for one target entity, or undefined when
 * the commit did not say which rows it moved — there is then nothing to update
 * from, and the whole update declines by returning undefined, which means "run
 * a pass". The caller must leave out workspaces outside the scope `prev` was
 * computed over, and must pass the nodes it was computed for.
 *
 * `prev` is not mutated, so a caller that wants to compare the two still can.
 * Nodes sharing a pass share the updated set, on computeRelKeys's terms.
 */
export async function updateRelKeys(
	tx: KVRead,
	registry: EntityRegistry,
	nodes: readonly RelNode[],
	scope: WorkspaceScope | undefined,
	prev: RelKeys,
	touchedOf: (entity: string) => readonly TouchedTarget[] | undefined,
	shared: RelPassCache = new Map(),
): Promise<RelKeys | undefined> {
	const out = new Map<RelNode, ReadonlyMap<string, RelWorkspaceKeys>>();
	for (const node of nodes) {
		// A workspace-qualified reference's pass reads every workspace, and the
		// caller only names the rows a commit touched inside the query's scope;
		// the pass serves rather than an update from too few rows.
		if (refQualified(node.shape)) return undefined;
		const def = registry.bySchema(node.targetSchema);
		const before = prev.get(node);
		if (before === undefined) return undefined;
		const shape = passKey(scope, def.name, node);
		const done = shared.get(shape);
		if (done !== undefined) {
			out.set(node, done);
			continue;
		}
		const touched = touchedOf(def.name);
		if (touched === undefined) return undefined;
		const perWs = new Map<string, Map<string, Scalar[]>>();
		for (const [ ws, keyed ] of before) perWs.set(ws, new Map(keyed));
		const rows = await readMergedRowsWithVersion(tx, registry, def.name, touched);
		for (let i = 0; i < touched.length; i++) {
			const { workspaceId, key } = touched[i]!;
			let keyed = perWs.get(workspaceId);
			if (keyed === undefined) perWs.set(workspaceId, keyed = new Map());
			const view = rows[i];
			// A deleted row and a row that stopped matching leave the set the same
			// way: a membership test only ever asks whether the key is still in it.
			const value = view === undefined ? undefined : decodeTarget(node.targetSchema, view.v);
			if (value !== undefined && evalCond(value, node.cond.cond, node.targetSchema)) {
				keyed.set(key, targetKeyTuple(node, value));
			} else {
				keyed.delete(key);
			}
		}
		shared.set(shape, perWs);
		out.set(node, perWs);
	}
	return out;
}

/**
 * What a membership move did, per node and workspace: the target key tuples
 * that entered, and the canonical keys (see relTupleKey) of those that left.
 *
 * The two halves are shaped differently because they are used differently. The
 * entrants become a membership test that selects the rows which may have
 * STARTED matching; the leavers are only ever asked "is this holder's stub one
 * of you", which is a lookup.
 */
export interface RelKeysDelta {
	added: ReadonlyMap<RelNode, ReadonlyMap<string, Scalar[][]>>;
	removed: ReadonlyMap<RelNode, ReadonlyMap<string, ReadonlySet<string>>>;
}

/**
 * What changed between two pre-pass results, for a caller that wants to
 * re-examine the rows a move reaches instead of all of them.
 *
 * Returns undefined when a key tuple has no canonical form — a NaN or a
 * non-scalar key field — since a leaver that cannot be recognized in a holder's
 * stub would leave that holder matching a target that is gone.
 */
export function diffRelKeys(prev: RelKeys, next: RelKeys): RelKeysDelta | undefined {
	const added = new Map<RelNode, Map<string, Scalar[][]>>();
	const removed = new Map<RelNode, Map<string, Set<string>>>();
	for (const [ node, perWs ] of next) {
		const before = prev.get(node);
		if (before === undefined) return undefined;
		const nodeAdded = new Map<string, Scalar[][]>();
		const nodeRemoved = new Map<string, Set<string>>();
		for (const ws of new Set([ ...perWs.keys(), ...before.keys() ])) {
			const now = perWs.get(ws);
			const then = before.get(ws);
			const enter: Scalar[][] = [];
			const leave = new Set<string>();
			if (now !== undefined) {
				for (const [ key, tuple ] of now) {
					if (then?.has(key) !== true) enter.push(tuple);
				}
			}
			if (then !== undefined) {
				for (const [ key, tuple ] of then) {
					if (now?.has(key) === true) continue;
					const canonical = relTupleKey(tuple);
					if (canonical === undefined) return undefined;
					leave.add(canonical);
				}
			}
			if (enter.length > 0) nodeAdded.set(ws, enter);
			if (leave.size > 0) nodeRemoved.set(ws, leave);
		}
		added.set(node, nodeAdded);
		removed.set(node, nodeRemoved);
	}
	return { added, removed };
}

/**
 * The canonical form of a key tuple, or undefined when one of its values has
 * none. Two tuples share a form exactly when the membership test would call
 * them the same target.
 */
export function relTupleKey(tuple: readonly Scalar[]): string | undefined {
	const parts: string[] = [];
	for (const v of tuple) {
		const key = scalarKey(v);
		if (key === undefined) return undefined;
		parts.push(key);
	}
	return JSON.stringify(parts);
}

/**
 * The target key tuples a holder row references through one node's ref field —
 * none for an unset ref, one for a to-one, one per element for a to-many.
 * Counterpart of targetKeyTuple on the other side of the reference: what this
 * returns is what that produced, for the same target.
 */
export function stubKeyTuples(node: RelNode, row: unknown): Scalar[][] {
	const { found, value: at } = resolvePath(row, node.fieldPath, node.holderSchema);
	if (!found || at === undefined || at === null) return [];
	const values = node.repeated ? at as unknown[] : [ at ];
	const out: Scalar[][] = [];
	for (const value of values) {
		const stub = refStub(node.shape, value as Message);
		if (stub === undefined) continue;
		out.push(
			node.keyFields.map((kf) => (stub as unknown as Record<string, unknown>)[kf.localName] as Scalar),
		);
	}
	return out;
}

/**
 * Whether every rel node of a condition sits under an even number of
 * negations, which is what makes a membership move monotone: a row can only
 * start matching because a key ENTERED some node's set. Under a negation the
 * implication runs the other way — `not(matches(…))` admits rows whose target
 * left — so a caller that only looks at the entrants would miss them.
 */
export function relsArePositive(cond: Cond | undefined): boolean {
	const walk = (c: Cond, negated: boolean): boolean => {
		switch (c.kind) {
			case 'rel':
				return !negated;
			case 'and':
			case 'or':
				return c.conds.every((sub) => walk(sub, negated));
			case 'not':
				return walk(c.cond, !negated);
			case 'exists':
				return walk(c.cond, negated);
			default:
				return true;
		}
	};
	return cond === undefined || walk(cond, false);
}

/** Identifies a pass: the rows it may read, crossed with what it selects. */
function passKey(scope: WorkspaceScope | undefined, entity: string, node: RelNode): string {
	return scopeKey(scope) + relShape(entity, node);
}

/**
 * Decodes a target row the same way the pass that is the definition does —
 * ordered fields sorted included, since the inner condition may read one.
 */
function decodeTarget(schema: DescMessage, bytes: Uint8Array): unknown {
	const value = fromBinary(schema, bytes);
	sortOrderedFields(schema, value);
	return value;
}

/**
 * One node's matching target rows, index-served where an index serves them and
 * read across `scope` otherwise.
 *
 * Either path yields each workspace's rows in row-key order, so the key tuples
 * a pass produces do not depend on which one served it — which is what lets
 * sameRelKeys compare two passes element-wise across a change of path.
 */
async function relTargets(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	node: RelNode,
	scope: WorkspaceScope | undefined,
	indexes: IndexSource | undefined,
): Promise<Row<unknown>[]> {
	const byIndex = await relTargetsByIndex(tx, registry, def, node, scope, indexes);
	if (byIndex !== undefined) return byIndex;
	const rows = await scanDecoded(tx, registry, def, node.targetSchema, scope);
	return rows.filter((row) => evalCond(row.value, node.cond.cond, node.targetSchema));
}

/**
 * A node's matching target rows from an index, or undefined for "read the
 * entity instead".
 *
 * One walk covers one workspace, so a scope naming workspaces is walked one
 * leg each — the treatment a named set's own query gets — and one leg
 * declining declines the pass, because half a key set is a membership test
 * that silently drops matching rows. A scope naming no workspace covers every
 * workspace the store holds, which has no legs to walk.
 */
async function relTargetsByIndex(
	tx: KVRead,
	registry: EntityRegistry,
	def: EntityDef,
	node: RelNode,
	scope: WorkspaceScope | undefined,
	indexes: IndexSource | undefined,
): Promise<Row<unknown>[] | undefined> {
	if (indexes === undefined || scope === undefined) return undefined;
	const workspaces = typeof scope === 'string' ? [ scope ] : [ ...new Set(scope) ];
	const compiled = { where: node.cond.cond, total: false };
	const built = indexes(def.name);
	const out: Row<unknown>[] = [];
	for (const workspaceId of workspaces) {
		const found = await queryByIndex(
			tx,
			registry,
			def,
			node.targetSchema,
			built,
			compiled,
			workspaceId,
		);
		if (found === undefined) return undefined;
		for (const entry of found.rows) out.push(entry.row);
	}
	return out;
}

/**
 * Identifies the pass a node needs, as opposed to the replacement it builds
 * from the result: the target entity and the inner condition decide which rows
 * match, while the ref field and its arity only decide how the membership test
 * is phrased. Two nodes sharing a shape share their pass.
 *
 * The key tuples a pass yields are read in the target's key-field order, which
 * is a property of the target entity, so the entity name pins it. Structural
 * equality is approximated by the serialized form — property order included —
 * which only ever costs a missed sharing opportunity.
 */
function relShape(entity: string, node: RelNode): string {
	return entity + '\u0000'
		+ JSON.stringify(node.cond.cond, (_k, v) => typeof v === 'bigint' ? `\u0000n${v}` : v);
}

/**
 * Identifies the rows a pass may read, which is the other half of what decides
 * that two passes are one. Serialized rather than concatenated so a workspace
 * id can hold any character without colliding with a set or with the
 * every-workspace scope; a named set is keyed in the order it was given, so two
 * spellings of one set miss each other — a repeated pass, never a wrong answer.
 */
function scopeKey(scope: WorkspaceScope | undefined): string {
	return scope === undefined ? '*' : JSON.stringify(scope);
}

/**
 * Rewrites a prepared condition row-local for one workspace, replacing every
 * rel node with the membership test its key set gives it there. Nodes the
 * condition does not contain are ignored; a node with no keys for the
 * workspace collapses to the empty membership test, which matches nothing.
 */
export function rewriteWithKeys(cond: Cond, keys: RelKeys, workspaceId: string): Cond {
	const replacements = new Map<RelCond, Cond>();
	for (const [ node, perWs ] of keys) {
		replacements.set(node.cond, relReplacementIn(node, perWs, workspaceId));
	}
	return rewriteRels(cond, replacements);
}

/**
 * Whether two pre-pass results hold the same memberships — the memo that lets a
 * watcher skip rebuilding its match set on a target-entity commit that did not
 * move any. Compared by target row key, so it holds whichever way each side was
 * arrived at: a pass over the entity and an update from the rows a commit named
 * agree on the set, not on an order.
 */
export function sameRelKeys(a: RelKeys, b: RelKeys): boolean {
	if (a.size !== b.size) return false;
	for (const [ node, perWs ] of a) {
		const other = b.get(node);
		if (other === undefined) return false;
		// An empty workspace and an absent one are the same membership, so the
		// maps are compared over the union of their workspaces rather than by
		// size — an update leaves behind the workspaces it emptied.
		for (const ws of new Set([ ...perWs.keys(), ...other.keys() ])) {
			const mine = perWs.get(ws);
			const theirs = other.get(ws);
			if ((mine?.size ?? 0) !== (theirs?.size ?? 0)) return false;
			if (mine === undefined || theirs === undefined) continue;
			for (const [ key, tuple ] of mine) {
				const o = theirs.get(key);
				if (o === undefined || o.length !== tuple.length) return false;
				if (tuple.some((v, i) => v !== o[i])) return false;
			}
		}
	}
	return true;
}
