import {
	clone,
	create,
	type DescField,
	type DescMessage,
	type DescOneof,
	equals,
	fromBinary,
	getOption,
	hasOption,
	type Message,
	type MessageInitShape,
	type MessageShape,
	ScalarType,
	toBinary,
} from '@bufbuild/protobuf';
import { protoCamelCase } from '@bufbuild/protobuf/reflect';
import { anyPack } from '@bufbuild/protobuf/wkt';
import { FieldMaskSchema } from '@bufbuild/protobuf/wkt';

import {
	type BatchMutation,
	BatchMutationSchema,
	type CrudMutation,
	CrudMutationSchema,
	CrudOp,
	type CustomMutation,
	CustomMutationSchema,
} from './gen/syncplane/v1/mutation_pb.js';
import {
	type AccessRule,
	entity as entityExt,
	type EntityOptions,
	field_behavior as fieldBehaviorExt,
	type FieldBehavior,
	type GrantOptions,
	reference as referenceExt,
} from './gen/syncplane/v1/options_pb.js';
import type { Condition } from './gen/syncplane/v1/query_pb.js';
import {
	rowKey,
	scalarKeyString,
	validateEntityName,
	validateKeyField,
	validateRelationName,
	validateRowKey,
	validateWorkspaceId,
} from './naming.js';
import { RESERVED_QUERY_NAMES } from './query/proxy.js';
import type { DeletedSegment, InsertedRange } from './text/doc.js';
import {
	decodeTextDoc,
	encodeTextDoc,
	isFragment,
	isPlainString,
	mergeText,
	plainString,
} from './text/index.js';

// Descriptor-derived facts are memoized per descriptor below. getOption decodes
// the extension out of the descriptor's raw option bytes on every call, and the
// write path asks the same questions of the same fields for every row it
// touches; a DescField/DescMessage is an immutable singleton created once per
// generated schema, so one answer per descriptor is the whole lifetime's worth.
// A memo holds null for "declares none", which keeps a WeakMap miss (undefined)
// distinct from a computed absence.
const fieldBehaviors = new WeakMap<DescField, FieldBehavior | null>();
const entityOptions = new WeakMap<DescMessage, EntityOptions | null>();
const referenceMessages = new WeakMap<DescMessage, boolean>();
const refShapeLists = new WeakMap<DescField, RefShape[]>();
const entityKeys = new WeakMap<DescMessage, DescField[] | null>();
const listKeyFields = new WeakMap<DescField, DescField[]>();
const schemaRefFields = new WeakMap<DescMessage, [DescField, RefShape[]][]>();
const fieldsByLocalName = new WeakMap<DescMessage, Map<string, DescField>>();

/**
 * A field's (syncplane.v1.field_behavior), or undefined when it declares none.
 *
 * The returned message is shared with every other caller and must not be
 * modified.
 */
export function fieldBehaviorOf(field: DescField): FieldBehavior | undefined {
	let behavior = fieldBehaviors.get(field);
	if (behavior === undefined) {
		behavior = hasOption(field, fieldBehaviorExt) ? getOption(field, fieldBehaviorExt) : null;
		fieldBehaviors.set(field, behavior);
	}
	return behavior ?? undefined;
}

/**
 * A message's (syncplane.v1.entity), or undefined when it declares none — which
 * is also how a non-entity message is told from an entity.
 *
 * The returned message is shared with every other caller and must not be
 * modified.
 */
export function entityOptionsOf(desc: DescMessage): EntityOptions | undefined {
	let opts = entityOptions.get(desc);
	if (opts === undefined) {
		opts = hasOption(desc, entityExt) ? getOption(desc, entityExt) : null;
		entityOptions.set(desc, opts);
	}
	return opts ?? undefined;
}

/** A registered syncable entity, derived from (syncplane.v1.entity) options. */
export interface EntityDef {
	schema: DescMessage;
	name: string;
	keyFields: DescField[];
	/**
	 * The entity's secondary indexes, in the order the engine compiles them:
	 * field-level `indexed` marks first (in field order), then declared
	 * composites (in declaration order). Both surfaces desugar to the same
	 * shape here, which is what lets one planner read them.
	 *
	 * Reference indexes the engine derives for itself are NOT here: they exist
	 * to make a server-side reverse hop cheap, and the client resolves reverse
	 * relations differently.
	 */
	indexes: IndexDef[];
	/**
	 * (syncplane.v1.entity).server_only: the entity's rows never sync, so the
	 * local store holds none of them however the client is configured. It is
	 * registered to be reachable by name — `queryServer` resolves against the
	 * same registry — not to be synced.
	 */
	serverOnly: boolean;
	/**
	 * Which write operations at least one rule of the entity's
	 * `(syncplane.v1.entity).access` admits, or `undefined` when the entity
	 * declares no access. The server refuses an operation no rule admits to
	 * every caller it authorizes, so the client refuses it at the call site
	 * rather than applying it to the overlay and rolling it back on the
	 * rejection ack (see assertWritePolicy).
	 */
	writeAccess: WriteAccess | undefined;
	/**
	 * The entity's `(syncplane.v1.entity).access` rules, in declaration order,
	 * or `undefined` when the entity declares no access.
	 */
	accessRules: readonly AccessRuleDef[] | undefined;
	/**
	 * The entity's field guards, from `(syncplane.v1.entity).access.guard`, in
	 * declaration order. Empty when the entity declares none.
	 */
	guards: readonly GuardDef[];
	/**
	 * Top-level units, by proto name, that no client write changes after the
	 * create: the `creator` fields, and each unit whose guard has no rule that
	 * names update, a oneof named by its group.
	 *
	 * A `patch()` masking one is refused at the call site; a `put()` carries the
	 * stored value over instead, which is what the server does with the row it
	 * has. The server decides a guard that has an update rule for each caller,
	 * and may protect further fields per caller (its `FieldPermissions` hook):
	 * such a write enqueues and comes back as a rejection ack.
	 */
	protectedFields: string[];
	/**
	 * Top-level units, by proto name, that a client create stores as the zero
	 * value: each unit whose guard has no rule that names create.
	 */
	clearedOnCreate: string[];
	/**
	 * The fields marked `(syncplane.v1.field_behavior).creator`: a create
	 * stores the caller's primary subject in each, so a local put of a row
	 * this client does not hold does the same to the overlay.
	 */
	creatorFields: DescField[];
	/**
	 * Whether the entity's rows grant roles in the access model
	 * (`(syncplane.v1.entity).grant`), so a local write to one can change what
	 * the caller holds before the server says so.
	 */
	grant: boolean;
	/** The entity's `(syncplane.v1.entity).grant`, resolved; `undefined` when `grant` is false. */
	grantSpec: GrantSpec | undefined;
}

/** The grant option of a grant entity, resolved against its schema. */
export interface GrantSpec {
	/** The unit that holds the subject, by proto name: a field, or a oneof by its name. */
	subjectUnit: string;
	/** The field that holds the role: a `string` field, or an enum field whose value names are roles. */
	roleField: DescField | undefined;
	/** Every permission each declared role grants, its `includes` followed, sorted. */
	roles: ReadonlyMap<string, readonly string[]>;
}

/** Whether at least one of an entity's access rules admits each write
 * operation, whatever the rule's source. A rule with `write` admits all
 * three. An operation no rule admits is open only to trusted server-side
 * writes. */
export interface WriteAccess {
	create: boolean;
	update: boolean;
	delete: boolean;
	/**
	 * The `fields` lists of the update rules, by proto name, when every update
	 * rule has one; `undefined` when some update rule has none, which admits
	 * an update of any field.
	 */
	updateFields?: readonly (readonly string[])[];
}

/**
 * One rule of an entity's `(syncplane.v1.entity).access`, resolved against
 * the entity's schema. `write` is spread over `create`, `update` and
 * `delete`.
 */
export interface AccessRuleDef {
	read: boolean;
	create: boolean;
	update: boolean;
	delete: boolean;
	/**
	 * The permissions a caller must hold together, sorted. Empty for a rule
	 * that admits every admitted caller.
	 */
	requires: readonly string[];
	/** The field that must hold one of the caller's subjects. */
	callerIs: DescField | undefined;
	/** The units an update may change, by proto name, sorted. */
	fields: readonly string[] | undefined;
	/** The condition a row must match, with proto-name paths. */
	when: Condition | undefined;
	/** The rows the row must sit within; empty for a rule without the check. */
	within: readonly WithinDef[];
}

/**
 * One field guard of an entity, from `(syncplane.v1.entity).access.guard`,
 * resolved against the entity's schema.
 */
export interface GuardDef {
	/** The units the guard restricts, by proto name: fields, and oneofs by group. */
	units: readonly string[];
	/** The guard's rules. `write` is spread over `create` and `update`. */
	rules: readonly AccessRuleDef[];
}

/**
 * One entry of an access rule's `within`: the row must itself be, or sit below,
 * a row of `entity` that passes the entry's checks.
 */
export interface WithinDef {
	entity: string;
	/** The proto name of the field of `entity` that must hold one of the caller's subjects. */
	callerIs: string | undefined;
	/** The condition the row of `entity` must match, with proto-name paths. */
	when: Condition | undefined;
}

/** One secondary index of an entity: a name unique within it, and the
 * components it sorts by. Mirrors syncplane.v1.IndexSpec with the paths kept
 * as proto field names, which is the form both languages state identically. */
export interface IndexDef {
	name: string;
	key: { path: string[]; desc: boolean }[];
}

export function entityDefOf(schema: DescMessage): EntityDef {
	const opts = entityOptionsOf(schema);
	if (!opts) {
		throw new Error(`${schema.typeName} lacks (syncplane.v1.entity) options`);
	}
	if (!opts.name || opts.keyFields.length === 0) {
		throw new Error(`${schema.typeName}: entity options need name and key_fields`);
	}
	// The name and the key-field kinds are the shared naming rules (naming.ts);
	// checking them here means a schema the server would refuse to register never
	// reaches a local write either.
	validateEntityName(opts.name, schema.typeName);
	const keyFields = opts.keyFields.map((kf) => {
		const field = schema.fields.find((f) => f.name === kf);
		if (!field) throw new Error(`${schema.typeName}: key field "${kf}" not found`);
		validateKeyField(opts.name, field);
		// A key must be on every row. A oneof member is there only while its case
		// is the active one, so the row key would move — or collapse onto the zero
		// value — whenever a sibling won a write.
		if (field.oneof) {
			throw new Error(
				`${schema.typeName}: key field "${kf}" is a member of oneof "${field.oneof.name}"; `
					+ `a key field cannot be inside a oneof`,
			);
		}
		return field;
	});
	const accessRules = accessRulesOf(schema, opts.access?.allow);
	// A guarded key field is a schema mistake rather than a rule: a patch can
	// never move one, and a put that changed one would address a different row.
	(opts.access?.guard ?? []).forEach((g, i) => {
		const key = g.fields.find((name) => keyFields.some((f) => f.name === name));
		if (key !== undefined) {
			throw new Error(
				`${schema.typeName}: access guard[${i}] fields: "${key}" is a key field, which no update changes`,
			);
		}
	});
	const guards = (opts.access?.guard ?? []).map((g) => ({
		units: [ ...g.fields ],
		// A guard rule's write admits create and update only.
		rules: accessRulesOf(schema, g.allow)!.map((r) => ({ ...r, delete: false })),
	}));
	const creatorFields = schema.fields.filter((f) => fieldBehaviorOf(f)?.creator !== undefined);
	return {
		schema,
		name: opts.name,
		keyFields,
		indexes: indexesOf(schema, opts),
		serverOnly: opts.serverOnly,
		guards,
		protectedFields: [ ...creatorFields.map((f) => f.name), ...closedUnits(guards, 'update') ],
		clearedOnCreate: closedUnits(guards, 'create'),
		creatorFields,
		grant: opts.grant !== undefined,
		grantSpec: opts.grant === undefined ? undefined : grantSpecOf(schema, opts.grant),
		writeAccess: writeAccessOf(accessRules),
		accessRules,
	};
}

/**
 * Resolves a grant option against the entity's schema. The server refuses an
 * unknown role field, an unknown include and an include cycle at
 * registration, so none of them stops a client here.
 */
function grantSpecOf(schema: DescMessage, grant: GrantOptions): GrantSpec {
	const declared = new Map(grant.roles.map((r) => [ r.name, r ]));
	const expand = (name: string, into: Set<string>, seen: Set<string>) => {
		const role = declared.get(name);
		if (role === undefined || seen.has(name)) return;
		seen.add(name);
		for (const permission of role.grants) into.add(permission);
		for (const included of role.includes) expand(included, into, seen);
	};
	const roles = new Map<string, readonly string[]>();
	for (const role of grant.roles) {
		const permissions = new Set<string>();
		expand(role.name, permissions, new Set());
		roles.set(role.name, [ ...permissions ].sort());
	}
	return {
		subjectUnit: grant.subject,
		roleField: schema.fields.find((f) => f.name === grant.role),
		roles,
	};
}

/**
 * Resolves an entity's access rules against its schema. The server refuses a
 * rule whose `caller_is` names no field at registration, so such a check never
 * matches here.
 */
function accessRulesOf(
	schema: DescMessage,
	rules: readonly AccessRule[] | undefined,
): AccessRuleDef[] | undefined {
	if (rules === undefined) return undefined;
	return rules.map((rule) => ({
		read: rule.read,
		create: rule.write || rule.create,
		update: rule.write || rule.update,
		delete: rule.write || rule.delete,
		requires: [ ...rule.requires ].sort(),
		callerIs: rule.callerIs === '' ? undefined : schema.fields.find((f) => f.name === rule.callerIs),
		fields: rule.fields.length === 0 ? undefined : [ ...rule.fields ].sort(),
		when: rule.when,
		within: rule.within.map((w) => ({
			entity: w.entity,
			callerIs: w.callerIs === '' ? undefined : w.callerIs,
			when: w.when,
		})),
	}));
}

/**
 * Folds an entity's access rules into the write operations they admit. The
 * source of a rule and its checks on the row are left out: whether they hold
 * is decided on the stored row, which only the server has.
 */
function writeAccessOf(rules: readonly AccessRuleDef[] | undefined): WriteAccess | undefined {
	if (rules === undefined) return undefined;
	const out: WriteAccess = { create: false, update: false, delete: false };
	const lists: (readonly string[])[] = [];
	let unlisted = false;
	for (const rule of rules) {
		out.create ||= rule.create;
		out.delete ||= rule.delete;
		if (!rule.update) continue;
		out.update = true;
		if (rule.fields === undefined) unlisted = true;
		else lists.push(rule.fields);
	}
	if (out.update && !unlisted) out.updateFields = lists;
	return out;
}

/**
 * The units of `guards` that no guard rule admits `op` on to any caller,
 * whatever the rule's source and checks.
 */
export function closedUnits(guards: readonly GuardDef[], op: 'create' | 'update'): string[] {
	return guards.filter((g) => !g.rules.some((r) => r[op])).flatMap((g) => g.units);
}

/**
 * The protected field one path writes into, or `undefined` when it writes into
 * none. A path is matched by its first segment: naming the field replaces it,
 * and naming something below it writes into it.
 */
export function protectedFieldOf(def: EntityDef, path: string): string | undefined {
	if (def.protectedFields.length === 0) return undefined;
	const head = path.split('.', 1)[0]!;
	return def.protectedFields.includes(head) ? head : undefined;
}

/**
 * Throws if the entity's schema forbids this write to every untrusted caller,
 * which is the same decision the server makes in Server.checkWritePolicy — a
 * client that classified differently would either block a legal write or
 * enqueue one only to have it rejected. The shared vectors in
 * testdata/writepolicy pin the two against each other.
 *
 * An entity with access rules refuses a write that can only be an operation
 * no rule admits. Classification is a pure function of the write, so it needs
 * no row read: PATCH never upserts and is an update; DELETE is a delete; a
 * PUT asserting version 0 is provably a create and one asserting any other
 * version provably an update. A PUT asserting nothing is a create when the
 * row is absent and an update when it is present, so it is refused only where
 * no rule admits either, and the server decides on the stored row. An entity
 * without access rules is never refused for its op.
 *
 * `paths` is the effective mask of a patch, including any path a text-string
 * value contributed — the same mask the server authorizes. A path reaching a
 * protected field is refused; the field it reaches is its first segment, so
 * masking `audit.size` is refused wherever masking `audit` is. When every
 * update rule has a `fields` list, a patch is also refused unless one list
 * names every field the mask reaches, key fields left out.
 */
export function assertWritePolicy(
	def: EntityDef,
	op: CrudOp,
	expectedVersion: bigint | undefined,
	paths?: readonly string[],
): void {
	const refuse = (why: string): never => {
		throw new Error(`entity "${def.name}": ${why}`);
	};
	if (paths !== undefined) {
		for (const path of paths) {
			const head = protectedFieldOf(def, path);
			if (head !== undefined) {
				refuse(
					`field "${head}" may not be written by a client, so a patch masking "${path}" is refused. `
						+ `Drop the path from the mask, or make the change through a custom mutation.`,
				);
			}
		}
	}
	const access = def.writeAccess;
	if (access === undefined) return;
	const onlyTrusted = (what: string, verb: string): never =>
		refuse(`no access rule admits ${what}, so only a trusted server-side write may ${verb} its rows`);
	switch (op) {
		case CrudOp.DELETE:
			if (!access.delete) onlyTrusted('delete', 'delete');
			return;
		case CrudOp.PATCH: {
			if (!access.update) onlyTrusted('update', 'change');
			if (access.updateFields === undefined || paths === undefined) return;
			// A key field is left out: a patch that changes one is refused anyway.
			const keys = new Set(def.keyFields.map((f) => f.name));
			const heads = [ ...new Set(paths.map((p) => p.split('.', 1)[0]!)) ]
				.filter((h) => !keys.has(h))
				.sort();
			if (access.updateFields.some((list) => heads.every((h) => list.includes(h)))) return;
			refuse(
				`no access rule admits an update of ${heads.map((h) => `"${h}"`).join(', ')} together, `
					+ `so a patch masking them is refused`,
			);
		}
		case CrudOp.PUT:
			if (expectedVersion === undefined) {
				if (!access.create && !access.update) onlyTrusted('create or update', 'write');
			} else if (expectedVersion === 0n) {
				if (!access.create) onlyTrusted('create', 'create');
			} else if (!access.update) {
				onlyTrusted('update', 'change');
			}
			return;
		default:
			return;
	}
}

/**
 * Reads both index declaration surfaces off the descriptor and returns them in
 * the same order the engine compiles them, so the two sides agree on which
 * index is which without either naming the other's.
 *
 * A field mark desugars to the reserved name "__field_<proto name>", matching
 * the engine — the name is derived rather than authored so two schemas marking
 * the same field agree on it.
 */
function indexesOf(
	schema: DescMessage,
	opts: { index: { name: string; key: { path: string; desc: boolean }[] }[] },
): IndexDef[] {
	const out: IndexDef[] = [];
	for (const field of schema.fields) {
		if (!fieldBehaviorOf(field)?.indexed) continue;
		out.push({ name: `__field_${field.name}`, key: [ { path: [ field.name ], desc: false } ] });
	}
	for (const spec of opts.index) {
		out.push({
			name: spec.name,
			key: spec.key.map((k) => ({ path: k.path.split('.'), desc: k.desc })),
		});
	}
	return out;
}

/**
 * The stored shape of one typed reference field: the entity it points at and
 * where inside the stored value the target's key and workspace sit. Mirror of
 * lww.Ref in the engine.
 *
 * Two shapes make a reference. An ENTITY-TYPED field — a singular or repeated
 * message field whose (element) message carries (syncplane.v1.entity) — stores a
 * stub of the target itself, projected down to its key fields; `valueField` is
 * undefined and the stub is the field value. A REFERENCE MESSAGE field — one
 * whose (element) message carries (syncplane.v1.reference) — stores a wrapper
 * holding the entity stub under `valueField` and, when declared, the target's
 * workspace under `workspaceField`. Consumers address the key through
 * {@link refKeyPath} and the stub through {@link refStub}, so the two shapes
 * differ in nothing else.
 *
 * A POLYMORPHIC reference — one stored value that points at one of several
 * entities — is a oneof of references, and each member is its own shape, a
 * CASE of it. The oneof sits either on the holder (`group` is set, and the
 * field is the member) or inside a reference message (`valueField` is a member
 * of the message's oneof, and every case shares the field). A case holds a
 * value only while it is the active one, which {@link refStub} already
 * reports.
 */
export interface RefShape {
	/** The referenced entity's message schema. */
	targetSchema: DescMessage;
	/** The target's key fields, declared on the target. */
	keyFields: DescField[];
	/** The reference message's own schema; undefined for an entity-typed reference. */
	refSchema?: DescMessage;
	/** The entity-typed field of a reference message; undefined for an entity-typed reference. */
	valueField?: DescField;
	/** A reference message's workspace field; undefined when every value resolves in the holder's workspace. */
	workspaceField?: DescField;
	/** The holder's oneof the reference field is a member of. */
	group?: DescOneof;
	/**
	 * The accessor of the field that decides this case — the holder's member
	 * for a oneof on the holder, the reference message's member for a oneof
	 * inside it. Undefined for a reference to one entity.
	 */
	caseLocalName?: string;
}

/**
 * Classifies a field as a typed reference to one entity, or returns undefined
 * when it is not one: a scalar, a map, a message that is neither an entity nor
 * a well-formed reference message, or a reference message whose target is a
 * oneof of entities — a polymorphic reference has no single target, and
 * {@link refShapes} reports its cases. A malformed reference message is not a
 * reference either — registration reports it, and everything else sees an
 * ordinary sub-message.
 *
 * The returned shape is shared with every other caller and must not be
 * modified.
 */
export function refShape(field: DescField): RefShape | undefined {
	const shapes = refShapes(field);
	return shapes.length === 1 ? shapes[0] : undefined;
}

/**
 * Classifies a field as a typed reference and reports every case it can hold:
 * empty when the field is not a reference, one shape for a reference to one
 * entity, and one per entity-typed member of the oneof for a reference message
 * whose target is a oneof. A member of a oneof on the holder is classified on
 * its own, with `group` set; the other members of that oneof are its sibling
 * cases. Mirrors lww.RefsOf.
 *
 * The returned shapes are shared with every other caller and must not be
 * modified.
 */
export function refShapes(field: DescField): RefShape[] {
	let shapes = refShapeLists.get(field);
	if (shapes === undefined) {
		shapes = computeRefShapes(field);
		refShapeLists.set(field, shapes);
	}
	return shapes;
}

function computeRefShapes(field: DescField): RefShape[] {
	const elem = field.fieldKind === 'message'
		? field.message
		: field.fieldKind === 'list' && field.listKind === 'message'
		? field.message
		: undefined;
	if (!elem) return [];
	const group = field.oneof;
	const caseOf = group ? { group, caseLocalName: field.localName } : {};
	// The reference declaration wins over an entity declaration on the same
	// message: such a message is refused at registration, and until then it
	// must not pass for an entity-typed stub. Mirrors lww.RefsOf.
	if (!isReferenceMessage(elem)) {
		const keyFields = entityKeyFields(elem);
		return keyFields ? [ { targetSchema: elem, keyFields, ...caseOf } ] : [];
	}
	const parsed = parseReferenceMessage(elem);
	if (typeof parsed === 'string') return [];
	return parsed.map((shape) => ({ ...shape, ...caseOf }));
}

/** Whether a shape is one case of several (see {@link RefShape}). */
export function refPolymorphic(shape: RefShape): boolean {
	return shape.group !== undefined || refInnerCase(shape);
}

/**
 * Whether the case is decided inside the stored value — the reference
 * message's oneof — rather than by which holder field is set. Such cases
 * share the holder field and are told apart by `valueField`.
 */
export function refInnerCase(shape: RefShape): boolean {
	return shape.valueField?.oneof !== undefined;
}

/**
 * The relation accessor a reference field is reached by: the holder's oneof
 * when the field is a member of one — the property the message shape has, and
 * the one name its cases share — else the field's own.
 */
export function refSegmentOf(field: DescField): string {
	return field.oneof?.localName ?? field.localName;
}

/**
 * A field's value read the way the message shape stores it: a oneof member
 * sits under its group as `{ case, value }`, present only while it is the
 * active case; any other field is its own property.
 */
export function fieldValueOf(rec: Rec, field: DescField): unknown {
	if (field.oneof) {
		const group = rec[field.oneof.localName] as { case?: string; value?: unknown } | undefined;
		return group?.case === field.localName ? group.value : undefined;
	}
	return rec[field.localName];
}

/**
 * The query-builder path to one case of a reference, in accessor names: the
 * field through the oneofs on the way — the holder's for a member of one, the
 * reference message's for a case inside it. A reference to one entity is its
 * field alone.
 */
export function refCasePath(field: DescField, shape: RefShape): string[] {
	const path = field.oneof ? [ field.oneof.localName, field.localName ] : [ field.localName ];
	if (shape.valueField?.oneof) {
		path.push(shape.valueField.oneof.localName, shape.valueField.localName);
	}
	return path;
}

/** Writes a field's value the way {@link fieldValueOf} reads it. */
function setFieldValue(rec: Rec, field: DescField, value: unknown): void {
	if (field.oneof) {
		rec[field.oneof.localName] = { case: field.localName, value };
		return;
	}
	rec[field.localName] = value;
}

/**
 * Key fields of the entity targeted by a reference field, or [] when the field
 * is not a reference to one entity. The fields are the TARGET's: on a
 * reference message they sit under the entity field, not on the stored value —
 * address them through {@link refKeyPath}.
 */
export function refKeyFields(field: DescField): DescField[] {
	return refShape(field)?.keyFields ?? [];
}

/** Whether a field is a first-class typed reference (see refShapes), to one entity or to one of several. */
export function isRef(field: DescField): boolean {
	return refShapes(field).length > 0;
}

/**
 * Whether a field is a reference whose stored shape is a reference message —
 * the shape whose workspace and key are one merge unit: a singular field is
 * replaced whole on every write, a repeated one merges per element with the
 * whole element as the identity (see {@link refItemKey}).
 */
export function isRefMessageField(field: DescField): boolean {
	return refShapes(field)[0]?.valueField !== undefined;
}

/**
 * Renders one stored value of a repeated reference message as the mask segment
 * that addresses it, so a PATCH can add or remove an element on its own: the
 * element's workspace and its key, escaped and joined the way a composite
 * keyed-list identity is (see {@link encodeItemKey}). The workspace part is
 * always present, empty when the value names none, so a value with an empty
 * workspace and one naming the holder's own are two elements even though they
 * resolve to the same row. A reference without a workspace field renders the
 * key alone, exactly as an element of a repeated entity-typed reference.
 *
 * A case of a polymorphic reference message puts its name between the
 * workspace and the key, so two cases naming the same key are two elements;
 * the workspace part is present whenever the message declares one, and the
 * case part whenever the message's target is a oneof. Mirror of
 * lww.Ref.ItemKey, byte for byte; an element of a polymorphic reference
 * message is rendered by {@link refItemKeyOf}, which picks the case the value
 * holds.
 */
export function refItemKey(shape: RefShape, value: Message): string {
	const stub = (refStub(shape, value) ?? create(shape.targetSchema)) as unknown as Rec;
	const inner = refInnerCase(shape);
	if (shape.workspaceField === undefined && !inner) {
		return encodeItemKey(stub as unknown as Message, shape.keyFields);
	}
	const parts: string[] = [];
	if (shape.workspaceField !== undefined) {
		const ws = (value as unknown as Rec)[shape.workspaceField.localName];
		parts.push(escapeKeySegment(typeof ws === 'string' ? ws : '', true));
	}
	if (inner) parts.push(escapeKeySegment(shape.valueField!.name, true));
	for (const kf of shape.keyFields) {
		parts.push(escapeKeySegment(keyComponent(kf, stub[kf.localName]), true));
	}
	return parts.join('/');
}

/**
 * Renders one stored value of a repeated reference message field as the mask
 * segment that addresses it (see {@link refItemKey}), whichever case the value
 * holds. A value of a polymorphic reference message that holds no case renders
 * its workspace, an empty case and no key. Mirror of lww.RefItemKey, byte for
 * byte. Throws when the field is not a reference message field.
 */
export function refItemKeyOf(field: DescField, value: Message): string {
	const shapes = refShapes(field);
	if (shapes.length === 0 || shapes[0]!.valueField === undefined) {
		throw new Error(`${field.parent.typeName}.${field.name} is not a reference message field`);
	}
	for (const shape of shapes) {
		if (shapes.length === 1 || refStub(shape, value) !== undefined) return refItemKey(shape, value);
	}
	// No case set: the shape refItemKey renders, with the case and the key
	// both empty, so the segment still has every part a decoder expects.
	const shape = shapes[0]!;
	if (shape.workspaceField === undefined) return '/';
	const ws = (value as unknown as Rec)[shape.workspaceField.localName];
	return escapeKeySegment(typeof ws === 'string' ? ws : '', true) + '//';
}

/** The target entity's message schema of a reference field, or undefined. */
export function refTarget(field: DescField): DescMessage | undefined {
	return refShape(field)?.targetSchema;
}

/**
 * Whether a reference's stored values may name a workspace other than the
 * holder's: the reference message declares a workspace field.
 */
export function refQualified(shape: RefShape): boolean {
	return shape.workspaceField !== undefined;
}

/**
 * The accessor names from a stored reference value to one key field of the
 * target: `[key]` for an entity-typed reference, `[value, key]` for a
 * reference message.
 */
export function refKeyPath(shape: RefShape, keyField: DescField): string[] {
	return shape.valueField === undefined
		? [ keyField.localName ]
		: [ shape.valueField.localName, keyField.localName ];
}

/**
 * The target stub inside one stored reference value: the value itself for an
 * entity-typed reference, the embedded entity message for a reference
 * message. Undefined for a reference message holding no stub, which points at
 * nothing.
 */
export function refStub(shape: RefShape, value: Message): Message | undefined {
	if (shape.valueField === undefined) return value;
	const inner = fieldValueOf(value as unknown as Rec, shape.valueField);
	return inner === undefined || inner === null ? undefined : inner as Message;
}

/**
 * The workspace one stored reference value names, or undefined when it names
 * none or the reference has no workspace field — both mean the holder's own
 * workspace.
 */
export function refStubWorkspace(shape: RefShape, value: Message): string | undefined {
	if (shape.workspaceField === undefined) return undefined;
	const ws = (value as unknown as Rec)[shape.workspaceField.localName];
	return typeof ws === 'string' && ws !== '' ? ws : undefined;
}

/**
 * The workspace a stored reference value resolves in, given the holder's:
 * the one it names, else the holder's own.
 */
export function refWorkspaceOf(shape: RefShape, value: Message, holderWorkspace: string): string {
	return refStubWorkspace(shape, value) ?? holderWorkspace;
}

/**
 * Whether a message declares (syncplane.v1.reference), well-formed or not.
 */
export function isReferenceMessage(desc: DescMessage): boolean {
	let declared = referenceMessages.get(desc);
	if (declared === undefined) {
		declared = hasOption(desc, referenceExt);
		referenceMessages.set(desc, declared);
	}
	return declared;
}

/**
 * Reads a reference message's fields — one shape per entity-typed field, in
 * declaration order — or returns the reason it breaks the rules: not itself an
 * entity; its target named by exactly one singular entity-typed field, or by a
 * oneof whose members are all singular entity-typed fields; at most one
 * singular string field marked workspace, outside any oneof; no other fields.
 * Mirrors the engine's registration check.
 */
function parseReferenceMessage(desc: DescMessage): RefShape[] | string {
	if (entityOptionsOf(desc)) {
		return `reference message ${desc.typeName} is also an entity; a reference message wraps an entity, it is not one`;
	}
	const values: { field: DescField; keyFields: DescField[] }[] = [];
	let group: DescOneof | undefined;
	let workspaceField: DescField | undefined;
	for (const field of desc.fields) {
		const behavior = fieldBehaviorOf(field);
		if (behavior?.workspace) {
			if (field.oneof) {
				return `reference message ${desc.typeName}: workspace field "${field.name}" is a member of oneof "${field.oneof.name}"; the workspace names where every case resolves, so it sits beside the oneof`;
			}
			if (field.fieldKind !== 'scalar' || field.scalar !== ScalarType.STRING) {
				return `reference message ${desc.typeName}: workspace field "${field.name}" must be a singular string`;
			}
			if (workspaceField) {
				return `reference message ${desc.typeName}: fields "${workspaceField.name}" and "${field.name}" are both marked workspace; a reference names one workspace`;
			}
			workspaceField = field;
			continue;
		}
		if (field.fieldKind === 'message') {
			const kf = entityKeyFields(field.message);
			if (kf) {
				// Several entity-typed fields name one target each only as the
				// members of ONE oneof: the stored value then holds exactly one
				// case, which is what makes it a reference rather than several.
				if (values.length > 0 && (!field.oneof || field.oneof !== group)) {
					return `reference message ${desc.typeName}: fields "${
						values[0]!.field.name
					}" and "${field.name}" are both entity-typed; a reference names one target, or one of several as the members of a single oneof`;
				}
				group = field.oneof;
				values.push({ field, keyFields: kf });
				continue;
			}
		}
		return `reference message ${desc.typeName}: field "${field.name}" is neither the entity field nor a workspace field; a reference message holds exactly those`;
	}
	if (values.length === 0) {
		return `reference message ${desc.typeName} has no entity-typed field to name its target`;
	}
	return values.map(({ field, keyFields }) => ({
		targetSchema: field.message as DescMessage,
		keyFields,
		refSchema: desc,
		valueField: field,
		workspaceField,
		...(field.oneof ? { caseLocalName: field.localName } : {}),
	}));
}

/**
 * Registration-time guard: every message type reachable from an entity that
 * declares (syncplane.v1.reference) must be well-formed, and a field marked
 * workspace may sit nowhere else. Recursive types are visited once. Mirrors
 * the engine's registration check.
 */
function validateReferenceMessages(schema: DescMessage): void {
	const seen = new Set<string>();
	const walk = (desc: DescMessage): void => {
		if (seen.has(desc.typeName)) return;
		seen.add(desc.typeName);
		if (isReferenceMessage(desc)) {
			const parsed = parseReferenceMessage(desc);
			if (typeof parsed === 'string') throw new Error(parsed);
		} else {
			for (const field of desc.fields) {
				const behavior = fieldBehaviorOf(field);
				if (behavior?.workspace) {
					throw new Error(
						`field "${field.name}" is marked workspace, but ${desc.typeName} is not a reference message; the flag belongs inside a message declaring (syncplane.v1.reference)`,
					);
				}
			}
		}
		for (const field of desc.fields) {
			const sub = field.fieldKind === 'message'
				? field.message
				: field.fieldKind === 'list' && field.listKind === 'message'
				? field.message
				: field.fieldKind === 'map' && field.mapKind === 'message'
				? field.message
				: undefined;
			if (sub) walk(sub);
		}
	};
	walk(schema);
}

/**
 * Registration-time guard for (syncplane.v1.field_behavior).reverse_relation:
 * only a reference field declared on the entity itself has a reverse
 * relation, and the name must satisfy the relation-name rule. The walk covers
 * the embedded messages too, and does not enter an entity-typed field: the
 * target's own fields name the target's relations. Mirrors the engine's
 * checkReverseRelations.
 */
function validateReverseRelations(schema: DescMessage): void {
	for (const field of schema.fields) {
		const name = fieldBehaviorOf(field)?.reverseRelation;
		if (!name) continue;
		if (!isRef(field)) {
			throw new Error(
				`field "${field.name}" sets reverse_relation, but it is not a reference; only a reference has a reverse relation`,
			);
		}
		validateRelationName(name, `field "${field.name}"`);
	}
	const seen = new Set<string>([ schema.typeName ]);
	const walk = (desc: DescMessage): void => {
		for (const field of desc.fields) {
			const sub = field.fieldKind === 'message'
				? field.message
				: field.fieldKind === 'list' && field.listKind === 'message'
				? field.message
				: field.fieldKind === 'map' && field.mapKind === 'message'
				? field.message
				: undefined;
			if (!sub || seen.has(sub.typeName) || entityOptionsOf(sub)) continue;
			seen.add(sub.typeName);
			for (const f of sub.fields) {
				if (fieldBehaviorOf(f)?.reverseRelation) {
					throw new Error(
						`field "${f.name}" of ${sub.typeName} sets reverse_relation, but only a reference field declared on an entity has a reverse relation`,
					);
				}
			}
			walk(sub);
		}
	};
	walk(schema);
}

/**
 * An entity's key fields when its key can identify a stub, else undefined.
 * This is the detection rule for an entity-typed reference: no annotation on
 * the field, the target message being an entity makes it one.
 */
function entityKeyFields(desc: DescMessage): DescField[] | undefined {
	let keys = entityKeys.get(desc);
	if (keys === undefined) {
		keys = computeEntityKeyFields(desc) ?? null;
		entityKeys.set(desc, keys);
	}
	return keys ?? undefined;
}

function computeEntityKeyFields(desc: DescMessage): DescField[] | undefined {
	const names = entityOptionsOf(desc)?.keyFields;
	if (!names || names.length === 0) return undefined;
	const out: DescField[] = [];
	for (const n of names) {
		const f = desc.fields.find((ff) => ff.name === n);
		// A target whose key sits inside a oneof cannot be registered at all, and
		// a stub projected onto a field that is only sometimes there would not
		// identify a row. Not a ref, then.
		if (!f || f.oneof) return undefined;
		out.push(f);
	}
	return out;
}

/**
 * A forward reference: a ref field of one entity pointing at another, or one
 * CASE of a polymorphic one. The cases of one relation share a `localName` —
 * the relation accessor, which for a oneof on the holder is the oneof's — and
 * differ in `caseName`; a holder row takes at most one of them.
 */
export interface ForwardRef {
	/** Proto field name of the stored field (wire form). */
	fieldName: string;
	/** The relation accessor: the holder's oneof for a member of one, else the field's own. */
	localName: string;
	/** The accessor of the field deciding this case; undefined for a reference to one entity. */
	caseName?: string;
	/** The stored field. */
	field: DescField;
	targetEntity: string;
	targetSchema: DescMessage;
	/** To-many (repeated) when true; to-one otherwise. */
	repeated: boolean;
	/** Number of key fields in the target (1 = single key, >1 = composite). */
	keyArity: number;
	/** Where the key and workspace sit inside a stored value. */
	shape: RefShape;
}

/**
 * A reverse relation: the inverse of one forward case, surfaced on the case's
 * TARGET entity. Its name is the holder field's
 * (syncplane.v1.field_behavior).reverse_relation, or
 * `<plural(source)>By<Field>` in camelCase when the field sets none
 * (`issuesByAssignee` for `issue.assignee`).
 */
export interface ReverseRef {
	name: string;
	sourceEntity: string;
	sourceSchema: DescMessage;
	/** The forward relation this reverses (its accessor). */
	viaField: string;
	/** The stored field this reverses (proto name). */
	viaFieldName: string;
	/** The stored field this reverses. */
	holderField: DescField;
	/**
	 * The query-builder path to the case this reverses, in accessor names: the
	 * field, through the oneofs on the way — `[ 'parent', 'client' ]` for a
	 * member of the holder's oneof, `[ 'owner', 'owner', 'project' ]` for a
	 * case inside a reference message.
	 */
	viaPath: string[];
	repeated: boolean;
	/** The forward reference's stored shape. */
	shape: RefShape;
}

/**
 * Strips every embedded reference stub of a message down to the target's key
 * fields, mirroring the server's projectRefs exactly. Returns the input
 * unchanged when the entity has no ref fields; otherwise a projected clone (the
 * caller's value is never mutated). Idempotent.
 *
 * Throws when a reference message names a workspace that fails the
 * workspace-id rule: no client could hold such a workspace, so the reference
 * could never resolve, and the server refuses the same write.
 */
export function projectRefs<D extends DescMessage>(
	schema: D,
	value: MessageShape<D>,
): MessageShape<D> {
	const refs = refFieldsOf(schema);
	if (refs.length === 0) return value;
	const out = clone(schema, value);
	const rec = out as unknown as Rec;
	for (const [ field, shapes ] of refs) {
		if (field.fieldKind === 'list') {
			const list = rec[field.localName] as Message[] | undefined;
			if (list) rec[field.localName] = list.map((e) => projectValue(shapes, e));
		} else {
			const stub = fieldValueOf(rec, field) as Message | undefined;
			if (stub !== undefined) setFieldValue(rec, field, projectValue(shapes, stub));
		}
	}
	return out;
}

/** A schema's reference fields with their shapes, in field order. */
function refFieldsOf(schema: DescMessage): [DescField, RefShape[]][] {
	let refs = schemaRefFields.get(schema);
	if (refs === undefined) {
		refs = [];
		for (const field of schema.fields) {
			const shapes = refShapes(field);
			if (shapes.length > 0) refs.push([ field, shapes ]);
		}
		schemaRefFields.set(schema, refs);
	}
	return refs;
}

/**
 * One stored reference value projected down: the target stub keeps only its
 * key fields, and a reference message keeps its workspace around the projected
 * stub — of the case it holds, for a polymorphic one.
 */
function projectValue(shapes: RefShape[], value: Message): Message {
	const first = shapes[0]!;
	if (first.valueField === undefined || first.refSchema === undefined) {
		return projectStub(first.targetSchema, value, first.keyFields);
	}
	const fresh = create(first.refSchema) as unknown as Rec;
	const rec = value as unknown as Rec;
	if (first.workspaceField) {
		const ws = rec[first.workspaceField.localName];
		if (typeof ws === 'string' && ws !== '') {
			validateWorkspaceId(ws);
			fresh[first.workspaceField.localName] = ws;
		}
	}
	for (const shape of shapes) {
		const inner = refStub(shape, value);
		if (inner !== undefined) {
			setFieldValue(fresh, shape.valueField!, projectStub(shape.targetSchema, inner, shape.keyFields));
		}
	}
	return fresh as unknown as Message;
}

/** A fresh stub of the target type carrying only its key fields. */
function projectStub(schema: DescMessage, stub: Message, keyFields: DescField[]): Message {
	const fresh = create(schema) as unknown as Rec;
	const rec = stub as unknown as Rec;
	for (const kf of keyFields) {
		fresh[kf.localName] = rec[kf.localName];
	}
	return fresh as unknown as Message;
}

/** Combines an opaque field's current serialization with an incoming one. */
export type MergeFn = (base: Uint8Array, incoming: Uint8Array) => Uint8Array;

/** Per-entity merge hooks keyed by dotted proto field path. */
export type EntityMergers = Map<string, MergeFn>;

/**
 * Client mirror of a server merge hook (Server.RegisterMerger). field is the
 * dotted proto path of an opaque bytes/message field. Go/JS parity is the
 * app's responsibility — divergence self-heals on rebase.
 */
export interface MergerSpec {
	schema: DescMessage;
	field: string;
	merge: MergeFn;
}

export class EntityRegistry {
	private byName = new Map<string, EntityDef>();
	private byType = new Map<string, EntityDef>();
	private mergersByName = new Map<string, EntityMergers>();
	/** Dotted proto paths of each entity's text fields (empty when none). */
	private textPathsByName = new Map<string, string[]>();
	/** Forward refs keyed by source entity name. */
	private forwardRefs = new Map<string, ForwardRef[]>();
	/** Reverse relations keyed by target entity name. */
	private reverseRefs = new Map<string, ReverseRef[]>();

	constructor(schemas: readonly DescMessage[]) {
		for (const schema of schemas) {
			const def = entityDefOf(schema);
			if (this.byName.has(def.name)) {
				throw new Error(`entity "${def.name}" registered twice`);
			}
			validateOneofs(schema);
			validateReferenceMessages(schema);
			validateReverseRelations(schema);
			validateOrderFields(schema);
			warnShadowedQueryFields(schema);
			this.installTextMergers(def);
			this.byName.set(def.name, def);
			this.byType.set(schema.typeName, def);
		}
		this.buildRefGraph();
	}

	/**
	 * Auto-installs the convergent-text merger for every
	 * (syncplane.v1.field_behavior).text field of an entity (top-level and nested)
	 * — the client mirror of the server's registration-time install, so
	 * optimistic rebase merges text exactly like the server does. Validates
	 * each field: singular bytes, no key_field/order_field.
	 */
	private installTextMergers(def: EntityDef): void {
		const paths = collectTextFieldPaths(def.schema, '', []);
		if (paths.length === 0) return;
		this.textPathsByName.set(def.name, paths);
		const entityMergers: EntityMergers = new Map();
		for (const path of paths) {
			entityMergers.set(path, mergeText);
		}
		this.mergersByName.set(def.name, entityMergers);
	}

	/** Dotted proto paths of an entity's text fields (empty if none). */
	textPathsOf(entity: string): readonly string[] {
		return this.textPathsByName.get(entity) ?? [];
	}

	get(name: string): EntityDef | undefined {
		return this.byName.get(name);
	}

	bySchema(schema: DescMessage): EntityDef {
		const def = this.byType.get(schema.typeName);
		if (!def) throw new Error(`${schema.typeName} is not a registered entity`);
		return def;
	}

	names(): string[] {
		return [ ...this.byName.keys() ];
	}

	/**
	 * names() without the server-only entities: the ones a store can ever hold
	 * rows of. Every path that asks "which entities does this client sync" goes
	 * through here, so a registered-but-never-synced entity neither claims local
	 * state nor forces work to acquire state that is never sent.
	 */
	syncableNames(): string[] {
		return [ ...this.byName.values() ].filter((d) => !d.serverOnly).map((d) => d.name);
	}

	/** Forward refs declared by an entity (empty if none). */
	forwardRefsOf(entity: string): ForwardRef[] {
		return this.forwardRefs.get(entity) ?? [];
	}

	/** Reverse relations that target an entity (empty if none). */
	reverseRefsOf(entity: string): ReverseRef[] {
		return this.reverseRefs.get(entity) ?? [];
	}

	/**
	 * The cases of a forward relation of an entity, by its accessor
	 * (localName): one for a reference to one entity, one per case for a
	 * polymorphic one, empty when the entity has no such relation.
	 */
	forwardCasesOf(entity: string, localName: string): ForwardRef[] {
		return this.forwardRefs.get(entity)?.filter((r) => r.localName === localName) ?? [];
	}

	/** Looks up a reverse relation on a target entity by its name. */
	reverseRef(entity: string, name: string): ReverseRef | undefined {
		return this.reverseRefs.get(entity)?.find((r) => r.name === name);
	}

	/**
	 * Builds the reference graph. Forward refs come straight from each entity's
	 * ref fields (validated: the target must be a registered entity). Each
	 * forward case adds one reverse relation to its target, named by the holder
	 * field alone (see {@link ReverseRef}), so a name never changes when a
	 * sibling reference is added. Field names are unique, so two default names
	 * from different fields of one entity never clash; a reverse_relation, two
	 * cases of one polymorphic reference message on one target, or a forward
	 * ref field of the same name can. A clash throws, matching the entity-facade
	 * precedent.
	 *
	 * The two halves share ONE namespace per entity — what a follow segment, an
	 * `include` key and `client.db.<entity>.refs` all resolve against — so a
	 * reverse name must be unique against BOTH the other reverse names and that
	 * entity's forward ref fields. The engine's mirror of this builder rejects
	 * the same two clashes with the same wording.
	 */
	private buildRefGraph(): void {
		for (const [ name, def ] of this.byName) {
			const fields = [ ...def.schema.fields ].sort((a, b) => (a.name < b.name ? -1 : 1));
			const fwd: ForwardRef[] = [];
			for (const field of fields) {
				for (const shape of refShapes(field)) {
					const targetDef = this.byType.get(shape.targetSchema.typeName);
					// A ref whose target entity is not registered in this client cannot
					// be resolved or followed locally, so it is omitted from the graph
					// (it is not exposed as an accessor). Projection still applies — that
					// reads the target's own (syncplane.v1.entity) options, not the registry.
					if (!targetDef) continue;
					fwd.push({
						fieldName: field.name,
						localName: refSegmentOf(field),
						...(shape.caseLocalName !== undefined ? { caseName: shape.caseLocalName } : {}),
						field,
						targetEntity: targetDef.name,
						targetSchema: shape.targetSchema,
						repeated: field.fieldKind === 'list',
						keyArity: shape.keyFields.length,
						shape,
					});
				}
			}
			this.forwardRefs.set(name, fwd);
		}
		for (const [ source, refs ] of this.forwardRefs) {
			const sourceSchema = this.byName.get(source)!.schema;
			for (const r of refs) {
				const rev: ReverseRef = {
					name: reverseRelationName(source, r.field),
					sourceEntity: source,
					sourceSchema,
					viaField: r.localName,
					viaFieldName: r.fieldName,
					holderField: r.field,
					viaPath: refCasePath(r.field, r.shape),
					repeated: r.repeated,
					shape: r.shape,
				};
				const list = this.reverseRefs.get(r.targetEntity) ?? [];
				const prev = list.find((x) => x.name === rev.name);
				if (prev) throw reverseClashError(rev.name, r.targetEntity, prev, rev);
				// Forward names are complete by now (the loop above ran over every
				// entity), so this sees the whole namespace a segment resolves
				// against. Shadowing is worse than ambiguity: every resolver tries
				// forward first, so the reverse half would be unreachable and the
				// name would answer with a different row set, silently.
				if (this.forwardRefs.get(r.targetEntity)!.some((x) => x.localName === rev.name)) {
					const origin = reverseOrigin(rev);
					throw new Error(
						`reverse relation "${rev.name}" on entity "${r.targetEntity}" collides with the reference field of that name; `
							+ `set (syncplane.v1.field_behavior).reverse_relation on ${origin} to name it otherwise`,
					);
				}
				list.push(rev);
				this.reverseRefs.set(r.targetEntity, list);
			}
		}
	}

	/**
	 * Installs merge hooks; validates each path lands on an opaque field.
	 * Text fields already carry the auto-installed text merger — a manual
	 * spec for one is a registration error, matching the server.
	 */
	setMergers(specs: MergerSpec[]): void {
		// Rebuild from scratch, re-installing the schema-derived text mergers
		// first so a repeated setMergers call cannot wipe them.
		this.mergersByName.clear();
		for (const def of this.byName.values()) {
			this.installTextMergers(def);
		}
		for (const spec of specs) {
			const def = this.bySchema(spec.schema);
			validateMergerField(spec.schema, spec.field);
			let entityMergers = this.mergersByName.get(def.name);
			if (!entityMergers) {
				entityMergers = new Map();
				this.mergersByName.set(def.name, entityMergers);
			}
			if (entityMergers.has(spec.field)) {
				throw new Error(`merger for ${def.name}.${spec.field} registered twice`);
			}
			entityMergers.set(spec.field, spec.merge);
		}
	}

	mergersFor(entityName: string): EntityMergers | undefined {
		return this.mergersByName.get(entityName);
	}
}

function validateMergerField(schema: DescMessage, fieldPath: string): void {
	const segs = fieldPath.split('.');
	let desc: DescMessage | undefined = schema;
	let field: DescField | undefined;
	for (let i = 0; i < segs.length; i++) {
		if (!desc) throw new Error(`merger path ${fieldPath}: "${segs[i - 1]}" is not a message field`);
		field = desc.fields.find((f) => f.name === segs[i]);
		if (!field) {
			throw new Error(`merger path ${fieldPath}: ${desc.typeName} has no field "${segs[i]}"`);
		}
		// A oneof is replaced whole, so a merger anywhere inside one could never
		// fire — the same reason mergers cannot nest under an opaque field.
		if (field.oneof) {
			throw new Error(
				`merger path ${fieldPath}: "${segs[i]}" is inside oneof "${field.oneof.name}"; `
					+ `a oneof is replaced whole, so a merger there could never run`,
			);
		}
		if (i < segs.length - 1) {
			if (field.fieldKind !== 'message') {
				throw new Error(`merger path ${fieldPath}: "${segs[i]}" is not a singular message field`);
			}
			if (isOpaque(field)) {
				throw new Error(
					`merger path ${fieldPath}: "${segs[i]}" is opaque; mergers cannot nest under opaque fields`,
				);
			}
			desc = field.message;
		}
	}
	if (!field || !isOpaque(field)) {
		throw new Error(`merger path ${fieldPath}: field is not opaque`);
	}
	if (field.fieldKind !== 'message' && field.fieldKind !== 'scalar') {
		throw new Error(`merger path ${fieldPath}: field must be bytes or message`);
	}
	if (field.fieldKind === 'scalar' && field.scalar !== ScalarType.BYTES) {
		throw new Error(`merger path ${fieldPath}: field must be bytes or message`);
	}
}

/**
 * Derives the row key from the entity's key fields, rendering and joining them
 * by the shared rules in naming.ts (which engine/server/server.go mirrors). It
 * does not validate the result: reads derive keys too (refs.ts), so the check
 * belongs on the write path — see the crud builders below.
 */
export function keyOf(def: EntityDef, msg: Message): string {
	const record = msg as unknown as Record<string, unknown>;
	return rowKey(def.keyFields.map((f) => keyComponent(f, record[f.localName])));
}

/**
 * The name of the reverse relation a reference field of `source` adds to its
 * target: the field's reverse_relation, else `<plural(source)>By<Field>` in
 * camelCase — `issuesByAssignee` for `issue.assignee`, `taskListsByOwner` for
 * `task_list.owner`. Mirrors the engine's reverseRelationName.
 */
function reverseRelationName(source: string, field: DescField): string {
	const explicit = fieldBehaviorOf(field)?.reverseRelation;
	if (explicit) return explicit;
	// From the proto name rather than localName or jsonName: localName escapes
	// reserved words and jsonName follows a json_name option, and the engine
	// derives the name from the proto name alone.
	return `${pluralize(protoCamelCase(source))}By${capitalizeFirst(protoCamelCase(field.name))}`;
}

/** Naive English pluralization for the default reverse-relation name. */
function pluralize(s: string): string {
	if (/[^aeiou]y$/.test(s)) return s.slice(0, -1) + 'ies';
	if (/(s|x|z|ch|sh)$/.test(s)) return s + 'es';
	return s + 's';
}

function capitalizeFirst(s: string): string {
	return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * The forward case a reverse relation reverses as a dotted proto path: source
 * entity, holder field and, for a case inside a reference message, that
 * case's field.
 */
function reverseOrigin(rev: ReverseRef): string {
	const origin = `${rev.sourceEntity}.${rev.viaFieldName}`;
	return refInnerCase(rev.shape) ? `${origin}.${rev.shape.valueField!.name}` : origin;
}

/**
 * Two forward cases whose reverse relations take one name on one target,
 * named in sorted order so the message does not depend on which was seen
 * first. The advice differs for two cases of one field: the field gives every
 * case one name, so reverse_relation cannot tell them apart. Mirrors the
 * engine's reverseClashError.
 */
function reverseClashError(name: string, target: string, a: ReverseRef, b: ReverseRef): Error {
	const [ first, second ] = [ reverseOrigin(a), reverseOrigin(b) ].sort();
	const advice = a.holderField === b.holderField
		? `${a.sourceEntity}.${a.viaFieldName} gives all its cases one reverse relation name, so cases that point at one entity need fields of their own`
		: 'set (syncplane.v1.field_behavior).reverse_relation on one of them to give it another name';
	return new Error(
		`reverse relation "${name}" on entity "${target}" is named by both ${first} and ${second}; ${advice}`,
	);
}

// A field is replaced/merged whole when explicitly opaque, or when it is a
// collaborative-text field (text implies opaque — the encoded document is one
// atomic value merged by the auto-installed text merger).
function isOpaque(field: DescField): boolean {
	const b = fieldBehaviorOf(field);
	return b !== undefined && (b.opaque || b.text);
}

/** Whether a field carries (syncplane.v1.field_behavior).text. */
export function isTextField(field: DescField): boolean {
	return fieldBehaviorOf(field)?.text ?? false;
}

/**
 * Applies the masked fields of src onto dst (same message type) — the TS
 * mirror of the server's LWW merge. Paths use proto field names
 * ("settings.color"). Opaque fields are replaced whole, unless a merger is
 * registered for the field and both sides carry a value, in which case the
 * merger combines them (mirroring lww.MergeWithMergers).
 *
 * Mutates `dst` in place and leaves `src` untouched. Message, list and map
 * values are cloned on the way across; a `bytes` value is shared by reference,
 * so treat it as immutable afterwards.
 *
 * Throws if a path names no field of `schema`, names a oneof member rather than
 * its group, or sub-paths a field that cannot carry one. `dst` may be partially
 * merged when it throws — paths are applied in order.
 *
 * @example Step a row back one entry of its field history. A text field lands
 * as the plain-string value the record carries, which `plainTextOf` reads.
 * ```ts
 * const before = fromBinary(IssueSchema, rec.prior);
 * mergeMask(IssueSchema, row, before, rec.mask?.paths ?? []);
 * ```
 */
export function mergeMask<T extends Message>(
	schema: DescMessage,
	dst: T,
	src: T,
	paths: string[],
	mergers?: EntityMergers,
): void {
	for (const path of paths) {
		mergePath(schema, dst as unknown as Rec, src as unknown as Rec, path, '', mergers);
	}
}

type Rec = Record<string, unknown>;

function mergePath(
	schema: DescMessage,
	dst: Rec,
	src: Rec,
	path: string,
	prefix: string,
	mergers?: EntityMergers,
): void {
	const [ seg, rest ] = splitSeg(path);
	const field = schema.fields.find((f) => f.name === seg);
	if (!field) {
		// A oneof is one field here, named by the GROUP: a segment that resolves
		// to no field may still be a oneof name.
		const oneof = schema.oneofs.find((o) => o.name === seg);
		if (oneof) {
			if (rest !== '') throw new Error(`oneof "${seg}" does not support sub-paths`);
			copyOneof(schema, dst, src, oneof);
			return;
		}
		throw new Error(`unknown field "${seg}" in mask for ${schema.typeName}`);
	}
	if (field.oneof) {
		throw new Error(
			`field "${seg}" is a member of oneof "${field.oneof.name}"; mask the oneof instead`,
		);
	}

	const opaque = isOpaque(field);
	const collection = field.fieldKind === 'list' || field.fieldKind === 'map';
	// An empty item key encodes as the empty segment, so the path of that item
	// ends in the separator ('attrs.'). Only the absence of any separator makes
	// a bare path: reading 'attrs.' as 'attrs' would replace the whole
	// collection with the writer's view of it. Mirrors lww.go.
	const bare = rest === '' && (seg.length === path.length || !collection);
	// Whole-replace cases: a bare path, an opaque field (the escape hatch —
	// replaced/merged whole even when the mask names a sub-path into it), or a
	// singular reference message, whose workspace and key must never split
	// across a masked write. A repeated one falls through to mergeItem, which
	// keeps the two together by addressing each element as one value. Mirrors
	// lww.go.
	if (bare || opaque || (isRefMessageField(field) && field.fieldKind !== 'list')) {
		const merge = mergers?.get(prefix + seg);
		if (merge && opaque && applyMerger(dst, src, field, merge)) {
			return;
		}
		copyField(dst, src, field);
		return;
	}

	// An item path and non-opaque: a collection merges per item, a singular
	// message descends, a scalar cannot be sub-pathed.
	if (collection) {
		mergeItem(dst, src, field, seg, rest, prefix, mergers);
		return;
	}
	if (field.fieldKind !== 'message') {
		throw new Error(`field "${seg}" does not support sub-paths`);
	}

	// An absent src submessage means the writer cleared the subtree the mask
	// points into, so the same path is cleared on dst; the zero message stands
	// in as the source either way.
	const srcSub = src[field.localName] as Rec | undefined;
	if (srcSub === undefined && dst[field.localName] === undefined) return;
	const dstSub = (dst[field.localName] ??= create(field.message)) as Rec;
	mergePath(
		field.message,
		dstSub,
		srcSub ?? (create(field.message) as unknown as Rec),
		rest,
		prefix + seg + '.',
		mergers,
	);
	// A leaf write never moves its container's presence on its own: a container
	// left empty by the descent is dropped, whether it was already there or was
	// created just above. Mirrors lww.go — see the note there for the trade-off.
	if (isEmptyMessage(field.message, dstSub)) {
		delete dst[field.localName];
	}
}

/**
 * Whether a message carries no populated field and no unknown bytes. Unknown
 * fields count as content: a row written by a newer schema must not lose a
 * submessage because this build cannot see inside it (`equals` ignores them,
 * hence the explicit check).
 */
function isEmptyMessage(schema: DescMessage, msg: Rec): boolean {
	if ((msg as { $unknown?: unknown[] }).$unknown?.length) return false;
	return equals(schema, msg as unknown as Message, create(schema));
}

/**
 * Merges a single addressed item of a map or list field. afterField is the mask
 * remainder after the field name: "<key>" for a whole-item write/clear, or
 * "<key>.<deeper>" to descend into a message item. The leading "<key>" is the
 * item identity — a (possibly escaped) map key, encoded message-list key, or
 * scalar element value. Mirror of lww.go mergeItem.
 */
function mergeItem(
	dst: Rec,
	src: Rec,
	field: DescField,
	fieldSeg: string,
	afterField: string,
	prefix: string,
	mergers?: EntityMergers,
): void {
	const [ itemKey, rest ] = splitSeg(afterField);
	if (field.fieldKind === 'map') {
		mergeMapItem(dst, src, field, itemKey, rest, prefix + fieldSeg + '.', mergers);
		return;
	}
	if (field.fieldKind === 'list' && field.listKind === 'message') {
		if (isRefMessageField(field)) {
			mergeRefSetItem(dst, src, field, itemKey);
			return;
		}
		const keyFields = keyFieldsFor(field);
		if (keyFields.length === 0) {
			// No determinable identity: fall back to whole-replace (expansion never
			// emits such a sub-path).
			copyField(dst, src, field);
			return;
		}
		mergeKeyedListItem(dst, src, field, keyFields, itemKey, rest, prefix + fieldSeg + '.', mergers);
		return;
	}
	mergeScalarSetItem(dst, src, field, itemKey, rest);
}

/**
 * Applies one element of a repeated reference message. The stored element is
 * nothing but its identity — the workspace beside the target key, see
 * {@link refItemKey} — so the list merges like a scalar set: src has the
 * element => ensure present once, src lacks it => remove every copy. A
 * sub-path below the element addresses the element, the same way a sub-path
 * into a singular reference message names the whole message. Mirror of lww.go
 * mergeRefSetItem.
 */
function mergeRefSetItem(
	dst: Rec,
	src: Rec,
	field: MessageListField,
	itemKey: string,
): void {
	const srcList = (src[field.localName] as Message[] | undefined) ?? [];
	const srcElem = srcList.find((e) => refItemKeyOf(field, e) === itemKey);
	const dstList = (dst[field.localName] as Message[] | undefined) ?? [];
	const kept = dstList.filter((e) => refItemKeyOf(field, e) !== itemKey);
	if (srcElem !== undefined) kept.push(clone(field.message, srcElem));
	dst[field.localName] = kept;
}

type MapField = Extract<DescField, { fieldKind: 'map' }>;
type MessageListField = Extract<DescField, { listKind: 'message' }>;

function mergeMapItem(
	dst: Rec,
	src: Rec,
	field: MapField,
	itemKey: string,
	rest: string,
	prefix: string,
	mergers?: EntityMergers,
): void {
	const srcMap = (src[field.localName] as Rec | undefined) ?? {};
	const jsKey = decodeMapKey(field, itemKey);
	if (Object.hasOwn(srcMap, jsKey)) {
		const dm = (dst[field.localName] ??= {}) as Rec;
		if (rest === '') {
			dm[jsKey] = field.mapKind === 'message'
				? clone(field.message, srcMap[jsKey] as Message)
				: srcMap[jsKey];
			return;
		}
		if (field.mapKind !== 'message') {
			throw new Error(`map "${field.name}" value does not support sub-paths`);
		}
		if (dm[jsKey] === undefined) dm[jsKey] = create(field.message);
		mergePath(field.message, dm[jsKey] as Rec, srcMap[jsKey] as Rec, rest, prefix, mergers);
		return;
	}
	// src lacks the key → clear it on dst (clear-on-absent).
	const dm = dst[field.localName] as Rec | undefined;
	if (dm !== undefined) delete dm[jsKey];
}

function mergeKeyedListItem(
	dst: Rec,
	src: Rec,
	field: MessageListField,
	keyFields: DescField[],
	itemKey: string,
	rest: string,
	prefix: string,
	mergers?: EntityMergers,
): void {
	const srcList = (src[field.localName] as Message[] | undefined) ?? [];
	const srcIdx = srcList.findIndex((e) => encodeItemKey(e, keyFields) === itemKey);
	const dstList = (dst[field.localName] ??= []) as Message[];
	const dstIdx = dstList.findIndex((e) => encodeItemKey(e, keyFields) === itemKey);

	if (srcIdx === -1) {
		// src dropped the element: remove it from dst (clear-on-absent).
		if (dstIdx !== -1) dstList.splice(dstIdx, 1);
		return;
	}
	const srcElem = srcList[srcIdx]!;
	if (rest === '') {
		const cp = clone(field.message, srcElem);
		if (dstIdx !== -1) dstList[dstIdx] = cp;
		else dstList.push(cp);
		return;
	}
	let dstElem: Message;
	if (dstIdx !== -1) {
		dstElem = dstList[dstIdx]!;
	} else {
		dstElem = create(field.message) as Message;
		for (const kf of keyFields) {
			(dstElem as unknown as Rec)[kf.localName] = (srcElem as unknown as Rec)[kf.localName];
		}
		dstList.push(dstElem);
	}
	mergePath(
		field.message,
		dstElem as unknown as Rec,
		srcElem as unknown as Rec,
		rest,
		prefix,
		mergers,
	);
}

function mergeScalarSetItem(
	dst: Rec,
	src: Rec,
	field: DescField,
	itemKey: string,
	rest: string,
): void {
	if (rest !== '') {
		throw new Error(`field "${field.name}" scalar element does not support sub-paths`);
	}
	const srcList = (src[field.localName] as unknown[] | undefined) ?? [];
	let found = false;
	let val: unknown;
	for (const v of srcList) {
		if (encodeScalarElem(field, v) === itemKey) {
			found = true;
			val = v;
			break;
		}
	}
	const dstList = (dst[field.localName] ??= []) as unknown[];
	if (found) {
		if (!dstList.some((v) => encodeScalarElem(field, v) === itemKey)) dstList.push(val);
		return;
	}
	dst[field.localName] = dstList.filter((v) => encodeScalarElem(field, v) !== itemKey);
}

/**
 * Resolves the identity fields of a keyed message list: the explicit key_field
 * behavior, else the element entity's key_fields. Returns [] for a non-message
 * list or when no identity can be determined. Mirror of lww.go keyFieldsFor.
 *
 * The returned array is shared with every other caller and must not be
 * modified.
 */
export function keyFieldsFor(field: DescField): DescField[] {
	let fields = listKeyFields.get(field);
	if (fields === undefined) {
		fields = computeKeyFieldsFor(field);
		listKeyFields.set(field, fields);
	}
	return fields;
}

function computeKeyFieldsFor(field: DescField): DescField[] {
	const elem = field.fieldKind === 'list' && field.listKind === 'message'
		? field.message
		: undefined;
	if (!elem) return [];
	let names: readonly string[] = [];
	const kf = fieldBehaviorOf(field)?.keyField;
	if (kf && kf.length > 0) names = kf;
	if (names.length === 0) names = entityOptionsOf(elem)?.keyFields ?? [];
	if (names.length === 0) return [];
	const fields: DescField[] = [];
	for (const n of names) {
		const f = elem.fields.find((ff) => ff.name === n);
		// An identity that is only present while its case is active is not an
		// identity: elements would collide on the zero value the moment a sibling
		// won. Treated as "no identity", so the list replaces whole. Registration
		// reports it (validateOneofs).
		if (!f || f.oneof) return [];
		fields.push(f);
	}
	return fields;
}

/**
 * Walks a message's fields (descending through singular, non-opaque,
 * non-reference message fields, keyed message lists and message-valued maps)
 * and returns the dotted proto path of every
 * (syncplane.v1.field_behavior).text field, validating its shape. Mirror of the
 * server's collectTextFields.
 *
 * A text field below a keyed list or map is accepted: the element identity is
 * stable, a mask path can address the element ("items.<key>.body"), and the
 * merge descends into it, so the merger fires per element and the field holds
 * one document per element. The returned path is the SCHEMA path with the
 * item keys stripped ("items.body") — the merger key space.
 *
 * oneof names the group crossed on the way here, and unkeyed the message list
 * with no element identity (both empty when none). The walk still descends
 * through oneof members and unkeyed list elements so that a text field below
 * one is reported rather than quietly skipped: its merger could never fire,
 * because a oneof is replaced whole, and an element no mask can address is too.
 */
export function collectTextFieldPaths(
	schema: DescMessage,
	prefix: string,
	visiting: string[],
	oneof = '',
	unkeyed = '',
): string[] {
	const out: string[] = [];
	for (const field of schema.fields) {
		const behavior = fieldBehaviorOf(field);
		const memberOf = field.oneof?.name ?? oneof;
		if (behavior?.text) {
			if (field.fieldKind !== 'scalar' || field.scalar !== ScalarType.BYTES) {
				throw new Error(
					`${schema.typeName}.${field.name}: text field must be a singular bytes field`,
				);
			}
			if (behavior.keyField.length > 0 || behavior.orderField !== '') {
				throw new Error(
					`${schema.typeName}.${field.name}: text field cannot combine with key_field/order_field`,
				);
			}
			if (memberOf !== '') {
				throw new Error(
					`${schema.typeName}.${field.name}: text field is inside oneof "${memberOf}"; `
						+ `a oneof is replaced whole, so the text merger could never run`,
				);
			}
			if (unkeyed !== '') {
				throw new Error(
					`${schema.typeName}.${field.name}: text field is inside ${unkeyed}, `
						+ `whose elements have no identity; an element is replaced whole, so the `
						+ `text merger could never run — give the list a key_field to hold one `
						+ `document per element`,
				);
			}
			out.push(prefix + field.name);
			continue;
		}
		if (behavior?.opaque || isRef(field)) continue;
		// Descend into embedded sub-messages and collection elements — unkeyed
		// list elements purely to refuse text there.
		let sub: DescMessage | undefined;
		let enclosing = unkeyed;
		if (field.fieldKind === 'message') {
			sub = field.message;
		} else if (field.fieldKind === 'list' && field.listKind === 'message') {
			sub = field.message;
			if (keyFieldsFor(field).length === 0) {
				enclosing = `repeated field "${prefix}${field.name}"`;
			}
		} else if (field.fieldKind === 'map' && field.mapKind === 'message') {
			sub = field.message;
		}
		if (sub === undefined || visiting.includes(sub.typeName)) continue;
		out.push(...collectTextFieldPaths(
			sub,
			`${prefix}${field.name}.`,
			[ ...visiting, schema.typeName ],
			memberOf,
			enclosing,
		));
	}
	return out;
}

/**
 * Widens the bytes-typed fields of an init shape to also accept a plain
 * string: collaborative-text fields ((syncplane.v1.field_behavior).text) may
 * be written as strings, which the client diffs into CRDT operations at
 * enqueue time. The widening is recursive — through embedded messages, list
 * elements and map values, where a text field may live — and type-level only:
 * at runtime a string reaching a bytes field that is NOT a text field throws.
 */
export type TextInit<T> = T | TextInitDeep<T>;

type TextInitDeep<T> = NonNullable<T> extends Uint8Array ? T | string
	: NonNullable<T> extends readonly (infer E)[] ? readonly TextInit<E>[]
	: NonNullable<T> extends object ? { [K in keyof NonNullable<T>]?: TextInit<NonNullable<T>[K]> }
	: T;

/**
 * The init a PATCH takes: the fields to change, never a whole row.
 *
 * protobuf-es's own init shape is `Message | partial`, and the message arm is
 * what makes `patch(id, await get(id))` compile — a call that masks every field
 * and so writes back stale values for the ones the caller never touched.
 * Excluding `Message` drops that arm; the `$typeName` literal exists only so
 * that TypeScript prints the way out when someone tries anyway, since a bare
 * "not assignable" on an internal property is what sent them here.
 *
 * `patch` keeps a second overload for an explicit `paths` mask, where a whole
 * message is safe (the mask, not the object, decides what is written).
 */
export type PatchInit<D extends DescMessage> =
	& TextInit<Exclude<MessageInitShape<D>, Message>>
	& { $typeName?: 'a whole message is not a patch: use update() to edit a row, or put()' };

/**
 * Splits string-valued text fields out of an init object: returns the init
 * with those entries removed (shallow-copied along the touched paths; the
 * caller's object is never mutated) plus a map of dotted proto path → string.
 * A string on any other bytes-typed field throws — nothing else may coerce a
 * string into bytes silently.
 *
 * The strings stay strings until enqueue time (see attachTextStrings): the
 * diff must run against the row as it is INSIDE the enqueue transaction, not
 * against whatever the caller last read.
 */
export function extractTextStrings(
	schema: DescMessage,
	init: unknown,
): { init: unknown; texts?: Map<string, string> } {
	if (init === null || typeof init !== 'object') return { init };
	const texts = new Map<string, string>();
	const out = extractFrom(schema, init as Rec, '', texts);
	return texts.size > 0 ? { init: out, texts } : { init };
}

function extractFrom(
	desc: DescMessage,
	obj: Rec,
	prefix: string,
	texts: Map<string, string>,
): Rec {
	let out = obj;
	const ensure = (): Rec => (out === obj ? (out = { ...obj }) : out);
	for (const key of Object.keys(obj)) {
		const field = desc.fields.find((f) => f.localName === key);
		if (!field) continue; // create() reports unknown fields itself
		const v = obj[key];
		if (typeof v === 'string' && field.fieldKind === 'scalar' && field.scalar === ScalarType.BYTES) {
			if (!isTextField(field)) {
				throw new Error(
					`${desc.typeName}.${field.name}: a string can only initialize a text field`,
				);
			}
			texts.set(prefix + field.name, v);
			delete ensure()[key];
			continue;
		}
		if (
			field.fieldKind === 'message' && v !== null && typeof v === 'object' && !Array.isArray(v)
			&& !isOpaque(field) && refKeyFields(field).length === 0
		) {
			const sub = extractFrom(field.message, v as Rec, `${prefix}${field.name}.`, texts);
			if (sub !== v) ensure()[key] = sub;
			continue;
		}
		// A keyed list element or map value may hold text fields of its own;
		// their strings extract under the element's instance path.
		if (field.fieldKind === 'list' && field.listKind === 'message' && Array.isArray(v)) {
			const keyFields = keyFieldsFor(field);
			if (keyFields.length === 0) continue;
			let list = v;
			for (let i = 0; i < v.length; i++) {
				const elem = v[i];
				if (elem === null || typeof elem !== 'object') continue;
				const itemKey = encodeItemKey(elem as Message, keyFields);
				const sub = extractFrom(
					field.message,
					elem as Rec,
					`${prefix}${field.name}.${itemKey}.`,
					texts,
				);
				if (sub !== elem) {
					if (list === v) list = [ ...v ];
					list[i] = sub;
				}
			}
			if (list !== v) ensure()[key] = list;
			continue;
		}
		if (
			field.fieldKind === 'map' && field.mapKind === 'message' && v !== null && typeof v === 'object'
		) {
			let map = v as Rec;
			for (const jsKey of Object.keys(v as Rec)) {
				const entry = (v as Rec)[jsKey];
				if (entry === null || typeof entry !== 'object') continue;
				const sub = extractFrom(
					field.message,
					entry as Rec,
					`${prefix}${field.name}.${encodeMapKey(field, jsKey)}.`,
					texts,
				);
				if (sub !== entry) {
					if (map === (v as Rec)) map = { ...(v as Rec) };
					map[jsKey] = sub;
				}
			}
			if (map !== v) ensure()[key] = map;
		}
	}
	return out;
}

/**
 * Side channel carrying a crud's pending text strings from build time to the
 * enqueue transaction. A WeakMap rather than a field so the CrudMutation
 * stays a plain proto message — persistence and the wire never see the
 * strings, only the document bytes enqueue turns them into.
 */
const crudTextStrings = new WeakMap<CrudMutation, ReadonlyMap<string, string>>();

export function attachTextStrings(
	crud: CrudMutation,
	texts: ReadonlyMap<string, string> | undefined,
): void {
	if (texts !== undefined && texts.size > 0) crudTextStrings.set(crud, texts);
}

/** Removes and returns the crud's pending text strings, if any. */
export function takeTextStrings(crud: CrudMutation): ReadonlyMap<string, string> | undefined {
	const texts = crudTextStrings.get(crud);
	if (texts !== undefined) crudTextStrings.delete(crud);
	return texts;
}

/**
 * One resolved step of a text-field instance path: the field walked through,
 * plus the addressed element when the step crosses a keyed message list
 * (itemKey/keyFields) or a message-valued map (mapKey, the JS object key).
 */
interface TextRefStep {
	field: DescField;
	itemKey?: string;
	keyFields?: DescField[];
	mapKey?: string;
}

/**
 * Resolves a dotted path — instance form, with item keys where it crosses
 * keyed lists and maps — to the steps addressing a text field. Mirror of the
 * server's textFieldRefByPath.
 */
function textFieldSteps(schema: DescMessage, path: string): TextRefStep[] {
	const bad = () => new Error(`${schema.typeName} has no text field "${path}"`);
	const steps: TextRefStep[] = [];
	let cur: DescMessage = schema;
	let rest = path;
	for (;;) {
		const [ seg, afterSeg ] = splitSeg(rest);
		rest = afterSeg;
		const field = cur.fields.find((f) => f.name === seg);
		if (!field) throw bad();
		if (field.fieldKind === 'map') {
			if (field.mapKind !== 'message' || rest === '') throw bad();
			const [ keySeg, afterKey ] = splitSeg(rest);
			rest = afterKey;
			if (rest === '') throw bad();
			steps.push({ field, mapKey: decodeMapKey(field, keySeg) });
			cur = field.message;
			continue;
		}
		if (field.fieldKind === 'list') {
			const keyFields = keyFieldsFor(field);
			if (field.listKind !== 'message' || keyFields.length === 0 || rest === '') throw bad();
			const [ keySeg, afterKey ] = splitSeg(rest);
			rest = afterKey;
			if (rest === '') throw bad();
			steps.push({ field, itemKey: keySeg, keyFields });
			cur = field.message;
			continue;
		}
		if (rest === '') {
			if (!isTextField(field)) throw bad();
			steps.push({ field });
			return steps;
		}
		if (field.fieldKind !== 'message') throw bad();
		steps.push({ field });
		cur = field.message;
	}
}

function stepInto(cur: unknown, st: TextRefStep): unknown {
	if (cur === null || typeof cur !== 'object') return undefined;
	const raw: unknown = (cur as Rec)[st.field.localName];
	if (st.mapKey !== undefined) {
		if (raw === null || typeof raw !== 'object') return undefined;
		return (raw as Rec)[st.mapKey];
	}
	if (st.itemKey !== undefined) {
		if (!Array.isArray(raw)) return undefined;
		return raw.find((e) => encodeItemKey(e as Message, st.keyFields!) === st.itemKey);
	}
	return raw;
}

function getBytesAt(msg: Rec | undefined, steps: TextRefStep[]): Uint8Array | undefined {
	let cur: unknown = msg;
	for (const st of steps) cur = stepInto(cur, st);
	return cur instanceof Uint8Array && cur.length > 0 ? cur : undefined;
}

/**
 * Writes the leaf bytes, materializing singular intermediates. A missing
 * addressed element or entry is an error rather than created — an element is
 * more than its key, and a caller that means to create one puts it in the
 * value. Mirrors the server's textFieldRef.set.
 */
function setBytesAt(msg: Rec, steps: TextRefStep[], value: Uint8Array): void {
	let cur = msg;
	for (let i = 0; i < steps.length - 1; i++) {
		const st = steps[i]!;
		if (st.mapKey !== undefined || st.itemKey !== undefined) {
			const next = stepInto(cur, st);
			if (next === undefined || next === null) {
				throw new Error(
					`text field path addresses a ${st.mapKey !== undefined ? 'map entry' : 'list element'} `
						+ `the value does not carry (at "${st.field.name}")`,
				);
			}
			cur = next as Rec;
			continue;
		}
		let next = cur[st.field.localName] as Rec | undefined | null;
		if (next === undefined || next === null) {
			next = create(st.field.message!) as unknown as Rec;
			cur[st.field.localName] = next;
		}
		cur = next;
	}
	cur[steps[steps.length - 1]!.field.localName] = value;
}

/**
 * Enumerates the instance paths of one text field within a message: the schema
 * path itself when it crosses no keyed list or map (whether or not the leaf is
 * set), else one path per element reachable through present containers — the
 * mask form each per-element document is addressed by ("items.<key>.body").
 * Deterministic: list order, map entries sorted by encoded key. Mirror of the
 * server's textField.instances.
 */
export function textInstancePaths(
	schema: DescMessage,
	textPath: string,
	msg: Message,
): string[] {
	const out: string[] = [];
	const walk = (
		cur: unknown,
		desc: DescMessage,
		rest: string,
		prefix: string,
	): void => {
		const [ seg, after ] = splitSeg(rest);
		const field = desc.fields.find((f) => f.name === seg);
		if (!field) return;
		if (field.fieldKind === 'list' && field.listKind === 'message') {
			const keyFields = keyFieldsFor(field);
			if (keyFields.length === 0) return;
			const raw = cur === null || typeof cur !== 'object'
				? undefined
				: (cur as Rec)[field.localName];
			if (!Array.isArray(raw)) return;
			for (const elem of raw) {
				const key = encodeItemKey(elem as Message, keyFields);
				walk(elem, field.message, after, `${prefix}${seg}.${key}.`);
			}
			return;
		}
		if (field.fieldKind === 'map' && field.mapKind === 'message') {
			const raw = cur === null || typeof cur !== 'object'
				? undefined
				: (cur as Rec)[field.localName];
			if (raw === null || typeof raw !== 'object') return;
			const entries = Object.keys(raw as Rec)
				.map((k) => [ encodeMapKey(field, k), k ] as const)
				.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
			for (const [ encoded, jsKey ] of entries) {
				walk((raw as Rec)[jsKey], field.message, after, `${prefix}${seg}.${encoded}.`);
			}
			return;
		}
		if (after === '') {
			out.push(prefix + seg);
			return;
		}
		if (field.fieldKind !== 'message') return;
		const next = cur === null || typeof cur !== 'object'
			? undefined
			: (cur as Rec)[field.localName];
		if (next === undefined || next === null) return;
		walk(next, field.message, after, `${prefix}${seg}.`);
	};
	if (!textPathNested(schema, textPath)) return [ textPath ];
	walk(msg as unknown as Rec, schema, textPath, '');
	return out;
}

/**
 * Whether a text field's schema path crosses a keyed list or map — the shape
 * whose documents are per-element rather than per-row.
 */
export function textPathNested(schema: DescMessage, textPath: string): boolean {
	let cur: DescMessage = schema;
	let rest = textPath;
	for (;;) {
		const [ seg, after ] = splitSeg(rest);
		const field = cur.fields.find((f) => f.name === seg);
		if (!field) return false;
		if (field.fieldKind === 'list' || field.fieldKind === 'map') return true;
		if (after === '' || field.fieldKind !== 'message') return false;
		cur = field.message;
		rest = after;
	}
}

/**
 * Resolves a mask path back to the schema path of the text field it addresses
 * outright, or undefined when it lands anywhere else. This is the gate every
 * mask-driven text fast path asks at: an instance path ("items.<key>.body")
 * resolves to its merger key ("items.body"), a fixed path to itself, and a
 * path naming a whole element — which replaces it, document included — to
 * undefined.
 */
export function textSchemaPathOf(schema: DescMessage, maskPath: string): string | undefined {
	let cur: DescMessage = schema;
	let rest = maskPath;
	let out = '';
	for (;;) {
		const [ seg, after ] = splitSeg(rest);
		rest = after;
		const field = cur.fields.find((f) => f.name === seg);
		if (!field) return undefined;
		out += (out === '' ? '' : '.') + field.name;
		if (field.fieldKind === 'map') {
			if (field.mapKind !== 'message' || rest === '') return undefined;
			[ , rest ] = splitSeg(rest);
			if (rest === '') return undefined;
			cur = field.message;
			continue;
		}
		if (field.fieldKind === 'list') {
			if (field.listKind !== 'message' || keyFieldsFor(field).length === 0 || rest === '') {
				return undefined;
			}
			[ , rest ] = splitSeg(rest);
			if (rest === '') return undefined;
			cur = field.message;
			continue;
		}
		if (rest === '') return isTextField(field) ? out : undefined;
		if (field.fieldKind !== 'message') return undefined;
		cur = field.message;
	}
}

/**
 * Reads the encoded document of one text field, or undefined when the message
 * does not carry it (an unset intermediate, an absent element, or empty bytes
 * — for a state CRDT those mean the same thing). `path` is an instance path:
 * where the field sits under a keyed list or map, the element key is part of
 * it ("items.<key>.body").
 */
export function textFieldBytes(
	schema: DescMessage,
	path: string,
	msg: Message,
): Uint8Array | undefined {
	return getBytesAt(msg as unknown as Rec, textFieldSteps(schema, path));
}

/**
 * Writes one text field's encoded document, materializing singular
 * intermediates. Throws when the instance path addresses a list element or map
 * entry the message does not carry.
 */
export function setTextFieldBytes(
	schema: DescMessage,
	path: string,
	msg: Message,
	value: Uint8Array,
): void {
	setBytesAt(msg as unknown as Rec, textFieldSteps(schema, path), value);
}

/** What one text field's string conversion did (setString's op-level shapes),
 * keyed by path — the raw material for a symbolic undo inverse. */
export type TextStringEdits = Map<string, { deleted: DeletedSegment[]; inserted: InsertedRange[] }>;

/**
 * Whether any text field of a crud's value holds a plain-string value, and so
 * needs resolving into edits before the write.
 *
 * The enqueue path asks before it opens its transaction, because resolving
 * needs a leased writer peer and the lease has to be acquired first.
 */
export function crudHasPlainString(
	def: EntityDef,
	crud: CrudMutation,
	textPaths: readonly string[],
): boolean {
	if (textPaths.length === 0) return false;
	const value = fromBinary(def.schema, crud.value);
	return textPaths.some((path) =>
		textInstancePaths(def.schema, path, value).some((inst) =>
			isPlainString(getBytesAt(value as unknown as Rec, textFieldSteps(def.schema, inst)))
		)
	);
}

/**
 * Converts a crud's text strings into document bytes in place: each string is
 * diffed (multi-hunk) against the field's document in the merged row `prevRow`,
 * under the leased writer identity `peer`, and the resulting full document
 * replaces the field inside crud.value.
 *
 * Strings come from two places. `texts` is what the caller asked to write, and
 * a path there is added to a PATCH's mask when nothing covers it yet. The other
 * is a text field of crud.value already holding a plain-string value (see
 * `encodePlainString`, which is what a field-history record's before-state
 * carries), so restoring a record needs no per-field handling. That one does
 * not widen the mask; outside the mask it is cleared rather than left to ride
 * along, since the server refuses an unresolved value in any position.
 *
 * `prevRow` is read only when there is a string to diff, keeping a write that
 * touches no text off the O(document) merged-row read. `peer` may be undefined
 * only when there is nothing to convert — the caller takes the lease before its
 * transaction opens (`crudHasPlainString`) — and converting without one throws.
 *
 * Returns undefined when the crud carries no text strings at all.
 *
 * Must run inside the enqueue transaction with the peer lease held: the diff
 * base has to be the row the overlay will apply on top of, and the peer must
 * not be speakable by anyone else while counters are allocated.
 */
export async function applyTextStringsToCrud(
	def: EntityDef,
	crud: CrudMutation,
	prevRow: () => Promise<Uint8Array | undefined>,
	texts: ReadonlyMap<string, string> | undefined,
	peer: string | undefined,
	textPaths: readonly string[],
): Promise<TextStringEdits | undefined> {
	if ((texts === undefined || texts.size === 0) && textPaths.length === 0) return undefined;

	const value = fromBinary(def.schema, crud.value) as unknown as Rec;
	const masked = (path: string): boolean =>
		crud.op !== CrudOp.PATCH || !crud.fieldMask
		|| crud.fieldMask.paths.some((p) => p === path || path.startsWith(`${p}.`));

	// Embedded values first, so an explicit string for the same path wins: it
	// is the caller's stated intent for this write, while an embedded value
	// arrived with whatever they decoded it from. Paths here are instance
	// paths — one per element for a field under a keyed list or map.
	const pending = new Map<string, string>();
	let cleared = false;
	for (const schemaPath of textPaths) {
		for (const path of textInstancePaths(def.schema, schemaPath, value as unknown as Message)) {
			const steps = textFieldSteps(def.schema, path);
			const current = getBytesAt(value, steps);
			if (!isPlainString(current)) continue;
			if (!masked(path)) {
				setBytesAt(value, steps, new Uint8Array(0));
				cleared = true;
				continue;
			}
			pending.set(path, plainString(current!));
		}
	}
	if (texts) { for (const [ path, s ] of texts) pending.set(path, s); }

	if (pending.size === 0) {
		if (cleared) crud.value = toBinary(def.schema, value as unknown as Message);
		return undefined;
	}

	if (peer === undefined) {
		throw new Error(
			`text strings in a write to "${def.name}" need a leased writer peer, and none was taken`,
		);
	}
	const prevBytes = await prevRow();
	const prev = prevBytes !== undefined
		? fromBinary(def.schema, prevBytes) as unknown as Rec
		: undefined;
	const edits: TextStringEdits = new Map();
	for (const [ path, s ] of pending) {
		const steps = textFieldSteps(def.schema, path);
		const doc = decodeTextDoc(getBytesAt(prev, steps), peer);
		edits.set(path, doc.setString(s));
		setBytesAt(value, steps, encodeTextDoc(doc));
		if (crud.op === CrudOp.PATCH && crud.fieldMask && texts?.has(path)) {
			const covered = crud.fieldMask.paths.some(
				(p) => p === path || path.startsWith(`${p}.`),
			);
			if (!covered) crud.fieldMask.paths.push(path);
		}
	}
	crud.value = toBinary(def.schema, value as unknown as Message);
	return edits;
}

/**
 * Unions every text document of base into next in place, pairing documents by
 * instance path (where both sides carry one for the same position) — the
 * client mirror of the server's PUT exception: a full-row write must merge
 * text documents with the stored row rather than reset them. An element base
 * holds and next dropped is being deleted, and its document goes with it.
 */
export function mergeTextFieldsInto(
	schema: DescMessage,
	textPaths: readonly string[],
	base: Message,
	next: Message,
): void {
	for (const schemaPath of textPaths) {
		for (const path of textInstancePaths(schema, schemaPath, next)) {
			const steps = textFieldSteps(schema, path);
			const baseBytes = getBytesAt(base as unknown as Rec, steps);
			const nextBytes = getBytesAt(next as unknown as Rec, steps);
			if (baseBytes === undefined || nextBytes === undefined) continue;
			setBytesAt(next as unknown as Rec, steps, mergeText(baseBytes, nextBytes));
		}
	}
}

/**
 * Registration-time guard: what may not live inside a oneof.
 *
 * A oneof is one field — masks name the group and the whole group is replaced —
 * so nothing below one is ever addressed on its own. Anything that needs a
 * field to be reliably THERE breaks under that: an identity that vanishes when
 * a sibling case wins is not an identity. A reference may be a member: a
 * stored stub is replaced whole anyway, and a oneof of references is how a
 * polymorphic reference is declared (see {@link refShapes}). Mirrors the
 * engine's registration checks (text fields are caught by
 * collectTextFieldPaths).
 */
function validateOneofs(schema: DescMessage): void {
	for (const field of schema.fields) {
		// A oneof member is one case of the holder's oneof; a member that is
		// itself a oneof of targets would be cases inside a case, which the
		// reference graph has no name for.
		if (field.oneof && refShapes(field).length > 1) {
			throw new Error(
				`${schema.typeName}.${field.name}: reference field is a member of oneof `
					+ `"${field.oneof.name}" and names one of several targets itself; `
					+ `declare the targets as members of "${field.oneof.name}" instead`,
			);
		}
		if (field.fieldKind !== 'list' || field.listKind !== 'message') continue;
		const behavior = fieldBehaviorOf(field);
		const identity = [
			...(behavior?.keyField ?? []),
			...(behavior?.orderField ? [ behavior.orderField ] : []),
		];
		for (const name of identity) {
			const member = field.message.fields.find((f) => f.name === name);
			if (member?.oneof) {
				throw new Error(
					`${schema.typeName}.${field.name}: "${name}" is a member of oneof `
						+ `"${member.oneof.name}"; a list identity cannot be inside a oneof`,
				);
			}
		}
	}
}

/**
 * Registration-time guard: an order_field (movable list) is only valid on a
 * keyed message list whose element actually has the named position field.
 */
function validateOrderFields(schema: DescMessage): void {
	for (const field of schema.fields) {
		const order = fieldBehaviorOf(field)?.orderField;
		if (!order) continue;
		if (
			field.fieldKind !== 'list' || field.listKind !== 'message'
			|| keyFieldsFor(field).length === 0
		) {
			throw new Error(
				`${schema.typeName}.${field.name}: order_field requires a keyed message list`,
			);
		}
		if (!field.message.fields.some((f) => f.name === order)) {
			throw new Error(
				`${schema.typeName}.${field.name}: order_field "${order}" is not a field of the element`,
			);
		}
	}
}

/**
 * Registration-time warning: the query DSL claims a fixed set of property
 * names for its operators, so a field called `in`, `has` or `contains` is
 * unreachable through `where`/`orderBy` — it vanishes from the query surface
 * with no error at either the type level or runtime, which is a confusing way
 * to discover the clash.
 *
 * A warning rather than a throw: the field is perfectly usable for reading and
 * writing, and only queries are affected, so refusing to register an otherwise
 * valid schema would be worse than the problem. Nested messages are walked too
 * — a shadowed name is just as unreachable one level down.
 */
function warnShadowedQueryFields(schema: DescMessage): void {
	const seen = new Set<string>();
	const walk = (desc: DescMessage): void => {
		if (seen.has(desc.typeName)) return; // recursive schemas (self-refs)
		seen.add(desc.typeName);
		for (const field of desc.fields) {
			if (RESERVED_QUERY_NAMES.has(field.localName)) {
				console.warn(
					`sync: ${desc.typeName}.${field.name} is named after the query operator `
						+ `"${field.localName}", so it cannot be filtered or ordered on. Rename the field, `
						+ `or use json_name/a different proto name, to reach it from the query DSL.`,
				);
			}
			if (
				field.fieldKind === 'message' || (field.fieldKind === 'list' && field.listKind === 'message')
			) {
				walk(field.message);
			}
		}
		// A oneof group is a path segment of its own in the DSL (`w.choice.…`),
		// so its name is shadowable exactly like a field's.
		for (const oneof of desc.oneofs) {
			if (RESERVED_QUERY_NAMES.has(oneof.localName)) {
				console.warn(
					`sync: ${desc.typeName}.${oneof.name} is a oneof named after the query operator `
						+ `"${oneof.localName}", so its members cannot be filtered or ordered on. Rename `
						+ `the oneof to reach them from the query DSL.`,
				);
			}
		}
	};
	walk(schema);
}

/** Splits a mask path at the first unescaped '.'. Mirror of lww.go splitSeg. */
function splitSeg(path: string): [string, string] {
	for (let i = 0; i < path.length; i++) {
		const c = path[i];
		if (c === '\\') {
			i++; // skip the escaped char
			continue;
		}
		if (c === '.') return [ path.slice(0, i), path.slice(i + 1) ];
	}
	return [ path, '' ];
}

/**
 * Renders a message-list element's identity as a mask segment. A single key
 * field escapes "\" and "."; a composite key escapes "\", ".", and "/" per part
 * and joins them with "/". Mirror of lww.go encodeItemKey.
 */
export function encodeItemKey(elem: Message, keyFields: DescField[]): string {
	const rec = elem as unknown as Rec;
	if (keyFields.length === 1) {
		const kf = keyFields[0]!;
		return escapeKeySegment(keyComponent(kf, rec[kf.localName]), false);
	}
	return keyFields
		.map((kf) => escapeKeySegment(keyComponent(kf, rec[kf.localName]), true))
		.join('/');
}

/**
 * Parses an encoded item-key mask segment back into the element key-field
 * values it renders from — the inverse of encodeItemKey, for building a value
 * that carries the addressed element. Strings unescape; numbers parse base-10
 * (bigint where protobuf-es uses one); bools parse true/false; enums parse by
 * number.
 */
function decodeItemKeyInit(keyFields: DescField[], itemKey: string): Rec {
	const parts = keyFields.length === 1 ? [ itemKey ] : splitCompositeKey(itemKey);
	if (parts.length !== keyFields.length) {
		throw new Error(`item key "${itemKey}" does not match its ${keyFields.length} key fields`);
	}
	const out: Rec = {};
	for (let i = 0; i < keyFields.length; i++) {
		const field = keyFields[i]!;
		const raw = decodeStringKey(parts[i]!);
		out[field.localName] = decodeKeyPart(field, raw);
	}
	return out;
}

/** Splits a composite item key at unescaped '/' separators. */
function splitCompositeKey(s: string): string[] {
	const parts: string[] = [];
	let cur = '';
	for (let i = 0; i < s.length; i++) {
		const c = s[i]!;
		if (c === '\\' && i + 1 < s.length) {
			cur += c + s[i + 1];
			i++;
		} else if (c === '/') {
			parts.push(cur);
			cur = '';
		} else {
			cur += c;
		}
	}
	parts.push(cur);
	return parts;
}

function decodeKeyPart(field: DescField, raw: string): unknown {
	if (field.fieldKind === 'enum') return Number(raw);
	switch (field.fieldKind === 'scalar' ? field.scalar : undefined) {
		case ScalarType.STRING:
			return raw;
		case ScalarType.BOOL:
			return raw === 'true';
		case ScalarType.INT64:
		case ScalarType.SINT64:
		case ScalarType.SFIXED64:
		case ScalarType.UINT64:
		case ScalarType.FIXED64:
			return BigInt(raw);
		default:
			return Number(raw);
	}
}

/**
 * Builds a row init that carries `state` at one text field's instance path:
 * each keyed-list step on the way becomes a single-element list seeded with
 * the identity the path names, each map step a single entry, and singular
 * steps empty shells. Paired with the path as a PATCH mask this is the write a
 * text handle sends — the merge descends to the addressed element and the
 * text merger fires there, leaving the element's other fields alone.
 */
export function textFieldInit(schema: DescMessage, path: string, state: Uint8Array): object {
	const steps = textFieldSteps(schema, path);
	let inner: Rec = { [steps[steps.length - 1]!.field.localName]: state };
	for (let i = steps.length - 2; i >= 0; i--) {
		const st = steps[i]!;
		if (st.itemKey !== undefined) {
			inner = {
				[st.field.localName]: [ { ...decodeItemKeyInit(st.keyFields!, st.itemKey), ...inner } ],
			};
		} else if (st.mapKey !== undefined) {
			inner = { [st.field.localName]: { [st.mapKey]: inner } };
		} else {
			inner = { [st.field.localName]: inner };
		}
	}
	return inner;
}

/** Renders a scalar list element's value as a mask segment. */
function encodeScalarElem(field: DescField, value: unknown): string {
	const isEnum = field.fieldKind === 'list' && field.listKind === 'enum';
	const scalar = field.fieldKind === 'list' && field.listKind === 'scalar'
		? field.scalar
		: undefined;
	return escapeKeySegment(scalarKeyString(value, scalar, isEnum), false);
}

/**
 * Canonicalizes a singular key field's value (string/int/bool/enum). An item's
 * identity inside a row and a row's own key ask the same question, so both go
 * through the shared rendering (naming.ts) rather than a second copy that could
 * drift from what the server encodes.
 */
function keyComponent(field: DescField, value: unknown): string {
	const isEnum = field.fieldKind === 'enum';
	const scalar = field.fieldKind === 'scalar' ? field.scalar : undefined;
	return scalarKeyString(value, scalar, isEnum);
}

/** Encodes a JS map key back to a mask segment (string keys escaped). */
export function encodeMapKey(field: DescField, jsKey: string): string {
	return field.fieldKind === 'map' && field.mapKey === ScalarType.STRING
		? escapeKeySegment(jsKey, false)
		: jsKey;
}

/** Decodes a mask segment into the JS object key for a map (string keys
 * unescaped; integral/bool keys are already canonical). */
function decodeMapKey(field: DescField, seg: string): string {
	return field.fieldKind === 'map' && field.mapKey === ScalarType.STRING
		? decodeStringKey(seg)
		: seg;
}

/** Escapes "\" and "." (and "/" when alsoSlash). Mirror of lww.go. */
function escapeKeySegment(s: string, alsoSlash: boolean): string {
	const re = alsoSlash ? /[\\./]/g : /[\\.]/g;
	return s.replace(re, (c) => `\\${c}`);
}

/** Reverses escapeKeySegment for a single-part string key. */
function decodeStringKey(s: string): string {
	let out = '';
	let esc = false;
	for (const c of s) {
		if (esc) {
			out += c;
			esc = false;
		} else if (c === '\\') {
			esc = true;
		} else {
			out += c;
		}
	}
	return out;
}

/**
 * Combines both sides of an opaque field at the bytes level. Returns false
 * when either side carries no value ("has" semantics match the server:
 * undefined messages and empty bytes are absent) — the caller then falls
 * back to plain replace/clear.
 *
 * One exception: an incoming delta-state fragment must reach the merger even
 * with an empty destination, because a fragment is not a value that can be
 * stored as-is. Replacing with it would leave the row holding delta state
 * where every reader expects a document — which is precisely what happens the
 * first time a text handle edits a field of a row created without one.
 */
function applyMerger(dst: Rec, src: Rec, field: DescField, merge: MergeFn): boolean {
	if (field.fieldKind === 'message') {
		const dstVal = dst[field.localName] as Message | undefined;
		const srcVal = src[field.localName] as Message | undefined;
		if (dstVal === undefined || srcVal === undefined) return false;
		const merged = merge(toBinary(field.message, dstVal), toBinary(field.message, srcVal));
		dst[field.localName] = fromBinary(field.message, merged);
		return true;
	}
	const dstVal = dst[field.localName] as Uint8Array | undefined;
	const srcVal = src[field.localName] as Uint8Array | undefined;
	if (!srcVal?.length) return false;
	if (!dstVal?.length && !isFragment(srcVal)) return false;
	dst[field.localName] = merge(dstVal ?? new Uint8Array(), srcVal);
	return true;
}

/**
 * Replaces a whole oneof group on dst with src's state: the value of whichever
 * member src has set, or no case at all. The group is the unit of
 * last-writer-wins — see lww.copyOneof for why a member cannot be.
 *
 * protobuf-es models a group as ONE property holding `{ case, value }`, so
 * "replace the group" is a single assignment; a member has no property of its
 * own, which is why a member-named mask has nothing to write to either.
 */
function copyOneof(schema: DescMessage, dst: Rec, src: Rec, oneof: DescOneof): void {
	const from = src[oneof.localName] as OneofValue | undefined;
	const activeCase = from?.case;
	if (activeCase === undefined) {
		dst[oneof.localName] = { case: undefined };
		return;
	}
	const member = schema.fields.find(
		(f) => f.oneof?.localName === oneof.localName && f.localName === activeCase,
	);
	// A case this build's schema does not know cannot be deep-copied by
	// descriptor; pass it through rather than dropping the write.
	const value = member?.fieldKind === 'message'
		? clone(member.message, from?.value as Message)
		: from?.value;
	dst[oneof.localName] = { case: activeCase, value };
}

function copyField(dst: Rec, src: Rec, field: DescField): void {
	const v = src[field.localName];
	switch (field.fieldKind) {
		case 'list': {
			const list = (v as unknown[] | undefined) ?? [];
			dst[field.localName] = field.listKind === 'message'
				? list.map((item) => clone(field.message, item as Message))
				: [ ...list ];
			break;
		}
		case 'map': {
			const entries = Object.entries((v as Rec | undefined) ?? {});
			dst[field.localName] = Object.fromEntries(
				field.mapKind === 'message'
					? entries.map(([ k, item ]) => [ k, clone(field.message, item as Message) ])
					: entries,
			);
			break;
		}
		case 'message':
			dst[field.localName] = v === undefined ? undefined : clone(field.message, v as Message);
			break;
		default:
			// scalar / enum — proto3 implicit presence always materializes a value
			dst[field.localName] = v;
	}
}

/**
 * Replaces each bare, non-opaque collection path in a PATCH mask with per-item
 * paths (field.<encodedKey>) for the keys/values that were added, removed, or
 * changed between prior and next (unchanged items are skipped). Non-collection,
 * opaque, non-keyed-list, and already-sub-pathed paths pass through verbatim.
 * The crud value is NOT trimmed — only the mask narrows.
 *
 * This is the client-side guarantee that "adding something never replaces the
 * list": a bare collection path would whole-replace, so we expand it to name
 * exactly the touched items, which the merge core then merges per item.
 */
/**
 * Whether expandCollectionMask could change anything about `paths` — true only
 * when some path names a whole non-opaque list or map. Purely schema-driven, so
 * it answers before the two row decodes the expansion itself needs; a mask over
 * scalars, messages or text fields (the shape every text edit produces) skips
 * them entirely.
 */
export function maskNeedsExpansion(schema: DescMessage, paths: readonly string[]): boolean {
	for (const path of paths) {
		const [ seg ] = splitSeg(path);
		// Any separator, a trailing one included, makes an item path.
		if (seg.length !== path.length) continue;
		const field = schema.fields.find((f) => f.name === seg);
		if (!field || isOpaque(field)) continue;
		if (field.fieldKind === 'list' || field.fieldKind === 'map') return true;
	}
	return false;
}

export function expandCollectionMask(
	schema: DescMessage,
	prior: Message,
	next: Message,
	paths: string[],
): string[] {
	const priorRec = prior as unknown as Rec;
	const nextRec = next as unknown as Rec;
	const out: string[] = [];
	for (const path of paths) {
		const [ seg ] = splitSeg(path);
		// Only a bare path names the whole collection; 'attrs.' already names
		// the item with the empty key.
		const field = seg.length === path.length ? schema.fields.find((f) => f.name === seg) : undefined;
		if (
			!field || isOpaque(field)
			|| (field.fieldKind !== 'list' && field.fieldKind !== 'map')
		) {
			out.push(path);
			continue;
		}
		if (field.fieldKind === 'map') {
			out.push(...expandMap(field, seg, priorRec, nextRec));
		} else if (field.listKind === 'message') {
			const keyFields = keyFieldsFor(field);
			if (isRefMessageField(field)) {
				out.push(...expandRefSet(field, seg, priorRec, nextRec));
			} else if (keyFields.length === 0) {
				out.push(path); // non-keyed message list → whole-replace
			} else {
				out.push(...expandKeyedList(field, seg, keyFields, priorRec, nextRec));
			}
		} else {
			out.push(...expandScalarSet(field, seg, priorRec, nextRec));
		}
	}
	return out;
}

function expandMap(field: MapField, seg: string, prior: Rec, next: Rec): string[] {
	const pm = (prior[field.localName] as Rec | undefined) ?? {};
	const nm = (next[field.localName] as Rec | undefined) ?? {};
	const out: string[] = [];
	for (const k of new Set([ ...Object.keys(pm), ...Object.keys(nm) ])) {
		const inP = Object.hasOwn(pm, k);
		const inN = Object.hasOwn(nm, k);
		if (inP !== inN || mapValueEquals(field, pm[k], nm[k])) {
			if (inP !== inN) out.push(`${seg}.${encodeMapKey(field, k)}`);
			continue;
		}
		// Both sides carry the entry and it changed. When its type holds text,
		// the entry cannot be replaced whole — that would clobber concurrent
		// edits to its documents — so the diff narrows inside it.
		if (field.mapKind === 'message' && hasTextDescendant(field.message)) {
			expandElementDiff(
				field.message,
				`${seg}.${encodeMapKey(field, k)}.`,
				pm[k] as Message,
				nm[k] as Message,
				out,
			);
			continue;
		}
		out.push(`${seg}.${encodeMapKey(field, k)}`);
	}
	return out;
}

function expandKeyedList(
	field: MessageListField,
	seg: string,
	keyFields: DescField[],
	prior: Rec,
	next: Rec,
): string[] {
	const index = (rec: Rec): Map<string, Message> => {
		const m = new Map<string, Message>();
		for (const e of (rec[field.localName] as Message[] | undefined) ?? []) {
			const k = encodeItemKey(e, keyFields);
			if (!m.has(k)) m.set(k, e); // first wins on duplicate keys
		}
		return m;
	};
	const pMap = index(prior);
	const nMap = index(next);
	const out: string[] = [];
	for (const k of new Set([ ...pMap.keys(), ...nMap.keys() ])) {
		const p = pMap.get(k);
		const n = nMap.get(k);
		if (p === undefined || n === undefined) {
			if (p !== n) out.push(`${seg}.${k}`); // added or removed: the element is the unit
			continue;
		}
		if (bytesEqual(toBinary(field.message, p), toBinary(field.message, n))) continue;
		// A changed element whose type holds text cannot be replaced whole —
		// that would clobber concurrent edits to its documents — so the diff
		// narrows to the units that moved inside it.
		if (hasTextDescendant(field.message)) {
			expandElementDiff(field.message, `${seg}.${k}.`, p, n, out);
			continue;
		}
		out.push(`${seg}.${k}`);
	}
	return out;
}

/**
 * Narrows a changed element (or singular message on the way to one) to the
 * units that moved, emitting one mask path per unit under `prefix`. A changed
 * text field gets its own path, which is what routes it through the merger; a
 * changed singular message holding text recurses; a nested keyed list or map
 * holding text re-enters the per-item expansion; everything else is replaced
 * as the unit the mask names, exactly as a top-level diff would.
 */
function expandElementDiff(
	desc: DescMessage,
	prefix: string,
	p: Message,
	n: Message,
	out: string[],
): void {
	const before = out.length;
	for (const unit of maskFromDiff(desc, p, n)) {
		const field = desc.fields.find((f) => f.name === unit);
		if (!field) {
			// A oneof group; nothing below one can be text, so it replaces whole.
			out.push(prefix + unit);
			continue;
		}
		const pv = (p as unknown as Rec)[field.localName];
		const nv = (n as unknown as Rec)[field.localName];
		if (
			field.fieldKind === 'message' && !isOpaque(field) && refKeyFields(field).length === 0
			&& hasTextDescendant(field.message)
			&& pv !== undefined && pv !== null && nv !== undefined && nv !== null
		) {
			expandElementDiff(field.message, `${prefix}${unit}.`, pv as Message, nv as Message, out);
			continue;
		}
		if (
			field.fieldKind === 'list' && field.listKind === 'message'
			&& !isOpaque(field) && hasTextDescendant(field.message)
		) {
			const kfs = keyFieldsFor(field);
			if (kfs.length > 0) {
				out.push(...expandKeyedList(
					field,
					prefix + unit,
					kfs,
					p as unknown as Rec,
					n as unknown as Rec,
				));
				continue;
			}
		}
		if (
			field.fieldKind === 'map' && field.mapKind === 'message' && hasTextDescendant(field.message)
		) {
			out.push(...expandMap(field, prefix + unit, p as unknown as Rec, n as unknown as Rec));
			continue;
		}
		out.push(prefix + unit);
	}
	if (out.length === before) {
		// The bytes differ but no unit does (unknown fields, most likely). The
		// change must still land, and a whole-element write is the only mask
		// that can carry what the diff cannot name.
		out.push(prefix.slice(0, -1));
	}
}

/** Memoized: whether a message type carries a text field anywhere a mask can
 * reach — through singular messages, keyed lists and map values, past opaque
 * fields and references (where a merger could never fire anyway). */
const textDescendantMemo = new WeakMap<DescMessage, boolean>();

function hasTextDescendant(desc: DescMessage): boolean {
	const memo = textDescendantMemo.get(desc);
	if (memo !== undefined) return memo;
	const walk = (d: DescMessage, visiting: string[]): boolean => {
		for (const field of d.fields) {
			if (isTextField(field)) return true;
			if (isOpaque(field) || isRef(field)) continue;
			let sub: DescMessage | undefined;
			if (field.fieldKind === 'message') sub = field.message;
			else if (field.fieldKind === 'list' && field.listKind === 'message') sub = field.message;
			else if (field.fieldKind === 'map' && field.mapKind === 'message') sub = field.message;
			if (sub === undefined || visiting.includes(sub.typeName)) continue;
			if (walk(sub, [ ...visiting, d.typeName ])) return true;
		}
		return false;
	};
	const result = walk(desc, []);
	textDescendantMemo.set(desc, result);
	return result;
}

/**
 * A repeated reference message diffs as a set of identities (see
 * {@link refItemKey}): an element is added or removed, never edited, because
 * the stored value is nothing but its identity.
 */
function expandRefSet(
	field: MessageListField,
	seg: string,
	prior: Rec,
	next: Rec,
): string[] {
	const keysOf = (rec: Rec): Set<string> =>
		new Set(
			((rec[field.localName] as Message[] | undefined) ?? []).map((e) => refItemKeyOf(field, e)),
		);
	const pKeys = keysOf(prior);
	const nKeys = keysOf(next);
	const out: string[] = [];
	for (const k of pKeys) if (!nKeys.has(k)) out.push(`${seg}.${k}`);
	for (const k of nKeys) if (!pKeys.has(k)) out.push(`${seg}.${k}`);
	return out;
}

function expandScalarSet(field: DescField, seg: string, prior: Rec, next: Rec): string[] {
	const keysOf = (rec: Rec): Set<string> =>
		new Set(
			((rec[field.localName] as unknown[] | undefined) ?? []).map((v) => encodeScalarElem(field, v)),
		);
	const pKeys = keysOf(prior);
	const nKeys = keysOf(next);
	const out: string[] = [];
	for (const k of pKeys) if (!nKeys.has(k)) out.push(`${seg}.${k}`);
	for (const k of nKeys) if (!pKeys.has(k)) out.push(`${seg}.${k}`);
	return out;
}

function mapValueEquals(field: MapField, a: unknown, b: unknown): boolean {
	if (field.mapKind === 'message') {
		return bytesEqual(toBinary(field.message, a as Message), toBinary(field.message, b as Message));
	}
	if (a instanceof Uint8Array && b instanceof Uint8Array) return bytesEqual(a, b);
	return a === b;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
	if (a === b) return true;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * Whether a value is a protobuf message instance rather than a partial init.
 * protobuf-es hangs an own, enumerable `$typeName` off every message, which is
 * both how we tell the two apart and why a message cannot be masked by its keys.
 */
export function isMessageInstance(value: object): boolean {
	return Object.hasOwn(value, '$typeName');
}

/**
 * Maps init-object keys (localName) to proto field names for a mask.
 *
 * A whole message is refused rather than masked. Its keys are not a partial —
 * they are every field plus protobuf-es's `$typeName` — so deriving a mask from
 * them would name every field and turn the PATCH into a PUT, losing exactly the
 * per-field merge PATCH exists for. The verbs that do take a whole message say
 * so in the error: `update()` diffs one into a narrow mask, `put()` replaces the
 * row on purpose.
 */
export function maskFromInit(schema: DescMessage, init: object): string[] {
	if (isMessageInstance(init)) {
		throw new Error(
			`patch: this is a whole ${schema.typeName} message, not the fields to change. `
				+ `Pass just those ({ title: 'x' }), use update(key, draft => …) to edit a row in `
				+ `place, or put() to replace it. To copy named fields off a message, pass an `
				+ `explicit \`paths\` mask.`,
		);
	}
	const byLocalName = localNameIndexOf(schema);
	const paths: string[] = [];
	for (const key of Object.keys(init)) {
		const field = byLocalName.get(key);
		if (!field) {
			// A oneof is addressed in an init object by the group's own name, which
			// is not a field — and the group is exactly the unit a mask names, so
			// the key translates straight across.
			const oneof = schema.oneofs.find((o) => o.localName === key);
			if (oneof) {
				paths.push(oneof.name);
				continue;
			}
			throw new Error(`unknown field "${key}" on ${schema.typeName}`);
		}
		paths.push(field.name);
	}
	return paths;
}

/** A schema's fields indexed by their JS accessor name. */
function localNameIndexOf(schema: DescMessage): Map<string, DescField> {
	let index = fieldsByLocalName.get(schema);
	if (index === undefined) {
		index = new Map(schema.fields.map((f) => [ f.localName, f ]));
		fieldsByLocalName.set(schema, index);
	}
	return index;
}

/**
 * Top-level proto paths where `next` differs from `prior` — the mask a
 * read-modify-write update sends.
 *
 * Fields the caller left alone stay out of the mask, and that is the whole
 * point of the verb: a concurrent writer's change to an untouched field
 * survives, where a PUT of the same draft would have clobbered it.
 *
 * Collections compare whole here and are named whole. The enqueue path narrows
 * a changed collection to the items that actually moved
 * (expandCollectionMask), so an added element still never whole-replaces the
 * list — doing it there rather than here keeps one implementation of the
 * narrowing, against the row as it is inside the enqueue transaction.
 */
export function maskFromDiff(schema: DescMessage, prior: Message, next: Message): string[] {
	const p = prior as unknown as Rec;
	const n = next as unknown as Rec;
	const paths: string[] = [];
	for (const field of schema.fields) {
		if (field.oneof) continue; // handled per group below
		if (!fieldEquals(field, p[field.localName], n[field.localName])) paths.push(field.name);
	}
	// A oneof diffs as one field — a changed case and a changed value inside the
	// same case are the same edit, and the group is what the mask can name.
	for (const oneof of schema.oneofs) {
		const a = p[oneof.localName] as OneofValue | undefined;
		const b = n[oneof.localName] as OneofValue | undefined;
		if (!oneofEquals(schema, oneof.localName, a, b)) paths.push(oneof.name);
	}
	return paths;
}

/** The runtime shape of a oneof group: the selected case and its value. */
type OneofValue = { case?: string; value?: unknown };

function oneofEquals(
	schema: DescMessage,
	oneofLocalName: string,
	a: OneofValue | undefined,
	b: OneofValue | undefined,
): boolean {
	const caseA = a?.case;
	const caseB = b?.case;
	if (caseA !== caseB) return false;
	if (caseA === undefined) return true;
	const member = schema.fields.find(
		(f) => f.oneof?.localName === oneofLocalName && f.localName === caseA,
	);
	// An unknown case cannot be compared field-wise; treat it as changed so the
	// group is masked and copied through rather than silently dropped.
	if (!member) return false;
	return fieldEquals(member, a?.value, b?.value);
}

/** Whether one field holds the same value in two messages of the same type. */
function fieldEquals(field: DescField, a: unknown, b: unknown): boolean {
	switch (field.fieldKind) {
		case 'list': {
			const la = (a as unknown[] | undefined) ?? [];
			const lb = (b as unknown[] | undefined) ?? [];
			// Order counts: an atomic (opaque) list is replaced whole, so a reorder
			// is a real change, and for a merged list a spurious path costs only the
			// per-item narrowing that follows.
			if (la.length !== lb.length) return false;
			return la.every((v, i) =>
				field.listKind === 'message'
					? messageEquals(field.message, v, lb[i])
					: scalarEquals(v, lb[i])
			);
		}
		case 'map': {
			const ma = (a as Rec | undefined) ?? {};
			const mb = (b as Rec | undefined) ?? {};
			const keys = Object.keys(ma);
			if (keys.length !== Object.keys(mb).length) return false;
			return keys.every((k) =>
				Object.hasOwn(mb, k) && mapValueEquals(field as MapField, ma[k], mb[k])
			);
		}
		case 'message':
			return messageEquals(field.message, a, b);
		default:
			// scalar / enum, including explicit-presence fields where undefined is a
			// value of its own (absent ≠ empty).
			return scalarEquals(a, b);
	}
}

function messageEquals(schema: DescMessage, a: unknown, b: unknown): boolean {
	if (a === undefined || b === undefined) return a === b;
	return equals(schema, a as Message, b as Message);
}

function scalarEquals(a: unknown, b: unknown): boolean {
	if (a instanceof Uint8Array && b instanceof Uint8Array) return bytesEqual(a, b);
	// Object.is over === so NaN equals itself: a double field holding NaN must
	// not report as changed on every pass.
	return Object.is(a, b) || a === b;
}

/**
 * Gives every unit of a put's `value` that this client may not write the value
 * the server gives it: on a put that updates `base`, the row as this client
 * sees it, each unit of {@link EntityDef.protectedFields} keeps the value of
 * `base`; on a put of a row this client has none of, which is a create, each
 * unit of {@link EntityDef.clearedOnCreate} gets its zero value. A put names no
 * field, so the server does the same rather than refuse the write.
 *
 * Mutates and returns `value`, which is the freshly built message a put is
 * about to encode. Values taken from `base` are copies, so the caller's row
 * cannot be reached through the mutation.
 *
 * The local row may be behind the server's, and then so is the substituted
 * value: the server's own carry-over decides, and the next rebase corrects the
 * overlay. Doing it here is what keeps the optimistic view from showing a change
 * that was never going to land.
 */
export function carryProtectedFields<D extends DescMessage>(
	def: EntityDef,
	schema: D,
	value: MessageShape<D>,
	base: MessageShape<D> | undefined,
): MessageShape<D> {
	const units = base === undefined ? def.clearedOnCreate : def.protectedFields;
	if (units.length === 0) return value;
	const src = (base === undefined ? create(schema) : clone(schema, base)) as unknown as Rec;
	const out = value as unknown as Rec;
	for (const name of units) {
		const localName = schema.oneofs.find((o) => o.name === name)?.localName
			?? schema.fields.find((f) => f.name === name)?.localName;
		if (localName === undefined) continue;
		out[localName] = src[localName];
	}
	return value;
}

/**
 * Stores `subject` in every creator field of `value`, which is what the server
 * does on a create it authorizes. A put of a row this client does not hold is
 * the client's best guess at a create; a stale guess is corrected by the next
 * rebase. Mutates and returns `value`.
 */
export function stampCreatorFields<D extends DescMessage>(
	def: EntityDef,
	value: MessageShape<D>,
	subject: string,
): MessageShape<D> {
	const out = value as unknown as Rec;
	for (const field of def.creatorFields) out[field.localName] = subject;
	return value;
}

export function buildPut<D extends DescMessage>(
	def: EntityDef,
	schema: D,
	value: MessageShape<D>,
	expectedVersion?: bigint,
): CrudMutation {
	const key = keyOf(def, value);
	validateRowKey(def.name, key);
	return create(CrudMutationSchema, {
		entity: def.name,
		key,
		op: CrudOp.PUT,
		// A reference stores only its target's key fields; strip fat stubs before
		// marshaling, mirroring the server's projection (rebase parity).
		value: toBinary(schema, projectRefs(schema, value)),
		expectedVersion,
	});
}

export function buildPatch<D extends DescMessage>(
	def: EntityDef,
	schema: D,
	key: string,
	init: MessageInitShape<D>,
	paths?: string[],
	expectedVersion?: bigint,
): CrudMutation {
	validateRowKey(def.name, key);
	const value = create(schema, init);
	const maskPaths = paths ?? maskFromInit(schema, init as object);
	// Key fields define identity and travel in `key`; they are never patchable.
	// A mask naming one would let the merge drift the row's embedded key fields
	// away from its storage key (the server rejects this too — see push.go).
	const keyFieldNames = new Set(def.keyFields.map((f) => f.name));
	for (const path of maskPaths) {
		const top = path.split('.')[0]!;
		if (keyFieldNames.has(top)) {
			throw new Error(`entity "${def.name}": PATCH mask cannot name key field "${top}"`);
		}
	}
	return create(CrudMutationSchema, {
		entity: def.name,
		key,
		op: CrudOp.PATCH,
		value: toBinary(schema, projectRefs(schema, value)),
		fieldMask: create(FieldMaskSchema, { paths: maskPaths }),
		expectedVersion,
	});
}

export function buildDelete(def: EntityDef, key: string, expectedVersion?: bigint): CrudMutation {
	validateRowKey(def.name, key);
	return create(CrudMutationSchema, {
		entity: def.name,
		key,
		op: CrudOp.DELETE,
		expectedVersion,
	});
}

/** Bundles CRUD mutations into one atomic batch (see BatchMutation). */
export function buildBatch(mutations: CrudMutation[]): BatchMutation {
	return create(BatchMutationSchema, { mutations });
}

export function buildCustom<D extends DescMessage>(
	name: string,
	argsSchema?: D,
	args?: MessageInitShape<D>,
): CustomMutation {
	return create(CustomMutationSchema, {
		name,
		args: argsSchema ? anyPack(argsSchema, create(argsSchema, args)) : undefined,
	});
}
