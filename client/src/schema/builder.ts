/**
 * A TypeScript-first schema builder for syncable entities.
 *
 * The wire/schema contract is still protobuf — but instead of authoring a
 * `.proto` and running `buf`, you declare entities in TS and the builder
 * synthesizes the same descriptors the engine already speaks. One definition
 * yields three artifacts, shareable across an SPA and a Node backend:
 *
 *   1. inferred static types        — `type Todo = Infer<typeof todo>`
 *   2. runtime entity descriptors   — `defineSchema(todo).descMessages()`,
 *                                      fed straight into `new SyncClient(...)`
 *   3. a serialized FileDescriptorSet — `defineSchema(todo).toBinary()`, the
 *                                      bytes a Go server ingests through its
 *                                      runtime ("dynamic") entity registration.
 *
 * Full parity with the compiled proto tier (see engine/server/dynamic.go):
 * scalars and repeated scalars, opaque bytes, embedded sub-messages
 * (`message`), typed references to other entities (`ref`/`refList`, including
 * self, forward, and composite-key targets), keyed message lists (`keyedList`)
 * and movable order-field lists (`.order()`), and scalar-valued maps (`map`).
 *
 * The whole schema is synthesized into ONE proto file (`sync/dynamic/schema.proto`):
 * intra-file references are always legal, so mutual and self references resolve
 * by full name regardless of declaration order, and there is no circular
 * file-import graph for protodesc to reject. Reference detection is
 * annotation-free — a message field whose target message carries
 * (syncplane.v1.entity) IS a reference — so the engine needs no runtime changes
 * beyond its registration-time guards.
 */

import {
	create,
	createFileRegistry,
	type DescMessage,
	type FileRegistry,
	fromBinary,
	hasOption,
	setExtension,
	toBinary,
	toJson,
} from '@bufbuild/protobuf';
import {
	type DescriptorProto,
	DescriptorProtoSchema,
	type FieldDescriptorProto,
	FieldDescriptorProto_Label,
	FieldDescriptorProto_Type,
	FieldDescriptorProtoSchema,
	FieldOptionsSchema,
	file_google_protobuf_descriptor,
	type FileDescriptorProto,
	FileDescriptorProtoSchema,
	type FileDescriptorSet,
	FileDescriptorSetSchema,
	MessageOptionsSchema,
} from '@bufbuild/protobuf/wkt';

import {
	AccessOptionsSchema,
	AccessParentOptionsSchema,
	type AccessRule,
	AccessRuleSchema,
	AccessWithinSchema,
	AdmittedCallerSchema,
	CreatorOptionsSchema,
	entity as entityExt,
	EntityOptionsSchema,
	field_behavior as fieldBehaviorExt,
	FieldBehaviorSchema,
	FieldGuardSchema,
	file_syncplane_v1_options,
	GrantOptionsSchema,
	IndexKeySchema,
	IndexSpecSchema,
	MembersOptionsSchema,
	reference as referenceExt,
	ReferenceOptionsSchema,
	RoleSchema,
	SearchFieldSchema,
	SearchSpecSchema,
} from '../gen/syncplane/v1/options_pb.js';
import {
	type Condition,
	ConditionSchema,
	file_syncplane_v1_query,
} from '../gen/syncplane/v1/query_pb.js';
import {
	validateEntityName,
	validatePermissionName,
	validateRelationName,
	validateRoleName,
} from '../naming.js';
import type { Cond } from '../query/ast.js';
import { condToProto } from '../query/proto.js';
import { rootRef } from '../query/proxy.js';
import type { QueryRef } from '../query/types.js';

/**
 * Package the synthesized messages live in. Matches engine/server/dynamic.go's
 * default so a builder-produced descriptor and a Go-built one share a full
 * name, and the engine treats them identically.
 */
const DYNAMIC_PACKAGE = 'sync.dynamic';

/** The single synthesized file every entity (and its nested types) lives in. */
const SCHEMA_FILE = 'sync/dynamic/schema.proto';

/** Scalar proto kinds the builder supports (the CRUD/dynamic tier). */
type ProtoScalar = 'string' | 'bool' | 'int32' | 'int64' | 'uint64' | 'double' | 'bytes';

/**
 * Which scalar kinds may form a row key. The client mirror of the rule
 * naming.ts states (KEY_SCALARS) and the server enforces at registration:
 * `bytes` has no canonical text form, and a `double` renders differently in Go
 * and JS, so either would derive one key here and another there. Exhaustive by
 * construction — a new ProtoScalar has to answer the question.
 */
const SCALAR_CAN_KEY: Record<ProtoScalar, boolean> = {
	string: true,
	bool: true,
	int32: true,
	int64: true,
	uint64: true,
	double: false,
	bytes: false,
};

const SCALAR_TO_TYPE: Record<ProtoScalar, FieldDescriptorProto_Type> = {
	string: FieldDescriptorProto_Type.STRING,
	bool: FieldDescriptorProto_Type.BOOL,
	int32: FieldDescriptorProto_Type.INT32,
	int64: FieldDescriptorProto_Type.INT64,
	uint64: FieldDescriptorProto_Type.UINT64,
	double: FieldDescriptorProto_Type.DOUBLE,
	bytes: FieldDescriptorProto_Type.BYTES,
};

/** Proto key kinds allowed on a `map` field. */
type MapKeyKind = 'string' | 'int32' | 'int64' | 'uint64' | 'bool';

const MAP_KEY_TO_TYPE: Record<MapKeyKind, FieldDescriptorProto_Type> = {
	string: FieldDescriptorProto_Type.STRING,
	int32: FieldDescriptorProto_Type.INT32,
	int64: FieldDescriptorProto_Type.INT64,
	uint64: FieldDescriptorProto_Type.UINT64,
	bool: FieldDescriptorProto_Type.BOOL,
};

/** What flavour of field a FieldSpec describes. */
type FieldKind = 'scalar' | 'message' | 'ref' | 'map';

/**
 * The descriptor of a single field, in builder-internal form. `kind` is the
 * discriminant; the kind-specific slots carry the extra shape the synthesizer
 * needs. This is never serialized — it is lowered to a FieldDescriptorProto
 * (plus any nested types) by fieldProtoOf.
 */
interface FieldSpec {
	kind: FieldKind;
	repeated: boolean;
	key: boolean;
	opaque: boolean;
	/** kind 'scalar' — the proto scalar kind. */
	scalar?: ProtoScalar;
	/** kind 'message' — the embedded (non-entity) sub-message's fields. */
	message?: Fields;
	/** kind 'ref' — the target entity, deferred so self/forward refs resolve. */
	ref?: () => EntitySchema;
	/**
	 * kind 'ref' — store the reference as a reference message carrying the
	 * target's workspace beside the stub (see workspaceRef).
	 */
	refWorkspace?: boolean;
	/** kind 'map' — the key kind. */
	mapKey?: MapKeyKind;
	/** kind 'map' — the scalar value kind, when the value is a scalar. */
	mapValue?: ProtoScalar;
	/** kind 'map' — the value's fields, when the value is a message. Exactly one
	 * of mapValue/mapMessage is set. */
	mapMessage?: Fields;
	/** kind 'message' + repeated — keyed message-list identity field(s). */
	keyField?: string[];
	/** kind 'message' + repeated — movable-list position field. */
	orderField?: string;
	/** Maintain a single-component secondary index over this field. */
	indexed?: boolean;
	/** Include this field in the entity's full-text search. */
	searchable?: boolean;
	/** Record who created the row in this field; see Field.creator. */
	creator?: boolean;
	/** kind 'ref' — the name of the reverse relation on the target; see Field.reverseRelation. */
	reverseRelation?: string;
	/** kind 'ref' — place each row below the referenced row; see Field.accessParent. */
	accessParent?: boolean;
}

/**
 * A single entity field. The type parameter T is a phantom carrier for the
 * value the field decodes to; `Infer` reads it off to type the whole row.
 */
export class Field<T> {
	/** @internal */
	readonly spec: FieldSpec;
	/** @internal phantom — never assigned, exists only to carry T. */
	declare readonly _out: T;

	constructor(spec: FieldSpec) {
		this.spec = spec;
	}

	/**
	 * Marks this field as part of the entity's row key (its identity). Key
	 * fields are listed, in declaration order, in (syncplane.v1.entity).key_fields and
	 * are never patchable. Only singular scalar fields can be keys — a message,
	 * reference, map, or repeated field cannot (a row key is the key values
	 * rendered as text), and not every scalar kind renders alike on both sides:
	 * see SCALAR_CAN_KEY.
	 */
	key(): Field<T> {
		if (this.spec.kind !== 'scalar') {
			throw new Error('key() is only valid on a singular scalar field');
		}
		if (this.spec.repeated) throw new Error('a key field cannot be repeated');
		if (this.spec.creator) {
			throw new Error('a key field cannot be creator(): a key field never changes after the create');
		}
		if (!SCALAR_CAN_KEY[this.spec.scalar!]) {
			throw new Error(
				`key() is not valid on a ${this.spec.scalar}() field: a row key must be a string, `
					+ 'integer or bool (see docs/reference/naming.md)',
			);
		}
		return new Field<T>({ ...this.spec, key: true });
	}

	/**
	 * Marks a field opaque: replaced whole on merge, never structurally merged —
	 * the (syncplane.v1.field_behavior).opaque escape hatch for CRDT blobs and other
	 * atomic values. Valid on a singular `bytes()` or a singular `message()`.
	 */
	opaque(): Field<T> {
		const okBytes = this.spec.kind === 'scalar' && this.spec.scalar === 'bytes'
			&& !this.spec.repeated;
		const okMessage = this.spec.kind === 'message' && !this.spec.repeated;
		if (!okBytes && !okMessage) {
			throw new Error('opaque() is only valid on a singular bytes() or message() field');
		}
		return new Field<T>({ ...this.spec, opaque: true });
	}

	/**
	 * Maintains a secondary index over this field, so filtering or ordering by
	 * it costs O(result) rather than a scan of the entity — the
	 * (syncplane.v1.field_behavior).indexed mark.
	 *
	 * Reach for this first: it says "this field is worth looking up by" without
	 * coupling the schema to any particular query. What one marked field cannot
	 * do at O(limit) is serve a filter on itself combined with an ordering by
	 * ANOTHER field — a sorted key-value store has no posting lists to
	 * intersect, so that walks the sort order re-checking the predicate per
	 * candidate. Pass `indexes` to `schema.entity` when that shape is the hot
	 * one.
	 *
	 * The cost is on writes: every write to the entity re-stores this row's
	 * position in every one of its indexes.
	 *
	 * On a `list()` of scalars this is a MULTI-ENTRY index: the row takes one
	 * position per element, which is what makes `has`/`hasAny` a range read
	 * rather than a scan. The write cost grows with the list, since every write
	 * re-stores every element's position.
	 *
	 * Not valid on a leaf no comparison could reach — a map, or a message list
	 * whose elements have no single comparable value — nor on a
	 * collaborative-text field, which this builder cannot declare but a
	 * hand-written .proto can: every keystroke changes the value, so the entry
	 * would be rewritten with it. Registration rejects that.
	 *
	 * Nor on a `ref()`, for the opposite reason: the engine already maintains an
	 * index over every reference field, to-one and to-many alike, which is what
	 * makes a reverse lookup a range probe.
	 */
	indexed(): Field<T> {
		if (this.spec.kind === 'map') {
			// A mark has no room to say WHICH entry, or which variant of an
			// app-defined value — and an index over "every entry" is a shape, so
			// it belongs in an entity index where the path can spell it out.
			throw new Error(
				'indexed() is not valid on a map() field: name the entries in an entity index '
					+ 'instead, e.g. { path: "custom.*.number" }',
			);
		}
		if (this.spec.repeated && this.spec.kind === 'message') {
			// A message element has no single value to key by, so the index needs
			// a path INTO the element — which a field mark has no room to say.
			throw new Error(
				'indexed() is not valid on a message list: name a field of the elements in an entity index instead, e.g. { path: "items.sku" }',
			);
		}
		if (this.spec.kind === 'ref') {
			// Rejected rather than ignored: a reference is recognized by its
			// target being an entity, with no annotation of its own, so there is
			// nowhere on the descriptor for this mark to go — accepting it would
			// silently drop it and leave the schema claiming an index it has not
			// got. (It has one anyway, derived by the engine.)
			throw new Error(
				'indexed() is not valid on a ref() field: reference fields are already indexed by the engine',
			);
		}
		return new Field<T>({ ...this.spec, indexed: true });
	}

	/**
	 * Includes this field in the entity's full-text search, ahead of any fields
	 * the entity's `search` option declares (marked fields count as most
	 * important, in declaration order). Valid on a singular `text()` field
	 * alone: search reads strings, and other shapes are rejected here rather
	 * than at registration so the error names the declaration.
	 */
	searchable(): Field<T> {
		if (this.spec.kind !== 'scalar' || this.spec.scalar !== 'string' || this.spec.repeated) {
			throw new Error(
				'searchable() is only valid on a singular text() field: search reads singular string fields',
			);
		}
		return new Field<T>({ ...this.spec, searchable: true });
	}

	/**
	 * Records who created the row in this field: on a create that is not
	 * trusted, the server stores the caller's primary subject in it, whatever
	 * value the write carries. Afterwards no client changes it: a `patch()`
	 * whose mask reaches it throws at the call site, and a `put()` keeps the
	 * stored value. A trusted
	 * server-side write stores the value it carries.
	 *
	 * The local view shows the primary subject from the last access summary
	 * on a row this client creates, and an empty value before the first
	 * summary arrives. An access rule's `callerIs` check can name the field to
	 * admit only the author.
	 *
	 * Throws unless the field is a singular `text()` that is not a key. A field
	 * guard cannot name it.
	 *
	 * @example
	 * ```ts
	 * const comment = schema.entity('comment', {
	 *   id: schema.text().key(),
	 *   task: schema.ref(() => task).accessParent(),
	 *   authorId: schema.text().creator(),
	 *   body: schema.text(),
	 * }, {
	 *   access: {
	 *     allow: [
	 *       { read: true, requires: 'view_tasks' },
	 *       { create: true, delete: true, requires: 'comment', callerIs: 'authorId' },
	 *       { update: true, requires: 'comment', callerIs: 'authorId', fields: ['body'] },
	 *     ],
	 *   },
	 * });
	 * ```
	 */
	creator(): Field<T> {
		if (this.spec.kind !== 'scalar' || this.spec.scalar !== 'string' || this.spec.repeated) {
			throw new Error('creator() is only valid on a singular text() field');
		}
		if (this.spec.key) {
			throw new Error('creator() is not valid on a key field, which never changes after the create');
		}
		return new Field<T>({ ...this.spec, creator: true });
	}

	/**
	 * Names the reverse relation this reference adds to its target — the
	 * `db.<target>.refs.<name>` accessor, `include` key and `follow` segment
	 * that lists the rows pointing at one target row. Without it, the name is
	 * `<plural(entity)>By<Field>` in camelCase (`issuesByAssignee` for the
	 * `assignee` field of `issue`).
	 *
	 * Reverse relation names must be unique on the target, and must not be the
	 * name of a reference field of the target: the client and the server refuse
	 * a schema that breaks this. Throws when the field is not a reference, or
	 * when `name` breaks the identifier rule in docs/reference/naming.md.
	 *
	 * @example
	 * ```ts
	 * const issue = schema.entity('issue', {
	 *   id: schema.text().key(),
	 *   assignee: schema.ref(() => user), // user.refs.issuesByAssignee
	 *   reporter: schema.ref(() => user).reverseRelation('reportedIssues'),
	 * });
	 * ```
	 */
	reverseRelation(name: string): Field<T> {
		if (this.spec.kind !== 'ref') {
			throw new Error(
				'reverseRelation() is only valid on a ref(), refList(), workspaceRef() or workspaceRefList() field',
			);
		}
		validateRelationName(name, 'reverseRelation()');
		return new Field<T>({ ...this.spec, reverseRelation: name });
	}

	/**
	 * Places each row below the row this reference points at in the access
	 * model, or below each row a `refList()` points at: a permission held at a
	 * referenced row applies to this row. An entity can mark several fields,
	 * and a row then sits below every row they name. Both it and the
	 * referenced entity must declare `access`; see
	 * {@link EntityOptions.access}. A grant entity marks one singular field.
	 *
	 * Throws unless the field is a `ref()` or a `refList()`. A field guard
	 * cannot name it.
	 *
	 * @example
	 * ```ts
	 * const task = schema.entity('task', {
	 *   id: schema.text().key(),
	 *   project: schema.ref(() => project).accessParent(),
	 * }, {
	 *   access: {
	 *     allow: [
	 *       { read: true, requires: 'view_tasks' },
	 *       { write: true, requires: 'edit_tasks' },
	 *     ],
	 *   },
	 * });
	 * ```
	 */
	accessParent(): Field<T> {
		if (this.spec.kind !== 'ref' || this.spec.refWorkspace) {
			throw new Error('accessParent() is only valid on a ref() or refList() field');
		}
		return new Field<T>({ ...this.spec, accessParent: true });
	}
}

/**
 * A keyed message list. Adds `.order(posField)` to promote it to a movable
 * list ordered by a fractional-index position field on the element.
 */
export class KeyedListField<T> extends Field<T> {
	/**
	 * Makes this a movable list: `posField` (a real field on the element) holds
	 * the framework-managed fractional order key. Reordering patches that one
	 * field on one element; the merge layer treats it as ordinary data.
	 */
	order(posField: string): KeyedListField<T> {
		const elem = this.spec.message;
		if (!elem || !Object.hasOwn(elem, posField)) {
			throw new Error(`order() field "${posField}" is not a field of the list element`);
		}
		return new KeyedListField<T>({ ...this.spec, orderField: posField });
	}
}

const scalar = <T>(s: ProtoScalar): Field<T> =>
	new Field<T>({ kind: 'scalar', scalar: s, repeated: false, key: false, opaque: false });

/** A proto `string` field. */
export const text = (): Field<string> => scalar<string>('string');
/** A proto `bool` field. */
export const bool = (): Field<boolean> => scalar<boolean>('bool');
/** A proto `int32` field (decodes to a JS number). */
export const int32 = (): Field<number> => scalar<number>('int32');
/** A proto `int64` field (decodes to a JS bigint). */
export const int64 = (): Field<bigint> => scalar<bigint>('int64');
/** A proto `uint64` field (decodes to a JS bigint). */
export const uint64 = (): Field<bigint> => scalar<bigint>('uint64');
/** A proto `double` field. */
export const double = (): Field<number> => scalar<number>('double');
/** A proto `bytes` field. Add `.opaque()` to treat it as an atomic blob. */
export const bytes = (): Field<Uint8Array> => scalar<Uint8Array>('bytes');

/** A repeated field of a scalar element (proto `repeated`). */
export const list = <T>(inner: Field<T>): Field<T[]> => {
	if (inner.spec.kind !== 'scalar') {
		throw new Error('list() takes a scalar element; use keyedList() for message lists');
	}
	if (inner.spec.repeated) throw new Error('list() cannot nest lists');
	if (inner.spec.key) throw new Error('a key field cannot be repeated');
	if (inner.spec.opaque) {
		throw new Error('opaque() applies to a singular bytes field, not list elements');
	}
	return new Field<T[]>({ ...inner.spec, repeated: true });
};

/** The row shape an embedded message's fields decode to. */
export type RowOf<F extends Fields> = {
	[K in keyof F]: F[K] extends Field<infer T> ? T : never;
};

/**
 * An embedded (non-entity) sub-message field, e.g. `settings: message({ color:
 * text(), size: int32() })`. Per-field LWW descends into it on PATCH. Add
 * `.opaque()` to replace it whole instead. Also the element of a keyedList.
 */
export const message = <F extends Fields>(fields: F): Field<RowOf<F>> => {
	assertFields('message()', fields);
	return new Field<RowOf<F>>({
		kind: 'message',
		message: fields,
		repeated: false,
		key: false,
		opaque: false,
	});
};

/**
 * A singular typed reference to another entity. The target is a thunk so self
 * and forward references resolve (the entity may not be defined yet). The
 * stored row keeps only the target's key fields; the live target is resolved on
 * read. References are never keys and never opaque.
 */
export const ref = <E extends EntitySchema>(target: () => E): Field<Infer<E>> =>
	new Field<Infer<E>>({ kind: 'ref', ref: target, repeated: false, key: false, opaque: false });

/** A to-many typed reference (repeated `ref`). */
export const refList = <E extends EntitySchema>(target: () => E): Field<Infer<E>[]> =>
	new Field<Infer<E>[]>({ kind: 'ref', ref: target, repeated: true, key: false, opaque: false });

/**
 * The stored shape of a workspace-qualified reference: the target stub under
 * `value`, and the workspace it lives in under `workspaceId`. An empty
 * `workspaceId` means the holder's own workspace.
 */
export interface WorkspaceRef<T> {
	workspaceId: string;
	value?: T;
}

/**
 * A singular typed reference that may point into another workspace. Stored as
 * a reference message (see (syncplane.v1.reference)): the target's key fields
 * under `value` and the workspace they resolve in under `workspaceId`, both
 * replaced together on every write. Reading resolves the stub in the workspace
 * it names, from the workspaces the client holds; the server's `matches()`,
 * `include` and `follow` stay inside one workspace.
 *
 * @example
 * ```ts
 * const task = schema.entity('task', {
 *   id: schema.text().key(),
 *   assignee: schema.workspaceRef(() => person),
 * });
 * await client.ws('proj-1').db.task.mutate.create({
 *   id: 't1',
 *   assignee: { workspaceId: 'acme-org', value: { id: 'u1' } },
 * });
 * ```
 */
export const workspaceRef = <E extends EntitySchema>(
	target: () => E,
): Field<WorkspaceRef<Infer<E>>> =>
	new Field<WorkspaceRef<Infer<E>>>({
		kind: 'ref',
		ref: target,
		refWorkspace: true,
		repeated: false,
		key: false,
		opaque: false,
	});

/**
 * A to-many workspace-qualified reference (repeated `workspaceRef`). Each
 * element names its own workspace. The list merges as a set: an `update` that
 * adds or removes elements writes only those, identified by their workspace
 * and key together, so concurrent adds and removes from different clients all
 * land. An element with an empty `workspaceId` and one naming the holder's
 * own workspace are two elements, even though they resolve to the same row.
 */
export const workspaceRefList = <E extends EntitySchema>(
	target: () => E,
): Field<WorkspaceRef<Infer<E>>[]> =>
	new Field<WorkspaceRef<Infer<E>>[]>({
		kind: 'ref',
		ref: target,
		refWorkspace: true,
		repeated: true,
		key: false,
		opaque: false,
	});

/**
 * A keyed message list: a repeated embedded message merged per item by
 * identity. `keyFields` name the element field(s) that form an item's identity
 * (composite allowed). Chain `.order(posField)` to make it a movable list.
 */
export const keyedList = <F extends Fields>(
	elem: Field<RowOf<F>>,
	...keyFields: string[]
): KeyedListField<RowOf<F>[]> => {
	if (elem.spec.kind !== 'message' || !elem.spec.message) {
		throw new Error('keyedList() element must be a message()');
	}
	if (keyFields.length === 0) throw new Error('keyedList() needs at least one key field');
	for (const kf of keyFields) {
		if (!Object.hasOwn(elem.spec.message, kf)) {
			throw new Error(`keyedList() key field "${kf}" is not a field of the element`);
		}
	}
	return new KeyedListField<RowOf<F>[]>({
		kind: 'message',
		message: elem.spec.message,
		repeated: true,
		key: false,
		opaque: false,
		keyField: keyFields,
	});
};

/** The JS object-key type a proto map key decodes to. */
type MapKeyTs<K extends MapKeyKind> = K extends 'string' ? string
	: K extends 'int32' ? number
	: string;

/**
 * A map (`map<key, value>`), merged per entry by map key. The key is one of the
 * integral/string/bool proto key kinds; the value is a scalar or a `message()`.
 * Maps are never keys, opaque, or repeated.
 *
 * A MESSAGE value is what user-defined ("custom") fields are declared with: the
 * app states its own value type, typically a message whose fields are the
 * variants a custom field may hold, and a query names the variant it means
 * (`i.custom.at('priority')…`). The engine needs no value type of its own for
 * this — see MapOps in query/types.ts for what that buys and the one thing it
 * costs.
 */
export const map = <K extends MapKeyKind, V>(
	keyType: K,
	value: Field<V>,
): Field<Record<MapKeyTs<K>, V>> => {
	if (!Object.hasOwn(MAP_KEY_TO_TYPE, keyType)) {
		throw new Error(`map() key type "${keyType}" is not a valid map key kind`);
	}
	if (value.spec.kind !== 'scalar' && value.spec.kind !== 'message') {
		throw new Error('map() value must be a scalar or a message() field');
	}
	if (value.spec.repeated) throw new Error('map() value cannot be repeated');
	if (value.spec.key) throw new Error('map() value cannot be a key field');
	if (value.spec.opaque) throw new Error('map() value cannot be opaque');
	return new Field<Record<MapKeyTs<K>, V>>({
		kind: 'map',
		mapKey: keyType,
		mapValue: value.spec.scalar,
		mapMessage: value.spec.message,
		repeated: false,
		key: false,
		opaque: false,
	});
};

/** The field map an entity is declared with: lowerCamelCase keys → fields. */
export type Fields = Record<string, Field<unknown>>;

/** A declared entity: its wire name, its fields, and its proto full name. */
export interface EntitySchema<F extends Fields = Fields> {
	/** Logical entity name on the wire and in storage (e.g. "todo"). */
	readonly name: string;
	/** The declared fields, keyed by lowerCamelCase accessor name. */
	readonly fields: F;
	/** Composite secondary indexes declared on the entity, in declaration order. */
	readonly indexes: readonly IndexDecl[];
	/** Full-text search declaration, when the entity has one. */
	readonly search?: SearchOpts;
	/** Who can read and write the entity's rows; see {@link EntityOptions.access}. */
	readonly access?: AccessDecl;
	/** The roles the entity's rows grant; see {@link EntityOptions.grant}. */
	readonly grant?: GrantDecl;
	/** Who the members of each row are; see {@link EntityOptions.members}. */
	readonly members?: MembersDecl;
	/** @internal synthesized proto full name, e.g. "sync.dynamic.Todo". */
	readonly typeName: string;
}

/**
 * Who can read and write an entity's rows in the access model, mirroring
 * syncplane.v1.AccessOptions.
 *
 * An operation is allowed when at least one rule admits it, and the order of
 * the rules has no effect. An operation no rule admits is open only to
 * trusted server-side writes, such as connectors, effects and custom mutator
 * bodies, and the client refuses a write that can only be such an operation
 * at the call site.
 *
 * `allow` holds at least one rule, and no two rules the same.
 *
 * Within `entity()`, a rule's `callerIs` and `fields`, and a grant's `subject`
 * and `role`, accept only the names of the entity's fields. A declaration
 * annotated as `AccessDecl` takes any name, and does not fit an entity's
 * options; to share rules between entities, check them with `satisfies`.
 *
 * @example
 * ```ts
 * const readWrite = {
 *   allow: [ { read: true, requires: 'view_tasks' }, { write: true, requires: 'edit_tasks' } ],
 * } satisfies schema.AccessDecl;
 * const task = schema.entity('task', { id: schema.text().key() }, { access: readWrite });
 * ```
 */
export interface AccessDecl<F extends Fields = Fields> {
	allow: readonly AccessRuleDecl<F>[];
	/**
	 * The field guards: each one restricts who can write a set of fields,
	 * whatever rule in `allow` admits the write. A field that no guard names
	 * follows `allow` alone. See {@link FieldGuardDecl}.
	 */
	guard?: readonly FieldGuardDecl<F>[];
}

/**
 * Restricts who can write a set of an entity's fields, mirroring
 * syncplane.v1.FieldGuard.
 *
 * A write that sets a guarded field needs a rule in `allow` of the entity that
 * admits the write, and a rule of the guard that admits the field operation:
 * `create` for the value a create stores, `update` for a change of a row that
 * exists, and `write` for both. A guard rule decides as an entity rule does,
 * on the row as it will be stored and, for an update, on the stored row. A
 * field operation that no guard rule admits is open only to trusted
 * server-side writes.
 *
 * When the guard does not admit the caller, a `patch()` whose mask names a
 * guarded field is refused, a `put()` that updates a row keeps the stored
 * value of each guarded field, and a `put()` that creates a row stores the
 * zero value. The client refuses a patch at the call site, and keeps or clears
 * the field on a put, only where no guard rule admits the operation to any
 * caller; the server decides the rest, and a refused patch comes back as a
 * rejection ack.
 *
 * `fields` names the guarded fields by accessor name. A field is in at most
 * one guard, and cannot be a key field, a `.creator()` field or an
 * `.accessParent()` reference. `allow` holds the rule
 * `{ read: true, admitted: true }`, so that every reader of a row reads its
 * guarded fields, and rules that name `create`, `update` or `write`. A guard
 * rule cannot name `delete` or `fields`, and its `callerIs` and `when` cannot
 * read a guarded field.
 *
 * To let a caller change only some fields of a row, set `fields` on an update
 * rule in `allow` instead: that rule adds a narrow update, and a guard takes a
 * field away from every rule.
 *
 * @example
 * A total that only server code computes, and a status that only managers
 * set:
 * ```ts
 * const order = schema.entity('order', {
 *   id: schema.text().key(),
 *   total: schema.int64(),
 *   status: schema.text(),
 * }, {
 *   access: {
 *     allow: [ { read: true, requires: 'view_orders' }, { write: true, requires: 'edit_orders' } ],
 *     guard: [
 *       { fields: [ 'total' ], allow: [ { read: true, admitted: true } ] },
 *       {
 *         fields: [ 'status' ],
 *         allow: [ { read: true, admitted: true }, { write: true, requires: 'manage_orders' } ],
 *       },
 *     ],
 *   },
 * });
 * ```
 */
export interface FieldGuardDecl<F extends Fields = Fields> {
	fields: readonly FieldKeys<F>[];
	allow: readonly GuardRuleDecl<F>[];
}

/** The accessor names of the fields of F; any string where F is not known. */
type FieldKeys<F extends Fields> = Fields extends F ? string : Extract<keyof F, string>;

/** The accessor names of the `text()` fields of F; any string where F is not known. */
type TextKeys<F extends Fields> = Fields extends F ? string
	: {
		[K in Extract<keyof F, string>]: F[K] extends Field<string> ? K : never;
	}[Extract<keyof F, string>];

/**
 * The accessor names of the fields of F that can hold a grant subject: `text()`
 * fields and singular references. Any string where F is not known.
 */
type SubjectKeys<F extends Fields> = Fields extends F ? string : {
	[K in Extract<keyof F, string>]: F[K] extends Field<infer T> ? [T] extends [string] ? K
		: T extends readonly unknown[] | Uint8Array ? never
		: T extends object ? K
		: never
		: never;
}[Extract<keyof F, string>];

/**
 * One rule of an {@link AccessDecl}, mirroring syncplane.v1.AccessRule: the
 * operations it admits, who it admits them to, and the checks the row must
 * pass.
 *
 * At least one operation must be `true`. `write` admits `create`, `update`
 * and `delete`. The source is either `requires`, which admits a caller that
 * holds every permission it names at the workspace root, at the row, or at a
 * row above it through its `.accessParent()` references, each at any one of
 * these places, or `admitted: true`, which admits every caller admitted to the
 * workspace. `requires` names no permission twice. To admit a caller that
 * holds either of two permissions, declare two rules. The server's workspace
 * hook admits a caller; in a schema where some entity declares `grant`, the
 * caller must also hold at least one permission in the workspace.
 *
 * `callerIs` names a singular `text()` field that must hold one of the
 * caller's subjects; an empty value never matches, and a permission held at
 * the root does not skip the check. `fields` names the fields an update may
 * change, by accessor name; it is valid only on a rule whose one operation is
 * `update`, and cannot name a key field. A move changes an access parent
 * field, so a rule with `fields` admits a move only when it lists that field.
 *
 * `when` builds a condition the row must match, with the same field proxy a
 * query's `where` takes. It reads only the row: a `matches` or a `match`
 * condition is refused. A permission held at the root does not skip it.
 *
 * An update must be admitted by one rule for both the stored row and the row
 * as it will be stored, so a rule admits moving a row to another access parent
 * only when its source holds at both places, a rule with `callerIs` admits no
 * update that writes someone else into that field, and a rule with
 * `when` admits no update that makes the row fail the condition. A row with
 * several access parents shows to the readers of each, so a rule admits a
 * create, or an update that adds or removes a parent, only when its source
 * holds through each parent the write adds or removes on its own.
 *
 * @example
 * ```ts
 * const allow: schema.AccessRuleDecl[] = [
 *   { read: true, requires: 'view_tasks' },
 *   { create: true, delete: true, requires: 'comment', callerIs: 'authorId' },
 *   { update: true, requires: 'comment', callerIs: 'authorId', fields: ['body'] },
 *   { update: true, delete: true, requires: 'moderate_comments' },
 * ];
 * ```
 *
 * @example
 * Tasks that need a project role and a billing role at the root to edit:
 * ```ts
 * const allow: schema.AccessRuleDecl[] = [
 *   { read: true, requires: 'view_tasks' },
 *   { update: true, requires: ['edit_tasks', 'use_billing'] },
 * ];
 * ```
 *
 * @example
 * Tasks a writer edits until someone archives them:
 * ```ts
 * const task = schema.entity('task', {
 *   id: schema.text().key(),
 *   archived: schema.bool(),
 * }, {
 *   access: {
 *     allow: [
 *       { read: true, requires: 'view_tasks' },
 *       { update: true, requires: 'edit_tasks', when: (t) => t.archived.eq(false) },
 *       { update: true, requires: 'archive_tasks' },
 *     ],
 *   },
 * });
 * ```
 */
export type AccessRuleDecl<F extends Fields = Fields> =
	& {
		delete?: boolean;
		fields?: readonly FieldKeys<F>[];
	}
	& GuardRuleDecl<F>;

/**
 * One rule of a {@link FieldGuardDecl}: an {@link AccessRuleDecl} without
 * `delete` and `fields`. `write` admits `create` and `update`.
 */
export type GuardRuleDecl<F extends Fields = Fields> =
	& {
		read?: boolean;
		create?: boolean;
		update?: boolean;
		write?: boolean;
		callerIs?: TextKeys<F>;
		when?(row: QueryRef<RowOf<F>>): Cond;
		within?: WithinEntry<F> | readonly WithinEntry<F>[];
	}
	& (
		| { requires: string | readonly [string, ...string[]]; admitted?: never }
		| { admitted: true; requires?: never }
	);

/**
 * One entry of an access rule's `within`, mirroring syncplane.v1.AccessWithin:
 * the rule admits only rows that are, or sit below, a row of `entity` that
 * passes every check the entry sets. A row passes when the row itself, or any
 * row above it through `.accessParent()` references, is such a row.
 *
 * `entity` names an entity of the same schema whose rows the rule's rows can
 * sit below, by its wire name or as a thunk that returns it. A thunk is
 * called by `defineSchema()`, so it can name an entity declared later. For
 * the rule's own entity, when its rows sit below rows of their own, use
 * {@link SelfWithinDecl}. `callerIs` names a singular `text()` field of that
 * entity, by accessor name, that must hold one of the caller's subjects;
 * `when` is a condition that row must match, and reads only that row. At
 * least one of the two is set. Build an entry with `when` through
 * {@link within}, which types the condition against the entity's fields. A
 * permission held at the root does not skip the check.
 *
 * A write to a row that changes whether it passes changes what the rows below
 * it show, as a move does, so it needs a read rule of each entity whose rows
 * can sit below it. In a recursive entity, a row that fails does not hide the
 * rows below it when a row above it passes.
 *
 * @example
 * Comments the owner of their doc reads and writes:
 * ```ts
 * const comment = schema.entity('comment', {
 *   id: schema.text().key(),
 *   doc: schema.ref(() => doc).accessParent(),
 * }, {
 *   access: {
 *     allow: [
 *       { read: true, write: true, admitted: true, within: { entity: 'doc', callerIs: 'ownerId' } },
 *     ],
 *   },
 * });
 * ```
 */
export interface WithinDecl<F extends Fields = Fields> {
	entity: string | (() => EntitySchema);
	self?: never;
	callerIs?: Extract<keyof F, string>;
	when?(row: QueryRef<RowOf<F>>): Cond;
}

/**
 * A within entry on rows of the rule's own entity, for an entity whose rows
 * sit below rows of their own through an `.accessParent()` reference to
 * itself. It means the same as a {@link WithinDecl} that names the entity,
 * and types `callerIs` and `when` against the entity's fields.
 *
 * @example
 * Folders that every caller reads when the folder, or a folder above it, is
 * not private:
 * ```ts
 * const folder = schema.entity('folder', {
 *   id: schema.text().key(),
 *   parent: schema.ref(() => folder).accessParent(),
 *   private: schema.bool(),
 * }, {
 *   access: {
 *     allow: [
 *       { read: true, requires: 'view_private' },
 *       { read: true, admitted: true, within: { self: true, when: (f) => f.private.eq(false) } },
 *     ],
 *   },
 * });
 * ```
 */
export interface SelfWithinDecl<F extends Fields = Fields> {
	self: true;
	entity?: never;
	callerIs?: TextKeys<F>;
	when?(row: QueryRef<RowOf<F>>): Cond;
}

/** One entry of an access rule's `within`: {@link WithinDecl} or {@link SelfWithinDecl}. */
export type WithinEntry<F extends Fields = Fields> = WithinDecl | SelfWithinDecl<F>;

/**
 * Builds a within entry on rows of `target`, with `when` typed against its
 * fields. See {@link WithinDecl}. Pass a thunk that returns the target when
 * it is declared after the entity whose rule holds the entry.
 *
 * @example
 * Tasks that clerks read unless their project is private, and that nobody
 * edits while the project is archived:
 * ```ts
 * const task = schema.entity('task', {
 *   id: schema.text().key(),
 *   project: schema.ref(() => project).accessParent(),
 * }, {
 *   access: {
 *     allow: [
 *       { read: true, requires: 'view_tasks' },
 *       {
 *         read: true,
 *         requires: 'view_open_tasks',
 *         within: schema.within(() => project, { when: (p) => p.private.eq(false) }),
 *       },
 *       {
 *         write: true,
 *         requires: 'edit_tasks',
 *         within: schema.within(() => project, { when: (p) => p.archived.eq(false) }),
 *       },
 *     ],
 *   },
 * });
 * const project = schema.entity('project', {
 *   id: schema.text().key(),
 *   private: schema.bool(),
 *   archived: schema.bool(),
 * }, {
 *   access: { allow: [ { read: true, requires: 'view_tasks' } ] },
 * });
 * ```
 */
export function within<F extends Fields>(
	target: EntitySchema<F> | (() => EntitySchema<F>),
	checks: Omit<WithinDecl<F>, 'entity' | 'self'>,
): WithinDecl {
	return { ...checks, entity: typeof target === 'function' ? target : target.name } as WithinDecl;
}

/**
 * The roles an entity's rows grant, mirroring syncplane.v1.GrantOptions. Each
 * row gives the subject in its `subject` field the role in its `role` field,
 * at the row its `.accessParent()` reference points at, or at the workspace
 * root when the entity has no access parent.
 */
export interface GrantDecl<F extends Fields = Fields> {
	/**
	 * Accessor name of the field that holds the subject: a `text()` field,
	 * which names one caller, or a singular `ref()` to an entity that declares
	 * `members`, which names a team and grants to every member of it.
	 */
	subject: SubjectKeys<F>;
	/** Accessor name of the `text()` field that holds the role. */
	role: TextKeys<F>;
	/** The roles rows can grant. At least one, with names unique across the schema. */
	roles: readonly RoleDecl[];
}

/**
 * The entity whose rows say who the members of each team row are, mirroring
 * syncplane.v1.MembersOptions.
 *
 * Each live row of the member entity makes its subject a member of the team
 * row its `.accessParent()` reference points at, whatever its role. The member
 * entity is a grant entity whose access parent is this entity alone, and whose
 * subject is a `text()` field.
 */
export interface MembersDecl {
	/** Wire name of the member entity. */
	entity: string;
}

/**
 * One role of a {@link GrantDecl}, mirroring syncplane.v1.Role. A role grants
 * the permissions in `grants` and every permission of each role in
 * `includes`.
 *
 * @example
 * ```ts
 * roles: [
 *   { name: 'project_reader', grants: ['view_projects', 'view_tasks'] },
 *   { name: 'project_writer', includes: ['project_reader'], grants: ['edit_tasks'] },
 * ]
 * ```
 */
export type RoleDecl =
	& RoleFields
	& ({ grants: readonly string[] } | { includes: readonly string[] });

/** The fields of a {@link RoleDecl}. */
interface RoleFields {
	name: string;
	/**
	 * The permissions the role grants at its anchor, without duplicates. A
	 * permission that an included role also grants is accepted.
	 */
	grants?: readonly string[];
	/**
	 * The names of roles of the same grant whose permissions this role also
	 * grants, without duplicates. A role that includes itself, directly or
	 * through other roles, is refused.
	 */
	includes?: readonly string[];
}

/**
 * One composite secondary index, mirroring syncplane.v1.IndexSpec. Declare one
 * when a query FILTERS on one field and ORDERS by another — the shape a single
 * `.indexed()` field can only serve by re-checking every candidate.
 */
export interface IndexDecl {
	/**
	 * Unique within the entity. Must not start with "__", which is reserved for
	 * indexes the engine derives for itself (one per reference field).
	 */
	name: string;
	/** The index's components, in sort order. At least one. */
	key: IndexKeyDecl[];
}

/** One component of an {@link IndexDecl}. */
export interface IndexKeyDecl {
	/**
	 * Dotted field path in lowerCamelCase accessor names, e.g.
	 * "settings.size" — the same form a query path takes.
	 */
	path: string;
	/**
	 * Sort this component descending. Missing and NaN values sort last whatever
	 * this says, exactly as in a query ordering.
	 */
	desc?: boolean;
}

/** The row shape an entity decodes to: `type Todo = Infer<typeof todo>`. */
export type Infer<E extends EntitySchema> = RowOf<E['fields']>;

/** Validates a field map has at least one field and no proto-name collisions. */
function assertFields(where: string, fields: Fields): void {
	const keys = Object.keys(fields);
	if (keys.length === 0) throw new Error(`${where} declares no fields`);
	const protoNames = new Set<string>();
	for (const key of keys) {
		const protoName = snakeCase(key);
		if (protoNames.has(protoName)) {
			throw new Error(`${where}: field "${key}" collides on proto name "${protoName}"`);
		}
		protoNames.add(protoName);
	}
}

/**
 * Declares a syncable entity.
 *
 * Field declaration order is significant: fields are assigned proto numbers
 * 1..N in order, so schema evolution is append-only — adding a field at the end
 * is safe, but reordering or removing one renumbers the wire and breaks
 * compatibility (the engine's additive-evolution invariant).
 *
 * At least one field must be marked `.key()`.
 */
export function entity<F extends Fields>(
	name: string,
	fields: F,
	options?: EntityOptions<NoInfer<F>>,
): EntitySchema<F> {
	// The name reaches the wire, storage and the KV key layout, and is lowered to
	// a proto message name below — the shared rule (naming.ts) covers all of it.
	validateEntityName(name, 'entity()');
	assertFields(`entity "${name}"`, fields);

	let keyCount = 0;
	for (const key of Object.keys(fields)) {
		if (fields[key]!.spec.key) keyCount++;
	}
	if (keyCount === 0) {
		throw new Error(`entity "${name}" declares no key field (mark one with .key())`);
	}

	const indexes = options?.indexes ?? [];
	assertIndexes(name, indexes);
	// A rule's `when` is typed against F for the caller. Past this point only
	// the builder calls it, with a field proxy that serves any row type.
	const opts = options as EntityOptions | undefined;
	assertAccess(name, fields, opts);
	return {
		name,
		fields,
		indexes,
		...(options?.search === undefined ? {} : { search: options.search }),
		...(opts?.access === undefined ? {} : { access: opts.access }),
		...(opts?.grant === undefined ? {} : { grant: opts.grant }),
		...(options?.members === undefined ? {} : { members: options.members }),
		typeName: `${DYNAMIC_PACKAGE}.${pascalCase(name)}`,
	};
}

/** An entity's full-text search declaration; see {@link EntityOptions.search}. */
export interface SearchOpts {
	/** Dotted accessor paths to singular string fields, most important first. */
	fields: string[];
	/**
	 * Keep diacritics significant: "café" and "cafe" analyze to distinct
	 * terms.
	 *
	 * @defaultValue false
	 */
	keepDiacritics?: boolean;
}

/** Optional per-entity settings, beyond the fields themselves. */
export interface EntityOptions<F extends Fields = Fields> {
	/**
	 * Composite secondary indexes over this entity's fields. See
	 * {@link IndexDecl}; for a single field, `.indexed()` is the simpler
	 * surface and desugars to the same thing.
	 */
	indexes?: IndexDecl[];
	/**
	 * Full-text search over this entity's fields, mirroring
	 * syncplane.v1.SearchSpec. Fields are dotted accessor paths, most important
	 * first, counting any `.searchable()` marks ahead of them; a match query
	 * (`w.match(...)`) reads them through the entity's analyzer. Set
	 * `keepDiacritics` to keep "café" and "cafe" distinct.
	 */
	search?: SearchOpts;
	/**
	 * Who can read and write rows of this entity in the access model; see
	 * {@link AccessDecl}.
	 *
	 * When omitted, the entity is outside the access model: every caller the
	 * server admits to the workspace reads and writes its rows, subject only
	 * to the app's permission policy, and `can()` answers `'yes'` for them.
	 * A server built with `WithStrictAccess` refuses such an entity in a schema
	 * where any entity declares `access` or `grant`.
	 *
	 * Collaborative-text fields are edited by PATCH, so a client edits them
	 * only where a rule admits `update`.
	 *
	 * Permission names follow the identifier rule in docs/reference/naming.md.
	 * The server refuses a permission no role grants, and logs a warning for a
	 * role that grants a permission no rule uses.
	 *
	 * @example
	 * Rows a client can post but not change or delete afterwards. A `put()`
	 * that passes `expectedVersion: 0n` is a create; one that passes no
	 * version could replace a stored row, and the server refuses it when the
	 * row exists.
	 * ```ts
	 * const entry = schema.entity('entry', {
	 *   id: schema.text().key(),
	 *   body: schema.text(),
	 * }, {
	 *   access: {
	 *     allow: [ { read: true, create: true, requires: 'post_entries' } ],
	 *   },
	 * });
	 * ```
	 */
	access?: AccessDecl<F>;
	/**
	 * Declares the entity's rows as grant rows; see {@link GrantDecl}. A grant
	 * entity must also declare `access`.
	 *
	 * @example
	 * ```ts
	 * const projectMember = schema.entity('project_member', {
	 *   id: schema.text().key(),
	 *   project: schema.ref(() => project).accessParent(),
	 *   userId: schema.text(),
	 *   role: schema.text(),
	 * }, {
	 *   grant: {
	 *     subject: 'userId',
	 *     role: 'role',
	 *     roles: [
	 *       { name: 'project_reader', grants: ['view_projects', 'view_tasks'] },
	 *       { name: 'project_writer', includes: ['project_reader'], grants: ['edit_tasks'] },
	 *     ],
	 *   },
	 *   access: {
	 *     allow: [
	 *       { read: true, requires: 'view_members' },
	 *       { write: true, requires: 'manage_members' },
	 *     ],
	 *   },
	 * });
	 * ```
	 */
	grant?: GrantDecl<F>;
	/**
	 * Declares the entity's rows as teams; see {@link MembersDecl}. A grant
	 * whose subject is a `ref()` to this entity grants to every member of the
	 * team row it names. An entity with members must also declare `access`.
	 *
	 * A write that is not trusted cannot delete a team row while a live grant
	 * row names it as its subject.
	 *
	 * @example
	 * ```ts
	 * const team = schema.entity('team', { id: schema.text().key() }, {
	 *   members: { entity: 'team_member' },
	 *   access: { allow: [ { read: true, requires: 'view_teams' } ] },
	 * });
	 * const teamMember = schema.entity('team_member', {
	 *   id: schema.text().key(),
	 *   team: schema.ref(() => team).accessParent(),
	 *   userId: schema.text(),
	 *   role: schema.text(),
	 * }, {
	 *   grant: { subject: 'userId', role: 'role', roles: [ { name: 'team_member', grants: ['view_teams'] } ] },
	 *   access: { allow: [ { read: true, requires: 'view_teams' } ] },
	 * });
	 * const projectTeam = schema.entity('project_team', {
	 *   id: schema.text().key(),
	 *   project: schema.ref(() => project).accessParent(),
	 *   team: schema.ref(() => team),
	 *   role: schema.text(),
	 * }, {
	 *   grant: { subject: 'team', role: 'role', roles: [ { name: 'project_team_writer', grants: ['view_tasks', 'edit_tasks'] } ] },
	 *   access: { allow: [ { read: true, requires: 'view_tasks' } ] },
	 * });
	 * ```
	 */
	members?: MembersDecl;
}

/**
 * Validates the access declarations of one entity against the rules the
 * engine enforces on one entity at registration. defineSchema() checks the
 * rules that span the entities of one schema, and registration the rest, such
 * as coverage of permissions and unique role names.
 */
function assertAccess(
	entityName: string,
	fields: Fields,
	options: EntityOptions | undefined,
): void {
	const where = `entity "${entityName}"`;
	const access = options?.access;
	if (access !== undefined) assertAccessRules(where, entityName, access, fields, false);
	const parents = Object.keys(fields).filter((key) => fields[key]!.spec.accessParent === true);
	if (parents.length > 0 && access === undefined) {
		throw new Error(
			`${where}: field "${parents[0]}" is marked accessParent(), but the entity declares no access`,
		);
	}
	const members = options?.members;
	if (members !== undefined) {
		if (access === undefined) throw new Error(`${where}: an entity with members must declare access`);
		if (!members.entity) throw new Error(`${where}: members names no entity`);
	}
	const grant = options?.grant;
	if (grant === undefined) return;
	if (access === undefined) {
		throw new Error(`${where}: a grant entity must declare access`);
	}
	// A grant row applies at one anchor.
	if (parents.length > 1 || parents.some((key) => fields[key]!.spec.repeated)) {
		throw new Error(
			`${where}: a grant entity has one access parent: one singular ref() marked accessParent()`,
		);
	}
	for (const part of [ 'subject', 'role' ] as const) {
		const spec = fields[grant[part]]?.spec;
		if (spec === undefined) {
			throw new Error(`${where}: grant ${part} field "${grant[part]}" is not declared`);
		}
		const text = spec.kind === 'scalar' && spec.scalar === 'string' && !spec.repeated;
		const team = spec.kind === 'ref' && !spec.repeated && !spec.refWorkspace;
		if (part === 'subject' && !text && !team) {
			throw new Error(
				`${where}: grant subject field "${grant.subject}" must be a singular text() field or a singular ref()`,
			);
		}
		if (part === 'role' && !text) {
			throw new Error(`${where}: grant role field "${grant.role}" must be a singular text() field`);
		}
	}
	if (grant.subject === grant.role) {
		throw new Error(`${where}: grant names field "${grant.subject}" as both subject and role`);
	}
	if (grant.roles.length === 0) throw new Error(`${where}: grant declares no roles`);
	const names = new Set<string>();
	for (const role of grant.roles) {
		validateRoleName(role.name, `${where} grant`);
		if (names.has(role.name)) throw new Error(`${where}: grant declares role "${role.name}" twice`);
		names.add(role.name);
		const grants = role.grants ?? [];
		if (grants.length === 0 && (role.includes ?? []).length === 0) {
			throw new Error(`${where}: grant role "${role.name}" grants no permissions`);
		}
		const perms = new Set<string>();
		for (const perm of grants) {
			validatePermissionName(perm, `${where} grant role "${role.name}"`);
			if (perms.has(perm)) {
				throw new Error(`${where}: grant role "${role.name}" grants "${perm}" twice`);
			}
			perms.add(perm);
		}
	}
	assertRoleIncludes(where, grant.roles, names);
}

/**
 * Validates the `includes` of every role of one grant against the rules the
 * engine enforces at registration: each names a role of the same grant, once,
 * and no role includes itself through any chain.
 */
function assertRoleIncludes(where: string, roles: readonly RoleDecl[], names: Set<string>): void {
	const includes = new Map<string, readonly string[]>();
	for (const role of roles) {
		const seen = new Set<string>();
		for (const inc of role.includes ?? []) {
			if (!names.has(inc)) {
				throw new Error(
					`${where}: grant role "${role.name}" includes "${inc}", which is not a role of this grant; `
						+ 'a role includes only roles declared beside it',
				);
			}
			if (seen.has(inc)) {
				throw new Error(`${where}: grant role "${role.name}" includes "${inc}" twice`);
			}
			seen.add(inc);
		}
		includes.set(role.name, role.includes ?? []);
	}
	const done = new Set<string>();
	const visit = (name: string, path: string[]): void => {
		if (done.has(name)) return;
		const at = path.indexOf(name);
		if (at >= 0) {
			throw new Error(
				`${where}: grant role "${name}" includes itself through ${
					[ ...path.slice(at), name ].join(' → ')
				}`,
			);
		}
		for (const inc of includes.get(name)!) visit(inc, [ ...path, name ]);
		done.add(name);
	};
	for (const role of roles) visit(role.name, []);
}

/** The operations of an {@link AccessRuleDecl}, in syncplane.v1.AccessRule field order. */
const ACCESS_OPERATIONS = [ 'read', 'create', 'update', 'delete', 'write' ] as const;

/**
 * Validates the rules of entity `owner`'s access declaration, and its field
 * guards, against the rules the engine enforces on syncplane.v1.AccessOptions
 * at registration. A within thunk is called only when `resolve` is set; until
 * then a rule that holds one skips the check for repeated rules, which needs
 * the entity it names.
 */
function assertAccessRules(
	where: string,
	owner: string,
	access: AccessDecl,
	fields: Fields,
	resolve: boolean,
): void {
	// A plain-JS caller can pass a shape the types refuse, such as one with no
	// `allow` array.
	if (!Array.isArray(access.allow) || access.allow.length === 0) {
		throw new Error(`${where}: access.allow declares no rules`);
	}
	const seen = new Map<string, number>();
	access.allow.forEach((rule, i) => {
		const at = `${where} access.allow[${i}]`;
		const key = assertAccessRule(at, owner, rule, fields, resolve);
		if (key === undefined) return;
		const first = seen.get(key);
		if (first !== undefined) {
			throw new Error(`${at}: the rule repeats access.allow[${first}]`);
		}
		seen.set(key, i);
	});
	if (access.guard !== undefined) assertGuards(where, owner, access.guard, fields, resolve);
}

/**
 * Validates one access rule by itself, and returns a key that is equal for two
 * rules that are the same, or undefined when a within thunk that `resolve`
 * does not allow to call leaves the key open. `at` names the rule in an error.
 */
function assertAccessRule(
	at: string,
	owner: string,
	rule: AccessRuleDecl,
	fields: Fields,
	resolve: boolean,
): string | undefined {
	if (!ACCESS_OPERATIONS.some((op) => rule[op] === true)) {
		throw new Error(
			`${at}: the rule admits no operation; set read, create, update, delete or write`,
		);
	}
	const requires = requiresOf(rule);
	const hasAdmitted = rule.admitted !== undefined;
	if (requires !== undefined && hasAdmitted) {
		throw new Error(`${at}: the rule sets both requires and admitted; set exactly one`);
	}
	if (requires === undefined && !hasAdmitted) {
		throw new Error(`${at}: the rule has no source; set requires or admitted: true`);
	}
	if (requires !== undefined) {
		// A plain-JS caller can pass the empty list the types refuse.
		if (requires.length === 0) throw new Error(`${at}: requires names no permission`);
		const named = new Set<string>();
		for (const permission of requires) {
			validatePermissionName(permission, `${at} requires`);
			if (named.has(permission)) throw new Error(`${at}: requires "${permission}" twice`);
			named.add(permission);
		}
	} else if (rule.admitted !== true) {
		throw new Error(`${at}: admitted must be true`);
	}
	if (rule.callerIs !== undefined) {
		const spec = fields[rule.callerIs]?.spec;
		if (spec === undefined) {
			throw new Error(`${at}: callerIs field "${rule.callerIs}" is not declared`);
		}
		if (spec.kind !== 'scalar' || spec.scalar !== 'string' || spec.repeated) {
			throw new Error(`${at}: callerIs field "${rule.callerIs}" must be a singular text() field`);
		}
	}
	if (rule.fields !== undefined) {
		if (
			ACCESS_OPERATIONS.some((op) => op !== 'update' && rule[op] === true) || rule.update !== true
		) {
			throw new Error(`${at}: fields is valid only on a rule whose one operation is update`);
		}
		if (rule.fields.length === 0) throw new Error(`${at}: fields lists no field`);
		const listed = new Set<string>();
		for (const name of rule.fields) {
			const spec = fields[name]?.spec;
			if (spec === undefined) throw new Error(`${at}: fields names "${name}", which is not declared`);
			if (spec.key) {
				throw new Error(`${at}: fields names key field "${name}", which no update changes`);
			}
			if (listed.has(name)) throw new Error(`${at}: fields names "${name}" twice`);
			listed.add(name);
		}
	}
	const when = whenOf(at, rule.when, fields);
	const within = withinKeysOf(at, owner, rule, resolve);
	if (within === undefined) return undefined;
	// The key is the rule as the proto message holds it: an operation left
	// unset and one set to false encode the same, so they must collide, and
	// the order of a requires, a fields or a within list does not count.
	return JSON.stringify([
		...ACCESS_OPERATIONS.map((op) => rule[op] === true),
		requires === undefined ? null : [ ...requires ].sort(),
		rule.callerIs ?? null,
		rule.fields === undefined ? null : [ ...rule.fields ].sort(),
		when === undefined ? null : toJson(ConditionSchema, when),
		within,
	]);
}

/**
 * Validates the field guards of entity `owner` against the rules the engine
 * enforces on syncplane.v1.FieldGuard at registration; see
 * {@link FieldGuardDecl}.
 */
function assertGuards(
	where: string,
	owner: string,
	guards: readonly FieldGuardDecl[],
	fields: Fields,
	resolve: boolean,
): void {
	// A plain-JS caller can pass a shape the types refuse.
	if (!Array.isArray(guards as unknown)) throw new Error(`${where}: access.guard must be a list`);
	const guardOf = new Map<string, number>();
	guards.forEach((guard, i) => {
		const at = `${where} access.guard[${i}]`;
		if (!Array.isArray(guard.fields as unknown) || guard.fields.length === 0) {
			throw new Error(`${at}: names no field; list the fields it restricts`);
		}
		const listed = new Set<string>();
		for (const name of guard.fields) {
			const spec = fields[name]?.spec;
			if (spec === undefined) throw new Error(`${at}: fields names "${name}", which is not declared`);
			if (spec.key) {
				throw new Error(`${at}: fields names key field "${name}", which no update changes`);
			}
			if (listed.has(name)) throw new Error(`${at}: fields names "${name}" twice`);
			listed.add(name);
			if (spec.accessParent) {
				throw new Error(
					`${at}: names "${name}", which is an access parent; a guard cannot restrict it`,
				);
			}
			if (spec.creator) {
				throw new Error(`${at}: names "${name}", which is a creator field; a guard cannot restrict it`);
			}
			const other = guardOf.get(name);
			if (other !== undefined) {
				throw new Error(
					`${at} and access.guard[${other}] both name "${name}"; a field has at most one guard`,
				);
			}
			guardOf.set(name, i);
		}
		if (!Array.isArray(guard.allow as unknown)) throw new Error(`${at}: allow must be a list`);
		let read = false;
		const seen = new Map<string, number>();
		guard.allow.forEach((rule, j) => {
			const ruleAt = `${at}.allow[${j}]`;
			// A plain-JS caller can set what the guard rule type leaves out.
			const plain = rule as AccessRuleDecl;
			if (plain.delete === true) {
				throw new Error(
					`${ruleAt}: names delete; a guard restricts writing a field, and a delete writes none`,
				);
			}
			if (plain.fields !== undefined) {
				throw new Error(`${ruleAt}: names fields; the guard's own fields are the ones it restricts`);
			}
			const key = assertAccessRule(ruleAt, owner, rule, fields, resolve);
			if (rule.read === true) {
				// A read rule that narrows needs the server to send a row without
				// the fields a caller may not read, which it does not do.
				if (
					rule.admitted !== true || rule.callerIs !== undefined || rule.when !== undefined
					|| rule.within !== undefined
				) {
					throw new Error(
						`${ruleAt}: names read with a source or a check other than admitted; `
							+ 'the read rule of a guard is { read: true, admitted: true }',
					);
				}
				read = true;
			}
			if (key === undefined) return;
			const first = seen.get(key);
			if (first !== undefined) throw new Error(`${ruleAt}: the rule repeats ${at}.allow[${first}]`);
			seen.set(key, j);
		});
		if (!read) {
			throw new Error(
				`${at}: has no read rule; add { read: true, admitted: true }, so that the readers of a row `
					+ 'read its guarded fields',
			);
		}
	});
	// Once every guard is known: a check that read a guarded field would depend
	// on the value a guard decides about.
	const guarded = new Set([ ...guardOf.keys() ].map(snakeCase));
	guards.forEach((guard, i) => {
		guard.allow.forEach((rule, j) => {
			const reads = rule.callerIs === undefined ? [] : [ snakeCase(rule.callerIs) ];
			const when = whenOf('', rule.when, fields);
			if (when !== undefined) reads.push(...conditionHeads(when));
			for (const name of reads) {
				if (guarded.has(name)) {
					throw new Error(
						`${where} access.guard[${i}].allow[${j}]: reads "${name}", which a guard restricts; `
							+ "a guard's checks read only fields that no guard restricts",
					);
				}
			}
		});
	});
}

/**
 * The first segment of each path that `cond` reads on the row. An exists
 * condition reads the list at its path, and its inner condition reads that
 * list's elements, so only its path counts.
 */
function conditionHeads(cond: Condition): string[] {
	const out: string[] = [];
	const walk = (value: unknown): void => {
		if (value === null || typeof value !== 'object') return;
		if (Array.isArray(value)) {
			for (const v of value) walk(v);
			return;
		}
		const rec = value as Record<string, unknown>;
		const path = rec['path'];
		if (Array.isArray(path) && typeof path[0] === 'string') out.push(path[0]);
		if (rec['$typeName'] === 'syncplane.v1.ExistsCond') return;
		for (const [ k, v ] of Object.entries(rec)) {
			if (k !== 'path' && k !== '$typeName') walk(v);
		}
	};
	walk(cond);
	return out;
}

/** A rule's within entries as a list. */
function withinListOf(rule: AccessRuleDecl): readonly WithinEntry[] {
	const within = rule.within;
	if (within === undefined) return [];
	return Array.isArray(within) ? within : [ within as WithinEntry ];
}

/**
 * The wire name of the entity that a within entry of a rule of entity `owner`
 * names. A thunk is called only when `resolve` is set, because before
 * defineSchema() the entity it returns can be one not declared yet; until then
 * it reads as undefined, as does a thunk that returns nothing.
 */
function withinEntityOf(owner: string, w: WithinEntry, resolve: boolean): string | undefined {
	if (w.self === true) return owner;
	if (typeof w.entity !== 'function') return w.entity;
	return resolve ? w.entity()?.name : undefined;
}

/**
 * Checks what a rule's within entries say by themselves, and returns a key for
 * each, sorted: an entity and at least one check, and no entry twice. Returns
 * undefined when an entry names its entity with a thunk that `resolve` does
 * not allow to call. That the entity is in the schema and its fields fit the
 * checks is validateWithinTargets's to check. `at` names the rule in an error.
 */
function withinKeysOf(
	at: string,
	owner: string,
	rule: AccessRuleDecl,
	resolve: boolean,
): string[] | undefined {
	const keys: string[] = [];
	let lazy = false;
	withinListOf(rule).forEach((w, j) => {
		const where = `${at} within[${j}]`;
		// A plain-JS caller can set both, which the types refuse.
		if (w.self !== undefined && w.entity !== undefined) {
			throw new Error(`${where}: sets both self and entity; set one`);
		}
		if (w.self !== undefined && w.self !== true) throw new Error(`${where}: self must be true`);
		if (w.self === undefined && !w.entity) throw new Error(`${where}: names no entity`);
		const entity = withinEntityOf(owner, w, resolve);
		if (entity !== undefined) validateEntityName(entity, where);
		if (w.callerIs === undefined && w.when === undefined) {
			throw new Error(`${where}: sets no check; set callerIs or when`);
		}
		if (entity === undefined) {
			lazy = true;
			return;
		}
		// The condition is keyed as built, before its paths are lowered, which
		// needs the entity's fields.
		const key = JSON.stringify([
			entity,
			w.callerIs ?? null,
			w.when === undefined ? null : w.when(rootRef()),
		]);
		if (keys.includes(key)) throw new Error(`${where}: repeats an earlier entry`);
		keys.push(key);
	});
	return lazy ? undefined : keys.sort();
}

/**
 * Checks each within entry of every rule against the entity it names: one of
 * `entities`, with a singular `text()` `callerIs` field, and a condition over
 * its fields that reads only its row. Then checks the rules that hold a within
 * thunk for repeats, which entity() could not. Whether the rule's rows can sit
 * below that entity's rows is the server's to check at registration.
 */
function validateWithinTargets(entities: readonly EntitySchema[]): void {
	const byName = new Map(entities.map((e) => [ e.name, e ]));
	for (const e of entities) {
		let lazy = false;
		const rules: [string, AccessRuleDecl][] = [
			...(e.access?.allow ?? []).map((
				rule,
				i,
			): [string, AccessRuleDecl] => [ `access.allow[${i}]`, rule ]),
			...(e.access?.guard ?? []).flatMap((g, i) =>
				g.allow.map((rule, j): [string, AccessRuleDecl] => [ `access.guard[${i}].allow[${j}]`, rule ])
			),
		];
		rules.forEach(([ at, rule ]) => {
			withinListOf(rule).forEach((w, j) => {
				const where = `entity "${e.name}" ${at} within[${j}]`;
				const name = withinEntityOf(e.name, w, true);
				const target = name === undefined ? undefined : byName.get(name);
				if (target === undefined) {
					throw new Error(
						`${where}: names entity ${
							name === undefined ? '<unresolved>' : `"${name}"`
						}, which is not part of this schema`,
					);
				}
				if (typeof w.entity === 'function') lazy = true;
				if (w.callerIs !== undefined) {
					const spec = target.fields[w.callerIs]?.spec;
					if (spec === undefined) {
						throw new Error(
							`${where}: callerIs field "${w.callerIs}" is not declared on entity "${target.name}"`,
						);
					}
					if (spec.kind !== 'scalar' || spec.scalar !== 'string' || spec.repeated) {
						throw new Error(`${where}: callerIs field "${w.callerIs}" must be a singular text() field`);
					}
				}
				whenOf(where, w.when, target.fields);
			});
		});
		if (lazy) assertAccessRules(`entity "${e.name}"`, e.name, e.access!, e.fields, true);
	}
}

/**
 * Checks the references that join entities in the access model, which the
 * server refuses at registration: each access parent points at an entity that
 * declares access, each grant subject that is a reference points at an entity
 * that declares members, and each members option names a grant entity of the
 * schema whose one access parent points at the team entity alone and whose
 * subject is a `text()` field. Reference targets must already be checked.
 */
function validateAccessTargets(entities: readonly EntitySchema[]): void {
	const byName = new Map(entities.map((e) => [ e.name, e ]));
	for (const e of entities) {
		for (const [ key, field ] of Object.entries(e.fields)) {
			if (field.spec.accessParent !== true) continue;
			const target = field.spec.ref!();
			if (target.access === undefined) {
				throw new Error(
					`entity "${e.name}" field "${key}" is marked accessParent(), but points at entity "${target.name}", `
						+ `which declares no access; declare access on "${target.name}"`,
				);
			}
		}
		const subject = e.grant === undefined ? undefined : e.fields[e.grant.subject]!.spec;
		if (subject?.kind === 'ref') {
			const target = subject.ref!();
			if (target.members === undefined) {
				throw new Error(
					`entity "${e.name}" grant subject field "${
						e.grant!.subject
					}" points at entity "${target.name}", `
						+ `which declares no members; declare members on "${target.name}" to grant to its members`,
				);
			}
		}
		if (e.members !== undefined) validateMembers(e, byName);
	}
}

/** Checks the members option of `team` against the entities of its schema; see validateAccessTargets. */
function validateMembers(team: EntitySchema, byName: ReadonlyMap<string, EntitySchema>): void {
	const where = `entity "${team.name}" members`;
	const member = byName.get(team.members!.entity);
	if (member === undefined) {
		throw new Error(
			`${where} names entity "${team.members!.entity}", which is not part of this schema`,
		);
	}
	if (member.grant === undefined) {
		throw new Error(
			`${where} names entity "${member.name}", which is not a grant entity; `
				+ 'its rows must grant a role at the team they make their subject a member of',
		);
	}
	const parents = Object.values(member.fields).filter((f) => f.spec.accessParent === true);
	if (parents.length !== 1 || parents[0]!.spec.ref!().name !== team.name) {
		throw new Error(
			`${where} names entity "${member.name}", whose accessParent() does not point at "${team.name}" alone`,
		);
	}
	if (member.fields[member.grant.subject]!.spec.kind === 'ref') {
		throw new Error(
			`${where} names entity "${member.name}", whose grant subject is not a text() field; `
				+ 'a team cannot be a member of a team',
		);
	}
}

/** The permissions a rule requires as a list, `undefined` when it sets none. */
function requiresOf(rule: AccessRuleDecl): readonly string[] | undefined {
	return typeof rule.requires === 'string' ? [ rule.requires ] : rule.requires;
}

/**
 * Builds a `when` condition over the `fields` of the row it reads and lowers
 * its accessor-name paths to proto names, as the server reads them. Refuses a
 * condition that reads anything but the row, which the server refuses at
 * registration. `at` names the rule in an error.
 */
function whenOf(
	at: string,
	build: ((row: QueryRef<RowOf<Fields>>) => Cond) | undefined,
	fields: Fields,
): Condition | undefined {
	if (build === undefined) return undefined;
	const cond = build(rootRef());
	const refuse = (c: Cond): void => {
		switch (c.kind) {
			case 'rel':
				throw new Error(
					`${at}: when uses matches(), which reads another row; a when condition reads only the row`,
				);
			case 'match':
				throw new Error(`${at}: when uses match(), which cannot decide access`);
			case 'and':
			case 'or':
				c.conds.forEach(refuse);
				break;
			case 'not':
				refuse(c.cond);
				break;
		}
	};
	refuse(cond);
	return condToProto(condProtoNames(fields, cond));
}

/**
 * Lowers the accessor-name paths of `cond`, scoped to a row of `fields`, to
 * proto names. A segment that names no declared field keeps its spelling, for
 * the server to report.
 */
function condProtoNames(fields: Fields | undefined, cond: Cond): Cond {
	switch (cond.kind) {
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => condProtoNames(fields, c)) };
		case 'not':
			return { ...cond, cond: condProtoNames(fields, cond.cond) };
		case 'match':
			return cond;
		case 'exists': {
			const { path, spec } = pathProtoNames(fields, cond.path);
			// The sub-condition reads the element: a message of a list, or an
			// entry of a map, whose `key` and `value` are not field names.
			if (spec?.kind === 'map') {
				return { ...cond, path, cond: entryProtoNames(spec.mapMessage, cond.cond) };
			}
			return { ...cond, path, cond: condProtoNames(spec?.message, cond.cond) };
		}
		default:
			// A rel condition keeps its sub-condition, which whenOf refuses.
			return { ...cond, path: pathProtoNames(fields, cond.path).path };
	}
}

/** condProtoNames inside a map entry whose value has `value` fields. */
function entryProtoNames(value: Fields | undefined, cond: Cond): Cond {
	switch (cond.kind) {
		case 'and':
		case 'or':
			return { ...cond, conds: cond.conds.map((c) => entryProtoNames(value, c)) };
		case 'not':
			return { ...cond, cond: entryProtoNames(value, cond.cond) };
		case 'match':
			return cond;
		default: {
			const [ head, ...rest ] = cond.path;
			if (head !== 'value' || rest.length === 0) return cond;
			return { ...cond, path: [ head, ...pathProtoNames(value, rest).path ] } as Cond;
		}
	}
}

/**
 * Lowers one accessor-name path scoped to a row of `fields`, and returns the
 * spec of the field it ends at. A map field's next segment is an entry key,
 * which keeps its spelling.
 */
function pathProtoNames(
	fields: Fields | undefined,
	path: readonly string[],
): { path: string[]; spec?: FieldSpec } {
	const out: string[] = [];
	let scope = fields;
	let spec: FieldSpec | undefined;
	for (let i = 0; i < path.length; i++) {
		const seg = path[i]!;
		spec = scope?.[seg]?.spec;
		if (spec === undefined) {
			out.push(seg);
			scope = undefined;
			continue;
		}
		out.push(snakeCase(seg));
		if (spec.kind === 'map') {
			if (i + 1 < path.length) out.push(path[++i]!);
			scope = spec.mapMessage;
			continue;
		}
		scope = spec.kind === 'message'
			? spec.message
			: spec.kind === 'ref'
			? spec.ref!().fields
			: undefined;
	}
	return { path: out, spec };
}

/**
 * Validates index declarations against the rules the engine enforces at
 * registration, so a schema that would be rejected there fails where it was
 * written instead. Whether each PATH is indexable is NOT checked here — that
 * needs the resolved descriptors, which only registration has.
 */
function assertIndexes(entityName: string, indexes: readonly IndexDecl[]): void {
	const seen = new Set<string>();
	for (const idx of indexes) {
		const where = `entity "${entityName}" index "${idx.name}"`;
		if (!idx.name) throw new Error(`entity "${entityName}": an index declares no name`);
		if (idx.name.startsWith('__')) {
			throw new Error(
				`${where}: the "__" prefix is reserved for indexes the engine derives`,
			);
		}
		validateEntityName(idx.name, `${where} name`);
		if (seen.has(idx.name)) throw new Error(`${where} is declared twice`);
		seen.add(idx.name);
		if (idx.key.length === 0) throw new Error(`${where}: declares no key components`);
		for (const k of idx.key) {
			if (!k.path) throw new Error(`${where}: a key component declares no path`);
		}
	}
}

/**
 * A compiled set of entities, ready to register and to sync. Build it once and
 * share it between the client and a Node backend.
 */
export interface SchemaBundle {
	/** The entities in this schema, in declaration order. */
	readonly entities: readonly EntitySchema[];
	/** The synthesized descriptors (in-memory message form). */
	toFileDescriptorSet(): FileDescriptorSet;
	/**
	 * The serialized FileDescriptorSet — the wire form a Go server ingests via
	 * runtime entity registration (protodesc.NewFiles + RegisterEntity).
	 */
	toBinary(): Uint8Array;
	/** The runtime descriptor registry backing the entity descriptors. */
	toRegistry(): FileRegistry;
	/** Entity descriptors for `new SyncClient({ entities })`. */
	descMessages(): DescMessage[];
	/** A single entity descriptor by its wire name. */
	descMessage(name: string): DescMessage;
}

/**
 * Assembles a self-contained FileDescriptorSet in topological order:
 * `google/protobuf/descriptor.proto` and `syncplane/v1/query.proto`, then
 * `syncplane/v1/options.proto` (which extends the first with the entity
 * options and imports the second), then ONE synthesized file holding every
 * entity (and its nested non-entity types). Bundling the framework's own files
 * means any consumer — a fresh TS client or a Go server — resolves the whole
 * schema from these bytes alone, with no `buf`, no codegen, and no published
 * options module.
 */
function buildSet(entities: readonly EntitySchema[]): FileDescriptorSet {
	return create(FileDescriptorSetSchema, {
		file: [
			file_google_protobuf_descriptor.proto,
			file_syncplane_v1_query.proto,
			file_syncplane_v1_options.proto,
			oneSchemaFile(entities),
		],
	});
}

/** Compiles entities into a shareable, registerable schema bundle. */
export function defineSchema(...entities: EntitySchema[]): SchemaBundle {
	if (entities.length === 0) throw new Error('defineSchema() needs at least one entity');
	const seen = new Set<string>();
	for (const e of entities) {
		if (seen.has(e.name)) throw new Error(`entity "${e.name}" defined twice`);
		seen.add(e.name);
	}
	validateRefTargets(entities);
	validateAccessTargets(entities);
	validateWithinTargets(entities);

	const fdset = buildSet(entities);
	let registry: FileRegistry | undefined;
	const reg = (): FileRegistry => (registry ??= createFileRegistry(fdset));

	return {
		entities,
		toFileDescriptorSet: () => fdset,
		toBinary: () => toBinary(FileDescriptorSetSchema, fdset),
		toRegistry: reg,
		descMessages: () => entities.map((e) => mustMessage(reg(), e.typeName)),
		descMessage: (name) => {
			const e = entities.find((x) => x.name === name);
			if (!e) throw new Error(`no entity "${name}" in this schema`);
			return mustMessage(reg(), e.typeName);
		},
	};
}

/**
 * Resolves every reference thunk and checks its target is one of the entities
 * in this schema. A ref to an out-of-schema (or undefined) entity could not be
 * synthesized into the single file or followed at runtime, so it is rejected
 * eagerly here rather than surfacing as an opaque descriptor build failure.
 */
function validateRefTargets(entities: readonly EntitySchema[]): void {
	const byTypeName = new Set(entities.map((e) => e.typeName));
	for (const e of entities) {
		for (const [ key, field ] of Object.entries(e.fields)) {
			if (field.spec.kind !== 'ref') continue;
			const target = field.spec.ref!();
			if (!target || !byTypeName.has(target.typeName)) {
				throw new Error(
					`entity "${e.name}" field "${key}": reference target ${
						target ? `"${target.name}"` : '<unresolved>'
					} is not part of this schema`,
				);
			}
		}
	}
}

/**
 * Rebuilds entity descriptors from a serialized FileDescriptorSet — the client
 * side of "the server ships its schema". Pass the bytes a Go server handed back
 * (or anything `defineSchema().toBinary()` produced) and get the syncable
 * entity descriptors, ready for `new SyncClient({ entities })`. Messages
 * without (syncplane.v1.entity) options (embedded types, map entries) are ignored.
 */
export function entitiesFromDescriptorSet(set: Uint8Array): DescMessage[] {
	const fdset = fromBinary(FileDescriptorSetSchema, set);
	const reg = createFileRegistry(fdset);
	const out: DescMessage[] = [];
	for (const file of fdset.file) {
		for (const msg of file.messageType) {
			const full = file.package ? `${file.package}.${msg.name}` : (msg.name ?? '');
			const desc = reg.getMessage(full);
			if (desc && hasOption(desc, entityExt)) out.push(desc);
		}
	}
	return out;
}

/** Synthesizes the single file holding every entity and its nested types. */
function oneSchemaFile(entities: readonly EntitySchema[]): FileDescriptorProto {
	return create(FileDescriptorProtoSchema, {
		name: SCHEMA_FILE,
		package: DYNAMIC_PACKAGE,
		syntax: 'proto3',
		dependency: [ 'syncplane/v1/options.proto' ],
		messageType: entities.map((e) => entityMessageOf(e, entities)),
	});
}

/**
 * Lowers one access rule of entity `owner`, with `fields`, to its
 * syncplane.v1.AccessRule. `entities` are the schema's, which within entries
 * name.
 */
function accessRuleOf(
	rule: AccessRuleDecl,
	owner: string,
	fields: Fields,
	entities: readonly EntitySchema[],
): AccessRule {
	return create(AccessRuleSchema, {
		read: rule.read === true,
		create: rule.create === true,
		update: rule.update === true,
		delete: rule.delete === true,
		write: rule.write === true,
		requires: [ ...(requiresOf(rule) ?? []) ],
		admitted: rule.requires === undefined ? create(AdmittedCallerSchema) : undefined,
		// Accessor names lower to proto names, as a grant's fields do.
		callerIs: rule.callerIs === undefined ? '' : snakeCase(rule.callerIs),
		fields: (rule.fields ?? []).map(snakeCase),
		when: whenOf('', rule.when, fields),
		within: withinListOf(rule).map((w) => {
			const entity = withinEntityOf(owner, w, true)!;
			const target = entities.find((e) => e.name === entity)!;
			return create(AccessWithinSchema, {
				entity,
				callerIs: w.callerIs === undefined ? '' : snakeCase(w.callerIs),
				when: whenOf('', w.when, target.fields),
			});
		}),
	});
}

/**
 * Lowers one entity to a top-level DescriptorProto with (syncplane.v1.entity).
 * `entities` are the schema's.
 */
function entityMessageOf(e: EntitySchema, entities: readonly EntitySchema[]): DescriptorProto {
	const fullName = `${DYNAMIC_PACKAGE}.${pascalCase(e.name)}`;
	const built = buildMessage(pascalCase(e.name), fullName, e.fields);

	const keyFieldNames: string[] = [];
	for (const key of Object.keys(e.fields)) {
		if (e.fields[key]!.spec.key) keyFieldNames.push(snakeCase(key));
	}
	const messageOptions = create(MessageOptionsSchema);
	setExtension(
		messageOptions,
		entityExt,
		create(EntityOptionsSchema, {
			name: e.name,
			keyFields: keyFieldNames,
			...(e.access === undefined ? {} : {
				access: create(AccessOptionsSchema, {
					allow: e.access.allow.map((r) => accessRuleOf(r, e.name, e.fields, entities)),
					guard: (e.access.guard ?? []).map((g) =>
						create(FieldGuardSchema, {
							// Accessor names lower to proto names, as a rule's fields do.
							fields: g.fields.map(snakeCase),
							allow: g.allow.map((r) => accessRuleOf(r, e.name, e.fields, entities)),
						})
					),
				}),
			}),
			...(e.grant === undefined ? {} : {
				grant: create(GrantOptionsSchema, {
					// Accessor names lower to proto names, as a key field's does.
					subject: snakeCase(e.grant.subject),
					role: snakeCase(e.grant.role),
					roles: e.grant.roles.map((r) =>
						create(RoleSchema, {
							name: r.name,
							grants: [ ...(r.grants ?? []) ],
							includes: [ ...(r.includes ?? []) ],
						})
					),
				}),
			}),
			...(e.members === undefined ? {} : {
				members: create(MembersOptionsSchema, { entity: e.members.entity }),
			}),
			// Paths are written in accessor names and lowered to proto names
			// here, which is the same translation a query path takes on the way
			// to the wire — so an index and a query naming one field agree.
			...(e.search === undefined ? {} : {
				search: create(SearchSpecSchema, {
					// Accessor names lower to proto names, the same translation an
					// index path takes, so a declaration and a query agree.
					field: e.search.fields.map((p) =>
						create(SearchFieldSchema, {
							path: p.split('.').map(snakeCase).join('.'),
						})
					),
					keepDiacritics: e.search.keepDiacritics === true,
				}),
			}),
			index: e.indexes.map((idx) =>
				create(IndexSpecSchema, {
					name: idx.name,
					key: idx.key.map((k) =>
						create(IndexKeySchema, {
							// `*` is the map fan-out marker and `key`/`value` are the
							// selectors after it — none is a field name, so all three must
							// survive the accessor→proto translation untouched or the
							// engine would look for a field literally called "*".
							// `snakeCase` happens to be the identity on `key` and `value`,
							// but they are listed rather than relied on: they are not
							// field names, and a field named `key` next to one of these is
							// a different segment that DOES translate.
							path: k.path.split('.').map((seg, i, segs) =>
								seg === '*' || (i > 0 && segs[i - 1] === '*') ? seg : snakeCase(seg)
							).join('.'),
							desc: k.desc ?? false,
						})
					),
				})
			),
		}),
	);
	built.options = messageOptions;
	return built;
}

/**
 * Builds a DescriptorProto from a field map, recursively synthesizing the
 * nested types (embedded messages, keyed-list elements, map entries) the fields
 * reference. `fullName` is the message's own proto full name, used to qualify
 * the `type_name` of message-typed fields.
 */
function buildMessage(simpleName: string, fullName: string, fields: Fields): DescriptorProto {
	const fieldProtos: FieldDescriptorProto[] = [];
	const nested: DescriptorProto[] = [];
	let number = 0;
	for (const key of Object.keys(fields)) {
		number++;
		const protoName = snakeCase(key);
		const { field, nestedTypes } = fieldProtoOf(protoName, number, fields[key]!.spec, fullName);
		fieldProtos.push(field);
		nested.push(...nestedTypes);
	}
	return create(DescriptorProtoSchema, {
		name: simpleName,
		field: fieldProtos,
		nestedType: nested,
	});
}

/**
 * Lowers one FieldSpec to its FieldDescriptorProto, plus any nested types it
 * introduces (an embedded/keyed-list element message, or a map entry message)
 * for the caller to attach to the containing message.
 */
function fieldProtoOf(
	name: string,
	number: number,
	spec: FieldSpec,
	parentFullName: string,
): { field: FieldDescriptorProto; nestedTypes: DescriptorProto[] } {
	const label = spec.repeated
		? FieldDescriptorProto_Label.REPEATED
		: FieldDescriptorProto_Label.OPTIONAL;
	const nestedTypes: DescriptorProto[] = [];

	const field = create(FieldDescriptorProtoSchema, { name, number, label });

	switch (spec.kind) {
		case 'scalar': {
			field.type = SCALAR_TO_TYPE[spec.scalar!];
			if (spec.opaque) setOpaque(field, spec.indexed ?? false);
			else if (spec.indexed || spec.searchable || spec.creator) {
				setScalarMarks(field, spec);
			}
			break;
		}
		case 'message': {
			const simple = pascalCase(name);
			const full = `${parentFullName}.${simple}`;
			nestedTypes.push(buildMessage(simple, full, spec.message!));
			field.type = FieldDescriptorProto_Type.MESSAGE;
			field.typeName = `.${full}`;
			setBehavior(field, spec);
			break;
		}
		case 'ref': {
			const target = spec.ref!();
			field.type = FieldDescriptorProto_Type.MESSAGE;
			if (spec.refWorkspace) {
				// A workspace-qualified reference is stored as a reference message
				// synthesized per field: `workspace_id` beside a `value` of the
				// target type, the shape (syncplane.v1.reference) declares.
				const simple = `${pascalCase(name)}Ref`;
				const full = `${parentFullName}.${simple}`;
				nestedTypes.push(referenceMessageOf(simple, target.typeName));
				field.typeName = `.${full}`;
			} else {
				field.typeName = `.${target.typeName}`;
			}
			// Entity-typed reference detection is annotation-free, so the only
			// marks the field itself carries say who may write it and what its
			// reverse relation is called.
			setRefMarks(field, spec);
			break;
		}
		case 'map': {
			const entrySimple = `${pascalCase(name)}Entry`;
			const entryFull = `${parentFullName}.${entrySimple}`;
			// A message value gets its own nested type, a SIBLING of the entry
			// rather than a child of it: a map_entry message is a synthetic shape
			// both protodesc and protobuf-es pattern-match on, and hanging
			// unrelated nested types off it is asking for trouble.
			let valueFull: string | undefined;
			if (spec.mapMessage) {
				const valueSimple = `${pascalCase(name)}Value`;
				valueFull = `${parentFullName}.${valueSimple}`;
				nestedTypes.push(buildMessage(valueSimple, valueFull, spec.mapMessage));
			}
			nestedTypes.push(mapEntryOf(entrySimple, spec, valueFull));
			field.label = FieldDescriptorProto_Label.REPEATED;
			field.type = FieldDescriptorProto_Type.MESSAGE;
			field.typeName = `.${entryFull}`;
			break;
		}
	}
	return { field, nestedTypes };
}

/**
 * Synthesizes a map's entry message: a `map_entry` nested type with key=1 and
 * value=2. protobuf-es and protodesc recognize exactly this shape as a map; a
 * repeated field referencing it then reports `fieldKind === 'map'`.
 */
function mapEntryOf(
	simpleName: string,
	spec: FieldSpec,
	valueTypeFullName?: string,
): DescriptorProto {
	const options = create(MessageOptionsSchema, { mapEntry: true });
	const value = create(FieldDescriptorProtoSchema, {
		name: 'value',
		number: 2,
		label: FieldDescriptorProto_Label.OPTIONAL,
	});
	if (valueTypeFullName) {
		value.type = FieldDescriptorProto_Type.MESSAGE;
		value.typeName = `.${valueTypeFullName}`;
	} else {
		value.type = SCALAR_TO_TYPE[spec.mapValue!];
	}
	return create(DescriptorProtoSchema, {
		name: simpleName,
		options,
		field: [
			create(FieldDescriptorProtoSchema, {
				name: 'key',
				number: 1,
				label: FieldDescriptorProto_Label.OPTIONAL,
				type: MAP_KEY_TO_TYPE[spec.mapKey!],
			}),
			value,
		],
	});
}

/**
 * The reference message a workspaceRef field stores: `workspace_id` marked
 * (syncplane.v1.field_behavior).workspace beside a `value` of the target type,
 * with (syncplane.v1.reference) on the message.
 */
function referenceMessageOf(simpleName: string, targetTypeName: string): DescriptorProto {
	const workspaceOptions = create(FieldOptionsSchema);
	setExtension(workspaceOptions, fieldBehaviorExt, create(FieldBehaviorSchema, { workspace: true }));
	const options = create(MessageOptionsSchema);
	setExtension(options, referenceExt, create(ReferenceOptionsSchema));
	return create(DescriptorProtoSchema, {
		name: simpleName,
		field: [
			create(FieldDescriptorProtoSchema, {
				name: 'workspace_id',
				number: 1,
				label: FieldDescriptorProto_Label.OPTIONAL,
				type: FieldDescriptorProto_Type.STRING,
				options: workspaceOptions,
			}),
			create(FieldDescriptorProtoSchema, {
				name: 'value',
				number: 2,
				label: FieldDescriptorProto_Label.OPTIONAL,
				type: FieldDescriptorProto_Type.MESSAGE,
				typeName: `.${targetTypeName}`,
			}),
		],
		options,
	});
}

/** Sets (syncplane.v1.field_behavior).opaque on a field. */
function setOpaque(field: FieldDescriptorProto, indexed = false): void {
	const fieldOptions = create(FieldOptionsSchema);
	setExtension(
		fieldOptions,
		fieldBehaviorExt,
		create(FieldBehaviorSchema, { opaque: true, indexed }),
	);
	field.options = fieldOptions;
}

/** Sets the (syncplane.v1.field_behavior) marks a plain scalar field carries. */
function setScalarMarks(field: FieldDescriptorProto, spec: FieldSpec): void {
	const fieldOptions = create(FieldOptionsSchema);
	setExtension(
		fieldOptions,
		fieldBehaviorExt,
		create(FieldBehaviorSchema, {
			indexed: spec.indexed ?? false,
			searchable: spec.searchable ?? false,
			...(spec.creator ? { creator: create(CreatorOptionsSchema) } : {}),
		}),
	);
	field.options = fieldOptions;
}

/**
 * Sets the (syncplane.v1.field_behavior) marks a reference field carries: its
 * reverse relation's name and its access-parent mark. No-op when none is set:
 * a reference the builder annotated would report a behaviour it does not have.
 */
function setRefMarks(field: FieldDescriptorProto, spec: FieldSpec): void {
	if (spec.reverseRelation === undefined && !spec.accessParent) return;
	const fieldOptions = create(FieldOptionsSchema);
	setExtension(
		fieldOptions,
		fieldBehaviorExt,
		create(FieldBehaviorSchema, {
			reverseRelation: spec.reverseRelation ?? '',
			...(spec.accessParent ? { accessParent: create(AccessParentOptionsSchema) } : {}),
		}),
	);
	field.options = fieldOptions;
}

/**
 * Sets (syncplane.v1.field_behavior) on a message-typed field for the keyed-list
 * key_field/order_field identity and the opaque escape hatch. No-op when none
 * apply (a plain embedded sub-message).
 */
function setBehavior(field: FieldDescriptorProto, spec: FieldSpec): void {
	const keyField = spec.keyField ?? [];
	const orderField = spec.orderField ?? '';
	const indexed = spec.indexed ?? false;
	if (!spec.opaque && !indexed && keyField.length === 0 && orderField === '') return;
	const fieldOptions = create(FieldOptionsSchema);
	setExtension(
		fieldOptions,
		fieldBehaviorExt,
		create(FieldBehaviorSchema, {
			opaque: spec.opaque,
			keyField,
			orderField,
			indexed,
		}),
	);
	field.options = fieldOptions;
}

function mustMessage(reg: FileRegistry, typeName: string): DescMessage {
	const desc = reg.getMessage(typeName);
	if (!desc) throw new Error(`internal: synthesized message "${typeName}" missing from registry`);
	return desc;
}

/** snake_case (or single-word) entity name → PascalCase proto message name. */
function pascalCase(s: string): string {
	const camel = s.replace(/_(\w)/g, (_, c: string) => c.toUpperCase());
	return camel.charAt(0).toUpperCase() + camel.slice(1);
}

/** lowerCamelCase field accessor → snake_case proto field name. */
function snakeCase(s: string): string {
	return s.replace(/[A-Z]/g, (c) => '_' + c.toLowerCase());
}
