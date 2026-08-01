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
	entity as entityExt,
	EntityOptionsSchema,
	field_behavior as fieldBehaviorExt,
	FieldBehaviorSchema,
	file_syncplane_v1_options,
	IndexKeySchema,
	IndexSpecSchema,
} from '../gen/syncplane/v1/options_pb.js';
import { validateEntityName } from '../naming.js';

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
	/** kind 'map' — the key kind. */
	mapKey?: MapKeyKind;
	/** kind 'map' — the (scalar) value kind. */
	mapValue?: ProtoScalar;
	/** kind 'message' + repeated — keyed message-list identity field(s). */
	keyField?: string[];
	/** kind 'message' + repeated — movable-list position field. */
	orderField?: string;
	/** Maintain a single-component secondary index over this field. */
	indexed?: boolean;
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
			throw new Error('indexed() is not valid on a map() field: a map cannot be compared');
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
 * A scalar-valued map (`map<key, value>`), merged per entry by map key. The key
 * is one of the integral/string/bool proto key kinds; the value is a scalar
 * field. Maps are never keys, opaque, or repeated.
 */
export const map = <K extends MapKeyKind, V>(
	keyType: K,
	value: Field<V>,
): Field<Record<MapKeyTs<K>, V>> => {
	if (!Object.hasOwn(MAP_KEY_TO_TYPE, keyType)) {
		throw new Error(`map() key type "${keyType}" is not a valid map key kind`);
	}
	if (value.spec.kind !== 'scalar') throw new Error('map() value must be a scalar field');
	if (value.spec.repeated) throw new Error('map() value cannot be repeated');
	if (value.spec.key) throw new Error('map() value cannot be a key field');
	if (value.spec.opaque) throw new Error('map() value cannot be opaque');
	return new Field<Record<MapKeyTs<K>, V>>({
		kind: 'map',
		mapKey: keyType,
		mapValue: value.spec.scalar,
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
	/** @internal synthesized proto full name, e.g. "sync.dynamic.Todo". */
	readonly typeName: string;
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
	options?: EntityOptions,
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
	return { name, fields, indexes, typeName: `${DYNAMIC_PACKAGE}.${pascalCase(name)}` };
}

/** Optional per-entity settings, beyond the fields themselves. */
export interface EntityOptions {
	/**
	 * Composite secondary indexes over this entity's fields. See
	 * {@link IndexDecl}; for a single field, `.indexed()` is the simpler
	 * surface and desugars to the same thing.
	 */
	indexes?: IndexDecl[];
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
 * `google/protobuf/descriptor.proto`, then `syncplane/v1/options.proto` (which
 * extends it with the entity options), then ONE synthesized file holding every
 * entity (and its nested non-entity types). Bundling the framework's own files
 * means any consumer — a fresh TS client or a Go server — resolves the whole
 * schema from these bytes alone, with no `buf`, no codegen, and no published
 * options module.
 */
function buildSet(entities: readonly EntitySchema[]): FileDescriptorSet {
	return create(FileDescriptorSetSchema, {
		file: [
			file_google_protobuf_descriptor.proto,
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
		messageType: entities.map(entityMessageOf),
	});
}

/** Lowers one entity to a top-level DescriptorProto with (syncplane.v1.entity). */
function entityMessageOf(e: EntitySchema): DescriptorProto {
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
			// Paths are written in accessor names and lowered to proto names
			// here, which is the same translation a query path takes on the way
			// to the wire — so an index and a query naming one field agree.
			index: e.indexes.map((idx) =>
				create(IndexSpecSchema, {
					name: idx.name,
					key: idx.key.map((k) =>
						create(IndexKeySchema, {
							path: k.path.split('.').map(snakeCase).join('.'),
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
			else if (spec.indexed) setIndexed(field);
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
			field.typeName = `.${target.typeName}`;
			// No field_behavior: reference detection is annotation-free.
			break;
		}
		case 'map': {
			const entrySimple = `${pascalCase(name)}Entry`;
			const entryFull = `${parentFullName}.${entrySimple}`;
			nestedTypes.push(mapEntryOf(entrySimple, spec));
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
function mapEntryOf(simpleName: string, spec: FieldSpec): DescriptorProto {
	const options = create(MessageOptionsSchema, { mapEntry: true });
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
			create(FieldDescriptorProtoSchema, {
				name: 'value',
				number: 2,
				label: FieldDescriptorProto_Label.OPTIONAL,
				type: SCALAR_TO_TYPE[spec.mapValue!],
			}),
		],
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

/** Sets (syncplane.v1.field_behavior).indexed on a field that needs nothing else. */
function setIndexed(field: FieldDescriptorProto): void {
	const fieldOptions = create(FieldOptionsSchema);
	setExtension(fieldOptions, fieldBehaviorExt, create(FieldBehaviorSchema, { indexed: true }));
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
		create(FieldBehaviorSchema, { opaque: spec.opaque, keyField, orderField, indexed }),
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
