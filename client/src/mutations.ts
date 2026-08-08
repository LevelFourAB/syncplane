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
	CreatePolicy,
	DeletePolicy,
	entity as entityExt,
	field_behavior as fieldBehaviorExt,
	UpdatePolicy,
} from './gen/syncplane/v1/options_pb.js';
import {
	rowKey,
	scalarKeyString,
	validateEntityName,
	validateKeyField,
	validateRowKey,
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
	 * The entity's (syncplane.v1.entity) write policies. `'trustedOnly'` means
	 * the server refuses that class of write to every caller it authorizes, so
	 * the client refuses it at the call site rather than applying it to the
	 * overlay and rolling it back on the rejection ack (see assertWritePolicy).
	 */
	policies: WritePolicies;
}

/** Who may perform each class of write on an entity, mirroring the
 * syncplane.v1 CreatePolicy/UpdatePolicy/DeletePolicy enums. */
export interface WritePolicies {
	create: 'allow' | 'trustedOnly';
	update: 'allow' | 'trustedOnly';
	delete: 'allow' | 'trustedOnly';
}

/** One secondary index of an entity: a name unique within it, and the
 * components it sorts by. Mirrors syncplane.v1.IndexSpec with the paths kept
 * as proto field names, which is the form both languages state identically. */
export interface IndexDef {
	name: string;
	key: { path: string[]; desc: boolean }[];
}

export function entityDefOf(schema: DescMessage): EntityDef {
	if (!hasOption(schema, entityExt)) {
		throw new Error(`${schema.typeName} lacks (syncplane.v1.entity) options`);
	}
	const opts = getOption(schema, entityExt);
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
	return {
		schema,
		name: opts.name,
		keyFields,
		indexes: indexesOf(schema, opts),
		serverOnly: opts.serverOnly,
		policies: {
			create: opts.createPolicy === CreatePolicy.TRUSTED_ONLY ? 'trustedOnly' : 'allow',
			update: opts.updatePolicy === UpdatePolicy.TRUSTED_ONLY ? 'trustedOnly' : 'allow',
			delete: opts.deletePolicy === DeletePolicy.TRUSTED_ONLY ? 'trustedOnly' : 'allow',
		},
	};
}

/**
 * Throws if the entity's write policies forbid this write, which is the same
 * decision the server makes in Server.checkWritePolicy — a client that
 * classified differently would either block a legal write or enqueue one only
 * to have it rejected. The shared vectors in testdata/writepolicy pin the two
 * against each other.
 *
 * Classification is a pure function of the write, so it needs no row read:
 * PATCH never upserts and is an update; a PUT asserting version 0 is provably a
 * create and one asserting any other version provably an update; a PUT
 * asserting nothing could land either way and so needs both policies to admit
 * it. Calling an unguarded PUT a create or an update instead would let it
 * through where the opposite policy forbids it.
 */
export function assertWritePolicy(
	def: EntityDef,
	op: CrudOp,
	expectedVersion: bigint | undefined,
): void {
	const refuse = (why: string): never => {
		throw new Error(`entity "${def.name}": ${why}`);
	};
	const creates = def.policies.create === 'trustedOnly';
	const updates = def.policies.update === 'trustedOnly';
	switch (op) {
		case CrudOp.DELETE:
			if (def.policies.delete === 'trustedOnly') {
				refuse('deletePolicy is trustedOnly, so only a trusted server-side write may delete its rows');
			}
			return;
		case CrudOp.PATCH:
			if (updates) {
				refuse('updatePolicy is trustedOnly, so only a trusted server-side write may modify its rows');
			}
			return;
		case CrudOp.PUT:
			if (expectedVersion === undefined) {
				if (creates && updates) {
					refuse(
						'createPolicy and updatePolicy are trustedOnly, so only a trusted server-side write may '
							+ 'write its rows',
					);
				}
				if (creates) {
					refuse(
						'createPolicy is trustedOnly, and a put() without an expectedVersion would create the row '
							+ "if it were absent; pass the row's current version to assert an update",
					);
				}
				if (updates) {
					refuse(
						'updatePolicy is trustedOnly, and a put() without an expectedVersion would replace the row '
							+ 'if it were present; pass expectedVersion 0n to assert a create',
					);
				}
			} else if (expectedVersion === 0n) {
				if (creates) {
					refuse('createPolicy is trustedOnly, so only a trusted server-side write may create its rows');
				}
			} else if (updates) {
				refuse('updatePolicy is trustedOnly, so only a trusted server-side write may modify its rows');
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
		if (!hasOption(field, fieldBehaviorExt)) continue;
		if (!getOption(field, fieldBehaviorExt).indexed) continue;
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
 * Key fields of the entity targeted by a reference field, or [] when the field
 * is not a reference. Mirror of lww.go RefKeyFields: a singular or repeated
 * message field whose (element) message carries (syncplane.v1.entity). Detection is
 * the target message simply being an entity — no annotation. Maps are excluded.
 */
export function refKeyFields(field: DescField): DescField[] {
	const elem = field.fieldKind === 'message'
		? field.message
		: field.fieldKind === 'list' && field.listKind === 'message'
		? field.message
		: undefined;
	if (!elem || !hasOption(elem, entityExt)) return [];
	const names = getOption(elem, entityExt).keyFields;
	if (names.length === 0) return [];
	const out: DescField[] = [];
	for (const n of names) {
		const f = elem.fields.find((ff) => ff.name === n);
		// A target whose key sits inside a oneof cannot be registered at all, and
		// a stub projected onto a field that is only sometimes there would not
		// identify a row. Not a ref, then.
		if (!f || f.oneof) return [];
		out.push(f);
	}
	return out;
}

/** Whether a field is a first-class typed reference (see refKeyFields). */
export function isRef(field: DescField): boolean {
	return refKeyFields(field).length > 0;
}

/** The target (element) message of a reference field, or undefined. */
export function refTarget(field: DescField): DescMessage | undefined {
	if (field.fieldKind === 'message') return field.message;
	if (field.fieldKind === 'list' && field.listKind === 'message') return field.message;
	return undefined;
}

/** A forward reference: a ref field of one entity pointing at another. */
export interface ForwardRef {
	/** Proto field name (wire form). */
	fieldName: string;
	/** JS accessor name on the holder's message shape. */
	localName: string;
	targetEntity: string;
	targetSchema: DescMessage;
	/** To-many (repeated) when true; to-one otherwise. */
	repeated: boolean;
	/** Number of key fields in the target (1 = single key, >1 = composite). */
	keyArity: number;
}

/**
 * A reverse relation: the inverse of a forward ref, surfaced on the TARGET
 * entity. Its name is the pluralized source entity when a single field of that
 * source targets here, else `<plural(source)>By<Field>` (issuesByAssignee).
 */
export interface ReverseRef {
	name: string;
	sourceEntity: string;
	sourceSchema: DescMessage;
	/** The forward ref field this reverses (JS accessor name). */
	viaField: string;
	/** The forward ref field this reverses (proto name). */
	viaFieldName: string;
	repeated: boolean;
}

/**
 * Strips every embedded reference stub of a message down to the target's key
 * fields, mirroring the server's projectRefs exactly. Returns the input
 * unchanged when the entity has no ref fields; otherwise a projected clone (the
 * caller's value is never mutated). Idempotent.
 */
export function projectRefs<D extends DescMessage>(
	schema: D,
	value: MessageShape<D>,
): MessageShape<D> {
	const refs = schema.fields.filter(isRef);
	if (refs.length === 0) return value;
	const out = clone(schema, value);
	const rec = out as unknown as Rec;
	for (const field of refs) {
		const kf = refKeyFields(field);
		// isRef guarantees a (element) message type, but narrow for the compiler.
		const target = field.fieldKind === 'message'
			? field.message
			: field.fieldKind === 'list' && field.listKind === 'message'
			? field.message
			: undefined;
		if (!target) continue;
		if (field.fieldKind === 'list') {
			const list = rec[field.localName] as Message[] | undefined;
			if (list) rec[field.localName] = list.map((e) => projectStub(target, e, kf));
		} else {
			const stub = rec[field.localName] as Message | undefined;
			if (stub !== undefined) rec[field.localName] = projectStub(target, stub, kf);
		}
	}
	return out;
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

	/** Looks up a forward ref of an entity by its JS accessor (localName). */
	forwardRef(entity: string, localName: string): ForwardRef | undefined {
		return this.forwardRefs.get(entity)?.find((r) => r.localName === localName);
	}

	/** Looks up a reverse relation on a target entity by its name. */
	reverseRef(entity: string, name: string): ReverseRef | undefined {
		return this.reverseRefs.get(entity)?.find((r) => r.name === name);
	}

	/**
	 * Builds the reference graph. Forward refs come straight from each entity's
	 * ref fields (validated: the target must be a registered entity). Reverse
	 * relations are derived and auto-named: pluralized source when a single
	 * field of a source targets here, else <plural(source)>By<Field>. Names are
	 * deterministic (fields sorted) and must be unique per target — a clash
	 * throws, matching the entity-facade precedent.
	 *
	 * The two halves share ONE namespace per entity — what a follow segment, an
	 * `include` key and `client.db.<entity>.refs` all resolve against — so a
	 * derived reverse name must be unique against BOTH the other reverse names
	 * and that entity's forward ref fields. The engine's mirror of this builder
	 * rejects the same two clashes with the same wording.
	 */
	private buildRefGraph(): void {
		for (const [ name, def ] of this.byName) {
			const fields = [ ...def.schema.fields ].sort((a, b) => (a.name < b.name ? -1 : 1));
			const fwd: ForwardRef[] = [];
			for (const field of fields) {
				const kf = refKeyFields(field);
				if (kf.length === 0) continue;
				const targetSchema = refTarget(field)!;
				const targetDef = this.byType.get(targetSchema.typeName);
				// A ref whose target entity is not registered in this client cannot
				// be resolved or followed locally, so it is omitted from the graph
				// (it is not exposed as an accessor). Projection still applies — that
				// reads the target's own (syncplane.v1.entity) options, not the registry.
				if (!targetDef) continue;
				fwd.push({
					fieldName: field.name,
					localName: field.localName,
					targetEntity: targetDef.name,
					targetSchema,
					repeated: field.fieldKind === 'list',
					keyArity: kf.length,
				});
			}
			this.forwardRefs.set(name, fwd);
		}
		for (const [ source, refs ] of this.forwardRefs) {
			const sourceSchema = this.byName.get(source)!.schema;
			const byTarget = new Map<string, ForwardRef[]>();
			for (const r of refs) {
				const group = byTarget.get(r.targetEntity);
				if (group) group.push(r);
				else byTarget.set(r.targetEntity, [ r ]);
			}
			for (const [ target, group ] of byTarget) {
				const single = group.length === 1;
				for (const r of group) {
					const relName = single
						? pluralize(source)
						: `${pluralize(source)}By${capitalizeFirst(r.localName)}`;
					const list = this.reverseRefs.get(target) ?? [];
					if (list.some((x) => x.name === relName)) {
						throw new Error(
							`reverse relation "${relName}" on entity "${target}" is ambiguous; rename a ref field to disambiguate`,
						);
					}
					// Forward names are complete by now (the loop above ran over every
					// entity), so this sees the whole namespace a segment resolves
					// against. Shadowing is worse than ambiguity: every resolver tries
					// forward first, so the reverse half would be unreachable and the
					// name would answer with a different row set, silently.
					if (this.forwardRefs.get(target)!.some((x) => x.localName === relName)) {
						throw new Error(
							`reverse relation "${relName}" on entity "${target}" collides with the reference field of that name; rename a ref field to disambiguate`,
						);
					}
					list.push({
						name: relName,
						sourceEntity: source,
						sourceSchema,
						viaField: r.localName,
						viaFieldName: r.fieldName,
						repeated: r.repeated,
					});
					this.reverseRefs.set(target, list);
				}
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

/** Naive English pluralization for reverse-relation auto-naming. */
function pluralize(s: string): string {
	if (/[^aeiou]y$/.test(s)) return s.slice(0, -1) + 'ies';
	if (/(s|x|z|ch|sh)$/.test(s)) return s + 'es';
	return s + 's';
}

function capitalizeFirst(s: string): string {
	return s.charAt(0).toUpperCase() + s.slice(1);
}

// A field is replaced/merged whole when explicitly opaque, or when it is a
// collaborative-text field (text implies opaque — the encoded document is one
// atomic value merged by the auto-installed text merger).
function isOpaque(field: DescField): boolean {
	if (!hasOption(field, fieldBehaviorExt)) return false;
	const b = getOption(field, fieldBehaviorExt);
	return b.opaque || b.text;
}

/** Whether a field carries (syncplane.v1.field_behavior).text. */
export function isTextField(field: DescField): boolean {
	return hasOption(field, fieldBehaviorExt) && getOption(field, fieldBehaviorExt).text;
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
	// Whole-replace cases: a bare path, or an opaque field (the escape hatch —
	// replaced/merged whole even when the mask names a sub-path into it).
	if (rest === '' || opaque) {
		const merge = mergers?.get(prefix + seg);
		if (merge && opaque && applyMerger(dst, src, field, merge)) {
			return;
		}
		copyField(dst, src, field);
		return;
	}

	// rest !== '' and non-opaque: a collection merges per item, a singular
	// message descends, a scalar cannot be sub-pathed.
	if (field.fieldKind === 'list' || field.fieldKind === 'map') {
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
 */
export function keyFieldsFor(field: DescField): DescField[] {
	const elem = field.fieldKind === 'list' && field.listKind === 'message'
		? field.message
		: undefined;
	if (!elem) return [];
	let names: string[] = [];
	if (hasOption(field, fieldBehaviorExt)) {
		const kf = getOption(field, fieldBehaviorExt).keyField;
		if (kf.length > 0) names = kf;
	}
	if (names.length === 0 && hasOption(elem, entityExt)) {
		names = getOption(elem, entityExt).keyFields;
	}
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
 * non-reference message fields) and returns the dotted proto path of every
 * (syncplane.v1.field_behavior).text field, validating its shape. Mirror of the
 * server's collectTextFields.
 *
 * oneof names the group crossed on the way here (empty when none). The walk
 * still descends through oneof members so that a text field below one is
 * reported rather than quietly skipped: its merger could never fire, because a
 * oneof is replaced whole and no mask path reaches inside it.
 */
export function collectTextFieldPaths(
	schema: DescMessage,
	prefix: string,
	visiting: string[],
	oneof = '',
): string[] {
	const out: string[] = [];
	for (const field of schema.fields) {
		const behavior = hasOption(field, fieldBehaviorExt)
			? getOption(field, fieldBehaviorExt)
			: undefined;
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
			out.push(prefix + field.name);
			continue;
		}
		if (
			field.fieldKind !== 'message' || behavior?.opaque || isRef(field)
			|| visiting.includes(field.message.typeName)
		) {
			continue;
		}
		out.push(...collectTextFieldPaths(
			field.message,
			`${prefix}${field.name}.`,
			[ ...visiting, schema.typeName ],
			memberOf,
		));
	}
	return out;
}

/**
 * Widens the bytes-typed fields of an init shape to also accept a plain
 * string: collaborative-text fields ((syncplane.v1.field_behavior).text) may
 * be written as strings, which the client diffs into CRDT operations at
 * enqueue time. The widening is shallow and type-level only — at runtime a
 * string reaching a bytes field that is NOT a text field throws.
 */
export type TextInit<T> = {
	[K in keyof T]: NonNullable<T[K]> extends Uint8Array ? T[K] | string : T[K];
};

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

/** Resolves a dotted proto path to a text field's descriptor chain. */
function textFieldChain(schema: DescMessage, path: string): DescField[] {
	const segs = path.split('.');
	const chain: DescField[] = [];
	let cur: DescMessage = schema;
	for (let i = 0; i < segs.length; i++) {
		const field = cur.fields.find((f) => f.name === segs[i]);
		if (!field) throw new Error(`${schema.typeName} has no text field "${path}"`);
		chain.push(field);
		if (i < segs.length - 1) {
			if (field.fieldKind !== 'message') {
				throw new Error(`${schema.typeName} has no text field "${path}"`);
			}
			cur = field.message;
			continue;
		}
		if (!isTextField(field)) {
			throw new Error(`${schema.typeName} has no text field "${path}"`);
		}
	}
	return chain;
}

function getBytesAt(msg: Rec | undefined, chain: DescField[]): Uint8Array | undefined {
	let cur: unknown = msg;
	for (const f of chain) {
		if (cur === null || typeof cur !== 'object') return undefined;
		cur = (cur as Rec)[f.localName];
	}
	return cur instanceof Uint8Array && cur.length > 0 ? cur : undefined;
}

function setBytesAt(msg: Rec, chain: DescField[], value: Uint8Array): void {
	let cur = msg;
	for (let i = 0; i < chain.length - 1; i++) {
		const f = chain[i]!;
		let next = cur[f.localName] as Rec | undefined | null;
		if (next === undefined || next === null) {
			next = create(f.message!) as unknown as Rec;
			cur[f.localName] = next;
		}
		cur = next;
	}
	cur[chain[chain.length - 1]!.localName] = value;
}

/**
 * Reads the encoded document of one text field, or undefined when the message
 * does not carry it (an unset intermediate, or empty bytes — for a state CRDT
 * those mean the same thing).
 */
export function textFieldBytes(
	schema: DescMessage,
	path: string,
	msg: Message,
): Uint8Array | undefined {
	return getBytesAt(msg as unknown as Rec, textFieldChain(schema, path));
}

/** Writes one text field's encoded document, materializing intermediates. */
export function setTextFieldBytes(
	schema: DescMessage,
	path: string,
	msg: Message,
	value: Uint8Array,
): void {
	setBytesAt(msg as unknown as Rec, textFieldChain(schema, path), value);
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
	const value = fromBinary(def.schema, crud.value) as unknown as Rec;
	return textPaths.some((path) =>
		isPlainString(getBytesAt(value, textFieldChain(def.schema, path)))
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
	// arrived with whatever they decoded it from.
	const pending = new Map<string, string>();
	let cleared = false;
	for (const path of textPaths) {
		const chain = textFieldChain(def.schema, path);
		const current = getBytesAt(value, chain);
		if (!isPlainString(current)) continue;
		if (!masked(path)) {
			setBytesAt(value, chain, new Uint8Array(0));
			cleared = true;
			continue;
		}
		pending.set(path, plainString(current!));
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
		const chain = textFieldChain(def.schema, path);
		const doc = decodeTextDoc(getBytesAt(prev, chain), peer);
		edits.set(path, doc.setString(s));
		setBytesAt(value, chain, encodeTextDoc(doc));
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
 * Unions every text field of base into next in place (where both sides carry
 * a document) — the client mirror of the server's PUT exception: a full-row
 * write must merge text fields with the stored row rather than reset them.
 */
export function mergeTextFieldsInto(
	schema: DescMessage,
	textPaths: readonly string[],
	base: Message,
	next: Message,
): void {
	for (const path of textPaths) {
		const chain = textFieldChain(schema, path);
		const baseBytes = getBytesAt(base as unknown as Rec, chain);
		const nextBytes = getBytesAt(next as unknown as Rec, chain);
		if (baseBytes === undefined || nextBytes === undefined) continue;
		setBytesAt(next as unknown as Rec, chain, mergeText(baseBytes, nextBytes));
	}
}

/**
 * Registration-time guard: what may not live inside a oneof.
 *
 * A oneof is one field — masks name the group and the whole group is replaced —
 * so nothing below one is ever addressed on its own. Anything that needs a
 * field to be reliably THERE breaks under that: an identity that vanishes when
 * a sibling case wins is not an identity, and a reference the graph addresses
 * by a fixed accessor cannot be sometimes-absent. Mirrors the engine's
 * registration checks (text fields are caught by collectTextFieldPaths).
 */
function validateOneofs(schema: DescMessage): void {
	for (const field of schema.fields) {
		if (field.oneof && isRef(field)) {
			throw new Error(
				`${schema.typeName}.${field.name}: reference field is a member of oneof `
					+ `"${field.oneof.name}"; references inside a oneof are not supported`,
			);
		}
		if (field.fieldKind !== 'list' || field.listKind !== 'message') continue;
		const behavior = hasOption(field, fieldBehaviorExt)
			? getOption(field, fieldBehaviorExt)
			: undefined;
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
		if (!hasOption(field, fieldBehaviorExt)) continue;
		const order = getOption(field, fieldBehaviorExt).orderField;
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
		const [ seg, rest ] = splitSeg(path);
		if (rest !== '') continue;
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
		const [ seg, rest ] = splitSeg(path);
		const field = rest === '' ? schema.fields.find((f) => f.name === seg) : undefined;
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
			if (keyFields.length === 0) {
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
		const changed = inP !== inN || !mapValueEquals(field, pm[k], nm[k]);
		if (changed) out.push(`${seg}.${encodeMapKey(field, k)}`);
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
		const changed = (p === undefined) !== (n === undefined)
			|| (p !== undefined && n !== undefined
				&& !bytesEqual(toBinary(field.message, p), toBinary(field.message, n)));
		if (changed) out.push(`${seg}.${k}`);
	}
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
	const paths: string[] = [];
	for (const key of Object.keys(init)) {
		const field = schema.fields.find((f) => f.localName === key);
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
