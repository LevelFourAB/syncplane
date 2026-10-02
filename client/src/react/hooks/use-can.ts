import {
	create,
	type DescMessage,
	type Message,
	type MessageInitShape,
	type MessageShape,
	toBinary,
} from '@bufbuild/protobuf';
import { useEffect, useMemo, useState } from 'react';

import {
	accessInputsOf,
	type AccessOperation,
	type CanAnswer,
	type CanDetail,
	childEntitiesOf,
	detailOf,
	subjectEntitiesOf,
	unknownBecause,
	type Verdict,
} from '../../access.js';
import type { CanOptions, DetailOptions, SyncClient, WorkspaceClient } from '../../client.js';
import { useSyncClient, useWorkspaceScope } from '../context.js';
import { rowWatchIdentity, rowWatchOf, watchRowCell } from '../row-store.js';
import type { UseWorkspaceOptions } from '../types.js';

/** Options for {@link useCan}. */
export interface UseCanOptions<D extends DescMessage = DescMessage>
	extends UseWorkspaceOptions, CanOptions<D>
{}

/** Options for {@link useCanCreate} and {@link useHasPermission}. */
export interface UseDetailOptions extends UseWorkspaceOptions, DetailOptions {}

/**
 * Predicts whether the entity's access rules admit `operation` on the row of
 * `schema` with `key`, for the resolved workspace. See `SyncClient.can` for
 * what the answer means; it is advisory, so use it to shape the UI, not to
 * authorize.
 *
 * Re-evaluates when an access input in the row changes: whether the row is
 * held locally, the row its access parent names, or the value of a field that
 * a check of the entity's access rules or field guards reads: a `callerIs`
 * field, a field a `when` condition reads, and the same for a `within` entry
 * on the entity itself. A change of any other field does not re-evaluate.
 * Also re-evaluates when `SyncClient.onAccessChange` reports the workspace:
 * a new summary of the caller's access arrives, a local write that can
 * change the caller's grants is queued, acked or rolled back, or a row that
 * other rows sit below changes, in this tab or another sharing the store.
 * For a `'delete'`, also re-evaluates when a row is written of an entity
 * whose rows can name the row as their access parent or their subject.
 *
 * The first render gives an answer at once, before the hook reads the store:
 * `'yes'` or `'no'` when the summary the client read last decides it without
 * a row, else the answer the same question got last in this client, else
 * `'unknown'` with the reason `'loading'`. An answer from the client's
 * memory can be out of date until the read finishes. A failed read gives
 * `'unknown'` with the reason `'error'`.
 *
 * With `opts.detail`, the hook returns a {@link CanDetail}, which gives the
 * reason for an `'unknown'` answer.
 *
 * The access hooks and `useRow` calls without `include` on rows of one entity
 * share one watch of those rows.
 *
 * @example
 * ```tsx
 * function DeleteButton({ id }: { id: string }) {
 *   const can = useCan(CommentSchema, id, 'delete');
 *   return <button disabled={can !== 'yes'}>Delete</button>;
 * }
 *
 * function EditButton({ id }: { id: string }) {
 *   const can = useCan(CommentSchema, id, 'update', { detail: true });
 *   if (can.reason === 'loading') return <Spinner />;
 *   return <button disabled={can.answer !== 'yes'}>Edit</button>;
 * }
 * ```
 */
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	operation: Exclude<AccessOperation, 'create'>,
	opts: UseCanOptions<D> & { detail: true },
): CanDetail;
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	operation: Exclude<AccessOperation, 'create'>,
	opts?: UseCanOptions<D>,
): CanAnswer;
export function useCan<D extends DescMessage>(
	schema: D,
	key: string,
	operation: Exclude<AccessOperation, 'create'>,
	opts?: UseCanOptions<D>,
): CanAnswer | CanDetail {
	const fields = opts?.fields;
	// A fields array written inline is a new array on every render.
	const fieldsKey = fields === undefined ? undefined : JSON.stringify(fields);
	const canOpts = fields === undefined ? undefined : { fields };
	const detail = useAnswer(
		opts?.workspaceId,
		schema,
		{ key, checks: true, naming: operation === 'delete' },
		[ 'can', schema.typeName, key, operation, fieldsKey ],
		(scope) => scope.can(schema, key, operation, { ...canOpts, detail: true }),
		(client, ws) => client.peekCanIn(ws, schema, key, operation, canOpts),
		[ schema, key, operation, fieldsKey ],
	);
	return opts?.detail === true ? detail : detail.answer;
}

/**
 * Predicts whether the entity's access rules admit creating `row`, for the
 * resolved workspace. See `SyncClient.canCreate` for what the answer means.
 *
 * Re-evaluates when `row` changes by value, and when
 * `SyncClient.onAccessChange` reports the workspace. Starts from an answer
 * it can give at once, and takes `opts.detail`, as {@link useCan} does.
 *
 * @example
 * ```tsx
 * function CommentBox({ taskId }: { taskId: string }) {
 *   const can = useCanCreate(CommentSchema, { id: 'draft', task: { id: taskId } });
 *   return can === 'no' ? null : <textarea />;
 * }
 * ```
 */
export function useCanCreate<D extends DescMessage>(
	schema: D,
	row: MessageInitShape<D>,
	opts: UseDetailOptions & { detail: true },
): CanDetail;
export function useCanCreate<D extends DescMessage>(
	schema: D,
	row: MessageInitShape<D>,
	opts?: UseDetailOptions,
): CanAnswer;
export function useCanCreate<D extends DescMessage>(
	schema: D,
	row: MessageInitShape<D>,
	opts?: UseDetailOptions,
): CanAnswer | CanDetail {
	// An object literal is a new object on every render; its encoding is not.
	const encoded = useMemo(() => toBinary(schema, create(schema, row)).join(','), [ schema, row ]);
	const detail = useAnswer(
		opts?.workspaceId,
		schema,
		undefined,
		[ 'create', schema.typeName, encoded ],
		(scope) => scope.canCreate(schema, row, { detail: true }),
		(client, ws) => client.peekCanCreateIn(ws, schema, row),
		[ schema, encoded ],
	);
	return opts?.detail === true ? detail : detail.answer;
}

/**
 * Predicts whether the caller holds `permission` at the row of `schema` with
 * `key`, for the resolved workspace. See `SyncClient.hasPermission` for what
 * the answer means. Re-evaluates, starts from an answer it can give at once,
 * and takes `opts.detail`, as {@link useCan} does, except that the fields
 * the checks of rules and guards read are not inputs.
 *
 * @example
 * ```tsx
 * function InviteButton({ projectId }: { projectId: string }) {
 *   const can = useHasPermission(ProjectSchema, projectId, 'invite_members');
 *   return <button disabled={can !== 'yes'}>Invite</button>;
 * }
 * ```
 */
export function useHasPermission<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts: UseDetailOptions & { detail: true },
): CanDetail;
export function useHasPermission<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts?: UseDetailOptions,
): CanAnswer;
export function useHasPermission<D extends DescMessage>(
	schema: D,
	key: string,
	permission: string,
	opts?: UseDetailOptions,
): CanAnswer | CanDetail {
	const detail = useAnswer(
		opts?.workspaceId,
		schema,
		{ key, checks: false, naming: false },
		[ 'permission', schema.typeName, key, permission ],
		(scope) => scope.hasPermission(schema, key, permission, { detail: true }),
		(client, ws) => client.peekHasPermissionIn(ws, schema, key, permission),
		[ schema, key, permission ],
	);
	return opts?.detail === true ? detail : detail.answer;
}

/** How many answers {@link lastAnswers} keeps for each client. */
const LAST_ANSWERS_MAX = 1000;

/**
 * Per client, the newest answer each question got from the store, by
 * question id, the least recent first: what a hook that asks the same
 * question again starts from.
 */
const lastAnswers = new WeakMap<SyncClient, Map<string, CanDetail>>();

function rememberAnswer(client: SyncClient, id: string, detail: CanDetail): void {
	let answers = lastAnswers.get(client);
	if (answers === undefined) lastAnswers.set(client, answers = new Map());
	answers.delete(id);
	answers.set(id, detail);
	if (answers.size > LAST_ANSWERS_MAX) answers.delete(answers.keys().next().value!);
}

/**
 * The answer a hook shows before its first read of the store: what `peek`
 * decides from memory when it decides, else the last answer to the question.
 */
function seedOf(client: SyncClient, id: string, peek: () => Verdict | undefined): CanDetail {
	const now = peek();
	// An unknown peek is only a peek that read no rows.
	if (now === 'yes' || now === 'no') return detailOf(now);
	return lastAnswers.get(client)?.get(id) ?? unknownBecause('loading');
}

/** The row an answer is for, and which of its fields are inputs of the answer. */
interface AnswerRow {
	key: string;
	/** Whether the fields the checks of the entity's access rules and guards read are inputs. */
	checks: boolean;
	/**
	 * Whether the rows that can name the row as their access parent or their
	 * subject are inputs, as they are for a delete.
	 */
	naming: boolean;
}

/**
 * Holds the newest answer `ask` gives, asked again on every access change in
 * the workspace and, when there is a row, on every change of what
 * accessInputsOf reads from it. Before the first answer, holds the one
 * seedOf gives. `question` names what `ask` asks, as JSON values, and
 * `deps` decide when `ask` itself changes.
 */
function useAnswer<D extends DescMessage>(
	workspaceId: string | undefined,
	schema: D,
	row: AnswerRow | undefined,
	question: readonly unknown[],
	ask: (scope: WorkspaceClient) => Promise<CanDetail>,
	peek: (client: SyncClient, workspaceId: string) => Verdict | undefined,
	deps: readonly unknown[],
): CanDetail {
	const client = useSyncClient();
	const scope = useWorkspaceScope(workspaceId);
	const id = JSON.stringify([ scope.workspaceId, ...question ]);
	// The answer held is for the question it was asked for, so a hook asked a
	// new question shows that question's seed and not the old answer.
	const [ held, setHeld ] = useState<{ id: string; detail: CanDetail }>();
	// `peek` is a new closure on every render; `id` names what it reads.
	const seed = useMemo(() => seedOf(client, id, () => peek(client, scope.workspaceId)), [
		client,
		id,
	]);
	const key = row?.key;
	const checks = row?.checks ?? false;
	const naming = row?.naming ?? false;
	useEffect(() => {
		// Resolved first: an unregistered schema throws here, before anything
		// below subscribes.
		const def = key === undefined ? undefined : client.entityRegistry.bySchema(schema);
		let live = true;
		// Answers resolve out of order; only the newest one may land.
		let generation = 0;
		const land = (detail: CanDetail) =>
			setHeld((prev) => (prev?.id === id && prev.detail === detail ? prev : { id, detail }));
		const evaluate = () => {
			const mine = ++generation;
			ask(scope).then(
				(detail) => {
					if (!live || mine !== generation) return;
					rememberAnswer(client, id, detail);
					land(detail);
				},
				() => {
					if (live && mine === generation) land(unknownBecause('error'));
				},
			);
		};
		// The access change, the row's own inputs and, for a delete, the rows
		// that can name the row cover every input of the answer. The sync status
		// is not an input, and it changes on every local write: asking again on
		// it makes each write cost one prediction per mounted hook.
		const offAccess = client.onAccessChange((ws) => {
			if (ws === scope.workspaceId) evaluate();
		});
		let offNaming: (() => void) | undefined;
		if (def !== undefined && naming) {
			const entities = [
				...childEntitiesOf(client.entityRegistry, def.name),
				...subjectEntitiesOf(client.entityRegistry, def.name),
			].map((d) => d.name);
			if (entities.length > 0) {
				offNaming = client.onEntitiesChange(scope.workspaceId, entities, evaluate);
			}
		}
		let offRow: (() => void) | undefined;
		if (def === undefined || key === undefined) evaluate();
		else {
			// The first ask waits for the row's first delivery and starts from
			// it, rather than running here as well. Its read of the store starts
			// after the watch read the row, and the watch delivers every later
			// change of the row, so no change falls between the two. A row that
			// another call site already holds is delivered at once.
			let seen: string | undefined;
			offRow = watchRowCell<MessageShape<D>>(
				client,
				rowWatchIdentity(scope.workspaceId, schema.typeName),
				rowWatchOf(scope, schema),
				{ workspaceId: scope.workspaceId, key },
				(cell) => {
					// A watch that fails delivers its error once, with the row it
					// last had or none, so the first ask still runs.
					const inputs = accessInputsOf(def, cell.row?.value as Message | undefined, checks);
					if (inputs === seen) return;
					seen = inputs;
					evaluate();
				},
			);
		}
		return () => {
			live = false;
			offAccess();
			offNaming?.();
			offRow?.();
		};
		// `ask` is a new closure on every render; deps lists what it reads.
	}, [ client, scope, key, checks, naming, id, ...deps ]);
	return held?.id === id ? held.detail : seed;
}
