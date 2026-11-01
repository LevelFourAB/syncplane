# Building a UI with React

This guide shows how to integrate Syncplane into a React application using `syncplane/react`. Use this guide to set up the client provider, scope workspaces, read data with reactive queries, and execute mutations in your React components.

## Prerequisites

Before you begin, verify that you have:

- `react` version 18 or later installed.
- One or more defined entity schemas, such as `TodoSchema`.

## Integrating Syncplane into your application

Follow these steps to set up and use Syncplane in your React application:

1. **Create the React sync layer:**

   Call `createReactSync` with your schema definitions to generate the provider components and typed hook collections:

   ```tsx
   import { createReactSync } from 'syncplane/react';

   const { SyncProvider, UndoScopeProvider, db, useSyncStatus, useUndo } =
   	createReactSync([
   		TodoSchema,
   	]);
   ```

   `createReactSync(entities)` mirrors `client.db` with the same uncapitalized schema keys (`uncapitalize(schema.name)`). It throws an error if two entities share the same simple name. Each hook reads the client from context at call time. This allows one `db` instance to work across providers.

2. **Mount the `<SyncProvider>`:**

   Wrap your component tree in `<SyncProvider>`. Pass exactly one of `options` or `client`:

   ```tsx
   // The provider builds the client, start()s it, and close()s it when the tree
   // unmounts.
   <SyncProvider
   	options={ { url, workspaceId: 'ws-123', entities: [ TodoSchema ] } }
   >
   	<TodoList />
   </SyncProvider>;
   ```

   - **`options`** — The provider constructs the client on mount, calls `start()`, and calls `close()` on unmount. The provider reads `options` once. Changing `options` later has no effect. To switch accounts, remount the provider with a different React `key`.
   - **`client`** — Pass an existing client when you manage the client lifecycle outside React, such as in a test, worker, or non-React route.

   Children render immediately in both cases. Reads and writes are local-first and function before and without a successful `start()`. If `start()` fails locally (such as from a bad configuration or a key-value format mismatch), the provider throws during render for the nearest error boundary to catch. Pass `onStartError` to handle it directly. An unreachable server appears on `useSyncStatus()` and retries automatically. The provider is safe under `<StrictMode>`.

3. **Scope workspaces:**

   If your application uses multiple workspaces, scope subtrees with `<WorkspaceProvider>` or specify an explicit `workspaceId` in hook options:

   ```tsx
   // One route, one workspace: scope the subtree once.
   <WorkspaceProvider workspaceId={ teamId }>
   	<TodoList />
   </WorkspaceProvider>;

   // Or name it per call, when one component shows two workspaces at once.
   const personal = db.todo.useQuery({}, { workspaceId: 'personal' });
   ```

   Each hook resolves its workspace in this order: an explicit `{ workspaceId }` option, the nearest `<WorkspaceProvider>`, then the client's `defaultWorkspace`. One client covers an account and all its workspaces (see [Hold several workspaces on one client](multiple-workspaces.md)), so switching workspaces triggers a re-render rather than a reconnect. Reads, writes, text handles, undo, and `useWorkspaceStatus()` follow the resolved workspace. `useSyncStatus()` remains account-wide. If a client holds multiple workspaces with no default workspace, hooks throw an error.

4. **Query data in components:**

   Call `db.<entity>.useQuery` to subscribe to query results:

   ```tsx
   function TodoList() {
   	const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });

   	if (todos.loading || todos.hydrating) return <Spinner />;
   	if (todos.error) return <Failed error={ todos.error } />;
   	return <ul>{ todos.map((t) => <Todo key={ t.key } row={ t } />) }</ul>;
   }
   ```

   The query is compiled and structurally hashed on every render. A structurally equal query reuses the subscription. A changed query resubscribes.

5. **Mutate data:**

   Use `db.<entity>.useMutate` or row-level mutators to modify data:

   ```tsx
   function TodoList() {
   	const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
   	const { put, patch, del } = db.todo.useMutate();
   	const { online, pendingMutations } = useSyncStatus();
   	// ...
   }
   ```

   Mutations are optimistic. Mutate promises resolve on local commit.

6. **Handle mutation rejections:**

   Register a callback with `useOnMutationRejected` to handle mutations rejected by the server:

   ```tsx
   useOnMutationRejected((info) => {
   	const what = info.targets.map((t) => `${t.entity} ${t.key}`).join(', ');
   	toast(`Could not save ${what}: ${info.errorMessage}`);
   });
   ```

   The callback receives an `info` object containing `targets` (the rows targeted by the mutation) and `mutator` (when the mutation used a custom mutator). By default, only the resolved workspace's rejections are delivered. Pass `{ allWorkspaces: true }` to receive rejections across the entire application.

## Confirming the result

To verify that Syncplane is working in your React application:

1. Render a status component using `useSyncStatus()` or `useWorkspaceStatus()`:

   ```tsx
   function SyncState() {
   	const { online, pendingMutations } = useSyncStatus();
   	const { hydration } = useWorkspaceStatus();

   	if (hydration) {
   		return <Loading done={ hydration.done } total={ hydration.total } />;
   	}

   	return (
   		<div>
   			{ online ? 'Connected' : 'Offline' } ({ pendingMutations } pending)
   		</div>
   	);
   }
   ```

2. Perform a mutation, such as creating or updating an item. Verify that the UI updates immediately and that `pendingMutations` decrements once synced.

## Virtualizing a long list

Pass a `window` option instead of placing `offset` and `limit` inside the query. The subscription holds the complete match set, so adjusting the window re-pages existing data without a store read or resubscription. The `total` property provides the full item count to your virtualized list component:

```tsx
function TodoList() {
	const [ first, setFirst ] = useState(0);
	const rows = db.todo.useQuery(
		{ orderBy: (t) => [ t.createdAtMs.desc() ] },
		{ window: { offset: first, limit: 50 } },
	);

	return (
		<Scroller
			count={ rows.total }
			rowHeight={ 32 }
			onRangeChange={ setFirst }
			offset={ first }
		>
			{ rows.map((row) => <Todo key={ row.key } row={ row } />) }
		</Scroller>
	);
}
```

Because the window is defined outside the query, a commit modifying an out-of-view row does not re-render the component unless `total` changes.

`useSuspenseQuery` is an exception: the window is part of the subscription identity, so moving it re-reads and re-suspends. Use `useQuery` for a list that tracks scroll position.

## A dialog with its own undo history

A workspace has one undo stack. By default, pressing Cmd+Z inside a task dialog undoes changes in the underlying workspace. Wrap the dialog in an `<UndoScopeProvider>` to isolate its undo stack:

```tsx
function TaskDialog({ id }: { id: string }) {
	return (
		<UndoScopeProvider>
			<TaskDialogBody id={ id } />
		</UndoScopeProvider>
	);
}

function TaskDialogBody({ id }: { id: string }) {
	// This useUndo drives the dialog's stack; the board's useUndo, outside the
	// provider, still drives the workspace's.
	const { canUndo, undo } = useUndo();
	const title = db.task.useText(id, 'title');
	const { update } = db.task.useRow(id);
	// …and so does the delete, unless it says otherwise:
	const { del } = db.task.useMutate({ undo: 'workspace' });
}
```

Writes under the provider record to the isolated scope stack. Reads are shared with the workspace's subscriptions, store, and queue. When the dialog unmounts, the scope stack is discarded without altering the workspace history. Outside React, use `useUndoScope()` or `client.ws(id).undoScope()`.

## Keeping undo history when a view unmounts

An `<UndoScopeProvider>` without props drops its stack when it unmounts. For tabs or pages that unmount and mount again, create the scopes yourself and give one to the provider with `scope`. The provider never closes a scope that it gets this way.

Keep one scope for each page, and close the oldest scope when you keep too many:

```tsx
const KEEP = 10;
// Map order is insertion order, so the first key is the page used least recently.
const scopes = new Map<string, UndoScope>();

function scopeFor(ws: WorkspaceClient, pageId: string): UndoScope {
	let scope = scopes.get(pageId);
	if (scope !== undefined) {
		scopes.delete(pageId);
	} else {
		scope = ws.undoScope();
	}
	scopes.set(pageId, scope);
	if (scopes.size > KEEP) {
		const [ oldest, evicted ] = scopes.entries().next().value!;
		scopes.delete(oldest);
		evicted.close();
	}
	return scope;
}

function PageTab({ pageId }: { pageId: string }) {
	const ws = useWorkspaceScope();
	const scope = useMemo(() => scopeFor(ws, pageId), [ ws, pageId ]);
	return (
		<UndoScopeProvider scope={ scope }>
			<PageEditor pageId={ pageId } />
		</UndoScopeProvider>
	);
}
```

Key the map by page, not by tab. Two tabs that show the same page then share one history, and a page that the user opens again gets its history back.

Each open scope keeps up to `maxDepth` undo groups in memory. A custom mutation clears every scope in the workspace, including scopes that no tab shows. The stacks are not saved, so a page reload clears all of them.

## Editing a row outside the subscription scope

A row found by `queryServer`, or opened from a link, is outside every declared subscription. Pass `{ hold: true }` to keep it in scope while the component is mounted:

```tsx
function DocumentEditor({ id }: { id: string }) {
	const { row } = db.document.useRow(id, { hold: true });
	const body = db.document.useText(id, 'body', { hold: true });
	if (row === undefined || body.handle === undefined) return <Spinner />;
	return <textarea
		value={ body.value }
		onChange={ (e) => void body.setText(e.target.value) }
	/>;
}
```

The row arrives on the next sync cycle. `useText` opens its handle once the row has synced, and both hooks release the hold on unmount. See [Holds](../reference/typescript-client.md#holds-one-row-in-scope-on-demand).

## Compare-and-swap from a row hook

`useRow` writer functions accept `{ cas: true }`, which binds the row's current `version` as `expectedVersion` so the write succeeds only if no other write has modified the row:

```tsx
const { row, update } = db.issue.useRow(id);

// Merges. Two people editing different fields both keep their edit.
const rename = (title: string) =>
	update((d) => {
		d.title = title;
	});

// Refuses. Rejected as `conflict` if the row moved at all since it was read.
const archive = () =>
	update((d) => {
		d.archived = true;
	}, { cas: true });
```

Use compare-and-swap only for one-shot writes when a concurrent edit must be rejected rather than merged. Do not use compare-and-swap on a text input. `Row.version` does not change until the next pull returns the row, so after the first keystroke the next keystrokes conflict and are lost. See [the convergence model](../explanation/convergence-model.md). For concurrent text editing, use a [collaborative text field](use-collaborative-text.md).

## Related

- [TypeScript client API](../reference/typescript-client.md#react-layer)
  — exact signatures, including the hooks table.
- [Handle failures](handle-failures.md) — where a rejected mutation shows up,
  since it is not on the hook's promise.
