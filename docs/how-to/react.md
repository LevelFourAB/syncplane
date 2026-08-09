# Build a UI with React

`syncplane/react` is a first-party hooks layer over the same client — a separate
entry point, with `react >= 18` as an optional peer dependency, so a non-React
app never pulls it in.

```tsx
import { createReactSync } from 'syncplane/react';

const { SyncProvider, UndoScopeProvider, db, useSyncStatus, useUndo } =
	createReactSync([
		TodoSchema,
	]);

// The provider builds the client, start()s it, and close()s it when the tree
// unmounts.
<SyncProvider
	options={ { url, workspaceId: 'ws-123', entities: [ TodoSchema ] } }
>
	<TodoList />
</SyncProvider>;

function TodoList() {
	const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
	const { put, patch, del } = db.todo.useMutate();
	const { online, pendingMutations } = useSyncStatus();
	// ...
}
```

`createReactSync(entities)` mirrors `client.db` exactly: same
`uncapitalize(schema.name)` keys, same throw on two entities whose simple names
collide. Every hook reads the live client from context at call time, so one `db`
works under any provider (tests, several clients, a swapped connection).

## Who owns the client

`<SyncProvider>` takes **either** `options` **or** `client` — exactly one:

- **`options`** — the provider constructs the client on mount, `start()`s it and
  `close()`s it on unmount. `options` is read **once**; changing it later does
  nothing, because swapping the store under a mounted tree is not something a
  re-render can do safely. To move to another account, remount with a different
  React `key`.
- **`client`** — you built it and you keep its lifecycle. Right when something
  outside React (a test, a worker, a non-React route) also holds it.

Children render immediately in both cases: reads and writes are local-first and
work before — and without — a successful `start()`. A `start()` that fails
_locally_ (bad configuration, a KV format mismatch) is thrown during render for
the nearest error boundary to catch; pass `onStartError` to handle it yourself.
An unreachable **server** is not that: it shows up on `useSyncStatus()` and
retries on its own.

The provider is safe under `<StrictMode>`, whose mount → unmount → remount would
otherwise close a client that cannot be reopened.

## Choosing a workspace

Every hook resolves its workspace in this order: an explicit `{ workspaceId }`
option, then the nearest `<WorkspaceProvider>`, then the client's
`defaultWorkspace`. A single-workspace app never thinks about it.

```tsx
// One route, one workspace: scope the subtree once.
<WorkspaceProvider workspaceId={ teamId }>
	<TodoList />
</WorkspaceProvider>;

// Or name it per call, when one component shows two workspaces at once.
const personal = db.todo.useQuery({}, { workspaceId: 'personal' });
```

One client covers an account and all its workspaces
([Hold several workspaces on one client](multiple-workspaces.md)), so switching
workspaces is a re-render, not a reconnect. Reads, writes, text handles, undo
and `useWorkspaceStatus()` all follow the resolved workspace; only
`useSyncStatus()` stays account-wide. A client that holds several workspaces
with no default throws a hook-level error naming the fix.

## The hooks

- **`useQuery(query, opts?)`** — the query is compiled and structurally hashed on
  every render instead of taking a deps array: a structurally equal query (new
  lambdas included) reuses the subscription, a changed one resubscribes. The
  result is the same `coverage`-carrying array `client.query` returns, and it
  keeps its reference until the rows actually change, so a memoized child does
  not re-render on unrelated commits.

  It also carries `loading`, `hydrating` and `error` — non-enumerable, like
  `coverage`, so `toEqual([])` and `JSON.stringify` do not see them:

  ```tsx
  const todos = db.todo.useQuery({ where: (t) => t.completed.eq(false) });
  if (todos.loading || todos.hydrating) return <Spinner />; // vs. genuinely empty
  if (todos.error) return <Failed error={ todos.error } />;
  return <ul>{ todos.map((t) => <Todo key={ t.key } row={ t } />) }</ul>;
  ```

  `loading` is true until the first callback — the only way to tell "not read
  yet" from "read, and there is nothing", since both are `[]`. `hydrating` is
  the same question one level out: a cold client's first snapshot lands entity
  by entity, so a result can be read, empty, and still on its way. Guard an
  empty state with both. `error` is set when the subscription could not be
  established at all (the store never came up), which means no rows will ever
  arrive; rows already delivered stay put. `{ include }` hydrates references and
  is folded into the subscription identity.

- **`useQueryAll(query, opts?)`** — the same read over **every** workspace the
  client holds, as one subscription rather than one per workspace. Each row
  carries the `workspaceId` it came from, which is what groups the result and
  routes a write back through `client.ws(row.workspaceId)`. Reach for it when
  the workspaces are a list the app learns at runtime: a hook per workspace is a
  hook count that depends on the data, which hooks cannot have. It takes no
  `workspaceId` and has no suspense flavor. See
  [Hold several workspaces on one client](multiple-workspaces.md).

  ```tsx
  const everywhere = db.todo.useQueryAll({
  	where: (t) => t.completed.eq(false),
  });
  const byWorkspace = Map.groupBy(everywhere, (row) => row.workspaceId);
  ```

- **`useRow(key, opts?)`** — `{ row, loading, hydrating, error, patch, update,
  del }`, where the writers go through per-field last-writer-wins like every
  other mutate call. Pass `{ cas: true }` to guard one on the row's current
  `version` (see [compare-and-swap](#compare-and-swap-from-a-row-hook)). They
  reject with a clear error while the row is undefined, and
  `loading`/`hydrating` separate the reasons it can be undefined — the entity
  has not been read yet, its rows are still arriving, or it has all of them and
  this key is not one.

  Every `useRow` on the same workspace, entity and `include` shares **one**
  subscription with a key index in front of it, so a list of row hooks costs one
  watch, and a commit re-renders only the hooks whose own row moved.

- **`useSuspenseQuery(query, opts?)` / `useSuspenseRow(key, opts?)`** — the same
  hooks, waiting for the first callback instead of reporting `loading`. They are
  separate hooks rather than an option because the choice picks a fixed hook path
  and must not change between renders of a call site. A subscription that fails
  throws here for the nearest error boundary, since a suspended component has no
  render to carry an error state.

- **`useMutate(opts?)`** — the stable `{ put, patch, update, del }` facade.
  Mutations are optimistic, so these promises resolve on **local** commit; a
  server rejection arrives later, on the hook below.

- **`useOnMutationRejected(handler, opts?)`** — runs `handler` for every mutation
  that will never be applied: a server rejection, a poison drop, a purge, a
  format upgrade. This is what a "your change could not be saved" toast hangs
  off:

  ```tsx
  useOnMutationRejected((info) => {
  	const what = info.targets.map((t) => `${t.entity} ${t.key}`).join(', ');
  	toast(`Could not save ${what}: ${info.errorMessage}`);
  });
  ```

  `info.targets` names the rows the mutation would have written (one for a CRUD
  mutation, one per member for a batch), and `info.mutator` names the custom
  mutator when it was one. The handler is held in a ref, so an inline arrow does
  not resubscribe. Only the hook's resolved workspace is delivered by default;
  pass `{ allWorkspaces: true }` for an app-wide surface. A rejection whose push
  response was lost arrives a cycle late — see
  [Handle failures](handle-failures.md).

- **`useText(key, field, opts?)`** — see
  [Use collaborative text](use-collaborative-text.md#in-react).

- **`useSyncStatus()`** — the live account-wide status object.
  **`useWorkspaceStatus(opts?)`** — one workspace's slice of it (its pending
  count, scope, access verdict and first-snapshot progress), which a busy
  neighbour workspace does not re-render.

  `hydration` is set only while a cold client is still downloading its first
  snapshot — `{ pending, total, done }`, so a one-off progress bar is
  `done / total` and disappears on its own:

  ```tsx
  const { hydration } = useWorkspaceStatus();
  if (hydration) {
  	return <Loading done={ hydration.done } total={ hydration.total } />;
  }
  ```

- **`useUndo(opts?)`** — `{ canUndo, canRedo, undo, redo }` over the resolved
  scope's undo stacks: the nearest `<UndoScopeProvider>`'s if there is one, else
  the workspace's. Stacks never span workspaces, because the mutation ids they
  reference do not.

All of them are built on `useSyncExternalStore` with an SSR snapshot, and
nothing hands out a mutable row: results are readonly, which is what makes the
snapshot stable and tearing-free.

## Virtualizing a long list

Pass a `window` instead of putting `offset`/`limit` in the query. The
subscription holds the whole match set, so moving the window re-pages what it
already has — no store read, no resubscription — and `total` tells the scroller
how tall the list is. `<Scroller>` below stands for whichever virtualizer you
use; the two things it needs from us are that count and a page that follows it.

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

Two things follow from the window living outside the query. A commit that
changes a row the page cannot see does not re-render the component — only
`total` moving or the page itself moving does. And rows keep their identity
across commits, so a memoized `<Todo>` re-renders only when its own row changes.

`useSuspenseQuery` is the exception: there the window is part of the
subscription identity, so moving it re-reads and re-suspends. Use `useQuery` for
a page that follows a scroll position.

## A dialog with its own undo history

A workspace has one undo stack, so by default the Cmd+Z inside a task dialog
would walk back out into whatever the board behind it did. Wrap the dialog in an
`<UndoScopeProvider>` and it gets a stack of its own:

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

Every write below the provider — the mutate hooks, `useText`, transactions —
records there instead, including the per-keystroke text edits that would
otherwise flood the board's history. Reads are untouched: a scope shares the
workspace's subscriptions, store and queue, so nothing re-subscribes and nothing
syncs twice.

The scope lives exactly as long as the subtree. Closing the dialog drops its
stack rather than merging it into the workspace's — the board's Cmd+Z picks up
where it left off, and does not replay someone's typing. Outside React (an
editor instance, an imperative modal controller) the same thing is
`useUndoScope()`, or `client.ws(id).undoScope()` with a `close()` you call
yourself.

## Compare-and-swap from a row hook

`useRow`'s writers take `{ cas: true }`, which binds the row's current
`version` as `expectedVersion` so the write applies only if nobody else has
moved the row:

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

Reach for it where a concurrent edit must be **noticed** rather than merged, and
only for a considered, one-shot write. Two things make it the wrong tool for a
field being typed into:

- `Row.version` is the SERVER-confirmed version. The optimistic overlay does not
  advance it, so it stands still from the moment an edit is enqueued until a
  pull brings the row back down. Every edit issued in that window carries the
  same guard, and the first to reach the server pushes the row past it — so the
  rest conflict with nothing but their own predecessor, and a conflict is
  resolved by dropping the edit. Typing produces exactly that pattern.
- The guard covers the whole **row**. There are no per-field versions
  ([the convergence model](../explanation/convergence-model.md) explains why),
  so a guarded `title` edit also fails when someone else changed `status`.

An unguarded write is what asks for per-field last-writer-wins, which is the
engine's own answer to concurrent edits. For text two people edit at once, a
[collaborative text field](use-collaborative-text.md) merges instead of
choosing.

## Server components and SSR

The `syncplane/react` entry carries a `'use client'` directive, so a React
Server Components build (the Next.js App Router, for one) can import it directly
without an app-side wrapper. The hooks are still client-side: on the server they
render their empty snapshot, since the data lives in the browser's store.

## Related

- [TypeScript client API](../reference/typescript-client.md#12-react-layer-syncplanereact)
  — exact signatures.
- [Handle failures](handle-failures.md) — where a rejected mutation shows up,
  since it is not on the hook's promise.
