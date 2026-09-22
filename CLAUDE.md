**syncplane** — a sync engine for local-first apps. Clients read and write a
local KV store; a server-authoritative mutation log keeps replicas converged by
optimistic rebase on the client. It ships as two halves joined by a protobuf
wire contract:

- **`proto/`** — source of truth for the wire protocol, split into three buf
  modules by who is allowed to see them. **`proto/contract/`** holds
  `syncplane/v1` and is the only one published, as
  `buf.build/levelfourab/syncplane`; **`proto/private/`** holds the server's
  cursor types and the test-suite entities; **`proto/examples/`** holds the demo
  todo entity. Only the first has a `name` in `buf.yaml`, and the other two
  cannot reach the registry — put a new file under the module that matches its
  audience. Entities are declared with
  custom proto options: `(syncplane.v1.entity)` on messages,
  `(syncplane.v1.field_behavior)` on fields (opaque, keyed lists, order fields,
  collaborative text). `task generate` emits Go into `engine/gen/` and TS into
  `client/src/gen/` — never edit generated code. Import paths are relative to
  each module root, so they stay `syncplane/v1/options.proto` regardless of
  which module a file lives in.
- **`engine/`** — Go server. The Go module is rooted at the repo root
  (`github.com/levelfourab/syncplane`), so engine packages import as
  `github.com/levelfourab/syncplane/engine/…`.
- **`client/`** — TypeScript client, published as `syncplane`, **ESM-only**.

### The model every change must respect

- The server linearizes mutations per workspace with a monotonic sequence. PATCH
  field masks applied in sequence order give **per-field last-writer-wins** —
  "last" means last to _arrive at the server_, not wall clock. There are no
  per-field timestamps.
- Clients write through an **optimistic overlay** plus a pending queue. A
  background coordinator pushes the queue, pulls server changes, and rebases
  (apply server patch → drop acked mutations → clear overlay → replay pending).
- Idempotency rides on per-client mutation-id **watermarks**; a rejected mutation
  is still an ack. Deletes leave **tombstones**; GC advances a horizon past which
  clients transparently re-bootstrap.
- `workspace_id` is a hard partition key everywhere. `client_id` is an
  unauthenticated idempotency handle, not an identity.
- PUT replaces whole rows — except collaborative-text fields, which union with
  the stored document on every PUT path, and fields a client may not write,
  which keep their stored value. Partial updates use PATCH with a field mask,
  and a mask reaching a field the caller may not write is refused. Schema
  evolution is additive.
- Wire cursors are opaque bytes to clients.

### Where things live

`engine/`: `server/` (RPC handlers, registration, permissions, partial sync, GC,
change feed, trusted writes), `storage/` (the `storage.Adapter` seam;
`sqlitestore/` is the single-process default, `pgstore/` the multi-node
PostgreSQL one and `prefixstore/` the workspace-id prefix a multi-app host
partitions one adapter with), `query/` (compiler + evaluator behind
the `Query` RPC and app-side queries), `text/` (the collaborative text CRDT),
`connect/` (bidirectional connectors to external stores), `effects/`
(server-side reactions to committed writes, over the change feed), `notify/`
(shared poke fan-out for multi-node deployments; `pgnotify/` over Postgres
`LISTEN`/`NOTIFY`), `presence/` (the ephemeral plane — rooms, presence and
broadcast, none of it durable; `pgpresence/` is the multi-node bus and
`presencetest/` the suite one must pass), `pglisten/` (the `LISTEN`/`NOTIFY`
transport `pgnotify` and `pgpresence` are both built on, and share one of when a
node runs both), `internal/` (`lww` field-mask merge semantics, `naming`,
`pubsub`, `pgtest` — all engine-only).
`engine/client/` is reserved for a future Go client; not built yet.

`cmd/`: every binary the repo produces. `syncplaned/` (the product: multi-app
host with a private admin listener), `testserver/` (spawned by the TS
integration and e2e suites), `todoserver/` (demo backend for `examples/todo`),
`mdtodo/` (worked connector example), `admindemo/` (throwaway harness for
eyeballing the admin UI).

`internal/`: module-private and importable from anywhere in the module, which is
why these sit above `engine/` rather than inside it — `cmd/` could not reach
them from `engine/internal/`. `admin/` (templ + htmx admin UI, with `static/`
and `ui/`), `protoopts/` (entity annotations recovered from raw option bytes;
also used by `engine/`), `clilog/` (flag-driven logger shared by the commands),
`exampleapp/` (the todo schema the commands and server tests register).

`examples/`: `todo/` (vite web app, pairs with `cmd/todoserver`), `register/`
(runtime schema registration over AdminService).

`client/src/`: `kv/` (IndexedDB + in-memory backends, multi-tab safety via Web
Locks), `optimistic/` (pending queue, rebase, compaction), `sync/` (background
coordinator, cross-tab poke/change sharing, and `presence.ts` for the second
plane's own stream and leadership), `read.ts` (the merged view every reader goes
through), `room.ts` (the `Room` handle apps hold), `text/` (TS mirror of
`engine/text`), `query/`, `db.ts` (the schema-bound per-entity API), `react/`.

Read the package docs and the code for specifics — they are documented at the
point of code, and that documentation is authoritative over any summary here.

`docs/` is the prose documentation, organized along [Diátaxis](https://diataxis.fr):
`tutorials/` (learning by doing), `how-to/` (task recipes), `reference/` (lookup:
server options, the TS client API, proto options, status codes, the storage
contract) and `explanation/` (the convergence model, partial sync, the text CRDT,
multi-tab, trade-offs). `docs/README.md` indexes all of it. Keep a change in the
quadrant it belongs to — a new option goes in `reference/`, the reasoning behind
it in `explanation/` — and keep the root `README.md` short: features, quick
start, a few examples, links into `docs/`.

## Commands

Tools are pinned in `mise.toml` (`mise install` gets go, node, pnpm, buf, task,
ginkgo). Tasks live in `Taskfile.yml`:

```sh
task generate          # regenerate Go + TS from proto/ (buf generate)
task ci                # everything CI runs: lint, breaking, vet, typecheck, all tests
task test:go           # Ginkgo suites under engine/
task test:ts:unit      # vitest unit suites under client/
task test:ts:react     # jsdom suites for syncplane/react
task test:ts:integration  # TS client against a spawned Go testserver
task test:e2e          # multi-tab browser tests (Playwright; not in `task ci`)
task demo:server       # todo demo sync server on :8787
task demo:web          # todo demo vite app (server must run in another terminal)
```

Running a single test:

```sh
# Go: pass ginkgo args through task, or run ginkgo directly from the repo root
task test:go -- --focus "rejects a stale version"
go tool ginkgo run ./engine/server                # one package

# TS: vitest in client/
cd client && pnpm vitest run --project unit test/unit/kv.test.ts
cd client && pnpm vitest run --project integration -t "poke"

# Browser: Playwright in client/ (task test:e2e:install downloads chromium once)
cd client && pnpm exec playwright test --project=chromium test/e2e/stream-leader.spec.ts
task test:e2e BROWSER=firefox -- -g "hands over"          # any installed engine
```

Formatting is dprint (`dprint fmt`): TS/JSON/Markdown via plugins, `.proto` via
clang-format. TS style is tabs, single quotes, spaces inside `{ }` braces.

### What an agent must not run

Ask the human to run these, then continue:

- `pnpm install` (and anything that triggers it) hangs in the agent sandbox, and
  the attempt purges `client/node_modules` on the way in. Reach the installed
  tools directly — `client/node_modules/.bin/tsc --noEmit`,
  `client/node_modules/.bin/vitest run --project unit` — rather than through
  `pnpm run`/`pnpm exec`, which re-check dependency status first.
- `playwright install` downloads from a host the sandbox does not allow.

Playwright needs the sandbox off to run at all: Chromium dies at startup on
`bootstrap_check_in ... Permission denied` because it cannot claim its Mach port.
Everything else — Go, vitest, dprint — runs sandboxed.

## Testing

**Fixing a bug is test-first.** Write the failing test that reproduces it, watch
it fail for the right reason, then fix. A bug that shipped without a test means
the suite had a hole; closing the hole is part of the fix, not optional
follow-up. For new features, tests land with the change.

Pick the tier that can actually observe the behavior:

- **Go** — Ginkgo/Gomega suites colocated with the code, run randomized in CI.
- **`client/test/unit`** — vitest against fake-indexeddb.
- **`client/test/integration`** — spawns the real Go testserver via vitest
  `globalSetup`. **Cross-language wire behavior belongs here**, not in unit tests.
- **`client/test/react`** — jsdom project (`--project react`) for `syncplane/react`.
- **`client/test/e2e`** — Playwright, real tabs sharing real IndexedDB, Web Locks
  and BroadcastChannel. **Anything that depends on there being more than one tab
  is only really covered here** — Node has no shared lock manager, so it falls
  back to an in-process mutex, declines stream leadership and leases peers
  locally. Specs drive a JSON-only `window.harness`; invariants no tab can see
  are asserted against testserver's `/debug/rpc` counters.

Shared conformance vectors are contracts between the two implementations —
extend them when touching the semantics they pin, and update **both** sides
together:

- `testdata/queryeval/` — query-evaluation vectors.
- `testdata/indexkey/` — index-entry encoding (`encodeCases` pin the bytes),
  entry-vs-comparator agreement (`orderCases`) and the planner's decisions
  (`probeCases`). The planner half is the one nothing else would notice: a
  planner that chose differently still returns the right rows, just at a
  different cost.
- `testdata/text/` — text-CRDT vectors. The `encodedHex` checks pin the canonical
  encoding byte-for-byte, so any change to `engine/text` or `client/src/text`
  must land in the other implementation and the vectors in one change.

## Documentation

Every comment has exactly one reader, and writing for the wrong one is the
failure these rules exist to prevent:

- **Doc comment on an exported identifier** → a consumer who can only see the
  API. Contract only, and say the things a signature cannot: errors (name the
  sentinel or type), behaviour on nil/zero/empty, ownership of arguments and
  who closes what, concurrency safety, whether the call blocks, units and
  ranges, ordering guarantees, what a default or omitted option means, and cost
  when it would surprise a caller. Two tests: a sentence may only state facts
  observable from the caller's side, and it must stay true when the body is
  rewritten from scratch. Go comments open with the identifier and a summary
  sentence that stands alone in a package listing; TS uses TSDoc with a summary
  line and no leading tag.
- **Comment inside a function body** → a maintainer about to change this code.
  Why this approach and not the obvious alternative — a non-obvious ordering
  dependency, a workaround for an upstream bug (link it), a deliberate deviation
  from the surrounding pattern, a measured performance decision. Never narrate
  what the code already says.
- **`docs/`** → whichever Diátaxis mode the file lives in. Rationale, history
  and trade-offs live in `explanation/`; a doc comment links there instead of
  inlining them.
- **Commit message** → alternatives considered, benchmarks, "we chose X because
  Y". When such a sentence turns up in a doc comment, move it here rather than
  deleting it.

Never restate the signature. Delete any `@param` that expands the parameter name
into a sentence, any `@returns` that restates the return type, and any prose that
repeats a TypeScript type; encode a constraint in the type system rather than
documenting it. A compiling example (Go `Example` function, TSDoc `@example`)
outranks a paragraph.

Banned everywhere: filler (`note that`, `simply`, `just`, `basically`,
`under the hood`, `in order to`), self-narration (`we went with`,
`the honest option`, `it turns out that`), unverifiable superlatives (`robust`,
`powerful`, `seamless`, `blazingly fast`), and asides about alternatives that
were rejected — there is no alternative in the reader's world.

Load the `documentation` skill for the full rules — the revision checklist that
turns a finished draft into a shorter one carrying more information, per-language
tag usage, and worked before/after corrections. Reach for it when writing or
revising more than a line or two of doc comment, adding a page under `docs/`, or
reviewing someone else's comments and prose.

## Conventions

- Match the surrounding code's idiom, naming and comment density.
- Prefer the smallest change that fixes the actual problem; don't widen scope.
- Write non-printing, ambiguous or invisible characters as escapes rather than
  literally (`\uffff`, `\u0000` — lowercase hex), in code, comments and docs
  alike. A literal sentinel is invisible in a diff and easy to mangle in an
  editor.
