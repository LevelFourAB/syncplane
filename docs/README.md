# syncplane documentation

Organized along [Diátaxis](https://diataxis.fr): four kinds of documentation
serving four different needs. If you are not sure where to start, the first
tutorial builds a working app end to end.

| If you want to…                  | Read                            |
| -------------------------------- | ------------------------------- |
| Learn by building something      | [Tutorials](#tutorials)         |
| Solve a specific problem         | [How-to guides](#how-to-guides) |
| Look up an API, option or code   | [Reference](#reference)         |
| Understand why it works this way | [Explanation](#explanation)     |

## Tutorials

Learning-oriented, start to finish. Follow them in order.

1. [Your first sync app](tutorials/your-first-sync-app.md) — a todo app with a
   Go server and a TypeScript client, syncing between two browser tabs.
2. [Adding collaborative text](tutorials/adding-collaborative-text.md) — a
   shared note that two people can type into at once.

## How-to guides

Task-oriented recipes. Each assumes you already have something running.

**Modeling and schemas**

- [Define entities in your own module](how-to/define-entities.md)
- [Define schemas in TypeScript](how-to/define-schemas-in-typescript.md)
- [Recover a deleted row](how-to/recover-a-deleted-row.md)

**Server**

- [Authentication and permissions](how-to/authentication-and-permissions.md)
- [Write from server-side code](how-to/server-side-writes.md)
- [Query from server-side code](how-to/server-side-queries.md)
- [React to changes (server-side effects)](how-to/react-to-changes.md)
- [Follow committed changes (change feed)](how-to/change-feed.md)
- [Sync an external store (connectors)](how-to/sync-an-external-store.md)
- [Host multiple apps in one process](how-to/host-multiple-apps.md)
- [Run more than one node](how-to/run-multiple-nodes.md)
- [Deploy for high availability](how-to/deploy-for-high-availability.md)
- [Observe the engine (OpenTelemetry)](how-to/observe-the-engine.md)

**Client**

- [Query data](how-to/query-data.md)
- [Search data](how-to/search.md)
- [Sync only part of a workspace](how-to/partial-sync.md)
- [Use collaborative text](how-to/use-collaborative-text.md)
- [Add presence to an app](how-to/add-presence.md)
- [Build a UI with React](how-to/react.md)
- [Hold several workspaces on one client](how-to/multiple-workspaces.md)
- [Restore a field to a previous value](how-to/restore-a-field.md)
- [Handle failures](how-to/handle-failures.md)
- [Test your app without a server](how-to/test-your-app.md)

**Working on syncplane itself**

- [Testing](how-to/testing.md)

## Reference

Information-oriented, structured to be looked things up in.

- [Go server API](reference/server.md) — construction, options, limits, GC,
  trusted writes, the change feed, `Manager` and the admin service.
- [TypeScript client API](reference/typescript-client.md) — the full surface of
  `syncplane` and `syncplane/react`.
- [Proto options](reference/proto-options.md) — `(syncplane.v1.entity)` and
  `(syncplane.v1.field_behavior)`.
- [Connectors](reference/connectors.md) — the `Source` port and its optional
  halves, every connector `Config` field, error sentinels, and `Health`.
- [Naming rules](reference/naming.md) — what an entity may be called, which
  fields may be key fields, and what a row key may contain.
- [Sync status and failure codes](reference/sync-status-and-codes.md) — the
  status snapshot, which channel reports which failure, and the code vocabulary.
- [Storage adapter contract](reference/storage-adapter.md) — what an adapter
  must honor to be correct.
- [Telemetry](reference/telemetry.md) — every metric and span the engine emits,
  their attributes, and the cardinality rule they follow.

## Explanation

Understanding-oriented. Read these when a behaviour surprises you.

- [The convergence model](explanation/convergence-model.md) — sequences,
  per-field last-writer-wins, rebase, tombstones, compare-and-swap.
- [Partial sync and coverage](explanation/partial-sync-and-coverage.md) — why
  windows exist and why every result carries a coverage report.
- [Relational queries](explanation/relational-queries.md) — the one-hop
  semi-join (`matches`), its two-phase rewrite, and how `follow` makes it
  answerable locally.
- [Grouped reads](explanation/grouped-reads.md) — counts and distinct values
  without reading rows, the head of each group without reading the rest, the
  skip-scan that serves them all, and what a group is.
- [Full-text search](explanation/search.md) — why the analyzer is specified
  twice, ranking without corpus statistics, and an index that only narrows.
- [Collaborative text](explanation/collaborative-text.md) — Fugue ordering,
  state-based replication, delta sync, marks, undo, compaction.
- [Multi-tab behaviour](explanation/multi-tab.md) — one store, one stream
  leader per plane, and what only a real browser can verify.
- [Rooms: the ephemeral plane](explanation/rooms.md) — presence and broadcast,
  why the mutation log is the wrong home for them, and expiry by
  re-announcement.
- [Limits and trade-offs](explanation/limits-and-trade-offs.md) — what the
  engine deliberately does not do, and what to know before adopting.
