# Adversarial Test Report: syncplane

Date: 2026-10-11
Scope: branch `feature/permissions-schema-support` against `main` (52 commits, about 58k lines): the declarative access model on the server write and read paths, access-change sync (cursors, resync, move log, `CallerAccess` and its delta), schema registration and evolution, the TS client prediction API and schema builder, and cross-cutting areas (gate, mutators, trusted writes, rooms, prefixstore, pgstore). DX and forward compatibility are in scope. The removal of the entity and field write policies from the public `.proto` is intentional and is not reported. Section BLK-5 reports only the silent way in which old schemas meet that removal.
Not covered: Playwright e2e, which needs the sandbox off and a browser install (so no multi-tab races in real browsers). The IndexedDB store path. Enum role aliases. Guarded nested, repeated or map fields (no fixture). `MaxResyncScan` overflow under load. A two-app prefixstore test at server level (judged safe by reading the code).

## Fix progress

Work on all findings is in one "automated fixes" change. Status values: OPEN, IN PROGRESS, FIXED, PARTIAL, BLOCKED.

Decisions:

- BLK-1: a move is refused with `access_denied` when the mover cannot read the moved row, or a field that a guard hides, at the source.
- BLK-2: a client create is refused with `conflict` when live child rows or live grants already name its key (as for a client delete). Trusted writes can still adopt.
- BLK-4: on the server, a Pull from a cursor that had grants runs the revocation resync (tombstones and an empty summary) before it answers NO_ACCESS, as Subscribe does.
- MAJ-2: the next pull of a cursor that already holds the subscription runs a resync that drops the field. Only new subscriptions are refused.
- MAJ-4: the manager stores the merged live schema after each registration.
- MAJ-6: a DELETE of a missing row gets the same answer as a DELETE of a hidden row (`access_denied`, no tombstone).
- MAJ-8: the server refuses a write that changes the input of a hiding guard, unless the caller reads the hidden fields before and after the write.
- MAJ-12: the move log and the cycle check use the last valid model while the model is incomplete.
- FC-1 to FC-5: all are in scope.
- MIN-5: rules accept only `schema.within(...)` or self entries. `within` is new on this branch.
- MIN-8: a pending custom mutation gives `'unknown'` only when its mirror writes something that can change access.
- MIN-11: `MutatorAccess` gets an explicit trusted-only marker that turns the warning off.
- OBS-4: the behaviour stays as specified, and `explanation/` documents it.
- OBS-5: reads after `tx.AsCaller()` apply the caller's read rules and hidden fields. Plain `tx` reads stay unfiltered.
- OBS-1: a client write that adds an access parent in a recursive chain must read that parent.
- OBS-2: conflict messages stay generic for every caller.
- OBS-1 to OBS-7 are in scope. Each one gets a test first. A note that the test does not confirm is marked NOT REAL.

| Stream          | Findings                                                            | Status |
| --------------- | ------------------------------------------------------------------- | ------ |
| A: write path   | BLK-1, BLK-2, MAJ-6, MAJ-7, MAJ-8, MIN-1                            | FIXED  |
| B: registration | BLK-5, FC-1, MAJ-3, MAJ-4, MAJ-5, MAJ-13 (Go), MIN-3, MIN-4, MIN-11 | FIXED  |
| C: access sync  | BLK-3, BLK-4, MAJ-2, MAJ-9, MAJ-10, MAJ-11, MIN-2, FC-2, FC-3       | FIXED  |
| D: reads, rooms | MAJ-1, MAJ-14, MAJ-18, MIN-10, FC-4                                 | FIXED  |
| E: TS client    | MAJ-13 (TS), MAJ-15, MAJ-16, MAJ-17, MIN-5 to MIN-9                 | FIXED  |
| Wave 2: A       | MAJ-12, OBS-1, OBS-2, OBS-3, MAJ-7 cost                             | FIXED  |
| Wave 2: B       | MIN-11 marker, FC-5, OBS-7                                          | FIXED  |
| Wave 2: D       | OBS-5, OBS-4 (docs)                                                 | FIXED  |
| Wave 2: E       | MIN-8 (mirror writes), OBS-6                                        | FIXED  |
| Lead            | MIN-12                                                              | FIXED  |

| ID     | Fix status   | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| BLK-1  | FIXED        | A move is refused with `access_denied` when the mover cannot read the moved row, or a field that a guard hides, at the source (`checkReadsStored`).                                                                                                                                                                                                                                                                                                                                  |
| BLK-2  | FIXED        | A client create is refused with `conflict` while a live child row or a live grant row names the key (`checkNothingNames`). Trusted writes still adopt.                                                                                                                                                                                                                                                                                                                               |
| BLK-5  | FIXED        | Registration refuses `EntityOptions` fields 4, 7 and 8 and `FieldBehavior` field 8. The error names the entity, the field and what replaces the policy.                                                                                                                                                                                                                                                                                                                              |
| FC-1   | FIXED        | Registration refuses unknown fields at any depth of `EntityOptions` and of every `FieldBehavior`, for all entities.                                                                                                                                                                                                                                                                                                                                                                  |
| MAJ-3  | FIXED        | `engine/query/compile.go` accepts only known Cmp, Str, Rep and Present operator values, in every query (access rules, the Query RPC, subscriptions). The TS `condFromProto` already throws on an unknown op.                                                                                                                                                                                                                                                                         |
| MAJ-4  | FIXED        | The manager merges the incoming set over the stored schema, registers the merged set and stores it. A lock per app runs one registration of the app at a time. Edge: in a multi-node setup, the merge uses this node's schema, so it stays last-writer-wins across nodes.                                                                                                                                                                                                            |
| MAJ-5  | FIXED        | `RegisterEntity` accepts only model errors that a later entity can repair (`missingEntityError`) and refuses all others. The server keeps serving the old schema.                                                                                                                                                                                                                                                                                                                    |
| MAJ-6  | FIXED        | Under the access model, a PATCH or DELETE of a missing row or a tombstone gets `access_denied`, with no tombstone and no sequence used. A PUT is still a create.                                                                                                                                                                                                                                                                                                                     |
| MAJ-7  | FIXED        | A write that makes the path above a row plus the subtree below it longer than `MaxAccessDepth` is refused with `invalid_argument`, Trusted writes included. The walk down is skipped when the new path above is not longer than the old one, and the old path still fits the limit.                                                                                                                                                                                                  |
| MAJ-8  | FIXED        | An update that changes a field that a hiding guard's rule checks is refused unless the caller reads the guarded fields on the stored row and on the new row (`checkGuardInputs`). This also refuses a caller who gives the record to someone else and cannot read the fields after the change.                                                                                                                                                                                       |
| MAJ-13 | FIXED (Go)   | The server reserves the `syncplane_` prefix (`naming.IsReservedEntityName`). Only the engine's history entity uses it. The TS builder side is in stream E.                                                                                                                                                                                                                                                                                                                           |
| MIN-1  | FIXED        | The move check and the `within`-change check count only rules without `caller_is`/`when` as reading every row (`readers.everyRow`, `moveReadersBelow`). New unit tests are in `access_internal_test.go`, because the end-to-end fixture also triggers BLK-1.                                                                                                                                                                                                                         |
| MIN-3  | FIXED        | `accessSpecOf` includes the creator fields, so a change to a creator mark needs `AllowAccessChange`.                                                                                                                                                                                                                                                                                                                                                                                 |
| MIN-4  | FIXED        | Guard rule errors read `access guard[i] allow[j]`. `within` errors give the rule path and `within[k]`.                                                                                                                                                                                                                                                                                                                                                                               |
| MIN-11 | FIXED        | Registration warns about each mutator that declares no MutatorAccess, and the warning suggests `TrustedOnly`. The new `MutatorAccess{TrustedOnly: true}` turns the warning off and refuses non-trusted callers with `access_denied`. `TrustedOnly` with any other field is refused at registration. `MutatorAccess` exists only in Go, so this needs no proto or TS change.                                                                                                          |
| BLK-3  | FIXED        | The move log records every change of parents or `within` state of an anchor-entity row, and every delete, whether or not rows sit below it. The old-side chain walk reads a row created after the cursor floor as missing (`createdAfter`). All 161 convergence seeds pass.                                                                                                                                                                                                          |
| BLK-4  | FIXED        | A Pull from a cursor that held a permission gets the revocation resync (tombstones, an empty `CallerAccess`, `has_more`), and the next pull gets `workspace_no_access`. A cursor that never held a grant gets NO_ACCESS at once. The client needs no change. Open: a stream that opens from such a cursor still gets NO_ACCESS without tombstones. The client's retry Pull then converges.                                                                                           |
| MAJ-2  | FIXED        | A subscription that the cursor already holds is no longer refused. While the field stays hidden, the subscription selects no row, and the resync removes its rows. Only new subscriptions are refused.                                                                                                                                                                                                                                                                               |
| MAJ-9  | FIXED        | Each Bootstrap page pins its chains to the position at Begin (`pinChains`).                                                                                                                                                                                                                                                                                                                                                                                                          |
| MAJ-10 | FIXED        | A cursor that reuses its access record reads it (a stream at most once an hour) and writes it again if the store lost it. SQLite and Postgres renew `written_at` on a `Get` older than a quarter of the retention.                                                                                                                                                                                                                                                                   |
| MAJ-11 | FIXED        | Pull and Subscribe floor replica reads at max(seq, minSeq, movesThrough, resync position). `access_moves_through` records the read position.                                                                                                                                                                                                                                                                                                                                         |
| MIN-2  | FIXED        | `MaxAccessDepth` is in the read-policy fingerprint and the `ReadAccess` record, so a change to it resets clients.                                                                                                                                                                                                                                                                                                                                                                    |
| FC-2   | FIXED        | `EntityReadRule.rule` is replaced by structured fields (field 2 reserved), compared with `proto.Equal`. Every client resets once on deploy.                                                                                                                                                                                                                                                                                                                                          |
| FC-3   | FIXED        | New `canonical.go`: an encoding the engine controls, with no dependence on proto marshal output. It gives the access hashes and the `when`-spec strings. Golden bytes and hashes are pinned.                                                                                                                                                                                                                                                                                         |
| MAJ-15 | FIXED        | `can()` mirrors `checkGrantSubset` on delete, create and updates that reach subject, role or parent. A change of role or parent gives `'unknown'`.                                                                                                                                                                                                                                                                                                                                   |
| MAJ-16 | FIXED        | `can('update')` mirrors the read checks below a moved row, the source checks of BLK-1, the `everyRow` rule of MIN-1 and the guard inputs of MAJ-8. For a row the client does not hold, it gives `'unknown'` (`row_not_held`). The create `conflict` and the depth refusal are not mirrored, because the client cannot see the rows they need.                                                                                                                                        |
| MAJ-17 | FIXED        | `accessInputsOf` holds every field that a rule, guard or `within` check reads. The `useCan` docs are fixed.                                                                                                                                                                                                                                                                                                                                                                          |
| MIN-5  | FIXED        | `within` `callerIs` is type-checked. Rules accept only `schema.within(...)` or self entries, so a plain `{ entity, callerIs }` object no longer compiles. `within` is new on this branch.                                                                                                                                                                                                                                                                                            |
| MIN-6  | FIXED        | The TS builder refuses a `within` entry that reads a field a guard of the target can hide, the same as Go.                                                                                                                                                                                                                                                                                                                                                                           |
| MIN-7  | FIXED        | The test kit fake versions each summary and sends `CallerAccess`, or a delta, only when the version changes.                                                                                                                                                                                                                                                                                                                                                                         |
| MIN-8  | FIXED        | A queued custom mutation gives `'unknown'` (`pending_grant`) only when its mirror leaves a grant row or a deleted anchor row in the overlay. Without a mirror, it gives `'unknown'` only when the schema has grant entities. A replay that changes what the mirror wrote is seen, and `onAccessChange` fires for it.                                                                                                                                                                 |
| MIN-9  | FIXED        | Compaction does not fold a PUT onto a PATCH that clears a guarded unit the PUT leaves unset.                                                                                                                                                                                                                                                                                                                                                                                         |
| MAJ-13 | FIXED        | Go and the TS builder both refuse entity names that start with `syncplane_` (case-sensitive).                                                                                                                                                                                                                                                                                                                                                                                        |
| MAJ-12 | FIXED        | The catalog keeps the last access model that compiled (`lastAccess`). While the model is incomplete, `checkAccessChain` and `recordAccessMove` use it through `trackedAccess`. So Trusted writes still log moves and are refused on a cycle or a chain that is too deep.                                                                                                                                                                                                             |
| OBS-1  | FIXED        | Confirmed. A client PUT or PATCH that adds an access parent in a recursive chain must read each parent it adds. Hidden, missing and too-deep parents all get `access_denied`. Behaviour change (accepted): a write-only role cannot place rows below a folder it cannot read. Client mirror: `canCreate` answers `'no'` for a held parent the caller does not read, and `'unknown'` for a parent the client does not hold. A move of a recursive entity answers `'unknown'` at best. |
| OBS-2  | FIXED        | Confirmed. The conflict messages (anchor delete, subject delete, the two create cases) say "a live row" or "a live grant row", and the debug log names the entity.                                                                                                                                                                                                                                                                                                                   |
| OBS-3  | FIXED        | Confirmed. `admitsThrough` checks each `within` entry through each changed parent. Client mirror: `withinAtEach` in `canCreate` (unit test only: the testserver schema has no entity with several parents and a `within` rule).                                                                                                                                                                                                                                                      |
| MAJ-1  | FIXED        | `ViewTx.Query` and `ApplyTx.Query` refuse a `Where` or `OrderBy` that names a field a guard hides, rel targets included (`permission_denied`).                                                                                                                                                                                                                                                                                                                                       |
| MAJ-14 | FIXED        | Missing, deleted and unreadable anchors give one message: `room anchor E/K is not readable`. The server log keeps the detail.                                                                                                                                                                                                                                                                                                                                                        |
| MAJ-18 | FIXED        | Session bindings and presence records are keyed on issuer and subject, on every node. `pgpresence` carries the issuer as `"iss"`. In a rolling deploy, a new node drops state and leave events from an old node that sends no issuer, so watchers on new nodes do not see the sessions on old nodes until those nodes are gone. `presencetest` covers the issuer.                                                                                                                    |
| MIN-10 | FIXED        | A hook error that wraps a `WorkspaceDenial` gets code `internal`, or the code of a `*connect.Error` in the chain.                                                                                                                                                                                                                                                                                                                                                                    |
| FC-4   | FIXED        | `TopicDropped.Reason` has `REASON_NO_ACCESS = 3`, sent when the caller holds no grant yet. The TS client puts the room in a new `'waiting'` state and asks again with backoff, or at once on an access change. New tests in `access_roomgate_test.go` and `room-access.test.ts`. New test in `access_roomgate_test.go`.                                                                                                                                                              |
| DOC    | FIXED        | Found during the fixes: `docs/reference/server.md` named `CodeWorkspaceAccessRevoked` for the gate's no-permission refusal. It now names `CodeWorkspaceNoAccess`, and the code list includes that code.                                                                                                                                                                                                                                                                              |
| OBS-4  | FIXED (docs) | The behaviour is as specified. `explanation/access-model.md` says why a move into a subtree with a wider role shows nothing new, and how to keep a row in its subtree.                                                                                                                                                                                                                                                                                                               |
| OBS-5  | FIXED        | Reads through `tx.AsCaller()` in a mutator body, and reads in a `Server.Mutate` or `MutateIdempotent` for a caller that is not Trusted, apply the caller's read rules, guards and `ReadFilter` (`Get`, `GetMeta`, `Scan`, `ScanPage`, `Query`). Plain `tx` reads in a mutator body and Trusted reads stay unfiltered. Cost: one `ReadFilter` call per entity on the first read and after each write.                                                                                 |
| OBS-6  | FIXED        | Confirmed. `can(..., 'delete')` answers `'no'` while the client holds a live row below the row. Limits: `useCan` does not ask again when a child row comes or goes, and a grant row that names the row as its subject is not mirrored.                                                                                                                                                                                                                                               |
| DOC    | FIXED        | `client/src/sync/status.ts`: the `'none'` text now says that a caller that loses its last grant first gets tombstones for every row in the access model.                                                                                                                                                                                                                                                                                                                             |
| INFRA  | FIXED        | Found during the fixes: each TS integration run left a testserver build of about 40 MB in the temp directory, so the disk filled. `startTestServer` and the `app-schema` setup now delete the folder once the server listens, or when the build or the start fails. `cmd/testserver` also deletes its temp database on SIGTERM, because the suites stop it with a signal. Checked: a run leaves no folder. `cmd/syncplaned` has the same `defer` pattern but is not changed.         |
| FC-5   | FIXED        | New `CallerAccess.max_access_depth = 7`, where 0 means 32. The value is part of the version hash, and a change sends the whole summary. Client predictions and the test kit (`maxAccessDepth` option) use it, and the testserver has `-max-access-depth`. Go, unit and integration tests cover it.                                                                                                                                                                                   |
| OBS-7  | FIXED        | Confirmed. `ruleFieldsOf` ignores synthetic oneofs (`_status`), so a guard that names one is refused. protobuf-es leaves synthetic oneofs out, so the TS side needs no change.                                                                                                                                                                                                                                                                                                       |
| MIN-12 | FIXED        | The how-to now says that an unused permission gives a warning. The `Team team` examples in the how-to, `reference/proto-options.md` and `options.proto` keep `role = 4` and give `team` the new number 5. The generated code must be regenerated.                                                                                                                                                                                                                                    |

Final verification (lead, after all streams):

- `go vet ./...`: clean.
- `go tool ginkgo run -r --randomize-all ./engine/`: every suite passes. Server 2175/2175, Postgres Store 732/732, SQLite Store 241/241, Prefix Store 218/218, Presence 55/55, Postgres Presence 42/42, Query Evaluator 670/670, Text 257/257, and the others.
- `go test ./internal/... ./cmd/...`: pass.
- `client/node_modules/.bin/tsc --noEmit`: clean.
- `vitest run --project unit --project react --project integration`: 186 files, 3111/3111.
- `buf lint`: clean. `buf breaking` against `main` reports only the intended removals: the write-policy enums, `EntityOptions` fields 4, 7 and 8, `FieldBehavior` field 8, and the testdata messages `Activity`, `Comment` and `Ledger`.
- `task generate`: ran after the last proto change.
- `dprint check`: clean for every changed file. `CHANGELOG.md`, `internal/admin/static/admin.js` and two `engine/server` test files were already unformatted, and this change does not touch them.
- Not run: Playwright e2e (`task test:e2e`). It needs the sandbox off.

Open after this change (no fix made):

- Subscribe at stream open from a cursor that held grants answers NO_ACCESS without tombstones. The client's retry Pull then runs the revocation resync and converges (BLK-4).
- `useCan(..., 'delete')` does not ask again when a child row comes or goes (OBS-6).
- `pgpresence` drops old-node events without a counter or a log line during a rolling deploy (MAJ-18).
- `cmd/syncplaned` leaves its temp database behind on a signal, as `cmd/testserver` did (INFRA).
- Deploy cost: every client resets once (FC-2, FC-3) and gets one whole `CallerAccess` (FC-5).

## Environment

- Languages and tooling: Go (Ginkgo/Gomega, `go test`), TypeScript (vitest projects unit, react and integration; `tsc`), protobuf through buf, SQLite and PostgreSQL (docker, through `pgtest`).
- Test commands:
  - `go tool ginkgo run -r --randomize-all ./engine/` (baseline)
  - `go test ./engine/server ./internal/protoopts -count=1` (final)
  - `client/node_modules/.bin/tsc --noEmit`
  - `client/node_modules/.bin/vitest run --project unit --project react --project integration`
- Baseline:
  - Go: every suite passes (Server 1913/1913, Postgres Store 731/731 and the others).
  - TS unit and react: 2799 pass and 1 fails (`room.test.ts` "throttles a burst…", a timing flake in code this branch does not touch; it passed in the final run). `tsc` is clean.
- Final run with the new tests:
  - Go `engine/server`: 2076 pass and 48 fail. Every failure is a new test.
  - `internal/protoopts`: pass.
  - TS: 3071 pass and 14 fail. Every failure is a new test.
  - `tsc`: 2 expected errors (TS2578, from the compile-time findings in MIN-5).
- Sub-agents, each with its own test files:
  1. Server write path
  2. Server read path and leaks
  3. Access-change propagation (cursors, resync, move log, storage)
  4. Schema registration, DX and forward compatibility
  5. TS client
  6. Cross-cutting (gate, grant reuse, mutators, trusted writes, rooms, prefixstore, pgstore)
- The lead merged duplicate findings, re-ran BLK-1 and BLK-2 by hand and read their fixtures, re-ran everything together, and added the test that confirms BLK-5.

## Findings overview

| ID     | Title                                                                                                   | Severity | Likelihood | Status      | Blast radius                                                           |
| ------ | ------------------------------------------------------------------------------------------------------- | -------- | ---------- | ----------- | ---------------------------------------------------------------------- |
| BLK-1  | A move shows the mover the moved row, or its hidden fields, that it could not read before               | BLOCKER  | High       | CONFIRMED   | Permanent read of salaries, reviews, whole rows; silent                |
| BLK-2  | A create takes over orphan child rows and stale grants that name its key                                | BLOCKER  | Medium     | CONFIRMED   | Stale grant gives a full role on a new subtree; old private rows shown |
| BLK-3  | Changes to a row that has nothing below it are not logged, so a pull after an earlier move diverges     | BLOCKER  | Medium     | CONFIRMED   | Clients keep revoked rows or never get granted rows; silent            |
| BLK-4  | A pulling client whose last grant goes keeps every row and its old summary                              | BLOCKER  | High       | CONFIRMED   | Removed member keeps all data, `can()` says yes; Pull ≠ Subscribe      |
| BLK-5  | A schema that still sets a removed write policy registers, and the policy is dropped silently           | BLOCKER  | Medium     | CONFIRMED   | Trusted-only entity becomes writable by every admitted caller          |
| MAJ-1  | In-process `View`/`Apply` queries filter and order by hidden fields                                     | MAJOR    | Medium     | CONFIRMED   | Hidden values found by matching and order; silent                      |
| MAJ-2  | A grant change that hides a subscribed field stops sync and leaves the value on the client              | MAJOR    | Medium     | CONFIRMED   | Revoked values stay; workspace sync stays failed                       |
| MAJ-3  | Unknown operator values in `when`/`within` register, and `not(...)` admits every row                    | MAJOR    | Medium     | CONFIRMED   | Older node in a rolling deploy widens reads and writes                 |
| MAJ-4  | Manager restart after a partial registration loses the access model                                     | MAJOR    | Low–Med    | CONFIRMED   | Gate admits callers with no grant after a restart                      |
| MAJ-5  | `RegisterEntity` returns nil for a model it cannot repair, then every request fails                     | MAJOR    | Low        | CONFIRMED   | Whole server outage after a "successful" call                          |
| MAJ-6  | DELETE of a missing key skips every rule and sends a tombstone to all                                   | MAJOR    | High       | CONFIRMED   | Write bypass, feed spam, existence oracle                              |
| MAJ-7  | Moving an ancestor puts rows below it deeper than `MaxAccessDepth`                                      | MAJOR    | Low        | CONFIRMED   | Subtrees disappear for non-root readers                                |
| MAJ-8  | Rewriting a `caller_is` field to yourself shows you the field it hides                                  | MAJOR    | High       | CONFIRMED   | Any editor reads every guarded review                                  |
| MAJ-9  | Paged Bootstrap plus an A→B→A move ships a row that no later pull removes                               | MAJOR    | Low–Med    | CONFIRMED   | Stale row stays on the client                                          |
| MAJ-10 | The ReadAccess record expires while its cursor is in use, so a grant change resets instead of resyncing | MAJOR    | High       | CONFIRMED   | Re-bootstrap storm on an org-wide role change                          |
| MAJ-11 | A replica read below `movesThrough` skips a move                                                        | MAJOR    | Low–Med    | CONFIRMED   | Stale rows; access flaps between replica and primary                   |
| MAJ-12 | Trusted writes during an incomplete model skip the move log and the cycle check                         | MAJOR    | Low–Med    | CONFIRMED   | Stale rows on clients; stored cycle                                    |
| MAJ-13 | An app entity named `syncplane_access_moves` shares the move log                                        | MAJOR    | Low        | CONFIRMED   | Move records (parents, owners) reach clients                           |
| MAJ-14 | A room-anchor drop message tells whether a key exists                                                   | MAJOR    | High       | CONFIRMED   | Existence oracle for any key, grant rows included                      |
| MAJ-15 | `can()` says yes to grant writes that the subset rule refuses                                           | MAJOR    | High       | CONFIRMED   | Member-management UI offers actions that always fail                   |
| MAJ-16 | `can('update')` ignores the rule that a caller must read the rows below                                 | MAJOR    | Medium     | CONFIRMED   | UI offers moves and toggles that fail                                  |
| MAJ-17 | `useCan` does not re-evaluate when a field that a `when` or guard reads changes                         | MAJOR    | High       | CONFIRMED   | Stale "Edit" on archived rows                                          |
| MAJ-18 | Room Publish ignores the issuer of the session binding (on `main` too)                                  | MAJOR    | Low        | CONFIRMED   | Caller with no grant publishes as another session                      |
| MIN-1  | The move check drops `caller_is` (`loose()`)                                                            | MINOR    | Medium     | CONFIRMED   | Rows the mover cannot read reach a new audience                        |
| MIN-2  | Lowering `MaxAccessDepth` neither resets nor resyncs clients                                            | MINOR    | Low        | CONFIRMED   | Clients keep rows they can no longer read                              |
| MIN-3  | Dropping a `creator` mark is not an access change                                                       | MINOR    | Low        | CONFIRMED   | Forged authorship                                                      |
| MIN-4  | Registration errors for guard rules name the wrong rule                                                 | MINOR    | High       | CONFIRMED   | Confusing errors                                                       |
| MIN-5  | TS builder: `within` `callerIs` has no type check                                                       | MINOR    | Medium     | CONFIRMED   | Typos found only at runtime                                            |
| MIN-6  | TS builder accepts a guard hiding a field that a self `within` entry reads                              | MINOR    | Low        | CONFIRMED   | Client accepts, server refuses                                         |
| MIN-7  | Test kit fake re-sends `CallerAccess` on every pull                                                     | MINOR    | High       | CONFIRMED   | App tests hide MAJ-17-type bugs                                        |
| MIN-8  | A queued custom mutation that writes a grant gives `'no'`, not `'unknown'`                              | MINOR    | Medium     | CONFIRMED   | New project's controls hidden until the ack                            |
| MIN-9  | Compaction folds away a PATCH that clears a write-only hidden field                                     | MINOR    | Low        | CONFIRMED   | The clear is lost                                                      |
| MIN-10 | A hook error that wraps a workspace denial is reported as a denial                                      | MINOR    | Medium     | CONFIRMED   | Failed lookup read as a revocation                                     |
| MIN-11 | No warning for mutators that become trusted-only under the model                                        | MINOR    | High       | CONFIRMED   | Every client invocation fails after deploy                             |
| MIN-12 | Documentation errors (unused-permission claim, field-number reuse in an example)                        | MINOR    | Medium     | CONF./UNVER | Copy-paste traps                                                       |
| FC-1   | Unknown fields in `EntityOptions`/`FieldBehavior` are ignored in general                                | MAJOR    | Medium     | UNVERIFIED  | The next access mark is dropped silently by older nodes                |
| FC-2   | `EntityReadRule.rule` is an unversioned string stored in cursor-state records                           | MINOR    | Medium     | UNVERIFIED  | Mass reset or reset ping-pong when the format changes                  |
| FC-3   | `readAccessHash` hashes deterministic proto marshal output                                              | MINOR    | Low        | UNVERIFIED  | protobuf-go upgrade resets every client                                |
| FC-4   | Rooms have no `NO_ACCESS` drop reason                                                                   | MINOR    | Medium     | UNVERIFIED  | Clients treat "no grant yet" as revoked                                |
| FC-5   | Client chain limit is fixed at 32                                                                       | MINOR    | Low        | UNVERIFIED  | Predictions wrong under a non-default limit                            |
| OBS    | Smaller unverified notes (see the last section)                                                         | MINOR    | –          | UNVERIFIED  | –                                                                      |

Counts: 5 BLOCKER, 18 MAJOR (plus 1 unverified), 12 MINOR (plus 4 unverified). Failing tests confirm 34 findings.

## Findings

### BLK-1: A move shows the mover the moved row, or its hidden fields, that it could not read before

- Severity: BLOCKER
- Likelihood: High. A caller who writes in two projects with different read rights is common, for example an editor in one project and an owner in another.
- Why it's easy to miss: the move check (`readersBelow`) covers only the entities below the moved row. Every existing move test uses a mover who already reads the moved row.
- Location: `engine/server/accesswrite.go:730-766` (`finish`, the `moved` branch). The model never checks the read rules of the moved entity itself, or of its hiding guards, at the source.
- Evidence: `Expected <int64>: 5000 to be zero-valued` ("padmin read the salary of a record it moved out of p1"). In the row variant, `task/t1` appears in blind's pull after the move.
- Status: CONFIRMED. The lead re-ran it and checked the fixture.
- Blast radius:
  - Direct impact: a caller who may update a row reads its hidden fields or the whole row by moving it to a parent where it has read rights.
  - Downstream impact: every chain entity that has field guards or narrower read rules.
  - Loud or silent: silent.
  - Reversibility and recovery: the disclosure cannot be undone.
  - Scope affected: schemas with guards or read rules that differ between parents.
- Suggested fix direction: at the source, require the moved row's own read rule and the read rules of its hiding guards, as for the rows below it. Decide this explicitly, because the spec today guards only the readers below a moved row.

#### Scenarios

**S1.1: A move shows a hidden field to the mover**

- Given padmin is `project_admin` in p1, where it cannot read `salary`, and `project_owner` in p2, where it can
- And staff s1 is in p1 with salary 5000
- When padmin PATCHes s1.project to p2
- Then padmin's pull does not show salary 5000
- Test: `engine/server/access_movehidden_test.go::does not let a caller read a hidden field by moving the row to where it reads that field` | Result: FAIL

**S1.2: A move shows a whole row to the mover**

- Given blind is `project_editor` in p1 (`edit_tasks`, no `view_tasks`) and `project_writer` in p2
- When blind moves t1 from p1 to p2
- Then blind still does not read t1
- Test: `…::does not let a caller read a row by moving it to where it reads that entity` | Result: FAIL

**S1.3: The move is refused**

- Given the setup of S1.1
- When padmin moves s1
- Then the push is rejected with `access_denied`
- Test: `…::refuses a move by a caller who cannot read a hidden field of the row where it is` | Result: FAIL

### BLK-2: A create takes over orphan child rows and stale grants that name its key

- Severity: BLOCKER
- Likelihood: Medium. Orphans come from trusted deletes (documented as allowed), from connectors that write children first, and from root-holder writes. The docs say keys are not secret.
- Why it's easy to miss: the spec covers this takeover only for client deletes. The create path has no check, and the only related test seeds the orphan with a trusted write.
- Location: `engine/server/accesswrite.go:680-697` (`finish`, `case !w.live`).
- Evidence:
  - After editor creates folder `old`, mallory's pull contains `doc/d-new`, `folder/old` and `folder_member/fm-mallory`.
  - writer's pull contains `comment/c1` and `comment/c9`.
- Status: CONFIRMED. The lead re-ran it.
- Blast radius:
  - Direct impact: a stale grant becomes active again with a full role (for example `folder_manager`), and old private children are shown.
  - Downstream impact: other callers can then create rows below the adopted row.
  - Loud or silent: silent.
  - Reversibility and recovery: what was disclosed stays disclosed.
  - Scope affected: every anchor entity whose rows can be deleted by trusted code.
- Suggested fix direction: treat a create below which live rows or live grants already sit like a move from nowhere. Require the read permissions there, or refuse with `conflict` as a client delete does.

#### Scenarios

**S2.1: A create shows an orphan comment**

- Given comment c9 names task t9, which does not exist
- When writer creates t9 in p2
- Then writer does not read c9
- Test: `engine/server/access_adopt_test.go::does not show a caller a comment it could not read by creating the task the comment names` | Result: FAIL

**S2.2: A create shows the comments of a deleted private task**

- Given a trusted delete removed t1 in p1 and left c1
- When writer creates task t1 in p2
- Then writer does not read c1
- Test: `…::does not show a caller the comments of a deleted private task by creating a task with its key` | Result: FAIL

**S2.3: A create gives a stale grant its role again**

- Given mallory was `folder_manager` on `old`, and a trusted write deleted `old`
- When editor creates a new folder `old` and doc d-new in it
- Then mallory does not read d-new
- Test: `…::does not give a role to the subject of a grant left from a deleted folder when another caller creates a folder with its key` | Result: FAIL

### BLK-3: Changes to a row that has nothing below it are not logged, so a pull after an earlier move diverges

- Severity: BLOCKER
- Likelihood: Medium. "Move a subfolder out, then move or change the now-empty parent" is a normal reorganisation, and so is "move a matter out of a dossier, then make the dossier private".
- Why it's easy to miss: each step is correct alone. The bug needs two writes in order between pulls. Two randomized fuzzers found it on their own.
- Location:
  - `engine/server/accessmoves.go:142`: if `liveRowBelow(...)` returns `""`, no log record is written.
  - `engine/server/accesssync.go:337`: `oldChains.overrides` holds only logged rows, and the walk above an override reads the current snapshot.
- Evidence:
  - viewer reads `fa`. `fc` (with doc `dx`) moves from `fb` to `fz`, and then the empty `fb` moves to the top. viewer still holds `doc/dx`.
  - Fuzz seed 13 (topics): replica u3 keeps `lane_member/lm-u1-t0` after `t0` is detached.
- Status: CONFIRMED (4 targeted specs and 2 fuzz seeds)
- Blast radius:
  - Direct impact: the client keeps rows it lost, or never gets rows it gained.
  - Downstream impact: `can()` predictions use the wrong rows.
  - Loud or silent: silent.
  - Reversibility and recovery: it lasts until the affected rows are written again.
  - Scope affected: every schema with chains, `within` entries or several parents.
- Suggested fix direction: log a change of parents or `within` state of every anchor-entity row, whether or not rows sit below it. Or walk past an overridden row using that row's state as of the cursor.

#### Scenarios

**S3.1: A memo stays after its old dossier turns private**

- Given `memo-x` is below `m-x`, in open dossier `d-x`, and cora holds it
- When `m-x` moves to `d-priv`, then `d-x` turns private, then cora pulls
- Then cora does not hold `memo-x`
- Test: `engine/server/access_withinmove_test.go::removes a memo whose matter moved out of an open dossier that then turned private` | Result: FAIL

**S3.2: The same, with a trusted delete instead of a move**

- Test: `…::removes a memo whose matter was deleted from an open dossier that then turned private` | Result: FAIL

**S3.3: A memo never arrives after its old dossier turns open**

- Given `memo-y` is below `m-y`, in private dossier `d-y`
- When `m-y` moves to `d-open`, and then `d-y` turns open
- Then cora receives `memo-y`
- Test: `…::sends a memo whose matter moved out of a private dossier that then turned open` | Result: FAIL

**S3.4: Folder tree**

- Test: `…::removes a doc whose folder moved out of a shared folder that then moved too` | Result: FAIL

**S3.5: Fuzzers**

- Test: `engine/server/access_converge_test.go::converges every replica on what a fresh pull shows (seed 8)` (within schema) and `(seed 13)` (graph of parents) | Result: FAIL. All other seeds pass.

### BLK-4: A pulling client whose last grant goes keeps every row and its old summary

- Severity: BLOCKER. A decision is needed: "keep the local copy on NO_ACCESS" was designed for the "waiting for a first grant" case.
- Likelihood: High. Removing a user from their only project is common.
- Why it's easy to miss:
  - A partial revocation sends tombstones correctly.
  - A full revocation is refused by the workspace gate (`workspace_no_access`) before the resync runs.
  - An open stream sometimes drains first and tombstones everything (`emit.go:487`), so the result depends on timing.
- Location: the gate in `engine/server/workspaceaccess.go`, and `client/src/sync/coordinator.ts`, where `markNoAccess` (about line 1212) and the NO_ACCESS handling (about line 2493) keep the local data.
- Evidence: `access` becomes `'none'`, but `rita.get(TaskSchema, 't1')` still returns the row. The control, where rita keeps one other grant, removes t1.
- Status: CONFIRMED (Go and TS integration)
- Blast radius:
  - Direct impact: a removed member keeps all data on the device with no time limit, and `can()` answers from the old summary.
  - Downstream impact: losing all access leaks more than losing some.
  - Loud or silent: silent.
  - Reversibility and recovery: only an app-level purge.
  - Scope affected: every member removal from a last project or the workspace.
- Suggested fix direction: give one behaviour to Pull and Subscribe. For a cursor holder that had grants, send the revocation resync (tombstones and an empty summary) together with NO_ACCESS. Or let the client drop confirmed rows and keep the pending queue.

#### Scenarios

**S4.1: Pull path, server**

- Given rita reads p1 only, through one grant
- When the grant is deleted and rita pulls from her cursor
- Then the pull removes t1
- Test: `engine/server/access_lastgrant_test.go::tombstones the rows a pulling client holds when its last grant is revoked, as a stream does` | Result: FAIL

**S4.2: Stream path (pins the behaviour that is correct today)**

- Test: `…::tombstones the rows an open stream sent before it drops the scope with NO_ACCESS` | Result: PASS

**S4.3: End to end**

- Test: `client/test/integration/access-prediction.test.ts::drops the rows below an anchor the caller loses when it was its last grant` | Result: FAIL. The control `…while it keeps another grant` passes.

### BLK-5: A schema that still sets a removed write policy registers, and the policy is dropped silently

- Severity: BLOCKER. The break in the public proto is intentional, but here it is silent and makes access wider.
- Likelihood: Medium. Runtime registration with a descriptor set built against the old `options.proto` hits it: an older TS builder, `examples/register`, or vendored descriptors. Compiled Go schemas fail loudly at `buf` time, so they are safe.
- Why it's easy to miss: the field numbers are `reserved`, so the bytes decode as unknown fields. `refuseUnknownOptionFields` runs only on the access sub-messages, never on `EntityOptions` or `FieldBehavior`.
- Location: `engine/server/access.go:1571` (`refuseUnknownOptionFields`, called only at `access.go:630/742/881/900/968/1300/1336/1485` and `server.go:1653`); `proto/contract/syncplane/v1/options.proto:70-71`.
- Evidence: an entity whose options carry field 4 = 2 (`DELETE_POLICY_TRUSTED_ONLY`) registers. `Expected an error, got nil`.
- Status: CONFIRMED. The lead added the test.
- Blast radius:
  - Direct impact: an entity that was trusted-only for deletes, creates or updates, or a field that was trusted-only, becomes writable by every admitted caller. In a schema with roles it logs only a warning that the entity is outside the model.
  - Downstream impact: server-owned data.
  - Loud or silent: silent.
  - Reversibility and recovery: once the schema is fixed, writes made in between stay.
  - Scope affected: apps that register schemas at runtime across the upgrade.
- Suggested fix direction: refuse reserved field numbers 4, 7 and 8 of `EntityOptions` and 8 of `FieldBehavior` with an error that says how to migrate. See FC-1 for the general case.

#### Scenarios

**S5.1: A stale `delete_policy` is refused**

- Given widget is a plain entity whose `EntityOptions` still carries `delete_policy = TRUSTED_ONLY` as raw bytes
- When the descriptor set registers
- Then registration fails and names `"widget"`
- Test: `engine/server/access_forwardcompat_test.go::refuses an entity that still sets the removed delete_policy` | Result: FAIL

### MAJ-1: In-process `View`/`Apply` queries filter and order by hidden fields

- Severity: MAJOR
- Likelihood: Medium
- Why it's easy to miss: the rows come back with the hidden fields cleared. Only which rows match, and their order, leaks.
- Location: `engine/server/view.go:198` (`runQuerySpec`) never calls `refuseHiddenCond`/`refuseHiddenPaths`, and the rel pre-pass gets `emitter=nil`. `apply.go:435` shares this path. The RPC paths are correct.
- Evidence: `guest saw [s1]` for `salary > 4999`; `member saw [s2 s1] in salary order`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: hidden values found through matching and ordering.
  - Downstream impact: any app handler that passes user filters into `ViewTx.Query` or `ApplyTx.Query` under `WithPrincipal`.
  - Loud or silent: silent.
  - Reversibility and recovery: none; the disclosure cannot be undone.
  - Scope affected: every caller-scoped in-process query.
- Suggested fix direction: run the same refusal in `runQuerySpec` when a caller is bound, rel targets included.

#### Scenarios

**S1.1–S1.3**

- Tests in `engine/server/access_fieldread_test.go`:
  - `refuses in a View a query whose predicate names a field the caller does not read on every row`
  - `refuses in a View a query ordered by a field the caller does not read on every row`
  - `refuses in an Apply a query whose predicate names a field the caller does not read on every row`
- Result: FAIL ×3

### MAJ-2: A grant change that hides a subscribed field stops sync and leaves the value on the client

- Severity: MAJOR
- Likelihood: Medium
- Why it's easy to miss: the docs say that such a subscription "fails on the next pull", but every later pull fails as well, before any resync can run.
- Location: `engine/server/emit.go:493` (`bindAccess` → `refuseHiddenSubs`) runs before `compareAccess`; `fieldread.go:299`.
- Evidence: the client still shows `salary 5000` and reports `sync failure: ConnectError: [permission_denied] the subscription to "staff" names field "salary"…`.
- Status: CONFIRMED (integration, real testserver)
- Blast radius:
  - Direct impact: the revoked values stay on the client.
  - Downstream impact: the workspace sync stays failed until the app changes its subscriptions.
  - Loud or silent: silent for users.
  - Reversibility and recovery: only by app code.
  - Scope affected: every subscription over a guarded field whose readers can lose their role.
- Suggested fix direction: when an access change makes an existing cursor's subscription refusable, answer with a reset, or with a resync that drops the scope. Keep the refusal for new subscriptions.

#### Scenarios

**S2.1: A downgrade wedges a subscription**

- Given admin holds s1 (salary 5000) through a subscription `salary > 0`
- When admin's role changes to `org_member` and admin syncs
- Then the value is gone, and sync does not stay failed
- Test: `client/test/integration/hidden-fields.test.ts::does not leave a field on the client after a grant change hides it from a subscription that names it` | Result: FAIL

### MAJ-3: Unknown operator values in `when`/`within` register, and `not(...)` admits every row

- Severity: MAJOR
- Likelihood: Medium, in rolling deploys.
- Why it's easy to miss: `refuseUnknownCondFields` checks unknown fields, and an unknown enum value is not an unknown field.
- Location:
  - `engine/server/access.go:999-1019`
  - `engine/query/compile.go:191,222,283` checks only `*_UNSPECIFIED`.
  - `cmpNode.eval` returns false for an unknown op (`compile.go:961-980`), and `presentNode` treats one as IS_NOT_SET (`:1217`).
- Status: CONFIRMED
- Blast radius:
  - Direct impact: an older node widens reads and writes.
  - Downstream impact: the TS evaluator is likely to need the same fix.
  - Loud or silent: silent.
  - Reversibility and recovery: a fix needs a re-registration on every node.
  - Scope affected: every rolling deploy that adds an operator.
- Suggested fix direction: refuse CmpOp, StrOp and PresentOp values outside the known set in access conditions, as is done for RepOp.

#### Scenarios

- Tests in `engine/server/access_forwardcompat_test.go`:
  - `refuses a when condition with an operator this server does not know` (cmp, str and present entries)
  - `refuses a within condition with an operator this server does not know`
- Result: FAIL ×4

### MAJ-4: Manager restart after a partial registration loses the access model

- Severity: MAJOR
- Likelihood: Low–Medium
- Location: `engine/server/manager.go:412-423` stores only the incoming set; `register.go:174-177` keeps the other entities live; `manager.go:177` re-registers the stored set at boot as a fresh schema, with no `AllowAccessChange` check.
- Evidence: before the restart an anonymous pull gets PermissionDenied; after it, the pull succeeds.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: the gate admits every caller that `CanAccessWorkspace` lets in, and the org entities disappear.
  - Downstream impact: every hosted app that registered a partial set.
  - Loud or silent: silent, at restart.
  - Reversibility and recovery: register the full set again.
  - Scope affected: hosted apps that use partial registrations.
- Suggested fix direction: store the merged schema, or refuse a set that leaves out registered entities.

#### Scenarios

- Test: `engine/server/access_restart_test.go::keeps refusing a caller without a grant after a restart that follows a registration of some entities` | Result: FAIL

### MAJ-5: `RegisterEntity` returns nil for a model it cannot repair, then every request fails

- Severity: MAJOR (outage)
- Likelihood: Low
- Location: `engine/server/server.go:1348-1366`; `settleGate` returns nil when `accessErr` is set (`access.go:2502-2504`).
- Evidence: the call returns nil, and pulls then give `failed_precondition`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: the whole server stops serving.
  - Downstream impact: every client of the server.
  - Loud or silent: loud, but the call that caused it reports success.
  - Reversibility and recovery: only a descriptor-set registration recovers.
  - Scope affected: servers that use runtime single-entity registration.
- Suggested fix direction: refuse any model error that is not caused by an entity that is still missing.

#### Scenarios

- Tests in `engine/server/access_runtimeregister_test.go`:
  - `refuses an entity that declares a role another entity declares, and keeps serving`
  - `refuses an entity without access under WithStrictAccess, and keeps serving`
- Result: FAIL ×2

### MAJ-6: DELETE of a missing key skips every rule and sends a tombstone to all

- Severity: MAJOR
- Likelihood: High that it happens (harmless double deletes). Medium for abuse.
- Location: `engine/server/accesswrite.go:591-594` returns early when `!live`; `push.go:1279`.
- Evidence:
  - guest, who has no write rule anywhere, gets `APPLIED`.
  - writer's next pull gets `CHANGE task t-none true 3`.
  - A delete of hidden t1 gives `access_denied`, and a delete of missing t-none gives `APPLIED`. A PATCH of a missing row gives `conflict`, and of a hidden row `access_denied`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: any caller writes tombstones and uses up sequence numbers.
  - Downstream impact: a reliable existence oracle for hidden keys.
  - Loud or silent: silent.
  - Reversibility and recovery: none for the tombstones that went out.
  - Scope affected: every entity in the model.
- Suggested fix direction: check the operation's rules for an absent row, at the root at least, and answer a missing row exactly as a hidden one. Clients that resend deletes of rows already gone will then get rejections, so decide how to handle them.

#### Scenarios

- Tests in `engine/server/access_missingrow_test.go`:
  - `answers a delete of a missing row exactly as a delete of a hidden row`
  - `answers a patch of a missing row exactly as a patch of a hidden row`
  - `does not let a caller that may delete no task write a tombstone`
- Result: FAIL ×3

### MAJ-7: Moving an ancestor puts rows below it deeper than `MaxAccessDepth`

- Severity: MAJOR
- Likelihood: Low with the default 32; higher for outline trees or a lowered limit.
- Location: `engine/server/accesswrite.go:456-543` measures only the height above the written row.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: subtrees disappear for non-root readers.
  - Downstream impact: writes below the subtree are refused, and so is moving it back.
  - Loud or silent: silent.
  - Reversibility and recovery: move the ancestor back.
  - Scope affected: recursive schemas with deep trees.
- Suggested fix direction: on a recursive move, also bound the height of the subtree below the moved row, for Trusted writes too.

#### Scenarios

- Tests in `engine/server/access_subtreedepth_test.go`:
  - `refuses a move that would put a row below the moved row deeper than MaxAccessDepth`
  - `refuses a trusted move …`
- Result: FAIL ×2

### MAJ-8: Rewriting a `caller_is` field to yourself shows you the field it hides

- Severity: MAJOR
- Likelihood: High wherever a guard read rule uses `caller_is`.
- Why it's easy to miss: registration forbids guarding a field that a guard rule checks (`access.go:812-826`), so that field is always writable by anyone with the entity's update rule.
- Location: no check in `accesswrite.go:669-772` on a write that changes the input of a hiding guard.
- Evidence: padmin patches `person_id` to `"padmin"` and then reads the review `"needs coaching"`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: any holder of `edit_tasks` reads every review.
  - Downstream impact: the documented fixture pattern itself leaks.
  - Loud or silent: silent.
  - Reversibility and recovery: none; the disclosure cannot be undone.
  - Scope affected: schemas whose guard read rules use `caller_is`.
- Suggested fix direction: refuse such a write unless the caller reads the hidden units on both sides, or flag the pattern at registration unless a `fields` list protects the field.

#### Scenarios

- Test: `engine/server/access_callerisfield_test.go::keeps a review hidden from a caller that points the record at itself` | Result: FAIL

### MAJ-9: Paged Bootstrap plus an A→B→A move ships a row that no later pull removes

- Severity: MAJOR
- Likelihood: Low–Medium. Large workspaces bootstrap for minutes.
- Location: `engine/server/bootstrap.go:161` (`movesThrough` read at Begin) and `:258` (one transaction per page); `accesssync.go:337` compares only the first `from` with the state now.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: a stale row stays on the client.
  - Downstream impact: none found.
  - Loud or silent: silent.
  - Reversibility and recovery: it lasts until the row is written.
  - Scope affected: clients that bootstrap while rows move.
- Suggested fix direction: pin the pages to the move position of Begin, or record on the cursor that the pages read newer chains.

#### Scenarios

- Test: `engine/server/access_bootstrapmove_test.go::removes a comment the snapshot shipped while its task sat for a moment in a project the caller reads` | Result: FAIL

### MAJ-10: The ReadAccess record expires while its cursor is in use, so a grant change resets instead of resyncing

- Severity: MAJOR
- Likelihood: High. Grants are usually stable for weeks.
- Location: `engine/server/accesssync.go:127-136` writes the record once and never refreshes it; `sqlitestore/cursorstate.go:81-86` and pgstore prune by `written_at`; the memory store evicts by LRU, but no Get marks the record as used.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: one org-wide role change makes every long-lived client re-bootstrap.
  - Downstream impact: load on the server during the storm.
  - Loud or silent: loud as load, silent for users.
  - Reversibility and recovery: clients converge after the re-bootstrap.
  - Scope affected: every client whose access did not change for 30 days.
- Suggested fix direction: refresh the record while it is in use, for example when its age passes some fraction of the retention.

#### Scenarios

- Tests in `engine/server/access_staterecord_test.go`:
  - `resyncs a client that pulled every day for longer than the cursor-state retention when its access changes`
  - `resyncs a client whose access record other clients' entries would push out of a bounded store`
- Result: FAIL ×2

### MAJ-11: A replica read below `movesThrough` skips a move

- Severity: MAJOR
- Likelihood: Low–Medium (Postgres with a lagging replica)
- Location: `engine/server/pull.go:433` and `subscribe.go:454` floor the read at `max(seq, min_seq)`, which can be below `movesThrough` (`accesssync.go:327`).
- Status: CONFIRMED (SQLite primary with a `VACUUM INTO` snapshot as the lagging replica)
- Blast radius:
  - Direct impact: stale rows on the client.
  - Downstream impact: access flaps between replica and primary reads.
  - Loud or silent: silent.
  - Reversibility and recovery: it lasts until the rows are written.
  - Scope affected: deployments with read replicas.
- Suggested fix direction: floor at `max(seq, movesThrough, access record position)`.

#### Scenarios

- Test: `engine/server/access_replica_test.go::removes a comment a replica sent from before its task moved out of the caller's project` | Result: FAIL

### MAJ-12: Trusted writes during an incomplete model skip the move log and the cycle check

- Severity: MAJOR
- Likelihood: Low–Medium
- Location: `engine/server/accessmoves.go:111-114` and `accesswrite.go:465-468` return nil when the live model is nil; `trusted.go:91`; `access.go:2503`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: stale rows on clients, and a stored cycle that survives the registration.
  - Downstream impact: the "data never holds a cycle" invariant no longer holds.
  - Loud or silent: silent.
  - Reversibility and recovery: the cycle must be found and repaired by hand.
  - Scope affected: workspaces that a trusted writer touches during the registration window.
- Suggested fix direction: do the bookkeeping against the last valid model, or refuse trusted writes while `accessErr` is set.

#### Scenarios

- Tests in `engine/server/trusted_access_test.go`:
  - `tombstones the comments below a task that a trusted write moved out of the reader's project`
  - `refuses a trusted write that would put a folder below itself`
- Result: FAIL ×2

### MAJ-13: An app entity named `syncplane_access_moves` shares the move log

- Severity: MAJOR (impact), Low likelihood
- Location: `engine/server/accessmoves.go:49`; `feed.go:160`; `internal/naming/naming.go:92` reserves no `syncplane_` prefix. The history entity has its own guard (`history.go:364`).
- Status: CONFIRMED (two agents found it on their own)
- Blast radius:
  - Direct impact: move records (parents, owner subjects) reach clients.
  - Downstream impact: the app's own rows are read as moves and are purged by GC.
  - Loud or silent: silent.
  - Reversibility and recovery: rename the entity; purged rows are lost.
  - Scope affected: apps that choose that name.
- Suggested fix direction: reserve the engine's entity names at registration, in Go and in the TS builder.

#### Scenarios

- Tests:
  - `engine/server/access_enginenames_test.go::keeps the access move log out of an entity the application names syncplane_access_moves`
  - `engine/server/access_movelogname_test.go::refuses an entity that takes the name of the engine's access move log`
- Result: FAIL ×2

### MAJ-14: A room-anchor drop message tells whether a key exists

- Severity: MAJOR
- Likelihood: High. Any admitted caller can probe with `Watch`.
- Location: `engine/server/roomgate.go:99` ("does not exist") and `:125` ("is not readable"); `rooms.go:339` sends the message to the client. The wording predates this branch, but the access model makes hidden rows a declared guarantee.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: an existence oracle for any key, grant rows included (`org_member/om-admin`).
  - Downstream impact: none found.
  - Loud or silent: silent; probes are unlimited.
  - Reversibility and recovery: none; what was learned stays learned.
  - Scope affected: every workspace with rooms.
- Suggested fix direction: send one message for missing, deleted and unreadable anchors, and keep the detail in the server log.

#### Scenarios

- Test: `engine/server/access_read_test.go::drops a room anchored on a row the caller cannot read as it drops one anchored on no row` | Result: FAIL

### MAJ-15: `can()` says yes to grant writes that the subset rule refuses

- Severity: MAJOR
- Likelihood: High: "Remove member" or "Change role" by a project admin who is not an owner.
- Location: `client/src/client.ts:3590` (`canQuestion`) and `:3654` (`canCreateQuestion`) never look at grant roles. `checkGrantSubset` exists only on the server.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: the UI offers controls that always fail, with an optimistic apply and then a rollback.
  - Downstream impact: none found.
  - Loud or silent: loud (rejections).
  - Reversibility and recovery: the rollback reverses it.
  - Scope affected: every grant entity.
- Suggested fix direction: resolve role permissions in the client registry, answer `'no'` when the caller does not hold every permission of the role at the anchor, and answer `'unknown'` for an update whose mask reaches the role, subject or parent.

#### Scenarios

- Tests in `client/test/integration/access-prediction.test.ts`:
  - `does not answer yes for the delete of a grant whose role holds more than the caller holds`
  - `…create…`
  - `…update of the role, which the new role decides`
- Result: FAIL ×3

### MAJ-16: `can('update')` ignores the rule that a caller must read the rows below

- Severity: MAJOR
- Likelihood: Medium
- Location: `client/src/client.ts:3590-3650`; the server check is `readersBelow` in `accessWrite.finish`.
- Status: CONFIRMED
- Blast radius: the same as MAJ-15.
- Suggested fix direction: for an update that changes a parent, or a field that a `within` entry below reads, decide the read rules of each entity in `readersBelow` at the stored row.

#### Scenarios

- Tests:
  - `client/test/integration/access-prediction.test.ts::does not answer yes for a change of a field that a within entry below reads, by a caller who does not read the rows below`
  - `client/test/unit/can.test.ts::does not answer yes for a caller who does not read the rows below the row where it is`
- Result: FAIL ×2

### MAJ-17: `useCan` does not re-evaluate when a field that a `when` or guard reads changes

- Severity: MAJOR
- Likelihood: High: archiving or closing a row is the main use of `when`.
- Location: `client/src/access.ts:911` (`accessInputsOf` leaves out rule `when` fields, guard `callerIs`/`when`, and own-entity `within` `when`), used at `react/hooks/use-can.ts:310`.
- Status: CONFIRMED
- Blast radius:
  - Direct impact: a stale `'yes'` until some unrelated access change.
  - Downstream impact: writes made from the stale control are rejected.
  - Loud or silent: silent until a write fails.
  - Reversibility and recovery: the next unrelated access change refreshes it.
  - Scope affected: every `useCan` on a row with a `when` rule or a guard.
- Suggested fix direction: put every field that a rule or guard condition reads into the inputs key, and fix the `useCan` doc.

#### Scenarios

- Tests in `client/test/react/use-can.test.ts` ("useCan on a row whose fields a rule or a guard checks"):
  - `answers again when a field that a rule's when condition reads changes`
  - `…a field guard's condition reads changes`
  - `…the subject field that a field guard's rule reads changes`
- Result: FAIL ×3

### MAJ-18: Room Publish ignores the issuer of the session binding (on `main` too)

- Severity: MAJOR
- Likelihood: Low (multi-issuer deployments)
- Location: `engine/server/roomgate.go:286-316` keys on `principal.Subject` only; `rooms_publish.go:56,130` skips `openRoom` when the session is joined.
- Status: CONFIRMED. This code predates the branch, and the branch's issuer guidance does not reach it.
- Blast radius:
  - Direct impact: a caller with no grant publishes messages, presence and leave events under another issuer's session.
  - Downstream impact: other room members see forged events.
  - Loud or silent: silent.
  - Reversibility and recovery: it lasts while the victim's stream is joined.
  - Scope affected: multi-issuer deployments that use rooms.
- Suggested fix direction: key session bindings on issuer plus subject, as `watermarkKey` does.

#### Scenarios

- Test: `engine/server/access_roomgate_test.go::refuses a Publish into a room that only a caller of another issuer with the same subject joined` | Result: FAIL

### MIN-1: The move check drops `caller_is` (`loose()`)

- Severity: MINOR
- Likelihood: Medium
- Location: `engine/server/access.go:413-438`, used at `accesswrite.go:743-765`.
- Status: CONFIRMED
- Blast radius: a caller who reads only its own pages can move other people's pages to a new audience.
- Suggested fix direction: do not count a rule with row checks as "reads the entity".

#### Scenarios

- Test: `engine/server/access_callerisfield_test.go::refuses a move by a caller who reads only its own rows of an entity below the moved row` | Result: FAIL

### MIN-2: Lowering `MaxAccessDepth` neither resets nor resyncs clients

- Severity: MINOR
- Likelihood: Low
- Location: `engine/server/access.go:1991,2321` (the cursor fingerprint leaves out the limit); `accesssync.go:268-292`.
- Status: CONFIRMED
- Blast radius: clients keep rows they can no longer read.
- Suggested fix direction: put the limit into the read-policy fingerprint.

#### Scenarios

- Test: `engine/server/access_deep_test.go::takes a row past a lowered MaxAccessDepth from a client that synced it under the old limit` | Result: FAIL

### MIN-3: Dropping a `creator` mark is not an access change

- Severity: MINOR
- Likelihood: Low
- Location: `engine/server/access.go:2414` (`accessSpecOf`).
- Status: CONFIRMED
- Blast radius: authorship can be forged after such a registration.
- Suggested fix direction: include creator fields that `caller_is` reads in `accessSpecOf`.

#### Scenarios

- Test: `engine/server/access_forwardcompat_test.go::refuses a registration that drops the creator mark from a field a rule checks` | Result: FAIL

### MIN-4: Registration errors for guard rules name the wrong rule

- Severity: MINOR
- Likelihood: High
- Location: `engine/server/access.go:779-781`, which gives "access guard[0]: access allow[1]"; `resolveWithins` (`access.go:141-147,2019`) gives no rule index.
- Status: CONFIRMED
- Suggested fix direction: use one path format, `access guard[i] allow[j]`.

#### Scenarios

- Test: `engine/server/access_errors_test.go::names the guard and its rule` (3 entries) | Result: FAIL ×3

### MIN-5: TS builder: `within` `callerIs` has no type check

- Severity: MINOR
- Likelihood: Medium
- Location: `client/src/schema/builder.ts:940` (`Extract<keyof F,string>` and not `TextKeys<F>`), and `:977` (`WithinDecl` is not tied to the target's fields).
- Status: CONFIRMED (tsc TS2578 at `schemawithin.test.ts:37,68`)

#### Scenarios

- Tests in `client/test/unit/schemawithin.test.ts`:
  - `refuses a within() callerIs that is not a text() field, at compile time`
  - `refuses a misspelled callerIs in a plain within entry at compile time`
- Result: tsc FAIL, runtime PASS

### MIN-6: TS builder accepts a guard hiding a field that a self `within` entry reads

- Severity: MINOR
- Likelihood: Low
- Location: `builder.ts` `assertGuards` (around lines 1676-1690); the Go check is `access.go:2045-2050`.
- Status: CONFIRMED

#### Scenarios

- Test: `client/test/unit/schemawithin.test.ts::a guard that can hide a field a self within entry reads > is refused before registration` | Result: FAIL

### MIN-7: Test kit fake re-sends `CallerAccess` on every pull

- Severity: MINOR
- Likelihood: High
- Location: `client/src/testing/server.ts:1093`; `testing/access.ts` (`callerAccessOf` sets no version).
- Status: CONFIRMED
- Blast radius: app tests fire `onAccessChange` on every pull, which hides MAJ-17-type bugs, and they never use the delta path.

#### Scenarios

- Tests:
  - `client/test/unit/testing-access.test.ts::sends the summary on a pull only when it differs from the one the client holds, as the real server does` | Result: FAIL
  - The real server is pinned by `access-prediction.test.ts::is left out of a pull whose cursor already carries it, so no access listener runs` | Result: PASS

### MIN-8: A queued custom mutation that writes a grant gives `'no'`, not `'unknown'`

- Severity: MINOR
- Likelihood: Medium
- Location: `client/src/access.ts:1121` (`changesAccess` goes to `default: false` for `custom`).
- Status: CONFIRMED

#### Scenarios

- Test: `client/test/unit/can.test.ts::answers unknown while a custom mutation whose mirror writes a grant row waits for its ack` | Result: FAIL

### MIN-9: Compaction folds away a PATCH that clears a write-only hidden field

- Severity: MINOR
- Likelihood: Low
- Location: `client/src/optimistic/compact.ts:201-209`.
- Status: CONFIRMED
- Blast radius: the clear is lost, so the pin stays `'1234'`.

#### Scenarios

- Test: `client/test/unit/hidden-compact.test.ts::reaches the server when a put of the row under the same expected version follows it` | Result: FAIL

### MIN-10: A hook error that wraps a workspace denial is reported as a denial

- Severity: MINOR
- Likelihood: Medium
- Location: `engine/server/accessresolve.go:66` (the doc says "never read as a denial") and `:142` (`%w`); `workspaceaccess.go:229`.
- Status: CONFIRMED
- Blast radius: Subscribe drops the scope as ACCESS_REVOKED, and the client discards its local data.

#### Scenarios

- Test: `engine/server/access_callerhook_test.go::fails a request with code internal when the hook returns a workspace denial` | Result: FAIL

### MIN-11: No warning for mutators that become trusted-only under the model

- Severity: MINOR
- Likelihood: High for gradual adoption.
- Location: `engine/server/access.go:1874-1888`; `accesswrite.go:1172-1176`.
- Status: CONFIRMED

#### Scenarios

- Test: `engine/server/mutator_access_test.go::warns at registration about a mutator that no client can invoke because it declares no MutatorAccess` | Result: FAIL

### MIN-12: Documentation errors

- `docs/how-to/declare-roles-and-permissions.md:287` says that registration refuses a role that grants an unused permission. The code warns, as `access_test.go` and the same document's section 6 confirm. Status: CONFIRMED by an existing test.
- `docs/how-to/declare-roles-and-permissions.md:162-164` and `options.proto:516` evolve `ProjectMember` by reusing field number 4 for `Team team`. With a compiled `New`, stored role bytes then decode as a Team, and every project grant grants nothing. Status: UNVERIFIED.

### FC-1 to FC-5 (forward compatibility, UNVERIFIED)

- FC-1: `EntityOptions` and `FieldBehavior` unknown fields are never refused. This release itself added access-relevant fields at those levels (`access`/`grant`/`members` 10-12, `creator` 12), so the next such field is ignored silently by older nodes. BLK-5 is the case that exists today. Fix direction: refuse unknown fields there for entities in the model, or put future access marks into sub-messages that are checked.
- FC-2: `EntityReadRule.rule` (`access.go:2297`) is an ad-hoc string that is persisted in cursor-state records. Any change to its format resets every client, and old and new nodes then reset the same clients in turn. Fix direction: version it or encode it as a structure.
- FC-3: `readAccessHash` (`accesssync.go:70`) hashes deterministic proto marshal output, which protobuf-go does not promise to keep stable across versions.
- FC-4: `TopicDropped.Reason` has no `NO_ACCESS` value (`roomgate.go:37-44`). Add it before clients start to branch on the reason.
- FC-5: `client/src/access.ts:130` fixes `MAX_ACCESS_CHAIN = 32`, and no wire field carries the server limit.
- Checked and fine: `CallerAccess` field 1 and `FieldBehavior` field 11 were removed on this branch without `reserved`. They never shipped on `main` or in `v0.1.0`, so reserving them is optional hygiene. Add the reservations only if intermediate builds were deployed.

## Test execution results

| Scenario     | Test location                                                                       | Result             | Notes                        |
| ------------ | ----------------------------------------------------------------------------------- | ------------------ | ---------------------------- |
| S1.1–S1.3    | `engine/server/access_movehidden_test.go`                                           | FAIL ×3            | Lead re-ran                  |
| S2.1–S2.3    | `engine/server/access_adopt_test.go`                                                | FAIL ×3            | Lead re-ran                  |
| S3.1–S3.4    | `engine/server/access_withinmove_test.go`                                           | FAIL ×4            |                              |
| S3.5         | `engine/server/access_converge_test.go`                                             | FAIL ×2 of 160     | Other seeds pass; about 25 s |
| S4.1 / S4.2  | `engine/server/access_lastgrant_test.go`                                            | FAIL / PASS        |                              |
| S4.3         | `client/test/integration/access-prediction.test.ts`                                 | FAIL               | Control passes               |
| S5.1         | `engine/server/access_forwardcompat_test.go`                                        | FAIL               | Added by lead                |
| MAJ-1        | `engine/server/access_fieldread_test.go`                                            | FAIL ×3            |                              |
| MAJ-2        | `client/test/integration/hidden-fields.test.ts`                                     | FAIL               |                              |
| MAJ-3, MIN-3 | `engine/server/access_forwardcompat_test.go`                                        | FAIL ×5            |                              |
| MAJ-4        | `engine/server/access_restart_test.go`                                              | FAIL               |                              |
| MAJ-5        | `engine/server/access_runtimeregister_test.go`                                      | FAIL ×2            |                              |
| MAJ-6        | `engine/server/access_missingrow_test.go`                                           | FAIL ×3            |                              |
| MAJ-7        | `engine/server/access_subtreedepth_test.go`                                         | FAIL ×2            |                              |
| MAJ-8, MIN-1 | `engine/server/access_callerisfield_test.go`                                        | FAIL ×2            |                              |
| MAJ-9        | `engine/server/access_bootstrapmove_test.go`                                        | FAIL               |                              |
| MAJ-10       | `engine/server/access_staterecord_test.go`                                          | FAIL ×2            |                              |
| MAJ-11       | `engine/server/access_replica_test.go`                                              | FAIL               |                              |
| MAJ-12       | `engine/server/trusted_access_test.go`                                              | FAIL ×2            |                              |
| MAJ-13       | `access_enginenames_test.go`, `access_movelogname_test.go`                          | FAIL ×2            |                              |
| MAJ-14       | `engine/server/access_read_test.go`                                                 | FAIL               |                              |
| MAJ-15/16    | `client/test/integration/access-prediction.test.ts`, `client/test/unit/can.test.ts` | FAIL ×5            |                              |
| MAJ-17       | `client/test/react/use-can.test.ts`                                                 | FAIL ×3            |                              |
| MAJ-18       | `engine/server/access_roomgate_test.go`                                             | FAIL               | A Watch gap test passes      |
| MIN-2        | `engine/server/access_deep_test.go`                                                 | FAIL               |                              |
| MIN-4        | `engine/server/access_errors_test.go`                                               | FAIL ×3            |                              |
| MIN-5/6      | `client/test/unit/schemawithin.test.ts`                                             | tsc ×2, FAIL ×1    | One compile-time test passes |
| MIN-7        | `client/test/unit/testing-access.test.ts`                                           | FAIL               |                              |
| MIN-8        | `client/test/unit/can.test.ts`                                                      | FAIL               |                              |
| MIN-9        | `client/test/unit/hidden-compact.test.ts`                                           | FAIL               |                              |
| MIN-10       | `engine/server/access_callerhook_test.go`                                           | FAIL               | 2 gap tests pass             |
| MIN-11       | `engine/server/mutator_access_test.go`                                              | FAIL               | 1 gap test passes            |
| Totals       | Go `engine/server`                                                                  | 48 fail, 2076 pass | Every failure is new         |
| Totals       | TS unit, react and integration                                                      | 14 fail, 3071 pass | Every failure is new         |

## Files created

Each test is named after the correct behaviour, so it passes once its finding is fixed and can then be committed as a regression test.

New Go test files (`engine/server/`):

- `access_adopt_test.go`
- `access_bootstrapmove_test.go` (defines `hookedReadStore`)
- `access_callerhook_test.go`
- `access_callerisfield_test.go` (defines `pulledRowOf`)
- `access_converge_test.go` (model-based fuzzers; about 25 s)
- `access_enginenames_test.go`
- `access_errors_test.go`
- `access_forwardcompat_test.go`
- `access_lastgrant_test.go`
- `access_missingrow_test.go`
- `access_movehidden_test.go`
- `access_movelogname_test.go`
- `access_oneofguard_test.go` (all pass; covers guards over a oneof)
- `access_replica_test.go` (defines `laggingReplicaStore`)
- `access_restart_test.go`
- `access_revokerace_test.go` (passes; covers a coverage gap)
- `access_roomgate_test.go`
- `access_runtimeregister_test.go`
- `access_staterecord_test.go`
- `access_subtreedepth_test.go`
- `access_withinmove_test.go`
- `mutator_access_test.go`
- `trusted_access_test.go`

Edited Go test files:

- `engine/server/access_deep_test.go`
- `engine/server/access_fieldread_test.go`
- `engine/server/access_read_test.go`
- `internal/protoopts/protoopts_test.go` (passing test: unknown fields survive option recovery)

New TS test files:

- `client/test/integration/access-prediction.test.ts`
- `client/test/unit/hidden-compact.test.ts`
- `client/test/unit/schemawithin.test.ts`

Edited TS test files:

- `client/test/unit/can.test.ts`
- `client/test/unit/hidden-fields.test.ts` (adds a passing watcher-path test)
- `client/test/unit/testing-access.test.ts`
- `client/test/react/use-can.test.ts`
- `client/test/integration/hidden-fields.test.ts`

Report: `test-report-2026-10-11.md`.

Commit dependencies between files:

- `access_revokerace_test.go` uses `hookedReadStore` from `access_bootstrapmove_test.go`.
- `access_movehidden_test.go` uses `pulledRowOf` from `access_callerisfield_test.go`.

Commit each pair together, or move the helpers into a shared test helper file first. No production code was changed.

## Coverage gaps and follow-ups

- Multi-tab delta races in real browsers (Playwright e2e was not run).
- A Subscribe fuzzer on the `within` schema, team and field-guard worlds in the fuzzer, and Bootstrap resume combined with moves.
- The client evaluator compared with the server evaluator for `when` edge values: `testdata/queryeval` has no enum cases, and `rules.json` abstracts `when` to booleans.
- Smaller notes, all UNVERIFIED:
  - **Depth and cycle oracle.** `checkGrantRow` and `checkAccessChain` run before `finish` (`push.go:1080-1088`), so `invalid_argument` and `access_denied` answer differently for a hidden parent key.
  - **Hidden entity name in a delete refusal.** The refusal message for an anchor delete names the entity of a hidden child (`accesswrite.go:627-640`).
  - **Admitted plus `within` rules skip the per-parent share check.** `admitsThrough` returns true when `requires` is empty (`accesswrite.go:210`).
  - **Moving into your own subtree raises your role.** The spec allows it, but it gets around the intent of the subset rule.
  - **Mutator reads are unfiltered.** `Server.Mutate` reads through `EntityTx.Get`/`Query` without filters under a principal that is not trusted (`mutator.go:335,904`); document this or filter.
  - **`can('delete')` on an anchor row.** It could answer `'no'` when a live child row is held locally.
  - **Synthetic oneofs (`_x`).** They are not filtered in `ruleFieldsOf`, so a guard named through one may restrict nothing.
- Recommended order of work:
  1. BLK-1 and BLK-2 (write-path checks).
  2. BLK-5 and FC-1 (refuse reserved and unknown option fields).
  3. BLK-3 (log every anchor-row change).
  4. BLK-4 (one revocation behaviour for Pull and Subscribe).
  5. MAJ-6 and MAJ-8.
