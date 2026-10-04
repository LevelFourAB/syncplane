# The access model

This document explains how the engine decides what a caller can read and write from rows in the workspace, why the rules are declared in the schema instead of written as code, and what the model costs.

## Why access is declared

Application hooks such as `ReadFilter` decide one row at a time, and only from that row. A rule such as "a task is readable when its project grants it" depends on another row, so a hook must load the project grants before the read starts. That set can change while the read runs. When a parent changes, for example when a member is removed from a project, the visibility of every task below the project changes, but no task row changes. The engine cannot see that from code it cannot read, so the application must touch the rows or bump the filter epoch.

The access model puts the rule in the schema. The engine then knows which rows grant access, which reference connects a row to its parent, and which permission each entity needs. With this information it can find the rows whose visibility a grant change affects, read grants in the same snapshot as rows, and check a write against the parent row inside the write transaction.

## Grants, roles and anchors

A **grant row** gives one subject one role. A **role** is a named set of permissions, which are opaque strings. A grant applies at an **anchor**: the row that the grant row's `access_parent` reference points at, or the workspace root when the grant entity has no `access_parent` field.

A permission held at an anchor applies to the anchor row and to every row whose `access_parent` points at the anchor. With an org schema:

- An `OrgMember` grant has no parent, so its role applies at the root and reaches every row.
- A `ProjectMember` grant points at one project, so its role applies to that project and to the tasks and members below it.
- A `Task` points at its project, so a caller reads it when it holds `view_tasks` at the root or at that project.

What a caller holds is the union of every grant row that names one of its subjects, plus the global roles from `WithGlobalRoles`. Access only grows by adding grants. There is no deny rule: a rule that removes access would make access non-monotonic, and the engine could no longer compute changes from added and removed anchors. The application hooks run after the model and can only narrow what it allows.

A grant row grants nothing when its anchor row does not exist. Without this rule, a caller could create a grant for a project key before the project exists, and receive access when someone creates it later.

A grant row also grants nothing when the schema does not declare its role, and every write path refuses an undeclared role. Rows stored before the schema declared a role are the exception: when a registration adds the role, or adds `grant` to an entity that already holds rows, those rows start to grant it. Registration does not look for such rows, because that reads the grant entity in every workspace on each registration, and the rows were written under the rules the application had at that time. A runtime registration must pass `AllowAccessChange` for such a change, and that flag is where the operator accepts the stored values.

## Teams

A team is a row. The team entity's `members` option names a grant entity whose rows anchor at the team, and each such row makes its subject a member. A grant row whose subject field is a reference to the team gives its role to every member. The type of the subject field decides between a user and a team, so one roster entity with a oneof subject holds both, with one list of roles. Two entities would need two lists, because role names are unique across the schema.

**Membership is a row, not a permission.** A permission held at the root applies at every row, so if a permission made a caller a member, an admin would be a member of every team and would hold every team's grants. The caller's access set would then grow with every team grant in the workspace. A role that does not grant the permission would also make no member, and nothing would report it. With rows, a caller is a member of exactly the teams that its own rows name, and the engine finds them in the grant rows it already reads.

**A grant to a team is a delegation.** The subset rule still applies to each row: the writer of a team grant holds the role's permissions at the anchor, and the writer of a member row holds the member role's permissions at the team. But a grant to a team also gives its role to every caller who joins the team later, and a caller who can add members does not need to hold that role. This is what a team is for: the project admin decides that the team gets the role, and the team decides who is in it.

**A team cannot be a member of a team.** Grant resolution has two passes. The first reads the caller's own grant rows, member rows included, which gives the caller's teams. The second reads the grant rows that name those teams. A team inside a team would need a third pass, and nested teams in general need repeated passes with a depth limit. Registration therefore refuses a member entity whose subject is a team. Lifting the refusal later breaks no schema.

The caller's access set stays one list of permissions and anchors, whatever pass found them. Cursors, resyncs and the `CallerAccess` summary therefore work the same for team grants: a change of membership or of a team grant changes the anchors, and the next pull resyncs the rows below them.

A team row cannot be deleted while a live grant row names it, for the same reason as an access parent: a team created later with the same key would receive the grants.

## Rules

The `access` option of an entity lists rules. Each rule names operations (read, create, update, delete, or `write` for every write operation) and a source: a permission, or `admitted` for every caller that the workspace gate lets in. See [Access model](../reference/proto-options.md#access-model) for the options.

**Rules join with OR.** An operation is allowed when any rule admits it, and the order of the rules has no effect. Rules are monotonic for the same reason as grants: a new rule can only add access. Each rule therefore means the same thing whatever other rules the entity has, and a reviewer can read one rule and know what it allows. On the read side, the rows a caller reads are the union of the rows below the anchors of each read permission, which is the set that a grant change resyncs (see below).

**An update needs one rule for both sides.** An update can move a row to another access parent, and a rule admits the move only when its source holds where the row is and where it goes. If one rule could admit the old place and another rule the new place, two narrow rules together would allow moves that neither rule states. For example, a rule for triagers in an intake project and a rule for editors in their own projects would together let a caller with both roles move any intake task into a project that it edits. With one rule for both sides, each rule adds exactly the moves it states.

**An operation that no rule names is left to trusted writes.** The schema states what callers may do, and what it does not state is closed. A server-owned entity is then an entity with a read rule only, and an append-only entity is one with a read rule and a create rule. Trusted writes (connectors, effects, mutator bodies) are the application's own code, and they skip the rules as they skip the permission hooks. The operation that a write can be follows from the write alone: a `PATCH` is an update, a `DELETE` is a delete, and a `PUT` with version 0 is a create. The server therefore refuses such a write before it opens the write transaction, and the client refuses it before it enters the pending queue, where it could only end as a rejection.

**`admitted` is a source, not a switch.** Some entities, such as labels, are open to every member of the workspace. `admitted` states this for one rule, so one entity can let every member read and only admins write. The entity stays in the model: it can have an access parent, be the access parent of rows that need a permission, and be a grant entity. A caller is admitted when `CanAccessWorkspace` lets it in and it holds at least one permission in the workspace, so `admitted` still needs a grant row or a global role for the caller.

## Chains of parents

A row's access parent can have an access parent of its own. A comment sits below a task, and the task below a project, so a permission held at the project reaches the comment. An entity can also be its own access parent, as in a folder tree. A caller holds a permission at a row when it holds it at the root, at the row, or at any row up the row's chain.

The engine walks a chain upward from the row, one parent at a time, inside the read snapshot. It reads each row on a chain once per read, so the rows below one task cost one read of the task. For a chain of one level, the walk reads nothing: the row names its parent, and the parent has no parent to read.

To find the rows a caller can read without scanning an entity, the engine walks the other way: from the caller's anchors down through the index of each access-parent reference, entering only the entities that lead to the one it reads. A guest in one project reads the tasks of that project and the comments of those tasks, and nothing else.

Two limits keep the walks bounded. `Limits.MaxAccessDepth` caps how many rows a chain has: registration refuses a schema whose entities chain deeper, a write refuses a row that would sit deeper in a recursive chain, and a read stops walking at that depth. A write that would place a row below itself is refused on every write path, so the data never holds a cycle.

A row's access parent can also be polymorphic: a comment sits on a task or on a doc. The row still has one parent, and the case it holds decides which entity that parent is a row of, so a chain walk follows whichever case each row holds. The walk down probes the reference index of each case. Each case is its own index, so a guest in one project reaches the comments on that project's tasks without reading the comments on docs.

A polymorphic entity has one set of rules, not one for each case. The rules that decide whether a caller reads a comment then stay the same for the entity, whatever case the comment holds, and moving a comment from a task to a doc changes where it sits but not what it needs. The cost is that the roles of each tree must grant the permissions of the comment rules. Every member of the oneof must be marked: a row that held an unmarked case would anchor at nothing, and only a caller with the permission at the root could read it. A change of case is a move like any other, and two targets with the same key are two rows, because a row's parent is always an entity and a key together.

There is no stored chain. A stored chain for each row would have to be rewritten for every row below a row that moves, under the write lock, and that write would show the keys of those rows to clients that cannot read them. The engine reads the chain when it needs it instead, and records moves (see below).

## Reads use one snapshot

Every read path reads the caller's grants inside the same read snapshot as the rows it returns: Pull, Bootstrap, Subscribe, Query, `View` and `Apply`. A grant written in the same transaction as a task therefore never reaches the client before or after the task.

The workspace gate reads the grants once more, from the primary, before any read snapshot opens. It refuses a caller that holds no permission in the workspace. Because it reads from the primary, a replica that has not yet seen a new grant does not cause a permanent denial.

## Writes are checked inside the transaction

A write needs a rule that admits its operation where the row sits. The engine checks this inside the write transaction, so a write sees the grants that earlier writes of the same batch made:

- An update or delete is checked against the stored row, before the version check, so a refused caller does not learn the row's version.
- A create is checked against the new row after merges, carried-over fields and reference projection.
- An update that moves a row to another parent is checked at both parents, by one rule. Otherwise a caller could move a task into a project it can write and out of one it cannot.

A move carries every row below the moved row with it. A move therefore also needs, where the row is before the move, the read permission of every entity whose rows can sit below it. Without that rule, a caller who may edit tasks but not read their comments could move a task to a project where other people read comments.

A client cannot delete a row while a live row names it as its access parent. If it could, the rows below would anchor at nothing, and a caller who creates a row with the same key later would take them over. Trusted writes may delete such a row, because a connector or a mutator body sometimes must. The rows below it then anchor at nothing until a row with that key exists again.

Grant rows have a further rule: a write that can change what a grant row grants needs every permission of the role at the grant's anchor, before and after the write. A caller can therefore only grant or revoke what it holds itself. This stops a caller from raising its own role, from giving a grant to another subject by changing the subject field, from moving a grant to a project it does not manage, and from demoting a caller with a stronger role.

A custom mutator writes rows that the engine does not check one by one. In a schema that uses the model, an invocation needs a `MutatorAccess` that names the permission and the row it is checked at, or it is refused. The body can call `EntityTx.Can` to apply the model to the rows it writes. Mutators stay unchecked by default because some writes must break the subset rule on purpose, for example accepting an invitation or making the creator of a project its first admin.

## How changes of access reach clients

When a grant changes, the visibility of rows changes without a write to those rows, and a pull from the old cursor would not find them. Each cursor therefore records what its caller could read: a hash of the read anchors per permission, and a reference to the full record in the cursor-state store.

On the next pull, the engine compares the caller's current read anchors with the record:

- The same anchors: the pull continues as usual.
- Different anchors: the pull starts a resync. It reads only the rows below the anchors that were added or removed, through the reference index, and sends rows that became visible and tombstones for rows that became hidden.
- An entity that the caller could read at the root and now cannot: the resync drops the entity on the client and sends the rows the caller can still read. This costs what the caller keeps, not one tombstone for each row it lost.
- A change to the access model: the record also holds the read rule of each entity, which is who its read rules admit and its access parent. If an entity that the record and the current schema both know has a different read rule, the rows on the client were chosen by a rule that no longer applies, so the client resets and bootstraps again. Other model changes keep the cursor. A role that grants other permissions moves the anchors, which resync as above. An entity that is new to the model has no rows on the client to correct. Thus a client can move between nodes with the older and the newer schema during a rolling deploy, and it does not reset.
- A missing record or a cursor issued to other subjects: the client resets and bootstraps again.

The record lives in the store and never in the cursor itself. A resync tombstones rows by the anchors the record names, so a client that could write its own record could name anchors it never held and receive the keys of hidden rows.

### Moves

A move changes visibility without a grant change. When a task moves to another project, every comment below it changes visibility, but no comment is written, and neither the grants nor the change scan show it. The engine therefore keeps a move log: every write that changes the access parent of a row that rows sit below, or creates or deletes such a row, records the row and its parent before the write, in the same transaction. A row without rows below it moves nothing but itself, and the change scan already sends it.

On the next pull, the engine reads the moves after the cursor. For each moved row it compares the chain above the row as the client's rows reflect it with the chain now: if a read rule of an entity below the row admits the caller on one side and not on the other, the pull resyncs the rows below the moved row. The old side of that resync reads with the cursor's grants and with each moved row's parent from before its first move. A move that changes nothing the caller reads, such as a task moved between two projects the caller is not in, costs that caller one read of the log. After the resync the cursor records the newest move it reflects, so the next pull does not resync the same moves again.

A move during a resync resets the client when it changes what the caller reads, as a grant change during a resync does. A cursor that has more moves after it than `Limits.MaxResyncScan` also resets. Garbage collection removes moves at or below the tombstone horizon, where every cursor resets anyway.

## What the client knows

The client receives a `CallerAccess` summary: every permission the caller holds and its anchors. To answer for a row below an anchor, the client walks the row's chain through the rows it holds locally, its own queued writes included. When a row of the chain is not held locally, for example a task the caller cannot read above a comment it can, the answer is `'unknown'`. The server sends it when it changes, and the client stores it with the cursor. `can()` and `useCan` answer `'yes'`, `'no'` or `'unknown'` from it.

The answer is advisory. The client does not refuse a write when `can()` answers `'no'`, because the answer depends on rows and grants that the client may not hold. It refuses only a write that no rule admits to any caller, which it can tell from the schema alone. Every write is queued, and a refused write comes back as a rejection acknowledgement. A row the caller lost access to keeps showing a pending local write until that acknowledgement arrives.

## Costs and limits

- **Grant resolution.** Each request and each stream drain reads the caller's grant rows: one probe of the subject index for each grant entity, or a scan of the grant entity until the index is built. Team grants add one probe of the team reference index for each team the caller is a member of, and a read of the grant rows found.
- **Chain walks.** A read below the root reads each row on the chains of the rows it classifies once. A write reads the chain of the row it writes, and a move of a row in a recursive chain reads the chain of the new parent to refuse a cycle.
- **Moves.** A move of a row that rows sit below writes one log row. Every pull and stream drain reads the moves after its cursor, which is one row read when nothing moved. A delete of such a row probes the reference index of each entity that can sit below it.
- **Root changes.** Gaining or losing a permission at the root affects every row of the entities that use it, so the resync scans those entities.
- **Shared permission names.** A permission used at two depths that do not sit below one another, such as `view_members` for org members and for project members, lets a role that grants it at the root read the rosters of every project. Registration logs a warning for this case. A permission shared by a row and the rows below it, such as `view_tasks` for tasks and their comments, is the common case and logs nothing.
- **Tombstones reveal keys.** A tombstone for a hidden row still shows the row's entity, key and version. Use keys that are not secret.
- **Text deltas.** A caller below the root receives collaborative-text edits as deltas. A write that moves a row to another access parent, or that changes a field a read rule compares with the caller's subjects, sends the row's documents whole once to every reader whose next pull covers that write.
- **Query.** For a caller below the root, the `Query` RPC reads only the rows below the caller's anchors, which it finds through the reference index, and an index walk skips the entries of other rows without reading them. When the caller has more such rows than `MaxIndexScanCandidates`, the query reads like one from a caller at the root: through an index walk or a scan of the entity, with the model as a filter.
