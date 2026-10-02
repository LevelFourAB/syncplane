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

## One level deep

In this version, an `access_parent` must point at an entity without an `access_parent` of its own. A task can sit below a project, but a comment cannot sit below a task. One level is enough to compute visibility as a predicate on one reference field: a row is visible when its parent key is one of the caller's anchors. The engine already keeps an index for each reference field, so a caller with a few projects reads the rows below those projects by key and does not scan the entity.

Deeper trees need each level resolved before the next, and a move of an intermediate row changes the visibility of every row below it without a write to those rows. These need a move log and a different read path, and are not supported yet.

## Reads use one snapshot

Every read path reads the caller's grants inside the same read snapshot as the rows it returns: Pull, Bootstrap, Subscribe, Query, `View` and `Apply`. A grant written in the same transaction as a task therefore never reaches the client before or after the task.

The workspace gate reads the grants once more, from the primary, before any read snapshot opens. It refuses a caller that holds no permission in the workspace. Because it reads from the primary, a replica that has not yet seen a new grant does not cause a permanent denial.

## Writes are checked inside the transaction

A write needs the entity's `write` permission where the row sits. The engine checks this inside the write transaction, so a write sees the grants that earlier writes of the same batch made:

- An update or delete is checked against the stored row, before the version check, so a refused caller does not learn the row's version.
- A create is checked against the new row after merges, carried-over fields and reference projection.
- An update that moves a row to another parent is checked at both parents. Otherwise a caller could move a task into a project it can write and out of one it cannot.

Grant rows have a further rule: a write that can change what a grant row grants needs every permission of the role at the grant's anchor, before and after the write. A caller can therefore only grant or revoke what it holds itself. This stops a caller from raising its own role, from giving a grant to another subject by changing the subject field, from moving a grant to a project it does not manage, and from demoting a caller with a stronger role.

A custom mutator writes rows that the engine does not check one by one. In a schema that uses the model, an invocation needs a `MutatorAccess` that names the permission and the row it is checked at, or it is refused. The body can call `EntityTx.Can` to apply the model to the rows it writes. Mutators stay unchecked by default because some writes must break the subset rule on purpose, for example accepting an invitation or making the creator of a project its first admin.

## How grant changes reach clients

When a grant changes, the visibility of rows changes without a write to those rows, and a pull from the old cursor would not find them. Each cursor therefore records what its caller could read: a hash of the read anchors per permission, and a reference to the full record in the cursor-state store.

On the next pull, the engine compares the caller's current read anchors with the record:

- The same anchors: the pull continues as usual.
- Different anchors: the pull starts a resync. It reads only the rows below the anchors that were added or removed, through the reference index, and sends rows that became visible and tombstones for rows that became hidden.
- An entity that the caller could read at the root and now cannot: the resync drops the entity on the client and sends the rows the caller can still read. This costs what the caller keeps, not one tombstone for each row it lost.
- A change to the access model: the record also holds the read rule of each entity, which is its read permission and its access parent. If an entity that the record and the current schema both know has a different read rule, the rows on the client were chosen by a rule that no longer applies, so the client resets and bootstraps again. Other model changes keep the cursor. A role that grants other permissions moves the anchors, which resync as above. An entity that is new to the model has no rows on the client to correct. Thus a client can move between nodes with the older and the newer schema during a rolling deploy, and it does not reset.
- A missing record or a cursor issued to other subjects: the client resets and bootstraps again.

The record lives in the store and never in the cursor itself. A resync tombstones rows by the anchors the record names, so a client that could write its own record could name anchors it never held and receive the keys of hidden rows.

## What the client knows

The client receives a `CallerAccess` summary: every permission the caller holds and its anchors. The server sends it when it changes, and the client stores it with the cursor. `can()` and `useCan` answer `'yes'`, `'no'` or `'unknown'` from it.

The answer is advisory. The client does not refuse writes locally, because the rule depends on rows and grants that the client may not hold. Every write is queued, and a refused write comes back as a rejection acknowledgement. A row the caller lost access to keeps showing a pending local write until that acknowledgement arrives.

## Costs and limits

- **Grant resolution.** Each request and each stream drain reads the caller's grant rows: one probe of the subject index for each grant entity, or a scan of the grant entity until the index is built.
- **Root changes.** Gaining or losing a permission at the root affects every row of the entities that use it, so the resync scans those entities.
- **Shared permission names.** A permission used at two depths, such as `view_members` for org members and for project members, lets a role that grants it at the root read the rosters of every project. Registration logs a warning for this case.
- **Tombstones reveal keys.** A tombstone for a hidden row still shows the row's entity, key and version. Use keys that are not secret.
- **Text deltas.** A caller who does not hold an entity's read permission at the root receives collaborative-text fields as whole documents, not as deltas.
- **Query.** For a caller below the root, the `Query` RPC reads only the rows below the caller's anchors, which it finds through the reference index, and an index walk skips the entries of other rows without reading them. When the caller has more such rows than `MaxIndexScanCandidates`, the query reads like one from a caller at the root: through an index walk or a scan of the entity, with the model as a filter.
