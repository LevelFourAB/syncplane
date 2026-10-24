# Choose an access control

Use this guide to select the correct access control for each read and write operation in your schema.

The access model and the application hooks overlap. The access model is declared in the schema, so the client predicts it and the engine knows which writes change which rows' access. A permission hook is code, so it can read facts outside the workspace's rows, and it only narrows what the model allows. For the reasons behind this design, see the [access model explanation](../explanation/access-model.md).

| You want                                                                                 | Use                                                                                             | The client predicts it               |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------ |
| Who reads, creates, updates and deletes rows                                             | Rules in `access.allow`                                                                         | Yes                                  |
| A caller who can change only some fields of a row, such as an assignee who sets a status | `fields` on an update rule                                                                      | Yes                                  |
| A field that only some callers change, whatever rule admits the update                   | A field guard                                                                                   | Yes                                  |
| A field that only server code changes                                                    | A field guard with only its read rule                                                           | Yes                                  |
| A field that only some readers of the row read, such as a salary                         | A field guard with a narrower read rule                                                         | Yes                                  |
| A field that no client reads                                                             | A field guard without a read rule                                                               | Yes                                  |
| An operation that only server code does                                                  | No rule for the operation; write it under `server.Trusted`, a connector, an effect or a mutator | Yes                                  |
| The creator of a row, who alone edits it later                                           | A `creator` field and a rule with `caller_is` on it                                             | Yes                                  |
| A client that starts server logic                                                        | A mutator with `MutatorAccess`                                                                  | No, the server checks the invocation |
| Rows that never sync to devices, and that clients still query                            | `server_only`. It controls replication, and read rules still decide which rows a query returns  | Not applicable                       |
| A check on facts outside the workspace's rows, such as a plan or a quota                 | `CanMutate`, `ReadFilter`, `RowWritePermissions` or `FieldPermissions`                          | No, the client gets a rejection      |

## Next steps

Implement your controls using these guides and reference topics:

- To declare rules for row operations: [Declare access on the entities the roles govern](declare-roles-and-permissions.md#3-declare-access-on-the-entities-the-roles-govern).
- To restrict caller access to specific fields: [Let only some callers change a field](declare-roles-and-permissions.md#let-only-some-callers-change-a-field) and [Let only some callers read a field](declare-roles-and-permissions.md#let-only-some-callers-read-a-field).
- To view field guard schema options: [Field guards](../reference/proto-options.md#field-guards).
- To restrict operations to server code: [Reserve an operation for trusted writes](authentication-and-permissions.md#reserve-an-operation-for-trusted-writes).
- To control mutator invocations: [Declare who may invoke each mutator](declare-roles-and-permissions.md#6-declare-who-may-invoke-each-mutator).
- To keep rows on the server: [Server-only entities](../reference/proto-options.md#server-only-entities).
- To implement permission checks in hook code: [Filtering reads and writes with CanMutate and ReadFilter](authentication-and-permissions.md#filtering-reads-and-writes-with-canmutate-and-readfilter).
