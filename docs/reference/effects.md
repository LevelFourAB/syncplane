# Effects runtime API

The effects runtime delivers change feed events to consumers and tracks consumer state. For the reasoning behind effects, see [Effects and the change feed](../explanation/effects-and-the-change-feed.md). For practical tasks, see [React to changes](../how-to/react-to-changes.md).

## Sink interface

A consumer consists of a durable name, a set of entities, and a `Sink`. The `Sink` interface receives change events:

```go
type Sink interface {
    Deliver(ctx context.Context, ch effects.Change) error
}
```

## Runtime registration

`Runtime.Register` connects a `Sink` directly to the runtime:

```go
rt.Register("publish", mySink)                       // every entity
rt.Register("publish-issues", mySink, &issuev1.Issue{})  // only these
```

Use `Runtime.Register` for consumers that are not a single typed handler, such as sinks that publish changes to an external message broker.

## Consumer names and durability

A consumer name identifies the durable position of the consumer.

Rules for consumer names and durability:

- Consumer names must remain stable across restarts.
- Renaming a consumer resets its progress.
- In a workspace with collected tombstones, a renamed consumer resumes at the current sequence number instead of replaying historical events.

## Change event fields

`Watch` declares the fields for which a consumer requires before-state. The runtime stores a shadow copy of only those watched fields. Each watched consumer stores one shadow row for every processed row. Declaring no watched fields stores no shadows and uses no additional storage.

Change events provide the following fields to watched consumers:

| Field                         | Description                                                                                                    |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `ev.After`                    | The new state of the row. This value is always complete. Zero when `ev.Deleted`.                               |
| `ev.Before`                   | The watched fields only, as they stood when the consumer last acted on the row.                                |
| `ev.Changed` / `ev.DidChange` | The watched fields that differ between the two states. `DidChange` returns `false` for all `FirstSeen` events. |
| `ev.FirstSeen`                | Indicates that the consumer held no shadow copy to compare against.                                            |

### FirstSeen conditions

`ev.FirstSeen` occurs in three cases:

- A row was created recently.
- A row existed before consumer registration.
- A row is sighted for the first time after a horizon reset.

These three cases are indistinguishable because shadow rows reflect consumer memory rather than row history. To react to observed transitions or existing rows, see [React to changes](../how-to/react-to-changes.md).
