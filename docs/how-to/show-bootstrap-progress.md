# Show download progress for the first sync

The first sync of a large workspace downloads a full snapshot, which can take minutes. Use the bootstrap progress in the sync status to show a progress bar and a time estimate, for example "62% · about 2 minutes left".

## Prerequisites

- A server that sends row estimates on `BootstrapResponse.Begin` (`row_estimates`). The size is unknown when the server sends none:
  - an older server, or a store whose transactions do not implement `storage.RowCounter`;
  - a caller whose read permissions (a read filter or the access model) hide rows of the shipped entities, because a count would tell it how many rows it may not read;
  - a subscription that narrows an entity with a predicate, a window, named keys or a follow closure, because a count of the whole entity costs more than the narrowed snapshot.
- A TypeScript `SyncClient`, or a Go `upstream.Link`.

## Steps

1. Read the progress.

   In TypeScript, read `bootstrap` from the workspace status. It is set while a bootstrap runs in this tab, and unset when it ends:

   ```ts
   client.onStatusChange((status) => {
   	const progress = status.workspaces[workspaceId]?.bootstrap;
   	render(progress);
   });
   ```

   With React, `useWorkspaceStatus().bootstrap` gives the same value.

   In Go, read `Status.Bootstrap` in `Config.OnStatus`. It is `nil` while no bootstrap runs:

   ```go
   cfg.OnStatus = func(s upstream.Status) {
   	render(s.Workspace, s.Bootstrap)
   }
   ```

   Both carry the same four numbers: rows applied, rows estimated, entities done and entities total.

2. Show the fraction done.

   When the estimate is known (above 0), the fraction is `rowsApplied / rowsEstimated`. It is at most 1, and it does not go down while the same snapshot downloads, so you can show it as it is. When the estimate is 0, show an indeterminate bar, or `entitiesDone / entitiesTotal` as a coarse fallback.

   ```ts
   function fraction(p: BootstrapProgressInfo): number | undefined {
   	if (p.rowsEstimated > 0) return p.rowsApplied / p.rowsEstimated;
   	if (p.entitiesTotal > 0) return p.entitiesDone / p.entitiesTotal;
   	return undefined;
   }
   ```

   The estimate is the live rows at the start of the download. Rows written or deleted during the download can make it end below 100%, or reach 100% a little early. The progress goes away when the snapshot is in, so do not wait for 100% to hide the bar.

3. Compute a time estimate.

   Measure the rate in rows per second as a moving average over the last seconds, and divide the rows left by it. Show the estimate only after some seconds of download: the first chunks include the connection setup, and a rate from them is not stable.

   ```ts
   const WINDOW_MS = 10_000;
   const SHOW_AFTER_MS = 5_000;
   const samples: { at: number; rows: number }[] = [];
   let startedAt: number | undefined;

   function timeLeftMs(
   	p: BootstrapProgressInfo,
   	now = Date.now(),
   ): number | undefined {
   	startedAt ??= now;
   	samples.push({ at: now, rows: p.rowsApplied });
   	while (samples.length > 2 && now - samples[0]!.at > WINDOW_MS) {
   		samples.shift();
   	}
   	if (p.rowsEstimated === 0 || now - startedAt < SHOW_AFTER_MS) {
   		return undefined;
   	}
   	const first = samples[0]!;
   	const rate = (p.rowsApplied - first.rows) / (now - first.at); // rows per ms
   	if (!(rate > 0)) return undefined;
   	return (p.rowsEstimated - p.rowsApplied) / rate;
   }
   ```

   Reset `samples` and `startedAt` when the progress goes away. Round the result for display ("about 2 minutes left"). The rate can change, so the time is an estimate and not a bound.

   The TypeScript client updates the progress at most every 250 ms, and shows each change within 250 ms, plus at the start and the end of the download. The Go link reports progress to `OnStatus` at the same rate. Both are frequent enough for a moving average.

## Interrupted downloads

When the connection drops, the progress goes away until the next attempt starts. If the server continues the same snapshot, the next attempt starts from the rows the earlier attempts applied, also after a reload or a restart of the process. The bar continues where it stopped.

The server counts the whole snapshot again on a resumed attempt. The client keeps the smaller of the two estimates, so the fraction does not go down.

If the server cannot continue the snapshot (the subscriptions changed, or the cursor is older than the server keeps tombstones), it starts a new one. The progress then starts again at 0. Reset the time estimate when `rowsApplied` goes down.

## Show how far behind the client is after the first sync

After the snapshot, the client catches up with the writes made since it began, and later with the writes made while it was offline. Each pull and stream page reports the server's newest sequence (`PullResponse.head_seq`), and the clients report the distance to it:

- TypeScript: `WorkspaceStatus.behind` (and `SyncStatus.behind` for the default workspace). 0 is up to date.
- Go: `Status.Behind()`, from `Status.OriginHeadSeq` and `Status.OriginSeq`. `OriginHeadSeq` 0 is unknown.

```ts
const { behind } = status.workspaces[workspaceId] ?? {};
if (behind !== undefined && behind > 0) render(`${behind} changes to download`);
```

The distance counts writes, not rows: one write can change many rows, and a write the caller may not read still counts. Use it to tell "up to date" from "catching up", and as a rough count, not for a progress bar. It is as of the last page the client received, so it does not grow while the server cannot be reached. It is unknown with a server that does not report its head.

## Related

- [Sync status](../reference/typescript-client.md#sync-status): the TypeScript status types.
- [`Status`](../reference/upstream.md#status): the Go link status.
- [Storage adapter contract](../reference/storage-adapter.md#row-counts-optional): the store capability behind the estimate.
