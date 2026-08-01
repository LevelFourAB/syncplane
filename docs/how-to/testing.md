# Testing

Pick the tier that can actually observe the behavior you are changing.

| Tier                       | Runs                                                                  | Use it for                                     |
| -------------------------- | --------------------------------------------------------------------- | ---------------------------------------------- |
| `task test:go`             | Ginkgo/Gomega suites colocated with the engine code, randomized in CI | anything server-side                           |
| `task test:ts:unit`        | vitest against fake-indexeddb                                         | client logic in isolation                      |
| `task test:ts:react`       | vitest in jsdom, with @testing-library/react                          | `syncplane/react` hooks and providers          |
| `task test:ts:integration` | the TS client against a spawned Go testserver                         | **cross-language wire behavior**               |
| `task test:e2e`            | Playwright, real tabs                                                 | **anything that depends on more than one tab** |

`task ci` runs everything except the browser tier.

## Running a single test

```sh
# Go: pass ginkgo args through task, or run ginkgo directly in engine/
task test:go -- --focus "rejects a stale version"
cd engine && go tool ginkgo run ./server          # one package

# TS: vitest in client/
cd client && pnpm vitest run --project unit test/unit/kv.test.ts
cd client && pnpm vitest run --project react test/react/workspace.test.ts
cd client && pnpm vitest run --project integration -t "poke"

# Browser: Playwright in client/
cd client && pnpm exec playwright test --project=chromium test/e2e/stream-leader.spec.ts
task test:e2e BROWSER=firefox -- -g "hands over"          # any installed engine
```

## The browser tier

`task test:e2e` is not part of `task ci`: it drives a real browser, which
`task test:e2e:install` downloads once (~100 MB). It covers what only a browser
can — cross-tab change sharing, stream leadership and its handover when a tab
hides, mutation-id minting across tabs, and an IndexedDB store surviving the tab
that made it.

Node has no shared lock manager, so outside a browser the client falls back to
an in-process mutex, declines stream leadership and leases peers locally.
Multi-tab claims are therefore **only** really covered here. Specs drive a
JSON-only `window.harness`; invariants no tab can see are asserted against the
testserver's `/debug/rpc` counters.

Collaborative text has the largest share of the browser tier, because every part
of the feature is multi-tab: writer identities are leased from a device-wide
pool over Web Locks, and two tabs of one app share a store, a queue and a
`clientId`. The specs cover live handles typing at once and a second device
reaching the same document; string writes meeting a typist; the wire cost of an
edit in both directions, asserted against the server's own byte counters; and a
live handle healing when the server compacts the field under it.

Chromium by default. The suite uses no engine-specific APIs, so it runs on all
three — worth doing before a release, since Web Locks and IndexedDB are exactly
where engines differ:

```sh
task test:e2e:install BROWSER="firefox webkit"   # once, ~100 MB each
task test:e2e BROWSER=firefox
cd client && pnpm exec playwright test           # every installed engine at once
```

## Shared conformance vectors

Two directories are contracts between the Go and TypeScript implementations.
Extend them when touching the semantics they pin, and update **both** sides in
one change:

- `testdata/queryeval/` — query-evaluation vectors.
- `testdata/text/` — text-CRDT vectors. The `encodedHex` checks pin the canonical
  encoding byte for byte, so any change to `engine/text` or `client/src/text`
  must land in the other implementation and the vectors together.

## Fixing a bug

Test-first: write the failing test that reproduces it, watch it fail for the
right reason, then fix. A bug that shipped without a test means the suite had a
hole; closing the hole is part of the fix. For new features, tests land with the
change.
