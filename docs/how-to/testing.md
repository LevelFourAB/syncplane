# Running tests

Use this guide to select and run tests across the server, client, and browser tiers, or to run individual test cases during development.

## Prerequisites

Before you run end-to-end browser tests, install the browser binaries:

```sh
task test:e2e:install
```

This command downloads Chromium by default (approximately 100 MB).

## Steps

1. Select the test tier for the behavior that you are changing:

   | Tier               | Command                    | Description                                                                                         |
   | ------------------ | -------------------------- | --------------------------------------------------------------------------------------------------- |
   | Server             | `task test:go`             | Ginkgo and Gomega suites colocated with engine code, randomized in CI. Use for server-side changes. |
   | Client unit        | `task test:ts:unit`        | Vitest against `fake-indexeddb`. Use for client logic in isolation.                                 |
   | Client React       | `task test:ts:react`       | Vitest in `jsdom` with `@testing-library/react`. Use for `syncplane/react` hooks and providers.     |
   | Integration        | `task test:ts:integration` | TypeScript client against a spawned Go test server. Use for cross-language wire behavior.           |
   | End-to-end browser | `task test:e2e`            | Playwright with real browser tabs. Use for behavior that depends on multiple tabs.                  |

2. Run the test suite for your chosen tier, or run all non-browser suites together:

   - To run all suites except the browser tier:

     ```sh
     task ci
     ```

   - To run a specific tier, execute the corresponding command listed in step 1.

3. Run individual tests to target a specific test case, package, or browser:

   - To run a single Go test by name:

     ```sh
     task test:go -- --focus "rejects a stale version"
     ```

   - To run Go tests for a single package:

     ```sh
     go tool ginkgo run ./engine/server
     ```

   - To run client unit tests for a specific file:

     ```sh
     cd client && pnpm vitest run --project unit test/unit/kv.test.ts
     ```

   - To run client React tests for a specific file:

     ```sh
     cd client && pnpm vitest run --project react test/react/workspace.test.ts
     ```

   - To run client integration tests matching a pattern:

     ```sh
     cd client && pnpm vitest run --project integration -t "poke"
     ```

   - To run a specific browser test in Chromium:

     ```sh
     cd client && pnpm exec playwright test --project=chromium test/e2e/stream-leader.spec.ts
     ```

   - To run browser tests matching a pattern in a specific browser engine:

     ```sh
     task test:e2e BROWSER=firefox -- -g "hands over"
     ```

4. Run end-to-end tests across multiple browser engines before a release:

   - To install additional browser engines:

     ```sh
     task test:e2e:install BROWSER="firefox webkit"
     ```

   - To run tests in an alternate browser engine:

     ```sh
     task test:e2e BROWSER=firefox
     ```

   - To run tests across every installed engine at once:

     ```sh
     cd client && pnpm exec playwright test
     ```

## Confirming test results

Verify that all executed tests pass:

- Go tests output passing spec summaries.
- Vitest and Playwright test runs report zero failed tests.
- `task ci` completes without error exit codes across all non-browser suites.

## Shared conformance vectors

Three directories serve as contracts between the Go and TypeScript implementations. Extend them when modifying the semantics that they pin, and update both implementations in one change:

- `testdata/queryeval/`: Query-evaluation vectors.
- `testdata/indexkey/`: Index-entry encoding (`encodeCases` pin the bytes), entry-vs-comparator agreement (`orderCases`), and query planner decisions (`probeCases`). A planner that chooses differently still returns the right rows, but at a different cost.
- `testdata/text/`: Text CRDT vectors. The `encodedHex` checks pin the canonical encoding byte for byte. Any change to `engine/text` or `client/src/text` must update the other implementation and the vectors together.

## Fixing a bug

Follow a test-first workflow when fixing bugs:

1. Write a failing test that reproduces the bug.
2. Verify that the test fails for the expected reason.
3. Apply the fix and verify that the test passes.

A bug that shipped without a test indicates a missing test case; adding the test is part of the fix. When adding new features, include tests with the change.

## Related

- [Multi-tab behaviour](../explanation/multi-tab.md)
