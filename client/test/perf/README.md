# Browser sync benchmarks

What syncing costs in a real browser, against a seeded corpus of up to half a
million rows.

```sh
task bench:browser                 # every tier, installed Google Chrome
task bench:browser TIERS=s,m       # quick pass while iterating
task bench:browser BROWSER=firefox -- -g "cold bootstrap"
```

The output is a table, printed at the end of the run and written to
`results/<run>.json`. A spec that "passes" only means its numbers were taken —
nothing here asserts a duration, because a threshold that holds on one machine
says nothing on another. The assertions that do exist check that the scenario
measured what it claims to (a warm restart that bootstrapped, a catch-up that
re-bootstrapped, a windowed client holding more rows than its window) — a
mislabelled number is worse than a missing one.

## Which browser

The default project is `chrome`: the **installed Google Chrome**, which must be
present on the machine. Playwright's bundled Chromium prices an IndexedDB `put`
by how many records the store already holds, so on it write cost grows with the
store rather than staying flat — 100k plain 420-byte rows, per 10k-row slice:

| Browser                            | first slice | last slice | total |
| ---------------------------------- | ----------- | ---------- | ----- |
| bundled Chromium (Playwright 1.62) | 272ms       | 4422ms     | 23.4s |
| Google Chrome 152                  | 95ms        | 95ms       | 1.0s  |

It carries into every write the suite does: the `l` tier's background index build
takes 238s on the bundled build against 5.9s on Chrome. Read-side numbers are the
same on both.

**Numbers taken before the tier moved to Chrome are not comparable with numbers
taken after** on anything that writes — they measure that build's storage, not
the client.

Another engine, deliberately: `task bench:browser BROWSER=chromium` (the bundled
build), `BROWSER=firefox`, `BROWSER=webkit`. Which one produced a run — project,
engine, channel and version — closes the run's output and is the `browser` field
of `results/<run>.json`, so two tables can be told apart later.

## What is measured

| Spec                | Question                                                      |
| ------------------- | ------------------------------------------------------------- |
| `01-bootstrap`      | Cold start, full vs windowed vs one project vs one thread     |
| `02-reads`          | Local query cost once the data is there                       |
| `02b-build-reads`   | Read cost while the cold bootstrap is still streaming         |
| `02c-pagination`    | Deep pages by cursor vs offset, and what a total costs        |
| `02d-search`        | Full-text search, by how many rows carry the query's terms    |
| `03-restart`        | Opening an app that has synced before                         |
| `04-scope`          | Widening, narrowing and switching what you are subscribed to  |
| `04b-overlay-reads` | The same reads through 100 and 1000 pending mutations         |
| `05-incremental`    | Catching up, realtime latency, rebase over pending writes     |
| `05b-live-queries`  | Catch-up cost at 0/10/50 open watches, watch-observed latency |
| `05c-writes`        | Per-write local latency: patch, put, patch with watches open  |
| `05d-rel-watches`   | Watches whose query crosses a reference, over target commits  |
| `06-resilience`     | Falling behind the GC horizon, and what a second tab costs    |
| `07-deletion-churn` | Applying 2000 deletions, and the store they leave behind      |

The file numbers are the run order, and they matter: everything below `05` only
reads the corpus, so it measures it in exactly the state the seeder left it
(`04b` writes, but offline into a fresh account it then discards — nothing
reaches the server). `05` and its siblings write, and `06` deletes rows and
advances the tombstone horizon, which is why `07` churns only rows it inserted
itself. `05d` rewrites issue states, so it works a range in the middle of the id
space rather than the newest end the other write specs share.

`02d-search` varies one thing: how SELECTIVE a query's terms are. The corpus
plants three terms in its issue bodies for it — one in ten rows whatever the
tier, one in a hundredth of them, one in every one — because that, and not the
number of words typed, is what a search costs. Read `rare` for whether the
index still bounds the walk (it should not move with the corpus), `common` for
the ceiling where nothing can be narrowed, and `common+rare` for whether a
second word is answered from the cheaper of the two terms.

A cold bootstrap writes its rows carrying their index positions and claims the
index-built markers in the transaction that installs the snapshot, so a workspace
is indexed the moment it is complete. The background build pass still runs behind
it; over a workspace whose markers are already claimed it walks nothing and
reports zero rows.

`02-reads` waits for that pass before measuring, so a read cannot land in the
window where the swap has yet to claim the markers and every query scans. The
wait itself is reported (`index build`: how long the pass took and the rows it
rewrote — zero after a cold bootstrap, the whole snapshot after a staged
re-bootstrap), and each read's `path` column names the index that served it or
`scan:<reason>` when one did not — the receipt that a number measured the plan it
claims to.

`02b-build-reads` measures the other side: reads issued WHILE the bootstrap is
still streaming, which is what first launch actually looks like. No marker exists
yet, so each one prices a scan over a store that is filling, and `firstIndexedMs`
is the first read the install let an index answer.

The two numbers to read first are `firstReadMs` and `completeMs` from
`01-bootstrap`: when the app could paint, and when the local store provably held
the whole scope. The gap between them is what grows as a corpus does.

`bootstrapMs` is split into the phases it is made of — `streamMs` waiting on the
server's snapshot, `stageMs` writing the chunks it delivered, `swapMs` in the
transaction that installs them — because they move independently, and a change
that shifts time from one to another nets out to nothing in the total.
`swapWrites` (and `swapWritesPerRow`) is what that transaction cost the store in
KV entries: row count alone hides write amplification, and a regression there is
otherwise invisible until it surfaces as `swapMs`.

## The corpus

An issue tracker — `proto/private/syncplane/bench/v1` — generated by
`internal/benchapp`. Issues declare full-text search over title and body, so
every issue write and every index build in these tables includes search
maintenance; numbers taken before that was true are not comparable. Projects, users and labels are the dimension entities;
issues are half of every tier; comments are the append-heavy child; notes carry
collaborative text; `flat` is a deliberately dull control entity whose per-row
cost is the floor the others are read against.

Four sizes, by total rows: `s` ~1k, `m` ~10k, `l` ~100k, `xl` ~500k. Full sync
stops at `l` — a full-sync client at 500k rows is the case partial sync exists
to answer, and the `xl` row of the windowed table is what it would have cost.

Generation is a pure function of (tier, seed), so a seeded database is cached
under `.corpus/` and reused. It regenerates when `benchapp.GeneratorVersion`,
the tier list, or `SEED` changes. The server always runs on a **copy** — the
benchmarks write, and a run that mutated the cache would silently change what
every later run measures.

The cached database has its server-side indexes already built, so a run
measures the steady state a server reaches a minute after declaring them,
rather than one whose background backfill is still rewriting the corpus
underneath the writes being timed.

Seeding is a one-time cost per fingerprint (roughly a second per 10k rows).
Delete `.corpus/` to force it again.

## Environment

| Variable | Effect                                                                                                |
| -------- | ----------------------------------------------------------------------------------------------------- |
| `TIERS`  | Comma-separated tiers to seed and run. Default all four.                                              |
| `SEED`   | Corpus PRNG seed. Default 1 — change it to check a result is not an artifact of one generated corpus. |

## Data volume, and what it means off localhost

Wall times here are measured over loopback, where the network costs nothing.
Bytes are the part that does **not** change when the same client runs over a
real link, so the run ends with two more tables built on them.

**By RPC** — where the bytes went, and whether the engine compressed them
itself:

```
   method  calls  selfZip  rawKB  wireKB  ratio  upKB  wireKB  ratio
Bootstrap      6       no   1997     483  4.13x     2       1  1.38x
     Pull      8      yes     85      85  1.00x    95      15  6.15x
```

`selfZip` is the column that makes the rest legible. Connect compresses a
**unary** response through the ordinary HTTP `Content-Encoding` header, which
the browser decodes transparently — so `Pull` already arrives compressed and its
raw size _is_ its wire size. A **streaming** response cannot use that header,
and `connect-web` refuses protocol-level frame compression outright, so
`Bootstrap` goes out as raw protobuf.

That gap is worth acting on: bootstrap is the largest payload the system moves,
and it compresses **~4×**. Any gzip-capable proxy in front of the engine
collects that for free. Until one is there, `rawKB` is what a bootstrap actually
costs a user.

`wireKB` is what a link carries — the body as sent when it was already
compressed, its gzipped size when it was not — and is the number the projection
uses. gzip runs at `BestSpeed`, so the ratio is a floor; a CDN at default gzip,
or brotli, does better.

**Per link** — each measured operation projected onto a few profiles:

```
total ≈ localMs + wireDown/bandwidth + wireUp/bandwidth + roundTrips × RTT
```

`roundTrips` counts Connect **calls**, not pages: a server-streaming bootstrap is
one round trip however many chunks it carries, while forty pull pages are forty.
On a high-RTT link that term is often the whole story — a warm restart moves 3KB
and still costs ~360ms on bad 3G, all of it latency.

The model is crude on purpose. It ignores TLS handshakes, TCP slow start and
HTTP/2 flow control (all make it slower — slow start especially punishes a large
bootstrap on a high-RTT link), and it ignores transfer overlapping with server
work and client apply (makes it faster). Read it as an order of magnitude, not a
prediction. It is most trustworthy where payloads are small and the RTT term
dominates, since that term is the one simple thing in it.

## Reading the numbers honestly

- The harness is served by Vite, unbundled and unminified. Absolute wall times
  are not what a production build posts; these numbers exist to be compared
  against each other and against their own history.
- `workers: 1`, no retries, no tracing. Anything running beside a measurement
  lands in it.
- `heapBytes` is Chromium-only and approximate; it is reported to make a run
  that quietly doubled its retained heap visible, not to be trusted to a
  megabyte.
- Wire bytes come from the server (`/debug/stats`), because no client-side
  measurement can tell a small request from a small request the server ignored.
