# Full-text search

Why search works the way it does: one deterministic analyzer implemented
twice, a relevance rank computed from each row alone, and an index that only
ever narrows candidates. Each is a consequence of the same constraint, so this
page starts there.

## The constraint: two evaluators, one answer

A match query runs in two places. The client evaluates it over whatever subset
partial sync brought down, offline, through the optimistic overlay; the server
evaluates it over everything, for `queryServer` and the `Query` RPC. The
engine's standing rule for queries — the same predicate selects the same rows
in the same order wherever it runs — is what lets an app fall back from one to
the other without the results jumping, and search inherits it.

That rule eliminates most of how search engines are usually built:

- **No ICU, no locale, no host tokenizer.** `Intl.Segmenter` in a browser and
  a Unicode segmentation library in Go each track their own tables, so two
  browsers can disagree with each other before either disagrees with the
  server. Everything the analyzer does had to be specified tightly enough to
  implement identically twice.
- **No corpus statistics — which rules out BM25 and TF-IDF.** Their scores
  weigh a term by how rare it is _in the collection_, and the client's
  collection is the synced subset: a locally computed score would differ from
  the server's for the same row, and the two orderings would disagree. Any
  signal the rank uses has to come from the row and the query alone.
- **No native engine on either side.** SQLite's FTS5, Postgres's `tsvector`
  and anything running in a browser each bring their own tokenizer and
  ranking, all different. A storage adapter may not change query results, so
  none of them can be the semantics. (A deployment that wants a heavyweight
  engine feeds one from the [change feed](../how-to/change-feed.md) instead —
  that path is server-only and plays by that engine's rules.)

What survives the constraint is deliberately modest machinery — closer to
Meilisearch's document-local ranking rules than to Lucene — that holds the one
property none of the alternatives could: byte-for-byte agreement between a Go
process and a browser tab, pinned by the shared vectors in `testdata/search/`.

## The analyzer

Analysis turns field text and query strings into terms through four steps,
each chosen to be implementable identically in both languages:

1. **Unicode NFKC normalization** — full-width forms, ligatures and
   compatibility characters compare equal to their plain spellings
   (`ﬁle` ≡ `file`, `Ｔｏｋｙｏ` ≡ `Tokyo`).
2. **Unicode full lowercase** with no locale — the mapping JavaScript's
   `toLowerCase` applies, mirrored in Go with `x/text`.
3. **A diacritic fold**, unless the schema keeps them: decompose, drop the
   combining marks of U+0300–U+036F, recompose. The range restriction is the
   point — it folds the accents of Latin, Greek and Cyrillic (`café` ≡ `cafe`)
   while leaving scripts whose marks carry meaning, such as Thai, Devanagari
   and Hebrew, untouched.
4. **Segmentation.** Runs of letters, digits and combining marks form one term
   each. Code points in the CJK ranges instead pair into character **bigrams**
   (`日本語` → `日本`, `本語`), the standard dictionary-free way to make
   scripts written without word separators searchable. The ranges are fixed
   code-point ranges in the specification rather than Unicode script
   properties, so the two implementations cannot drift when their Unicode
   tables do.

There is no stemming and no stopword list. Both are language-specific tables
that would have to ship twice and match exactly, and most of what stemming
buys in an interactive app, prefix matching already provides: the final term
of a query matches as a prefix whenever the query ends inside it, so `searc`
finds `searching` while the user is still typing. A trailing space makes the
final term exact — the user finished the word.

## Ranking from the row alone

A match requires every term of the query to occur in some searchable field of
the row. Relevance then orders the matches by a rank computed from the row and
the query — never from the store — as a tuple compared left to right:

1. terms found only as a prefix of a longer term (fewer is better — an exact
   hit beats a typeahead hit);
2. the first declared field containing _every_ term (searchable fields are
   declared most-important-first, so all terms in the title beats all terms in
   the body, which beats terms scattered across fields);
3. the smallest token window covering every term in that field (`red apple`
   ranks a row titled "red apple pie" over "red big juicy apple");
4. the earliest matched position in that field;
5. how many matching tokens the row holds in all.

Ties fall through to the row key, like every other ordering. The tuple packs
into one 48-bit integer, which is what lets a relevance position ride the
existing cursor machinery: a page cursor from a relevance-ordered query is the
rank plus the row key, minted on either side and replayed on the other. The
integer values themselves are pinned in the shared vectors, because a cursor
one side mints must mean the same position to the other.

The trade this makes is explicit: a common word counts as much as a rare one,
because knowing a word is rare requires the corpus statistics the constraint
forbids. In a workspace-partitioned store the corpora are small enough that
field weighting and proximity carry most of the signal IDF would have.

## The index narrows; it never answers

Without an index, a match query scans the entity and evaluates every row —
correct, and O(entity). The search index exists to narrow that, and it narrows
_only_: it maps each term a row's searchable fields analyze to into one
posting (`term → row key`), stored in the same secondary-index storage as
every other index, on both sides. A query takes its candidates from the
posting range of its most selective term — a prefix range for the typeahead
term — and then re-reads, re-checks and re-ranks them from the rows
themselves.

Which term that is, is discovered rather than guessed: each term is offered a
growing candidate cap and the first one to fit under a cap has the shortest
posting list. The remaining terms then narrow the set, but only while reading
their postings costs less than re-checking the rows they would remove — a term
so common that reading it would cost more than it saves is skipped. Skipping
is safe because candidates are only ever narrowed and never completed: fewer
narrowing passes mean more rows re-checked, never different rows returned. The
practical consequence is that typing a second word into a search box cannot
make the query slower than the first word alone.

Keeping the index down to postings is what preserves the query contract: an
index-served match returns exactly what the scan returns, because the rows
still decide everything. The index stores no positions and no frequencies, so
the analyzer can gain ranking signals without invalidating stored entries, and
the differential suites can hold both sides to the scan.

Two consequences of that design carry the sharp edges:

- **A row with more than 256 distinct terms stores no postings** — it stores a
  single _overflow marker_ instead, and every term probe unions the overflow
  rows into its candidate set unconditionally. An oversized document costs
  every search a candidate rather than becoming silently unfindable past its
  256th term; correctness never rests on a truncated posting list. The cap is
  a constant of the format, not a tunable, because the client and the server
  each maintain their own copy of the index and have to agree on which rows
  overflow.
- **Only a match that every result must satisfy can narrow.** A match under
  `or` or `not` constrains nothing by itself, so such a query falls back to
  the scan — still correct, just unindexed.

Collaborative-text fields are searchable even though they are rejected as
value-index components. The rejection there is about entry churn — every
keystroke rewrites a value index's entry. A search posting is a _term_, and a
keystroke that does not change the term set rewrites nothing worth worrying
about; the field is analyzed as the plain text the user sees.

## What it deliberately does not do

- **Typo tolerance.** Matching within an edit distance needs either a term
  dictionary walked per query or precomputed fuzzy structures, both large and
  both hard to hold byte-identical across two implementations. Prefix matching
  covers the typing-in-progress case; a misspelled word does not match.
- **Phrase and boolean term queries.** Terms are conjunctive, full stop.
  Proximity ranking rewards adjacency without a phrase operator, and `or`
  exists at the condition level for queries that need it.
- **Corpus-aware scoring** — see above; it is the constraint itself.

Each would be additive — new analyzer options and condition shapes version
their fingerprints, and a stored index whose fingerprint moves rebuilds
itself — so none is foreclosed, only declined until something needs it.
