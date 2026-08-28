# Full-text search

This document explains why full-text search works the way it does: how deterministic analysis, row-local relevance ranking, and candidate-narrowing indexes allow the client and server to return identical results. Each choice follows from a core constraint: a query must return identical results whether it runs on the client or the server.

## The constraint: two evaluators, one answer

A match query runs in two places. The client evaluates the query offline against the local subset provided by sync through the optimistic overlay. The server evaluates the query against the complete dataset for `queryServer` and the `Query` RPC.

The engine requires that the same predicate selects the same rows in the same order wherever it runs. This consistency allows an application to fall back from the server to the client without shifting result orders.

That constraint eliminates standard search engine implementations:

- **No International Components for Unicode (ICU), locales, or host tokenizers:** `Intl.Segmenter` in a browser and a Unicode segmentation library in Go track different tables. Two browsers can disagree with each other before either disagrees with the server. All analyzer behavior must be specified precisely to ensure identical behavior across implementations.
- **No corpus statistics, such as BM25 and term frequency-inverse document frequency (TF-IDF):** These algorithms score terms based on how rare they are across the entire collection. The client only contains a synced subset, so a locally computed score differs from the server score for the same row. Any ranking signal must come from the row and the query alone.
- **No native engines on either side:** SQLite FTS5, PostgreSQL `tsvector`, and browser-based engines each use different tokenizers and ranking algorithms. A storage adapter cannot alter query results, so native engines cannot define search semantics. A deployment that requires a dedicated search engine can feed one from the [change feed](../how-to/change-feed.md) on the server.

The search engine uses document-local ranking rules rather than an engine like Lucene. This design provides byte-for-byte agreement between a Go process and a browser tab, verified by shared test vectors in `testdata/search/`.

## The analyzer

Analysis converts field text and query strings into terms through four steps, each implementable identically in Go and TypeScript:

1. **Unicode Normalization Form KC (NFKC) normalization:** Full-width forms, ligatures, and compatibility characters normalize to plain spellings (`ﬁle` = `file`, `Ｔｏｋｙｏ` = `Tokyo`).
2. **Unicode full lowercase without locale:** Maps characters using the standard JavaScript `toLowerCase` behavior, mirrored in Go with `golang.org/x/text`.
3. **Diacritic folding:** Unless the schema preserves diacritics, the analyzer decomposes text, removes combining marks in the range U+0300 through U+036F, and recomposes the characters. This range folds accents in Latin, Greek, and Cyrillic scripts (`café` = `cafe`) while preserving combining marks that carry lexical meaning in scripts such as Thai, Devanagari, and Hebrew.
4. **Segmentation:** Runs of letters, digits, and combining marks form individual terms. Code points in Chinese, Japanese, and Korean (CJK) ranges pair into character bigrams (`日本語` becomes `日本` and `本語`), enabling dictionary-free search for scripts without word separators. The specification defines fixed code-point ranges rather than Unicode script properties, preventing drift across Unicode versions.

The analyzer does not use stemming or stopword lists. Both require language-specific tables that must match across implementations.

Prefix matching provides the primary benefit of stemming for interactive applications. The final term of a query matches as a prefix when the query ends inside the term, so `searc` matches `searching` while you type. A trailing space makes the final term exact, indicating you finished typing the word.

## Ranking from the row alone

A match query requires every term to occur in at least one searchable field of the row. Relevance ordering then ranks matching rows using a tuple computed solely from the row and the query, compared in the following order:

1. **Prefix match count:** The count of terms found only as prefixes of longer terms. Fewer prefix matches rank higher, prioritizing exact hits over typeahead hits.
2. **Field priority:** The first declared field containing every term. Searchable fields are declared in order of importance, so matching all terms in a title field ranks above matching all terms in a body field or across multiple fields.
3. **Token window:** The smallest token window covering every term in that field. For example, `red apple` ranks "red apple pie" higher than "red big juicy apple".
4. **Earliest position:** The earliest matched position in that field.
5. **Token count:** The total number of matching tokens in the row.

Ties fall back to the row key. The tuple packs into a single 48-bit integer, allowing relevance positions to use the existing cursor system. A page cursor from a relevance-ordered query consists of the rank plus the row key, generated on one side and replayed on the other. The integer values are pinned in shared test vectors to ensure cursors represent the same position across implementations.

This design makes an explicit tradeoff: a common word counts as much as a rare word because identifying rare words requires corpus statistics. In a workspace-partitioned store, corpora are small enough that field weighting and proximity provide most of the signal that inverse document frequency (IDF) would provide.

## The index narrows; it never answers

Without an index, a match query scans the entity and evaluates every row, with execution time proportional to the number of rows in the entity. Declaring search fields declares a term index, maintained on every write on both sides, that a match query uses to narrow its candidates instead of scanning the entity. Everything that applies to [secondary indexes](../reference/proto-options.md#secondary-indexes) applies to the term index: it is invisible and additive, and a new declaration backfills in the background. Queries scan correctly until the pass finishes and the `indexing` event fires, and index usage never changes query results.

The index maps each term in a searchable field to a posting (`term → row key`), stored in the same secondary-index storage as other indexes on both sides. A query retrieves candidates from the posting range of its most selective term—using a prefix range for the typeahead term—and then re-reads, re-checks, and re-ranks the candidate rows directly.

The engine discovers the most selective term dynamically:

1. The engine checks each term against a growing candidate cap.
2. The first term that fits under the cap provides the shortest posting list.
3. The remaining terms narrow the candidate set only when reading their postings costs less than re-checking the rows they would remove.

A term so common that reading its postings costs more than re-checking rows is skipped. A term that appears in most rows yields most rows as candidates. The query remains correct, but operates closer to a full scan, and a single common word cannot be narrowed. Skipping is safe because candidates are only narrowed, never completed. Fewer narrowing passes require re-checking more rows, but never return different rows. Relevance ranking, not the index, orders the best hits first. A multi-word query costs what its most selective word costs, so adding a word never slows down a search.

Restricting the index to postings preserves the query contract. An index-served match returns the exact results of a full scan because the rows determine the final match. The index stores no positions or frequencies, allowing the analyzer to add ranking signals without invalidating stored entries.

The candidate-narrowing design has two important constraints:

- **Rows with more than 256 distinct terms store no postings:** A write updates one posting per distinct term in the row's searchable fields (typically a few dozen per row). A row with more than 256 distinct terms stores a single overflow marker instead and is evaluated by every search. Every term probe includes overflow rows in its candidate set unconditionally. Large documents degrade narrowing efficiency rather than result accuracy, adding a candidate to every search instead of becoming unfindable past their 256th term. The 256-term limit is a format constant so the client and server agree on which rows overflow.
- **Only matches required by every result can narrow candidates:** A match inside an `or` or `not` expression does not constrain candidates on its own. Such queries fall back to a full scan, remaining correct without index acceleration.

Collaborative-text fields are searchable even though the engine rejects them as value-index components. Value indexes reject these fields to avoid entry churn caused by rewriting index entries on every keystroke. A search posting stores a term rather than a full value, so a keystroke that does not change the set of terms produces no index updates. The engine analyzes the field as plain text.

## What it deliberately does not do

The search engine excludes the following features:

- **Typo tolerance:** Matching within an edit distance requires walking a term dictionary per query or using precomputed fuzzy structures. Both approaches require large structures that are difficult to keep identical across implementations. Prefix matching handles in-progress typing, but misspelled words do not match.
- **Phrase and boolean term queries:** Terms are conjunctive only. Proximity ranking rewards adjacency without requiring a phrase operator, and `or` expressions exist at the query condition level.
- **Corpus-aware scoring:** Scoring based on corpus statistics conflicts with the requirement for identical results across client and server.

Each excluded feature is additive. New analyzer options and condition shapes version their fingerprints, and an index whose fingerprint changes rebuilds automatically.
