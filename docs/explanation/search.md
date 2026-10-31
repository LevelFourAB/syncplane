# Full-text search

This document explains how full-text search returns identical results across the client and the server. Deterministic analysis, row-local relevance ranking, and candidate-narrowing indexes enforce this consistency.

## The constraint: two evaluators, one answer

A match query runs in two places. The client evaluates the query offline against the local subset provided by sync through the optimistic overlay. The server evaluates the query against the complete dataset for `queryServer` and the `Query` RPC.

The same predicate must select the same rows in the same order wherever it runs. This consistency lets an application fall back from the server to the client without shifting result order.

This constraint eliminates standard search engine implementations:

- **No International Components for Unicode (ICU), locales, or host tokenizers:** `Intl.Segmenter` in a browser and a Unicode segmentation library in Go track different tables. Two browsers can disagree before either disagrees with the server. Analyzer behavior must be defined precisely to guarantee identical results across implementations.
- **No corpus statistics, such as BM25 and term frequency-inverse document frequency (TF-IDF):** These algorithms score terms based on how rare they are across the entire collection. The client contains only a synced subset, so a locally computed score differs from the server score for the same row. Any ranking signal must come from the row and the query alone.
- **No native engines on either side:** SQLite FTS5, PostgreSQL `tsvector`, and browser-based engines use different tokenizers and ranking algorithms. Storage adapters cannot alter query results, so native engines cannot define search semantics. A deployment that requires a dedicated search engine can feed one from the [change feed](../how-to/change-feed.md) on the server.

The search engine uses document-local ranking rules. This design provides byte-for-byte agreement between a Go process and a browser tab, verified by shared test vectors in `testdata/search/`.

## The analyzer

Analysis converts field text and query strings into terms through four steps that run identically in Go and TypeScript:

1. **Unicode Normalization Form KC (NFKC) normalization:** Full-width forms, ligatures, and compatibility characters normalize to plain spellings (`ﬁle` = `file`, `Ｔｏｋｙｏ` = `Tokyo`).
2. **Unicode full lowercase without locale:** The analyzer maps characters using standard JavaScript `toLowerCase` behavior, mirrored in Go with `golang.org/x/text`.
3. **Diacritic folding:** Unless the schema preserves diacritics, the analyzer decomposes text, removes combining marks in the range U+0300 through U+036F, and recomposes characters. This range folds accents in Latin, Greek, and Cyrillic scripts (`café` = `cafe`) while preserving combining marks that carry lexical meaning in scripts such as Thai, Devanagari, and Hebrew.
4. **Segmentation:** Runs of letters, digits, and combining marks form individual terms. Code points in Chinese, Japanese, and Korean (CJK) ranges pair into character bigrams (`日本語` becomes `日本` and `本語`), enabling dictionary-free search for scripts without word separators. The specification defines fixed code-point ranges rather than Unicode script properties to prevent drift across Unicode versions.

The analyzer does not use stemming or stopword lists. Both require language-specific tables that must match across implementations.

Prefix matching provides the primary benefit of stemming for interactive applications. The final term of a query matches as a prefix when the query ends inside the term, so `searc` matches `searching` while you type. A trailing space makes the final term exact, indicating that you finished typing the term.

## Ranking from the row alone

A match query requires every term to occur in at least one searchable field of the row. Relevance ordering ranks matching rows using a tuple computed solely from the row and the query, compared in this order:

1. **Prefix match count:** The count of terms found only as prefixes of longer terms. Fewer prefix matches rank higher, prioritizing exact hits over typeahead hits.
2. **Field priority:** The first declared field containing every term. Searchable fields are declared in order of importance. Matching all terms in a title field ranks above matching all terms in a body field or across multiple fields.
3. **Token window:** The smallest token window covering every term in that field. For example, `red apple` ranks "red apple pie" higher than "red big juicy apple".
4. **Earliest position:** The earliest matched position in that field.
5. **Token count:** The total number of matching tokens in the row.

Ties fall back to the row key. The tuple packs into a single 48-bit integer, allowing relevance positions to use the existing cursor system. A page cursor from a relevance-ordered query consists of the rank plus the row key, generated on one side and replayed on the other. Shared test vectors pin the integer values to ensure that cursors represent the same position across implementations.

This design makes an explicit tradeoff: a common word counts as much as a rare word because identifying rare words requires corpus statistics. In a workspace-partitioned store, corpora are small enough that field weighting and proximity provide most of the signal that inverse document frequency (IDF) would provide.

## The index narrows; it never answers

Without an index, a match query scans the entity and evaluates every row, with execution time proportional to the row count. Declaring search fields creates a term index, maintained on every write on both client and server. A match query uses this index to narrow candidate rows instead of scanning the full entity. The rules for [secondary indexes](../reference/proto-options.md#secondary-indexes) apply to the term index: the index is invisible and additive, and a new declaration backfills in the background. Queries scan correctly until backfill finishes and the `indexing` event fires. Index usage never alters query results.

The index maps each term in a searchable field to a posting (`term → row key`). A query retrieves candidates from the posting range of its most selective term—using a prefix range for the typeahead term—and then re-reads, re-checks, and re-ranks candidate rows directly.

The server keeps postings in its storage adapter's secondary-index storage. The client keeps them as ordinary key-value records instead. Rare terms share page records, each of which holds the terms of one entry range with their row keys. Each common term has a head record, which holds its posting count and its most recent row keys, and chunk records of a few hundred sorted row keys each. Each row has one term list. An IndexedDB index stores one index row per entry together with the key of the record that holds it, and WebKit stores each index row twice. With one entry per distinct term per row, those index rows were most of the disk space of a text-heavy store. Packed lists store each posting as the bytes of its row key, so the same text-heavy store takes less than a quarter of the space on WebKit.

The price is in how writes scale. A write touches the records of the terms that changed, not one record per row. An edit that changes a few words writes a few records, which costs less than rewriting all of the row's index entries. A pull that inserts or deletes hundreds of text-heavy rows touches most of the vocabulary, so it rewrites most pages and the heads of the common terms. Pages are what keep that to a few hundred records instead of one per term. A bootstrap and an index build collect the postings of hundreds or thousands of rows before they write them, so each record is written once per batch.

The engine discovers the most selective term dynamically:

1. The engine checks each term against a growing candidate cap.
2. The first term that fits under the cap provides the shortest posting list.
3. The remaining terms narrow the candidate set only when reading their postings costs less than re-checking the rows they would remove.

The engine skips a term when reading its postings costs more than re-checking rows directly. A term that appears in most rows yields most rows as candidates. The query remains correct, but operates closer to a full scan, and cannot narrow a single common word. Skipping is safe because indexes only narrow candidate sets and never finalize matches. Fewer narrowing passes require re-checking more rows, but never return different rows. Relevance ranking, not the index, orders the best hits first. A multi-word query costs what its most selective word costs, so adding a word never slows down a search.

Restricting the index to postings preserves the query contract. An index-served match returns the exact results of a full scan because rows determine the final match. The index stores no positions or frequencies, so the analyzer can add ranking signals without invalidating stored entries.

The candidate-narrowing design has two important constraints:

- **Rows with more than 256 distinct terms store no postings:** A write updates one posting per distinct term in the row's searchable fields (typically a few dozen per row). A row with more than 256 distinct terms stores a single overflow marker instead; every search evaluates this row. Every term probe includes overflow rows in its candidate set unconditionally. Large documents degrade narrowing efficiency rather than result accuracy: they add a candidate to every search instead of becoming unfindable past their 256th term. The 256-term limit is a format constant so that the client and server agree on which rows overflow.
- **Only matches required by every result can narrow candidates:** A match inside an `or` or `not` expression does not constrain candidates on its own. These queries fall back to a full scan and return correct results without index acceleration.

Collaborative-text fields are searchable even though the engine rejects them as value-index components. Value indexes reject these fields to avoid entry churn caused by rewriting index entries on every keystroke. A search posting stores a term rather than a full value, so a keystroke that does not change the set of terms produces no index updates. The engine analyzes the field as plain text.

## What it deliberately does not do

The search engine excludes the following features:

- **Typo tolerance:** Matching within an edit distance requires walking a term dictionary per query or using precomputed fuzzy structures. Both approaches require large structures that are difficult to keep identical across implementations. Prefix matching handles in-progress typing, but misspelled words do not match.
- **Phrase and boolean term queries:** Terms are conjunctive only. Proximity ranking rewards adjacency without requiring a phrase operator, and `or` expressions exist at the query condition level.
- **Corpus-aware scoring:** Scoring based on corpus statistics conflicts with the requirement for identical results across client and server.

Each excluded feature is additive. New analyzer options and condition shapes version their fingerprints, and an index whose fingerprint changes rebuilds automatically.
