---
name: documentation
description: House rules for writing or revising documentation in this repo — Go doc comments, TSDoc, body comments, and the Diátaxis prose under docs/. Load before writing a doc comment on a new exported identifier, editing an existing one, adding a comment inside a function, or adding to docs/; also when reviewing someone else's comments or prose, or when asked to "document", "add docs", "write doc comments" or "clean up comments".
---

# Documentation & comment guidelines

Documentation in this codebase is written for a stranger arriving in 6–12
months, and every location has exactly one reader. A doc comment on an exported
identifier serves a consumer who can only see the API; a comment inside a
function body serves a maintainer who is about to change that code; `docs/`
serves whichever Diátaxis mode the file lives in. Writing for the wrong reader
is the failure this document exists to prevent — most commonly by leaking
implementation detail and decision history into a consumer's doc comment, where
it goes stale the moment the implementation changes.

Two rules carry most of the weight. **The boundary test** (§1): a consumer doc
may only state facts observable from the caller's side. **The rewrite test**
(§2): a sentence that stops being true when the body is rewritten was never part
of the contract. Rationale, alternatives considered, and benchmarks are valuable
— they belong in the commit message or in `docs/explanation/`, not on the API
surface.

The sections below are ordered as a working sequence: §1–§4 decide what a
comment may contain, §5–§9 what it must contain and how it reads per language,
§10–§12 the cutting pass. Sections 5 and 11 are the two to reread when a comment
feels finished but long.

---

## 1. Know which reader you are writing for

Every comment has exactly one audience. Decide before writing; never mix.

| Location                              | Audience                                                           | May contain                                                     |
| ------------------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------------- |
| Doc comment on an exported identifier | **Consumer** — arrives cold, from outside the package, with a task | Contract only: behaviour, inputs, outputs, errors, cost, safety |
| Comment inside a function body        | **Maintainer** — is changing this code                             | Why this approach and not the obvious alternative               |
| Prose docs (`/docs`, README)          | Depends on Diátaxis mode — see §3                                  | Per mode                                                        |
| Commit message / PR description       | Reviewer, archaeologist                                            | All rationale, alternatives considered, benchmarks, trade-offs  |

**The boundary test.** A consumer doc may only state facts observable from the
caller's side of the API. If the reader could not detect the fact by calling the
function, it does not belong there.

```go
// BAD — describes the implementation, not the contract
// Cache stores entries in a sync.Map, which was the only thing fast enough
// for our write-heavy load. A plain mutex was the honest option but it
// contended badly under benchmark.

// GOOD — describes the contract
// Cache is a bounded, in-memory key/value store with LRU eviction.
// Safe for concurrent use by multiple goroutines.
```

**Where the deliberation goes.** Reasoning about alternatives is genuinely
valuable — it just belongs in the commit message, not the API surface. When you
find yourself writing "we chose X because Y", move that sentence to the commit
body and delete it from the doc.

---

## 2. The rewrite test

> If a sentence stops being true when the implementation is replaced, it was
> never part of the contract.

Apply this to every sentence in a doc comment. Anything that fails is either
deleted or demoted to a body comment.

---

## 3. Diátaxis mode is an input, not a choice

The mode is determined by _where you are writing_, and you do not get to blend
modes to be helpful.

- **Doc comments are always Reference.** Austere, complete, impersonal. No
  encouragement, no rationale, no tutorial voice, no "you'll probably want to".
- **Tutorial and How-to** live in `/docs`. They may address the reader as "you"
  and may state goals.
- **Explanation** is where rationale, history, and trade-offs live. Link to it
  from a doc comment rather than inlining it.

Sparing exception: a short `@remarks` (TS) or trailing paragraph (Go) may carry
one piece of rationale when it changes how a consumer uses the API — e.g. "This
is O(n) in the number of subscribers; prefer `BatchNotify` above ~100."

---

## 4. Never restate the signature

A doc comment that re-expresses the identifier and types in English carries zero
information and is worse than no comment, because it makes readers stop and read
it.

```ts
// BAD
/**
 * Gets a user by ID.
 * @param id - The ID of the user.
 * @returns A promise of the user.
 */
```

Delete any `@param` whose text is the parameter name expanded into a sentence.
Delete any `@returns` that restates the return type. Prefer no tag to an empty
tag.

In TypeScript, never repeat a type in prose — the type is already there and will
drift. If a constraint can be encoded in the type system, encode it instead of
documenting it. A doc comment reading "must be non-empty" is a smell.

---

## 5. Say the things that are actually missing

Cutting the filler exists to make room for these. Include every item that
applies; a doc comment that omits them is incomplete no matter how long it is.

- **Errors** — which values, as which sentinels or types, and how to test for them
- **Edge inputs** — behaviour on nil, zero values, empty slices/strings
- **Ownership** — whether arguments are retained, mutated, or copied; who closes what
- **Concurrency** — whether the value is safe for concurrent use
- **Blocking** — whether the call blocks; how it responds to context cancellation
- **Units and ranges** — milliseconds vs seconds, inclusive vs exclusive, time zone, encoding
- **Ordering** — guaranteed, or explicitly not guaranteed
- **Defaults** — what a zero value or omitted option means
- **Cost** — complexity or I/O, when it would surprise a caller

---

## 6. Examples outrank prose

A compiling example is worth three paragraphs of description.

- Go: an `Example` function in the `_test.go` file, which is verified by `go test`.
- TypeScript: `@example` containing real, type-checking code.

Write the example for the common case, not the exhaustive case.

---

## 7. Go specifics

- Begin a doc comment with the identifier: `// ParseDuration parses ...`.
- One sentence summary, then a blank `//` line, then detail. The first sentence
  appears alone in package listings, so it must stand by itself.
- Document error behaviour by naming the sentinel or type, so callers know what
  to pass to `errors.Is` / `errors.As`.
- Document concurrency safety on every exported type that holds state. This is
  the most frequently omitted and most frequently needed fact in Go docs.
- Document the meaning of the zero value when the type is usable at zero.
- Package comments go in one file (`doc.go` for anything substantial) and
  describe what the package is for, not how it is built.
- Don't use `@`-style tags. Godoc has no tags; write sentences.

```go
// GOOD
// ParseDuration parses a duration string such as "1h30m" or "-250ms".
//
// Valid units are ns, us, ms, s, m, and h. Values may be negative.
// ParseDuration returns ErrSyntax if s is not a valid duration string,
// and ErrRange if the value overflows time.Duration.
func ParseDuration(s string) (time.Duration, error)
```

---

## 8. TypeScript specifics

- TSDoc, `/** */`, on exported declarations.
- Summary line first, no tag. Detail in following paragraphs.
- Use `@param` only to add information the name and type cannot carry: units,
  ranges, ownership, what happens when it is omitted.
- Use `@throws` / document rejection reasons — async failure modes are the most
  under-documented thing in TS codebases.
- Use `@defaultValue` for optional options-bag fields.
- Use `@deprecated` with a concrete migration path, not just a notice.
- Document whether an object argument is retained or copied, and whether a
  returned object is a live view or a snapshot.

```ts
// GOOD
/**
 * Retries `fn` until it resolves or `attempts` is exhausted, waiting
 * `2 ** n * 100` milliseconds between attempts.
 *
 * Rejects with the error from the final attempt; earlier errors are discarded.
 * An `AbortError` is never retried and rejects immediately. `fn` is called at
 * least once even when `attempts` is 0.
 */
export function retry<T>(fn: () => Promise<T>, attempts = 3): Promise<T>;
```

---

## 9. Body comments

Comment the **why**, never the **what**. The code already says what it does; if
it does not, fix the code instead of narrating it.

Write a body comment only when a maintainer would otherwise be tempted to
"simplify" the code and break it. Good triggers: a non-obvious ordering
dependency, a workaround for an upstream bug (link it), a deliberate deviation
from the surrounding pattern, a performance decision with a measurement behind
it.

```go
// BAD — narrates the code
// increment the counter
count++

// GOOD — protects a non-obvious decision
// Reset before Close: Close flushes, and a flush of stale state
// re-enqueues the batch. See upstream issue #4412.
b.Reset()
```

---

## 10. Prohibited language

**Filler and hedging.** `note that`, `it's important to`, `simply`, `just`,
`basically`, `essentially`, `of course`, `arguably`, `under the hood`,
`in order to` (use "to"), `at the end of the day`.

**Self-narration.** Any phrasing that reports the author's decision process
rather than the code's behaviour: `the only thing that`, `the honest option`,
`we went with`, `it turns out that`, `I chose`, `this felt cleaner`. If the
information matters, it goes in the commit message.

**Empty superlatives.** `robust`, `powerful`, `seamless`, `elegant`, `simple`,
`easy`, `flexible`, `blazingly fast`. These are claims a reader cannot verify
and will resent.

**Comparative asides.** Do not mention alternatives that were considered and
rejected in a consumer doc. There is no alternative in the reader's world.

**Apology and reassurance.** The reader is not anxious. Do not comfort them.

---

## 11. Revision pass

Drafting is not the problem; cutting is. After writing any doc comment, run
this checklist and edit accordingly.

1. Does the reader learn anything the signature does not already tell them? If
   not, delete the whole comment.
2. Is every sentence still true if the body is rewritten from scratch? Delete or
   demote the ones that aren't.
3. Which single sentence can be deleted with the least loss? Delete it. Repeat
   until the next deletion would cost real information.
4. Is any sentence about the author rather than the reader? Move it to the
   commit message.
5. Check §5. Which required facts are missing? Add them.
6. Would a consumer be surprised by anything in production — an error type, a
   blocking call, a mutation, a cost? If so it is undocumented.

Target: shorter than the first draft and containing strictly more information.

---

## 12. Worked corrections

```go
// BEFORE
// NewClient creates a new client. It takes a config and returns a client.
// We use a struct for config rather than variadic options because it was
// the only thing that stayed readable as the option count grew — options
// were the honest option early on but this scales better. The client is
// robust and handles errors gracefully.
func NewClient(cfg Config) (*Client, error)

// AFTER
// NewClient returns a Client configured by cfg.
//
// The returned Client is safe for concurrent use and holds a connection
// pool; callers must Close it. NewClient does not dial — the first request
// establishes the connection. It returns ErrInvalidConfig if cfg.Endpoint
// is empty.
func NewClient(cfg Config) (*Client, error)
```

```ts
// BEFORE
/**
 * This function simply takes a list of items and a callback, and basically
 * maps over them in parallel. Note that we use Promise.allSettled under the
 * hood rather than Promise.all — allSettled was really the only thing that
 * let us return partial results, which felt like the honest option here.
 * It's a powerful and flexible utility.
 *
 * @param items - The items.
 * @param fn - The callback function.
 * @returns A promise of the results.
 */

// AFTER
/**
 * Applies `fn` to every item concurrently, with at most `limit` calls in
 * flight.
 *
 * Never rejects. Each result is `{ ok: true, value }` or `{ ok: false, error }`,
 * in input order regardless of completion order. A rejection in one call does
 * not cancel the others.
 *
 * @param limit - Maximum concurrent calls. @defaultValue 8
 */
```
