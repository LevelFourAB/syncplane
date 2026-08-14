/**
 * The text analyzer behind full-text search: the pipeline that turns field
 * text and query strings into the terms the match evaluator and the search
 * index agree on.
 *
 * This is a literal mirror of the Go implementation (engine/search);
 * cross-language conformance vectors live in testdata/search/vectors.json.
 * Term-for-term agreement between the two is the contract that makes a
 * locally evaluated match and a server evaluated match return the same rows,
 * so every stage is defined without reference to locale, ICU, or any other
 * environment-dependent tables:
 *
 * 1. Unicode NFKC normalization, so full-width forms, ligatures and
 *    compatibility characters compare equal to their plain spellings.
 * 2. Unicode full lowercase (Default Case Conversion, no locale) —
 *    String.prototype.toLowerCase.
 * 3. Unless `keepDiacritics` is set: NFD, drop the combining marks of the
 *    Combining Diacritical Marks block (U+0300–U+036F), NFC. The range
 *    restriction folds Latin, Greek and Cyrillic accents while leaving
 *    scripts whose marks carry meaning — Thai, Devanagari, Hebrew, Arabic —
 *    untouched.
 * 4. Segmentation. Runs of letters, digits and combining marks form one term
 *    each, capped at 64 code points. Code points in the CJK ranges below
 *    instead form runs emitted as character bigrams (a one-character run is
 *    emitted as is), which makes text in scripts written without word
 *    separators searchable without a dictionary.
 *
 * The CJK ranges are fixed by this module rather than taken from Unicode
 * script properties: U+3005–U+3007, U+3040–U+30FF (Hiragana, Katakana),
 * U+3400–U+4DBF, U+4E00–U+9FFF, U+F900–U+FAFF, U+20000–U+2FA1F (Han),
 * U+AC00–U+D7A3 (Hangul syllables).
 *
 * A term's position is its index in the returned array; scoring measures
 * proximity as index distance.
 */

/** Cap, in code points, past which a term is truncated. */
const MAX_TERM_LENGTH = 64;

/**
 * Analyzer configuration. An absent options object is the default analyzer,
 * and what a schema that sets no analyzer options gets.
 */
export interface AnalyzeOptions {
	/**
	 * Disables the diacritic fold, so "café" and "cafe" analyze to distinct
	 * terms.
	 *
	 * @defaultValue false
	 */
	keepDiacritics?: boolean;
}

/** One term of an analyzed query string. */
export interface QueryTerm {
	term: string;

	/**
	 * Marks a term that matches any indexed term it prefixes rather than only
	 * itself. Only the final term of a query can carry it, and only when the
	 * query ends in the term itself — a trailing space or other separator
	 * makes the final term exact.
	 */
	prefix: boolean;
}

/**
 * Returns the terms of `text` in document order. Empty when the text yields
 * no terms, including the empty string.
 */
export function analyze(text: string, options?: AnalyzeOptions): string[] {
	const terms: string[] = [];
	tokenize(normalize(text, options), (term) => terms.push(term));
	return terms;
}

/**
 * Returns the terms of a query string, in order, with the final term marked
 * as a prefix when the query ends inside it. Empty when the query yields no
 * terms.
 */
export function analyzeQuery(
	query: string,
	options?: AnalyzeOptions,
): QueryTerm[] {
	const terms: QueryTerm[] = [];
	tokenize(normalize(query, options), (term, atEnd) => terms.push({ term, prefix: atEnd }));
	return terms;
}

const nonAscii = /[^\x00-\x7f]/;

function normalize(s: string, options?: AnalyzeOptions): string {
	// All-ASCII text is its own NFKC and NFD form and carries no combining
	// mark, so every stage but the case mapping is provably identity on it \u2014
	// and over ASCII the Unicode default case conversion is A-Z to a-z and
	// nothing else. Skipping the three normalization passes there is what
	// keeps analysis of ordinary prose off the slow path, since every
	// searchable field of every candidate row is analyzed on every ranked read.
	if (!nonAscii.test(s)) return s.toLowerCase();
	s = s.normalize('NFKC').toLowerCase();
	if (!options?.keepDiacritics) {
		s = s
			.normalize('NFD')
			.replace(/[\u0300-\u036f]/g, '')
			.normalize('NFC');
	}
	return s;
}

const enum CharClass {
	Skip,
	Word,
	Cjk,
}

const wordChar = /[\p{L}\p{M}\p{N}]/u;

function classify(ch: string): CharClass {
	const cp = ch.codePointAt(0)!;
	if (cp < 0x80) {
		// No ASCII code point is CJK or a combining mark, so the class is
		// decided by letter-or-digit alone — and answering that from the code
		// point avoids a property-class regex per character of every document.
		const word = (cp >= 0x61 && cp <= 0x7a) || (cp >= 0x41 && cp <= 0x5a)
			|| (cp >= 0x30 && cp <= 0x39);
		return word ? CharClass.Word : CharClass.Skip;
	}
	if (isCjk(cp)) {
		return CharClass.Cjk;
	}
	if (wordChar.test(ch)) {
		return CharClass.Word;
	}
	return CharClass.Skip;
}

// Literal ranges rather than Unicode script properties so both languages
// agree independent of their Unicode table versions, and so U+30FC
// (KATAKANA-HIRAGANA PROLONGED SOUND MARK, script Common) stays inside a
// Katakana run instead of splitting it.
function isCjk(cp: number): boolean {
	return (
		(cp >= 0x3040 && cp <= 0x30ff)
		|| (cp >= 0x3400 && cp <= 0x4dbf)
		|| (cp >= 0x4e00 && cp <= 0x9fff)
		|| (cp >= 0xf900 && cp <= 0xfaff)
		|| (cp >= 0x20000 && cp <= 0x2fa1f)
		|| (cp >= 0xac00 && cp <= 0xd7a3)
		|| (cp >= 0x3005 && cp <= 0x3007)
	);
}

/**
 * Calls `emit` once per term of the normalized string, in document order.
 * `atEnd` marks a term whose final code point is the final code point of the
 * input, which is what lets analyzeQuery treat it as a prefix.
 *
 * Every term is a SLICE of the normalized string rather than characters
 * reassembled from it: a run of one class is contiguous by construction, and
 * neither the length cap nor the bigram split does anything but cut one.
 */
function tokenize(s: string, emit: (term: string, atEnd: boolean) => void): void {
	let runStart = -1;
	let runPoints = 0;
	let runClass = CharClass.Skip;

	const flush = (end: number): void => {
		const run = s.slice(runStart, end);
		const atEnd = end === s.length;
		switch (runClass) {
			case CharClass.Word:
				emit(runPoints > MAX_TERM_LENGTH ? cutPoints(run, MAX_TERM_LENGTH) : run, atEnd);
				break;
			case CharClass.Cjk:
				emitBigrams(run, runPoints, atEnd, emit);
				break;
		}
		runStart = -1;
		runPoints = 0;
	};

	let at = 0;
	for (const ch of s) {
		const c = classify(ch);
		if (c !== runClass) {
			if (runStart >= 0) flush(at);
			runClass = c;
			runStart = c === CharClass.Skip ? -1 : at;
			runPoints = 0;
		}
		if (c !== CharClass.Skip) runPoints++;
		at += ch.length;
	}
	if (runStart >= 0) flush(s.length);
}

/**
 * Splits an unsegmented run into overlapping character pairs. A one-character
 * run is emitted as itself, so a single CJK character is findable rather than
 * lost.
 */
function emitBigrams(
	run: string,
	points: number,
	atEnd: boolean,
	emit: (term: string, atEnd: boolean) => void,
): void {
	if (points === 1) {
		emit(run, atEnd);
		return;
	}
	let lo = 0;
	let mid = 0;
	for (const ch of run) {
		if (mid > 0) {
			const hi = mid + ch.length;
			emit(run.slice(lo, hi), atEnd && hi === run.length);
			lo = mid;
		}
		mid += ch.length;
	}
}

/** The first `points` code points of `s`, which must hold at least that many. */
function cutPoints(s: string, points: number): string {
	let at = 0;
	let seen = 0;
	for (const ch of s) {
		if (seen === points) break;
		at += ch.length;
		seen++;
	}
	return s.slice(0, at);
}
