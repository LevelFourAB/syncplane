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
	return tokenize(normalize(text, options)).map(t => t.term);
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
	const tokens = tokenize(normalize(query, options));
	return tokens.map((t, i) => ({
		term: t.term,
		prefix: i === tokens.length - 1 && t.atEnd,
	}));
}

function normalize(s: string, options?: AnalyzeOptions): string {
	s = s.normalize('NFKC').toLowerCase();
	if (!options?.keepDiacritics) {
		s = s
			.normalize('NFD')
			.replace(/[\u0300-\u036f]/g, '')
			.normalize('NFC');
	}
	return s;
}

interface Token {
	term: string;
	/**
	 * Marks a token whose final code point is the final code point of the
	 * input, which is what lets analyzeQuery treat it as a prefix.
	 */
	atEnd: boolean;
}

const enum CharClass {
	Skip,
	Word,
	Cjk,
}

const wordChar = /[\p{L}\p{M}\p{N}]/u;

function classify(ch: string): CharClass {
	if (isCjk(ch.codePointAt(0)!)) {
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

function tokenize(s: string): Token[] {
	const tokens: Token[] = [];
	let run: string[] = [];
	let runClass = CharClass.Skip;

	const flush = (atEnd: boolean): void => {
		switch (runClass) {
			case CharClass.Word: {
				const term = run.length > MAX_TERM_LENGTH
					? run.slice(0, MAX_TERM_LENGTH)
					: run;
				tokens.push({ term: term.join(''), atEnd });
				break;
			}
			case CharClass.Cjk: {
				if (run.length === 1) {
					tokens.push({ term: run[0]!, atEnd });
					break;
				}
				for (let i = 0; i + 2 <= run.length; i++) {
					tokens.push({
						term: run[i]! + run[i + 1]!,
						atEnd: atEnd && i + 2 === run.length,
					});
				}
				break;
			}
		}
		run = [];
	};

	for (const ch of s) {
		const c = classify(ch);
		if (c !== runClass) {
			flush(false);
			runClass = c;
		}
		if (c !== CharClass.Skip) {
			run.push(ch);
		}
	}
	flush(true);
	return tokens;
}
