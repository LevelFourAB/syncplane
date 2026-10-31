// Fractional indexing: order keys you can always insert between, for
// list-reordering PATCHes that touch one row instead of renumbering. An
// in-repo port of the public-domain reference algorithm from David
// Greenspan's "Implementing Fractional Indexing" (the `fractional-indexing`
// package), base-62, dependency-free.
//
// Keys are `integer part + fraction digits`. The integer part's first char
// encodes its length ('a' + 2 digits up through 'z' + 27; capitals mirror
// downward for negatives), so keys compare correctly as plain strings —
// the same code-unit order the query evaluators and KV scans use.

export const BASE_62_DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

const SMALLEST_INTEGER = 'A00000000000000000000000000';
const INTEGER_ZERO = 'a0';

/** midpoint of two fraction strings: a < b ("" = no bound). */
function midpoint(a: string, b: string, digits: string): string {
	const zero = digits.charAt(0);
	if (b !== '' && a >= b) {
		throw new Error(`midpoint: ${a} >= ${b}`);
	}
	if (a.slice(-1) === zero || (b !== '' && b.slice(-1) === zero)) {
		throw new Error('midpoint: trailing zero');
	}
	if (b !== '') {
		// Strip the common prefix and recurse past it.
		let n = 0;
		while (n < b.length && (n < a.length ? a.charAt(n) : zero) === b.charAt(n)) {
			n++;
		}
		if (n > 0) {
			return b.slice(0, n) + midpoint(a.slice(n), b.slice(n), digits);
		}
	}
	// First digits (or lack thereof) differ.
	const digitA = a === '' ? 0 : digits.indexOf(a.charAt(0));
	const digitB = b === '' ? digits.length : digits.indexOf(b.charAt(0));
	if (digitB - digitA > 1) {
		const midDigit = Math.round(0.5 * (digitA + digitB));
		return digits.charAt(midDigit);
	}
	// Consecutive digits.
	if (b.length > 1) {
		return b.slice(0, 1);
	}
	// b is empty or a single digit: a's digit, then the midpoint between
	// a's rest and the upper bound.
	return digits.charAt(digitA) + midpoint(a.slice(1), '', digits);
}

function validateInteger(int: string): void {
	if (int.length !== integerLengthOfHead(int.charAt(0))) {
		throw new Error(`invalid integer part of order key: ${int}`);
	}
}

function integerLengthOfHead(head: string): number {
	if (head >= 'a' && head <= 'z') {
		return head.charCodeAt(0) - 'a'.charCodeAt(0) + 2;
	}
	if (head >= 'A' && head <= 'Z') {
		return 'Z'.charCodeAt(0) - head.charCodeAt(0) + 2;
	}
	throw new Error(`invalid order key head: ${head}`);
}

function integerPartOf(key: string): string {
	const integerPartLength = integerLengthOfHead(key.charAt(0));
	if (integerPartLength > key.length) {
		throw new Error(`invalid order key: ${key}`);
	}
	return key.slice(0, integerPartLength);
}

function validateOrderKey(key: string, digits: string): void {
	if (key === SMALLEST_INTEGER) {
		throw new Error(`invalid order key: ${key}`);
	}
	const i = integerPartOf(key);
	for (let n = 1; n < key.length; n++) {
		if (!digits.includes(key.charAt(n))) {
			throw new Error(`invalid order key (character outside the digits): ${key}`);
		}
	}
	const f = key.slice(i.length);
	if (f.slice(-1) === digits.charAt(0)) {
		throw new Error(`invalid order key (trailing zero): ${key}`);
	}
}

/** The next whole integer (..., "a1", "a2") or undefined past the maximum. */
function incrementInteger(x: string, digits: string): string | undefined {
	validateInteger(x);
	const head = x.charAt(0);
	const rest = x.slice(1).split('');
	let carry = true;
	for (let i = rest.length - 1; carry && i >= 0; i--) {
		const d = digits.indexOf(rest.at(i) ?? '') + 1;
		if (d === digits.length) {
			rest[i] = digits.charAt(0);
		} else {
			rest[i] = digits.charAt(d);
			carry = false;
		}
	}
	if (carry) {
		if (head === 'Z') {
			return 'a' + digits.charAt(0);
		}
		if (head === 'z') {
			return undefined;
		}
		const h = String.fromCharCode(head.charCodeAt(0) + 1);
		if (h > 'a') {
			rest.push(digits.charAt(0));
		} else {
			rest.pop();
		}
		return h + rest.join('');
	}
	return head + rest.join('');
}

/** The previous whole integer or undefined past the minimum. */
function decrementInteger(x: string, digits: string): string | undefined {
	validateInteger(x);
	const head = x.charAt(0);
	const rest = x.slice(1).split('');
	let borrow = true;
	for (let i = rest.length - 1; borrow && i >= 0; i--) {
		const d = digits.indexOf(rest.at(i) ?? '') - 1;
		if (d === -1) {
			rest[i] = digits.slice(-1);
		} else {
			rest[i] = digits.charAt(d);
			borrow = false;
		}
	}
	if (borrow) {
		if (head === 'a') {
			return 'Z' + digits.slice(-1);
		}
		if (head === 'A') {
			return undefined;
		}
		const h = String.fromCharCode(head.charCodeAt(0) - 1);
		if (h < 'Z') {
			rest.push(digits.slice(-1));
		} else {
			rest.pop();
		}
		return h + rest.join('');
	}
	return head + rest.join('');
}

/**
 * Returns a key strictly between a and b. null bounds are open: (null, k)
 * is "before k", (k, null) is "after k", (null, null) is the first key.
 */
export function generateKeyBetween(
	a: string | null,
	b: string | null,
	digits: string = BASE_62_DIGITS,
): string {
	if (a !== null) {
		validateOrderKey(a, digits);
	}
	if (b !== null) {
		validateOrderKey(b, digits);
	}
	if (a !== null && b !== null && a >= b) {
		throw new Error(`${a} >= ${b}`);
	}
	if (a === null) {
		if (b === null) {
			return INTEGER_ZERO;
		}
		const ib = integerPartOf(b);
		const fb = b.slice(ib.length);
		if (ib === SMALLEST_INTEGER) {
			return ib + midpoint('', fb, digits);
		}
		if (ib < b) {
			return ib;
		}
		const res = decrementInteger(ib, digits);
		if (res === undefined) {
			throw new Error('cannot decrement any more');
		}
		if (res === SMALLEST_INTEGER) {
			// The smallest integer is reserved and never a key on its own, so
			// step into its fraction. b is a whole integer here, so every
			// fraction of the smallest integer still sorts before it.
			return res + midpoint('', '', digits);
		}
		return res;
	}
	if (b === null) {
		const ia = integerPartOf(a);
		const fa = a.slice(ia.length);
		const i = incrementInteger(ia, digits);
		return i === undefined ? ia + midpoint(fa, '', digits) : i;
	}
	const ia = integerPartOf(a);
	const fa = a.slice(ia.length);
	const ib = integerPartOf(b);
	const fb = b.slice(ib.length);
	if (ia === ib) {
		return ia + midpoint(fa, fb, digits);
	}
	const i = incrementInteger(ia, digits);
	if (i === undefined) {
		throw new Error('cannot increment any more');
	}
	if (i < b) {
		return i;
	}
	return ia + midpoint(fa, '', digits);
}

/**
 * Returns n keys strictly between a and b, evenly spread (recursively
 * bisecting so neighbors keep room for future inserts).
 */
export function generateNKeysBetween(
	a: string | null,
	b: string | null,
	n: number,
	digits: string = BASE_62_DIGITS,
): string[] {
	if (n === 0) {
		return [];
	}
	if (n === 1) {
		return [ generateKeyBetween(a, b, digits) ];
	}
	if (b === null) {
		let c = generateKeyBetween(a, b, digits);
		const result = [ c ];
		for (let i = 0; i < n - 1; i++) {
			c = generateKeyBetween(c, b, digits);
			result.push(c);
		}
		return result;
	}
	if (a === null) {
		let c = generateKeyBetween(a, b, digits);
		const result = [ c ];
		for (let i = 0; i < n - 1; i++) {
			c = generateKeyBetween(a, c, digits);
			result.push(c);
		}
		return result.reverse();
	}
	const mid = Math.floor(n / 2);
	const c = generateKeyBetween(a, b, digits);
	return [
		...generateNKeysBetween(a, c, mid, digits),
		c,
		...generateNKeysBetween(c, b, n - mid - 1, digits),
	];
}
