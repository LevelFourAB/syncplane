// Package fractional makes order keys: strings that sort a list and that
// always have room for one more key between any two of them. A move or an
// insert writes one key to one element instead of renumbering the list.
//
// A key is an integer part followed by fraction digits, all base 62
// ([Digits]). The first character of the integer part encodes its length, so
// keys compare correctly as plain byte strings — the order storage, queries
// and the TypeScript client use.
//
// The TypeScript client (`client/src/fractional.ts` and `client/src/ordering.ts`)
// makes the same keys from the same inputs, and the vectors under
// `testdata/fractional/` hold both to that. A key made here and a key made in a
// browser can sit in the same list.
package fractional

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"
)

// Digits is the alphabet of an order key, in ascending order.
const Digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

const (
	zeroDigit       = '0'
	smallestInteger = "A00000000000000000000000000"
	integerZero     = "a0"
)

var (
	// ErrInvalidKey reports a bound that is not an order key: an unknown head
	// character, an integer part longer than the key, a character outside
	// [Digits], a fraction that ends in '0', or the reserved smallest integer.
	ErrInvalidKey = errors.New("fractional: invalid order key")
	// ErrOutOfOrder reports a lower bound that does not sort before the upper
	// bound.
	ErrOutOfOrder = errors.New("fractional: lower bound is not below upper bound")

	// errNoInteger reports a step past the largest or smallest integer part.
	// Valid bounds never reach it: the largest integer is followed by its own
	// fractions, and the smallest is reserved so that one sits below every key.
	errNoInteger = errors.New("fractional: no integer part past the bound")
)

// KeyBetween returns an order key that sorts strictly after a and strictly
// before b.
//
// An empty bound is open: KeyBetween("", k) is a key before k,
// KeyBetween(k, "") a key after k, and KeyBetween("", "") the first key of an
// empty list, "a0". The result is a function of a and b alone, so two callers
// that insert into the same gap get the same key; [KeyBetweenFor] gives them
// different ones.
//
// KeyBetween returns [ErrInvalidKey] when a bound is not an order key and
// [ErrOutOfOrder] when a >= b. Valid bounds always have a key between them.
func KeyBetween(a, b string) (string, error) {
	if a != "" {
		if err := validateOrderKey(a); err != nil {
			return "", err
		}
	}
	if b != "" {
		if err := validateOrderKey(b); err != nil {
			return "", err
		}
	}
	if a != "" && b != "" && a >= b {
		return "", fmt.Errorf("%w: %q >= %q", ErrOutOfOrder, a, b)
	}
	if a == "" {
		if b == "" {
			return integerZero, nil
		}
		ib := integerPartOf(b)
		fb := b[len(ib):]
		if ib == smallestInteger {
			return withMidpoint(ib, "", fb)
		}
		if ib < b {
			return ib, nil
		}
		res, ok := decrementInteger(ib)
		if !ok {
			return "", fmt.Errorf("%w: before %q", errNoInteger, b)
		}
		if res == smallestInteger {
			// The smallest integer is reserved and never a key on its own, so
			// step into its fraction. b is a whole integer here, so every
			// fraction of the smallest integer still sorts before it.
			return withMidpoint(res, "", "")
		}
		return res, nil
	}
	ia := integerPartOf(a)
	fa := a[len(ia):]
	if b == "" {
		if i, ok := incrementInteger(ia); ok {
			return i, nil
		}
		return withMidpoint(ia, fa, "")
	}
	ib := integerPartOf(b)
	fb := b[len(ib):]
	if ia == ib {
		return withMidpoint(ia, fa, fb)
	}
	i, ok := incrementInteger(ia)
	if !ok {
		return "", fmt.Errorf("%w: after %q", errNoInteger, a)
	}
	if i < b {
		return i, nil
	}
	return withMidpoint(ia, fa, "")
}

// NKeysBetween returns n order keys in ascending order, all strictly between
// a and b.
//
// Empty bounds are open, as for [KeyBetween]. Between two bounds the keys are
// spread by bisection, so each pair of neighbors keeps room for later inserts;
// against an open bound they run one after another. A zero or negative n
// returns nil. The errors are those of [KeyBetween].
func NKeysBetween(a, b string, n int) ([]string, error) {
	switch {
	case n <= 0:
		return nil, nil
	case n == 1:
		k, err := KeyBetween(a, b)
		if err != nil {
			return nil, err
		}
		return []string{k}, nil
	case b == "":
		out := make([]string, 0, n)
		c := a
		for range n {
			var err error
			if c, err = KeyBetween(c, b); err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, nil
	case a == "":
		out := make([]string, n)
		c := b
		for i := n - 1; i >= 0; i-- {
			var err error
			if c, err = KeyBetween(a, c); err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	mid := n / 2
	c, err := KeyBetween(a, b)
	if err != nil {
		return nil, err
	}
	lo, err := NKeysBetween(a, c, mid)
	if err != nil {
		return nil, err
	}
	hi, err := NKeysBetween(c, b, n-mid-1)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	out = append(out, lo...)
	out = append(out, c)
	return append(out, hi...), nil
}

// KeyBetweenFor returns an order key strictly between a and b that also
// depends on actor, the position a client writes into a movable list
// (`order_field`).
//
// Pass the writer's client id as actor. Two actors that insert into the same
// gap get distinct keys whose order depends only on the two actor ids, and
// each key still leaves room for inserts on both sides. In a gap too narrow to
// hold the actor's suffix the result is a function of a and b alone, and two
// equal keys fall back to the element-id tie-break readers sort by.
//
// Empty bounds are open and the errors are those of [KeyBetween]. An empty
// actor is a valid actor. actor is read as UTF-16 code units, the way the
// TypeScript client reads it; invalid UTF-8 reads as U+FFFD.
func KeyBetweenFor(a, b, actor string) (string, error) {
	base, err := KeyBetween(a, b)
	if err != nil {
		return "", err
	}
	candidate := base + jitter(actor)
	// The suffix only lengthens base, so the candidate stays above a. When
	// base is a prefix of b the suffix can reach b; a deeper midpoint inside
	// (base, b) is then the only key left that is still in (a, b).
	if b != "" && candidate >= b {
		return KeyBetween(base, b)
	}
	return candidate, nil
}

// jitter returns six base-62 fraction digits derived from actor: FNV-1a over
// its UTF-16 code units, expanded with an LCG. The last digit is never '0', so
// the result keeps a key free of a trailing zero. All arithmetic is uint32 to
// match Math.imul and >>> 0 in client/src/ordering.ts.
func jitter(actor string) string {
	h := uint32(0x811c9dc5)
	for _, unit := range utf16.Encode([]rune(actor)) {
		h ^= uint32(unit)
		h *= 0x01000193
	}
	s := h
	if s == 0 {
		s = 1
	}
	out := make([]byte, 6)
	for i := range out {
		s = s*1103515245 + 12345
		out[i] = Digits[s%62]
	}
	if out[5] == zeroDigit {
		out[5] = '1'
	}
	return string(out)
}

// withMidpoint returns integer followed by the midpoint of fractions a and b.
func withMidpoint(integer, a, b string) (string, error) {
	m, err := midpoint(a, b)
	if err != nil {
		return "", err
	}
	return integer + m, nil
}

// midpoint returns fraction digits strictly between fractions a and b, where
// an empty b is no upper bound.
func midpoint(a, b string) (string, error) {
	if b != "" && a >= b {
		return "", fmt.Errorf("%w: fraction %q >= %q", ErrOutOfOrder, a, b)
	}
	if (a != "" && a[len(a)-1] == zeroDigit) || (b != "" && b[len(b)-1] == zeroDigit) {
		return "", fmt.Errorf("%w: fraction ends in a zero", ErrInvalidKey)
	}
	if b != "" {
		// Strip the common prefix (a reads as zero-padded) and recurse past it.
		n := 0
		for n < len(b) && digitOrZero(a, n) == b[n] {
			n++
		}
		if n > 0 {
			return withMidpoint(b[:n], a[min(n, len(a)):], b[n:])
		}
	}
	digitA := 0
	if a != "" {
		digitA = strings.IndexByte(Digits, a[0])
	}
	digitB := len(Digits)
	if b != "" {
		digitB = strings.IndexByte(Digits, b[0])
	}
	if digitB-digitA > 1 {
		// Rounds half up, as Math.round does in the TypeScript client.
		return string(Digits[(digitA+digitB+1)/2]), nil
	}
	if len(b) > 1 {
		return b[:1], nil
	}
	return withMidpoint(string(Digits[digitA]), a[min(1, len(a)):], "")
}

// digitOrZero returns s[i], or the zero digit past the end of s.
func digitOrZero(s string, i int) byte {
	if i < len(s) {
		return s[i]
	}
	return zeroDigit
}

// integerLengthOfHead returns the length of the integer part that head
// starts, or false when head starts none.
func integerLengthOfHead(head byte) (int, bool) {
	switch {
	case head >= 'a' && head <= 'z':
		return int(head-'a') + 2, true
	case head >= 'A' && head <= 'Z':
		return int('Z'-head) + 2, true
	}
	return 0, false
}

// integerPartOf returns the integer part of a key validateOrderKey accepted.
func integerPartOf(key string) string {
	n, _ := integerLengthOfHead(key[0])
	return key[:n]
}

func validateOrderKey(key string) error {
	if key == smallestInteger {
		return fmt.Errorf("%w: %q is reserved", ErrInvalidKey, key)
	}
	n, ok := integerLengthOfHead(key[0])
	if !ok {
		return fmt.Errorf("%w: %q has no integer head", ErrInvalidKey, key)
	}
	if n > len(key) {
		return fmt.Errorf("%w: %q is shorter than its integer part", ErrInvalidKey, key)
	}
	for i := 1; i < len(key); i++ {
		if strings.IndexByte(Digits, key[i]) < 0 {
			return fmt.Errorf("%w: %q has a character outside the digits", ErrInvalidKey, key)
		}
	}
	if len(key) > n && key[len(key)-1] == zeroDigit {
		return fmt.Errorf("%w: %q ends in a zero", ErrInvalidKey, key)
	}
	return nil
}

// incrementInteger returns the next integer part after x, or false when x is
// the largest.
func incrementInteger(x string) (string, bool) {
	head := x[0]
	rest := []byte(x[1:])
	carry := true
	for i := len(rest) - 1; carry && i >= 0; i-- {
		d := strings.IndexByte(Digits, rest[i]) + 1
		if d == len(Digits) {
			rest[i] = zeroDigit
		} else {
			rest[i] = Digits[d]
			carry = false
		}
	}
	if !carry {
		return string(head) + string(rest), true
	}
	switch head {
	case 'Z':
		return integerZero, true
	case 'z':
		return "", false
	}
	h := head + 1
	if h > 'a' {
		rest = append(rest, zeroDigit)
	} else {
		rest = rest[:len(rest)-1]
	}
	return string(h) + string(rest), true
}

// decrementInteger returns the integer part before x, or false when x is the
// smallest.
func decrementInteger(x string) (string, bool) {
	largest := Digits[len(Digits)-1]
	head := x[0]
	rest := []byte(x[1:])
	borrow := true
	for i := len(rest) - 1; borrow && i >= 0; i-- {
		d := strings.IndexByte(Digits, rest[i]) - 1
		if d == -1 {
			rest[i] = largest
		} else {
			rest[i] = Digits[d]
			borrow = false
		}
	}
	if !borrow {
		return string(head) + string(rest), true
	}
	switch head {
	case 'a':
		return "Z" + string(largest), true
	case 'A':
		return "", false
	}
	h := head - 1
	if h < 'Z' {
		rest = append(rest, largest)
	} else {
		rest = rest[:len(rest)-1]
	}
	return string(h) + string(rest), true
}
