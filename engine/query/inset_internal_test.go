package query

import (
	"math"
	"math/big"
	"testing"
)

// A membership test over many values answers from a hash of canonical keys
// instead of comparing each one, so scalarKey has to agree with equalsScalar
// on every pair — including the number/bigint pairs that compare across kinds,
// and the values that compare equal to nothing.
//
// The failure this catches is silent: a key that separates two values
// equalsScalar calls equal turns an `in` over 16 values into a no-match, while
// the same query over 15 keeps working.
func TestScalarKeyMatchesEqualsScalar(t *testing.T) {
	num := func(f float64) rowValue { return rowValue{kind: rvNumber, num: f} }
	bigv := func(i int64) rowValue { return rowValue{kind: rvBigint, big: big.NewInt(i)} }
	values := []rowValue{
		{kind: rvString, str: ""},
		{kind: rvString, str: "a"},
		{kind: rvString, str: "b"},
		{kind: rvBool, b: false},
		{kind: rvBool, b: true},
		num(0), num(math.Copysign(0, -1)), num(1), num(-1), num(1.5), num(-1.5),
		num(1 << 53), num(math.MaxInt64), num(math.NaN()),
		num(math.Inf(1)), num(math.Inf(-1)),
		bigv(0), bigv(1), bigv(-1), bigv(1 << 53), bigv(math.MaxInt64),
		{kind: rvOther},
	}
	for _, a := range values {
		for _, b := range values {
			equal := equalsScalar(a, b)
			ka, oka := scalarKey(a)
			kb, okb := scalarKey(b)
			// A value with no canonical key is never found in the set, so the
			// only agreement it owes is that it equals nothing that has one.
			if !oka || !okb {
				if equal && oka != okb {
					t.Errorf("%v == %v but only one has a canonical key", a, b)
				}
				continue
			}
			if same := ka == kb; same != equal {
				t.Errorf("%v vs %v: equalsScalar = %v, keys %q/%q equal = %v",
					a, b, equal, ka, kb, same)
			}
		}
	}
}

// The set is built above a threshold, so the two paths have to answer the same
// question the same way. Same values, one list short and one padded past the
// threshold with values nothing matches.
func TestInNodeSetAgreesWithScan(t *testing.T) {
	values := []condValue{
		{kind: rvString, str: "a"},
		{kind: rvNumber, num: 7},
		{kind: rvBigint, big: big.NewInt(9)},
		{kind: rvBool, b: true},
	}
	padded := append([]condValue{}, values...)
	for i := 0; len(padded) < inSetThreshold+4; i++ {
		padded = append(padded, condValue{kind: rvString, str: "pad" + string(rune('a'+i))})
	}
	short := newInNode(nil, values)
	long := newInNode(nil, padded)
	if short.set != nil {
		t.Fatalf("a %d-value list should scan, not hash", len(values))
	}
	if long.set == nil {
		t.Fatalf("a %d-value list should hash, not scan", len(padded))
	}
	rows := []rowValue{
		{kind: rvString, str: "a"}, {kind: rvString, str: "z"},
		{kind: rvNumber, num: 7}, {kind: rvNumber, num: 7.5},
		{kind: rvBigint, big: big.NewInt(7)}, {kind: rvBigint, big: big.NewInt(9)},
		{kind: rvNumber, num: 9}, {kind: rvBool, b: true}, {kind: rvBool, b: false},
		{kind: rvNumber, num: math.NaN()},
	}
	for _, row := range rows {
		var scanned bool
		for _, cv := range padded {
			if equalsScalar(row, cv) {
				scanned = true
				break
			}
		}
		key, ok := scalarKey(row)
		hashed := false
		if ok {
			_, hashed = long.set[key]
		}
		if hashed != scanned {
			t.Errorf("%v: scan = %v, set = %v", row, scanned, hashed)
		}
	}
}
