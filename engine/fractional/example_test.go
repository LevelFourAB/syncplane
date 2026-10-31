package fractional_test

import (
	"fmt"

	"github.com/levelfourab/syncplane/engine/fractional"
)

func Example() {
	first, _ := fractional.KeyBetween("", "")
	second, _ := fractional.KeyBetween(first, "")
	between, _ := fractional.KeyBetween(first, second)
	before, _ := fractional.KeyBetween("", first)
	fmt.Println(before, first, between, second)
	// Output: Zz a0 a0V a1
}

func ExampleKeyBetweenFor() {
	// Move an element between neighbors at "a0" and "a1", as client
	// "client-1" would.
	key, err := fractional.KeyBetweenFor("a0", "a1", "client-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(key > "a0" && key < "a1")
	// Output: true
}
