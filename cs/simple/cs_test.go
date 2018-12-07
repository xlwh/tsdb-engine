package simple

import (
	"fmt"
	"testing"
)

func Test_cs(t *testing.T) {
	cs := New(10)
	cs.Push(5, float64(-10))
	for i := 1; i < 10; i++ {
		cs.Push(10+int64(i), float64(i))
	}

	it := cs.Iter()

	for it.Next() {
		t, v := it.Values()
		fmt.Println(t, v)
	}
}
