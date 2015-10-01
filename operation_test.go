package scheduler

import "testing"

type testOp struct{ T int }

func (testOp) Execute() {}

func TestOperationClosure(t *testing.T) {
	ok := false
	tf := func() {
		ok = true
	}
	cl := Closure(tf)
	cl.Execute()
	if !ok {
		t.Fatal("operation failed")
	}
}
