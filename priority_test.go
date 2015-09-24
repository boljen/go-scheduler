package scheduler

import "testing"

func TestNewPriorityMetadata(t *testing.T) {
	p := newPriorityMetadata(12, 50)
	if p.weight != 12 {
		t.Fatal("wrong weight")
	}
	if p.maxops != 50 {
		t.Fatal("wrong maximum operations")
	}
	p = newPriorityMetadata(5, 0)
	if p.maxops < 100000 {
		t.Fatal("wrong maximum operations")
	}
}

func TestPriorityOperations(t *testing.T) {
	o1 := &testOp{}
	o2 := &testOp{}
	o3 := &testOp{}
	o4 := &testOp{}

	p := newPriorityMetadata(1, 2)
	if _, ok := p.GetOperation(); ok {
		t.Fatal("should not be ok")
	}
	if err := p.AddOperation(o1); err != nil {
		t.Fatal(err)
	}
	if err := p.AddOperation(o2); err != nil {
		t.Fatal(err)
	}
	if err := p.AddOperation(o3); err != ErrPriorityCapacity {
		t.Fatal(err)
	}
	if op, ok := p.GetOperation(); !ok || op != o1 {
		t.Fatal("should return operation 1")
	}
	if err := p.AddOperation(o4); err != nil {
		t.Fatal(err)
	}
	if op, ok := p.GetOperation(); !ok || op != o2 {
		t.Fatal("should return operation 2")
	}
	if op, ok := p.GetOperation(); !ok || op != o4 {
		t.Fatal("should return operation 4")
	}
	if _, ok := p.GetOperation(); ok {
		t.Fatal("should not be ok")
	}
}
