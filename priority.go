package scheduler

// Priority indicates a specific priority.
// The higher the value, the higher the priority.
type Priority int

// (TODO): Refactor weight to "p"

// priorityMetadata stores metadata of a priority inside the Scheduler.
type priorityMetadata struct {
	priority Priority
	oplist   map[int]Operation

	maxops uint32 // Maximum amount of operations
	curops uint32 // Current amount of operations

	first int
	last  int

	Minimum         uint32
	MinimumCallback func(Priority)
}

func getMaxops(maxops int) uint32 {
	maxops32 := uint32(maxops)
	if maxops32 == 0 {
		maxops32--
	}
	return maxops32
}

func newPriorityMetadata(p Priority, maxops int) *priorityMetadata {
	return &priorityMetadata{
		priority: p,
		oplist:   make(map[int]Operation),
		curops:   0,
		maxops:   getMaxops(maxops),
	}
}

// AddOperation adds a new operation to the priority.
// It might return ErrPriorityCapacity when the priority-specific queue is full.
func (p *priorityMetadata) AddOperation(o Operation) error {
	if p.curops == p.maxops {
		return ErrPriorityCapacity
	}
	p.curops++
	p.oplist[p.last] = o
	p.last++
	return nil
}

// GetOperation returns the next operation of this priority.
// If no operation is available, the returned bool will be false.
func (p *priorityMetadata) GetOperation() (Operation, bool) {
	if p.last == p.first {
		return nil, false
	}
	o := p.oplist[p.first]
	delete(p.oplist, p.first)
	p.first++
	p.curops--
	if p.curops == p.Minimum && p.MinimumCallback != nil {
		p.MinimumCallback(p.priority)
	}
	return o, true
}
