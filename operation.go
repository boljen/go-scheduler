package scheduler

// Operation is an operation that can be executed by the scheduler.
type Operation interface {
	Execute()
}

// Closure turns a closure into the Operation interface.
// It should do so with virtually no overhead.
func Closure(fx func()) Operation {
	return operationClosure(fx)
}

type operationClosure func()

func (f operationClosure) Execute() {
	f()
}
