package scheduler

// Operation is an operation that can be executed by the scheduler.
type Operation interface {
	Execute()
}
