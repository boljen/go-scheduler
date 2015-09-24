package scheduler

// Config configures the Ratelimitter.
type Config struct {
	// OPS stands for operations per second and is the amount of operations
	// that the scheduler should allow during the course of one second.
	OPS float32

	// Workers is the amount of goroutine workers that process operations.
	// If this is 0 then no worker goroutines will be used and operations will
	// be executed synchronously from within the main tick loop.
	Workers int

	// MaxQueueSize is the maximum size of the operations queue.
	// This maximum is always enforced, even when priority-specific
	// queue's have a higher maximum queue size.
	MaxQueueSize int

	// ExecutionBufferSize is the capacity of the buffered channel which
	// forwards operations to the various workers.
	// This should be as low as possible to keep the scheduler in sync with
	// the remote rate limit window as much as possible.
	ExecutionBufferSize int

	// Fallback is an (optional) operation that will be executed every time that
	// no other operations are available. It will be executed from within the
	// same loop that processes ticks even if there are workers available.
	// This is by design and allows using this hook to refill the operations
	// queue whenever it's empty.
	Fallback Operation
}

func (c Config) rate() float32 {
	if c.OPS <= 0 {
		return 1
	}
	return c.OPS
}

func (c Config) maxops() uint32 {
	if c.MaxQueueSize <= 0 {
		return ^uint32(0)
	}
	return uint32(c.MaxQueueSize)
}

func (c Config) opbuf() int {
	if c.ExecutionBufferSize <= 0 {
		return 1
	}
	return c.ExecutionBufferSize
}
