// Package scheduler implements a scheduler for rate limited operations
// using a prioritized queue.
//
// Use case
//
// This package is built to schedule operations against rate limited API's.
// More specifically it's meant for applications which need to perform both
// real-time operations as well as a hefty amount of background scraping.
//
// Scheduler
//
// The scheduler attempts to streamline the execution of operations by using
// a continuous ticker at the allowed operation rate. At each tick, exactly one
// operation will be executed. The order in which operations are executed is
// first-in-first-out, but higher-priority operations will be executed first.
// When no operations are available, a fallback operation will be called.
//
// By evenly spreading out the operations, there's a reasonable guarantee that
// the highest priority operation will be executed within deterministic time
// of one rate interval.
//
// Workers
//
// A scheduler can be configured to use one or multiple workers. Workers are
// simply goroutines that continuously take operations from a buffered channel
// and execute them.
//
// The advantage of using workers is that the main tick loop of the scheduler
// won't be blocked by any single operation. It will only block when the
// buffered channel transferring operations to workers has been filled entirely,
// which will be the case when all of the workers together can't process the
// flood of operations.
//
// The size of the buffered channel can be configured and does have an effect
// on the accuracy of the scheduler. It should be kept as small as possible.
// The moment an operation is sent to the buffered channel, it will seize to
// exist inside the scheduler.
//
// When configured to use 0 workers, the scheduler will execute all operations
// synchronously from within the loop that processes ticks from the internal
// ticker. The disadvantage here is that it will cause the main loop to block,
// the advantage is that it won't execute any expensive context switching.
//
// Bursts
//
// The Scheduler has no built-in support for bursts of operations.
// This is because the way the rate is calculated highly depends on the
// service that is being used and requires awareness of the strategy used.
//
// Implementing burst behavior can be done by lowering the allocated rate of
// operations of the scheduler and using a separate system to allocate those
// additional operations. It can also be done by 'saving up' operations through
// the Fallback operation.
package scheduler

// (TODO): Provide hooks for "queue entries above/below x" for both the
// global scheduler as well as the various priorities. This should be done
// by creating a new Counter struct that provides this functionality and
// which can be used by both the Priorities as well as the Scheduler itself.
// (TODO): Optionally make the Scheduler stand-by until it receives an operation.
// (TODO): Make the scheduler use an implementation of the "Tickable" interface.
// (TODO): Create exhaustive unit tests.

import (
	"errors"
	"sync"
	"time"
)

// These are possible errors that can be returned by the Scheduler instance.
var (
	ErrInvalidPriority  = errors.New("Scheduler: Priority is not initizlaized")
	ErrMaxCapacity      = errors.New("Scheduler: Maximum Queue Capacity Exceeded")
	ErrPriorityCapacity = errors.New("Priority: Maximum Priority-Specific Queue Capacity Exceeded")
)

func worker(ch chan Operation) {
	for {
		op, more := <-ch
		if !more {
			break
		}
		op.Execute()
	}
}

// Scheduler schedules operations against a specific rate limit.
type Scheduler struct {
	pause        time.Time      // The time until the scheduler must pause.
	usingWorkers bool           // Whether separate goroutine workers are used.
	opqueue      chan Operation // Queue of pending operations for the workers.
	fallback     Operation      // Fallback operation in case no operations are available.
	stop         chan bool      // Used to stop the ticker goroutine.
	ticker       *time.Ticker   // The internal ticker.

	pai bool // Priority Auto Initialization
	pdc int  // Priority default capacity

	mu     *sync.Mutex                    // Mutex
	pl     map[Priority]*priorityMetadata // Mapped priority list.
	opl    []*priorityMetadata            // Ordered priority list.
	curops uint32                         // total operations inside the scheduler queue.
	maxops uint32                         // max is the maximum amount of operations that can be in the scheduler.
}

// New creates a newly initialized Scheduler instance.
func New(c Config) *Scheduler {
	s := &Scheduler{
		mu:       new(sync.Mutex),
		pl:       make(map[Priority]*priorityMetadata, 5),
		opl:      make([]*priorityMetadata, 0, 5),
		pai:      c.PriorityAutoInit,
		pdc:      c.PriorityDefaultCapacity,
		maxops:   c.maxops(),
		fallback: c.Fallback,
		stop:     make(chan bool),
	}

	// When using workers we must initialize the workers and the operation queue.
	if c.Workers > 0 {
		s.opqueue = make(chan Operation, c.opbuf())
		s.usingWorkers = true
		for i := 0; i < c.Workers; i++ {
			go worker(s.opqueue)
		}
	}

	// Start a new ticker based on the configured rate and start processing ticks.
	s.ticker = time.NewTicker(time.Duration(float32(time.Second) / c.rate()))
	go s.processTicks()

	return s
}

// processTicks processes ticks in a background goroutine.
func (s *Scheduler) processTicks() {
	for {
		select {
		case t := <-s.ticker.C:
			if s.pause.Before(t) {
				s.execOp()
			}
		case <-s.stop:
			return
		}
	}
}

func (s *Scheduler) execOp() {
	o := s.getNextOp()
	if o == nil {
		if s.fallback == nil {
			return
		}
		s.fallback.Execute()
		return
	}

	if s.usingWorkers {
		s.opqueue <- o
	} else {
		o.Execute()
	}
}

// getOperation removes and returns the next pending operation.
func (s *Scheduler) getNextOp() Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < len(s.opl); i++ {
		op, ok := s.opl[i].GetOperation()
		if ok {
			s.curops--
			return op
		}
	}
	return nil
}

// InitPriority initializes a new priority and specifies the maximum
// operation queue for the specific priority. If maxops equals 0, no
// priority-specific limit will be applied.
func (s *Scheduler) InitPriority(p Priority, maxops int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the priority already exists, simply overwrite the maxops.
	// Make sure to lock the mutex to avoid any race-conditions.
	if pr, ok := s.pl[p]; ok {
		pr.maxops = getMaxops(maxops)
		return
	}

	pm := newPriorityMetadata(p, maxops)
	s.pl[p] = pm

	// Reorder the ordered priority list slice from back to front.
	// This is most likely the most efficient algorithm.
	s.opl = append(s.opl, pm)
	for i := len(s.opl) - 1; i > 0; i-- {
		if s.opl[i].priority < s.opl[i-1].priority {
			s.opl[i] = s.opl[i-1]
			s.opl[i-1] = pm
		}
	}
}

// Add adds a new operation to the scheduler.
// The priority must be initialized unless automated initialization is enabled.
func (s *Scheduler) Add(p Priority, o Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.curops >= s.maxops {
		return ErrMaxCapacity
	}

	pm, err := s.getPriorityMetadata(p)
	if err != nil {
		return err
	}

	if err := pm.AddOperation(o); err != nil {
		return err
	}

	s.curops++
	return nil
}

// SetMinimumCallback sets a callback that will be executed each time
// the amount of registered operations for a specific priority reaches
// the specified minimum. Only one callback per priority can be set.
// This will fail when the priority is not initialized and automated
// initialization is disabled.
func (s *Scheduler) SetMinimumCallback(p Priority, minimum int, cb func(Priority)) error {
	pm, err := s.getPriorityMetadata(p)
	if err != nil {
		return err
	}

	pm.Minimum = uint32(minimum)
	pm.MinimumCallback = cb
	if pm.Minimum >= pm.curops {
		pm.MinimumCallback(pm.priority)
	}
	return nil
}

func (s *Scheduler) getPriorityMetadata(p Priority) (*priorityMetadata, error) {
	pm, ok := s.pl[p]
	if !ok {
		if !s.pai {
			return nil, ErrInvalidPriority
		}
		s.InitPriority(p, s.pdc)
		return s.pl[p], nil
	}
	return pm, nil
}

// Pause pauses the scheduler for the specified duration.
// Use this when the rate limit has been exceeded and when you know
// the moment where the next window will become active.
func (s *Scheduler) Pause(d time.Duration) {
	s.pause = time.Now().Add(d)
}

// Stop stops the scheduler and all of it's background processes.
// This might lead to skipping operations that are currently queued.
// The operation is final. The scheduler shouldn't be used after
// Stop has been called.
func (s *Scheduler) Stop() {
	s.ticker.Stop()
	close(s.opqueue)
	s.stop <- true
}
