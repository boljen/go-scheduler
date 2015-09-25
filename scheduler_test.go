package scheduler

import (
	"testing"
	"time"
)

var defaultConfig = Config{}

func TestWorker(t *testing.T) {
	ch := make(chan Operation)

	go func() {
		ch <- &testOp{}
		close(ch)
	}()

	worker(ch)
}

func TestNew(t *testing.T) {
	New(Config{})
	rl := New(Config{
		Workers: 10,
	})
	time.Sleep(time.Second)

	rl.Stop()
}

func TestScheduler_Add(t *testing.T) {
	o := &testOp{}
	rl := New(Config{
		Workers:      4,
		MaxQueueSize: 2,
	})

	if err := rl.Add(1, o); err != ErrInvalidPriority {
		t.Fatal("wrong priority")
	}

	rl.InitPriority(1, 1)
	if err := rl.Add(1, o); err != nil {
		t.Fatal(err)
	}

	if err := rl.Add(1, o); err != ErrPriorityCapacity {
		t.Fatal("expected ErrPriorityCapacity")
	}

	rl.InitPriority(2, 0)
	if err := rl.Add(2, o); err != nil {
		t.Fatal(err)
	}

	if err := rl.Add(2, o); err != ErrMaxCapacity {
		t.Fatal("expected ErrMaxCapacity, got", err)
	}

	rl.Pause(time.Second)

	time.Sleep(time.Second)

}

func TestScheduler_InitPriority(t *testing.T) {
	rl := New(Config{})
	rl.InitPriority(10, 100)
	if rl.opl[0].priority != 10 {
		t.Fatal("wrong opl entry")
	}

	rl.InitPriority(5, 100)
	if rl.opl[0].priority != 5 || rl.opl[1].priority != 10 {
		t.Fatal("wrong opl entry")
	}

	rl.InitPriority(5, 50)
	if rl.opl[0].maxops != 50 {
		t.Fatal("wrong maxops entry")
	}
}

func TestSchedulerSetMinimumCallback(t *testing.T) {
	rl := New(Config{})
	rl.InitPriority(10, 100)
	if err := rl.SetMinimumCallback(Priority(1), 5, nil); err != ErrInvalidPriority {
		t.Fatal("expected invalid priority error")
	}

	done := false
	if err := rl.SetMinimumCallback(10, 5, func(p Priority) {
		done = true
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if !done {
		t.Fatal("should have launched the minimum callback	")
	}
}
