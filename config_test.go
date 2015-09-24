package scheduler

import "testing"

func TestConfigMaxops(t *testing.T) {
	cfg := Config{}
	if cfg.maxops() != 4294967295 {
		t.Fatal("wrong default max operations")
	}
	cfg.MaxQueueSize = 5
	if cfg.maxops() != 5 {
		t.Fatal("wrong maxops")
	}
	if cfg.rate() != 1 {
		t.Fatal("wrong default rate")
	}
	cfg.OPS = 4
	if cfg.rate() != 0.25 {
		t.Fatal("wrong rate")
	}
}
