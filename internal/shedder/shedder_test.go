package shedder

import (
	"sync"
	"testing"
)

func TestShedder_AdmitsUpToCapacity(t *testing.T) {
	s := New(2)
	if !s.Acquire() {
		t.Fatal("1st acquire should succeed")
	}
	if !s.Acquire() {
		t.Fatal("2nd acquire should succeed")
	}
	if s.Acquire() {
		t.Fatal("3rd acquire should be shed — capacity is 2")
	}
}

func TestShedder_ReleaseFreesASlot(t *testing.T) {
	s := New(1)
	if !s.Acquire() {
		t.Fatal("expected acquire to succeed")
	}
	if s.Acquire() {
		t.Fatal("expected second acquire to be shed")
	}
	s.Release()
	if !s.Acquire() {
		t.Fatal("expected acquire to succeed again after release")
	}
}

func TestShedder_InFlightReflectsOccupancy(t *testing.T) {
	s := New(3)
	s.Acquire()
	s.Acquire()
	if got := s.InFlight(); got != 2 {
		t.Fatalf("expected InFlight()=2, got %d", got)
	}
	s.Release()
	if got := s.InFlight(); got != 1 {
		t.Fatalf("expected InFlight()=1 after one release, got %d", got)
	}
}

func TestShedder_ConcurrentUseNeverExceedsCapacity(t *testing.T) {
	s := New(10)
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Acquire() {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted > 10 {
		t.Fatalf("admitted %d concurrent requests, more than capacity 10", admitted)
	}
}
