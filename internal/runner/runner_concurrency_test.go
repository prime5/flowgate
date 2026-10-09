package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// countingDispatcher is a stand-in for the MCP server so these tests
// exercise only the runner's own state handling.
type countingDispatcher struct{}

func (countingDispatcher) Call(_ context.Context, name string, _ json.RawMessage) (any, bool, error) {
	return map[string]any{"ok": name}, false, nil
}

func textEnv(conv, text string) Envelope {
	return Envelope{
		ConversationID: conv,
		Channel:        ChannelMeta{Type: ChannelCLI, Source: "test"},
		Payload:        Payload{Role: RoleUser, Text: text},
	}
}

// Each Handle appends a request and a response. If two turns on the same
// conversation both load the same snapshot, the later Save overwrites the
// earlier one and turns vanish from history. With the per-conversation
// lock every turn must be present. Run with -race.
func TestRunner_ConcurrentTurnsOnOneConversationAreNotLost(t *testing.T) {
	const turns = 40
	r := New(NewMemoryStore(), countingDispatcher{})
	r.MaxHistory = 10 * turns // keep the cap out of this test

	var wg sync.WaitGroup
	for i := 0; i < turns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.Handle(context.Background(), textEnv("conv-1", fmt.Sprintf("m%d", i))); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}(i)
	}
	wg.Wait()

	s, err := r.GetSession(context.Background(), "conv-1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(s.History), 2*turns; got != want {
		t.Fatalf("history has %d envelopes, want %d: concurrent turns overwrote each other", got, want)
	}
}

func TestRunner_DifferentConversationsDoNotShareState(t *testing.T) {
	r := New(NewMemoryStore(), countingDispatcher{})
	var wg sync.WaitGroup
	for c := 0; c < 5; c++ {
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				_, _ = r.Handle(context.Background(), textEnv(fmt.Sprintf("conv-%d", c), "hi"))
			}(c)
		}
	}
	wg.Wait()
	for c := 0; c < 5; c++ {
		s, err := r.GetSession(context.Background(), fmt.Sprintf("conv-%d", c))
		if err != nil {
			t.Fatal(err)
		}
		if len(s.History) != 16 {
			t.Fatalf("conv-%d history = %d, want 16", c, len(s.History))
		}
	}
}

func TestRunner_HistoryIsCappedKeepingNewest(t *testing.T) {
	r := New(NewMemoryStore(), countingDispatcher{})
	r.MaxHistory = 10

	for i := 0; i < 20; i++ {
		if _, err := r.Handle(context.Background(), textEnv("conv-cap", fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := r.GetSession(context.Background(), "conv-cap")
	if len(s.History) != 10 {
		t.Fatalf("history length = %d, want cap of 10", len(s.History))
	}
	// History is [req, resp, req, resp...]; the newest request is m19.
	if got := s.History[len(s.History)-2].Payload.Text; got != "m19" {
		t.Fatalf("newest request kept = %q, want m19", got)
	}
	if got := s.History[0].Payload.Text; got != "m15" {
		t.Fatalf("oldest request kept = %q, want m15", got)
	}
}

func TestRunner_LockMapDoesNotLeak(t *testing.T) {
	r := New(NewMemoryStore(), countingDispatcher{})
	for i := 0; i < 50; i++ {
		_, _ = r.Handle(context.Background(), textEnv(fmt.Sprintf("c%d", i), "x"))
	}
	r.lockMu.Lock()
	n := len(r.locks)
	r.lockMu.Unlock()
	if n != 0 {
		t.Fatalf("%d idle conversation locks retained, want 0", n)
	}
}
