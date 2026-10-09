package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prime5/flowgate/internal/mcp"
)

func TestEnvelope_JSONSerialization(t *testing.T) {
	env := Envelope{
		ConversationID: "slack-conv-42",
		Channel: ChannelMeta{
			Type:   ChannelSlack,
			Source: "#incidents-infra",
			Metadata: map[string]string{
				"team_id": "T12345",
				"user_id": "U98765",
			},
		},
		Payload: Payload{
			Role: RoleUser,
			Text: "run latency experiment on gateway",
			ToolCalls: []ToolCall{
				{
					ID:        "call-1",
					Name:      "run_experiment",
					Arguments: json.RawMessage(`{"fault":"latency","blast_radius":0.3,"duration_s":2}`),
				},
			},
		},
		Timestamp: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC),
	}

	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.ConversationID != env.ConversationID {
		t.Errorf("expected ConversationID %q, got %q", env.ConversationID, decoded.ConversationID)
	}
	if decoded.Channel.Type != ChannelSlack {
		t.Errorf("expected Channel.Type %q, got %q", ChannelSlack, decoded.Channel.Type)
	}
	if len(decoded.Payload.ToolCalls) != 1 {
		t.Fatalf("expected 1 ToolCall, got %d", len(decoded.Payload.ToolCalls))
	}
	if decoded.Payload.ToolCalls[0].Name != "run_experiment" {
		t.Errorf("expected ToolCall name run_experiment, got %s", decoded.Payload.ToolCalls[0].Name)
	}
}

func TestSessionStore_ConcurrencyRace(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	const workers = 15
	const iterations = 50
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			convID := fmt.Sprintf("conv-%d", workerID%3) // 3 shared conversations under contention
			for j := 0; j < iterations; j++ {
				s, err := store.Get(ctx, convID)
				if err != nil && err != ErrSessionNotFound {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if s == nil {
					s = &Session{
						ConversationID: convID,
						CreatedAt:      time.Now(),
					}
				}
				s.History = append(s.History, Envelope{
					ConversationID: convID,
					Payload:        Payload{Role: RoleUser, Text: fmt.Sprintf("msg %d", j)},
				})
				s.ActiveExperimentID = fmt.Sprintf("exp-%d", j)
				_ = store.Save(ctx, s)
			}
		}(i)
	}
	wg.Wait()
}

func setupTestRunner() (*Runner, *mcp.Server) {
	server := mcp.NewServer("flowgate-test", "0.1.0", "test instructions")
	lab := mcp.NewLab(mcp.DefaultLimits())
	lab.Register(server)

	store := NewMemoryStore()
	r := New(store, server)
	return r, server
}

func TestRunner_MultiTurnMCPToolDispatch(t *testing.T) {
	r, _ := setupTestRunner()
	ctx := context.Background()
	convID := "multi-turn-cli-001"

	meta := ChannelMeta{
		Type:   ChannelCLI,
		Source: "terminal-1",
	}

	// Turn 1: run_experiment
	turn1 := Envelope{
		ConversationID: convID,
		Channel:        meta,
		Payload: Payload{
			Role: RoleUser,
			ToolCalls: []ToolCall{
				{
					ID:        "tc-1",
					Name:      "run_experiment",
					Arguments: json.RawMessage(`{"fault":"latency","blast_radius":0.2,"duration_s":1,"recovery_timeout_s":1}`),
				},
			},
		},
		Timestamp: time.Now().UTC(),
	}

	resp1, err := r.Handle(ctx, turn1)
	if err != nil {
		t.Fatalf("turn 1 failed: %v", err)
	}
	if len(resp1.Payload.ToolResults) != 1 {
		t.Fatalf("expected 1 ToolResult, got %d", len(resp1.Payload.ToolResults))
	}
	tr1 := resp1.Payload.ToolResults[0]
	if tr1.IsError {
		t.Fatalf("expected tool success, got error: %v", tr1.Content)
	}

	resMap, ok := tr1.Content.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any content, got %T", tr1.Content)
	}
	expID, ok := resMap["experiment_id"].(string)
	if !ok || expID == "" {
		t.Fatalf("missing experiment_id in response: %v", resMap)
	}

	// Verify session retained the ActiveExperimentID
	sess, err := r.GetSession(ctx, convID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if sess.ActiveExperimentID != expID {
		t.Errorf("expected session.ActiveExperimentID = %q, got %q", expID, sess.ActiveExperimentID)
	}

	// Turn 2: get_verdict without arguments (should pick up ActiveExperimentID from session)
	turn2 := Envelope{
		ConversationID: convID,
		Channel:        meta,
		Payload: Payload{
			Role: RoleUser,
			ToolCalls: []ToolCall{
				{
					ID:   "tc-2",
					Name: "get_verdict",
				},
			},
		},
		Timestamp: time.Now().UTC(),
	}

	resp2, err := r.Handle(ctx, turn2)
	if err != nil {
		t.Fatalf("turn 2 failed: %v", err)
	}
	tr2 := resp2.Payload.ToolResults[0]
	if tr2.IsError {
		t.Fatalf("expected get_verdict to succeed, got error: %v", tr2.Content)
	}
	runObj, ok := tr2.Content.(mcp.Run)
	if !ok {
		t.Fatalf("expected mcp.Run content, got %T", tr2.Content)
	}
	if runObj.ID != expID {
		t.Errorf("expected verdict for %q, got %q", expID, runObj.ID)
	}

	// Turn 3: list_experiments
	turn3 := Envelope{
		ConversationID: convID,
		Channel:        meta,
		Payload: Payload{
			Role: RoleUser,
			ToolCalls: []ToolCall{
				{
					ID:   "tc-3",
					Name: "list_experiments",
				},
			},
		},
		Timestamp: time.Now().UTC(),
	}

	resp3, err := r.Handle(ctx, turn3)
	if err != nil {
		t.Fatalf("turn 3 failed: %v", err)
	}
	tr3 := resp3.Payload.ToolResults[0]
	if tr3.IsError {
		t.Fatalf("expected list_experiments to succeed, got %v", tr3.Content)
	}

	// Verify full session history has 6 envelopes (3 requests + 3 responses)
	finalSess, err := r.GetSession(ctx, convID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if len(finalSess.History) != 6 {
		t.Errorf("expected 6 envelopes in history, got %d", len(finalSess.History))
	}
}

func TestRunner_ServerGuardrailRefusal(t *testing.T) {
	r, _ := setupTestRunner()
	ctx := context.Background()

	// Request blast_radius = 1.0 (default cap is 0.5)
	req := Envelope{
		ConversationID: "policy-test-conv",
		Channel: ChannelMeta{
			Type:   ChannelWeb,
			Source: "dashboard",
		},
		Payload: Payload{
			Role: RoleUser,
			ToolCalls: []ToolCall{
				{
					ID:        "tc-invalid-radius",
					Name:      "run_experiment",
					Arguments: json.RawMessage(`{"fault":"latency","blast_radius":1.0,"duration_s":2}`),
				},
			},
		},
		Timestamp: time.Now().UTC(),
	}

	resp, err := r.Handle(ctx, req)
	if err != nil {
		t.Fatalf("Handle should not fail on policy refusal: %v", err)
	}
	if len(resp.Payload.ToolResults) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.Payload.ToolResults))
	}
	res := resp.Payload.ToolResults[0]
	if !res.IsError {
		t.Errorf("expected is_error=true for policy cap violation, got false")
	}
}
