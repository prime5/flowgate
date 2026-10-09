package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ToolDispatcher defines the contract for dispatching tool calls to the underlying MCP server.
type ToolDispatcher interface {
	Call(ctx context.Context, name string, args json.RawMessage) (any, bool, error)
}

// Runner coordinates incoming message envelopes, multi-turn session persistence,
// and MCP tool dispatches.
//
// As per repository design rules: this is an explicit, multi-turn tool-calling
// interface ONLY, NOT a dialog engine; it makes NO Natural Language Understanding claims.
type Runner struct {
	store      SessionStore
	dispatcher ToolDispatcher
}

// New creates a new orchestration Runner.
func New(store SessionStore, dispatcher ToolDispatcher) *Runner {
	if store == nil {
		store = NewMemoryStore()
	}
	return &Runner{
		store:      store,
		dispatcher: dispatcher,
	}
}

// Handle processes an incoming Envelope, loads/initializes session state,
// dispatches tool calls to the MCP tools, persists session state, and returns the response Envelope.
func (r *Runner) Handle(ctx context.Context, req Envelope) (Envelope, error) {
	if req.ConversationID == "" {
		return Envelope{}, errors.New("runner: conversation_id is required")
	}

	session, err := r.store.Get(ctx, req.ConversationID)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			session = &Session{
				ConversationID: req.ConversationID,
				Channel:        req.Channel,
				History:        make([]Envelope, 0),
				CreatedAt:      time.Now().UTC(),
			}
		} else {
			return Envelope{}, fmt.Errorf("runner: failed to load session: %w", err)
		}
	}

	results := make([]ToolResult, 0, len(req.Payload.ToolCalls))

	for _, call := range req.Payload.ToolCalls {
		args := call.Arguments

		// Multi-turn contextual convenience: if get_verdict is requested without
		// an explicit experiment_id, use the active experiment recorded in the session.
		if call.Name == "get_verdict" && (len(args) == 0 || string(args) == "{}" || string(args) == "null") {
			if session.ActiveExperimentID != "" {
				args = json.RawMessage(fmt.Sprintf(`{"experiment_id":%q}`, session.ActiveExperimentID))
			}
		}

		res, isErr, callErr := r.dispatcher.Call(ctx, call.Name, args)
		if callErr != nil {
			results = append(results, ToolResult{
				ToolCallID: call.ID,
				ToolName:   call.Name,
				Content:    map[string]any{"error": callErr.Error()},
				IsError:    true,
			})
			continue
		}

		// If a new experiment was started successfully, update session's ActiveExperimentID
		if call.Name == "run_experiment" && !isErr {
			if m, ok := res.(map[string]any); ok {
				if eid, ok := m["experiment_id"].(string); ok {
					session.ActiveExperimentID = eid
				}
			}
		}

		results = append(results, ToolResult{
			ToolCallID: call.ID,
			ToolName:   call.Name,
			Content:    res,
			IsError:    isErr,
		})
	}

	var statusText string
	if len(results) > 0 {
		statusText = fmt.Sprintf("executed %d tool call(s)", len(results))
	} else if req.Payload.Text != "" {
		statusText = fmt.Sprintf("received message: %q (no tool calls executed)", req.Payload.Text)
	}

	resp := Envelope{
		ConversationID: req.ConversationID,
		Channel:        req.Channel,
		Payload: Payload{
			Role:        RoleAgent,
			Text:        statusText,
			ToolResults: results,
		},
		Timestamp: time.Now().UTC(),
	}

	session.History = append(session.History, req, resp)
	session.UpdatedAt = time.Now().UTC()

	if err := r.store.Save(ctx, session); err != nil {
		return Envelope{}, fmt.Errorf("runner: failed to save session: %w", err)
	}

	return resp, nil
}

// GetSession returns the current session state for a conversation ID.
func (r *Runner) GetSession(ctx context.Context, conversationID string) (*Session, error) {
	return r.store.Get(ctx, conversationID)
}
