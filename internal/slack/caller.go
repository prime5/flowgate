package slack

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/prime5/flowgate/internal/runner"
)

// Conversation identifies who is talking, so multi-turn state ("verdict"
// with no id means the experiment I started a moment ago) is kept per
// person per channel rather than shared across a workspace.
type Conversation struct {
	ID        string
	ChannelID string
	UserID    string
}

// ToolCaller runs one MCP tool in a conversation.
type ToolCaller interface {
	Call(ctx context.Context, conv Conversation, name string, args json.RawMessage) (content any, isErr bool, err error)
}

// RunnerCaller is the ToolCaller backed by internal/runner. Slack is the
// second channel the runner serves (after the CLI), through the same
// envelope, which is the point of the envelope being channel-agnostic.
type RunnerCaller struct {
	R *runner.Runner
}

func (c RunnerCaller) Call(ctx context.Context, conv Conversation, name string, args json.RawMessage) (any, bool, error) {
	resp, err := c.R.Handle(ctx, runner.Envelope{
		ConversationID: conv.ID,
		Channel: runner.ChannelMeta{
			Type:     runner.ChannelSlack,
			Source:   conv.ChannelID,
			Metadata: map[string]string{"user_id": conv.UserID},
		},
		Payload: runner.Payload{
			Role:      runner.RoleUser,
			ToolCalls: []runner.ToolCall{{ID: "slack-call", Name: name, Arguments: args}},
		},
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		return nil, false, err
	}
	if len(resp.Payload.ToolResults) != 1 {
		return nil, false, errors.New("slack: runner returned no tool result")
	}
	tr := resp.Payload.ToolResults[0]
	return tr.Content, tr.IsError, nil
}
