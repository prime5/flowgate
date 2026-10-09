package runner

import (
	"encoding/json"
	"time"
)

// ChannelType distinguishes where the interaction originates (Slack, Web, CLI).
type ChannelType string

const (
	ChannelSlack ChannelType = "slack"
	ChannelWeb   ChannelType = "web"
	ChannelCLI   ChannelType = "cli"
)

// ChannelMeta contains metadata about the communication channel.
type ChannelMeta struct {
	Type     ChannelType       `json:"type"`
	Source   string            `json:"source"`             // e.g. channel ID "#ops-alerts", "cli-session", "web-client-1"
	Metadata map[string]string `json:"metadata,omitempty"` // arbitrary key-value pairs (user ID, tenant, token)
}

// Role defines the participant role in the multi-turn session.
type Role string

const (
	RoleUser   Role = "user"
	RoleAgent  Role = "agent"
	RoleSystem Role = "system"
	RoleTool   Role = "tool"
)

// ToolCall represents a structured request to invoke an MCP tool.
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ToolResult represents the output of an executed tool.
type ToolResult struct {
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name"`
	Content    any    `json:"content"`
	IsError    bool   `json:"is_error"`
}

// Payload contains the structured message contents.
type Payload struct {
	Role        Role         `json:"role"`
	Text        string       `json:"text,omitempty"`
	ToolCalls   []ToolCall   `json:"tool_calls,omitempty"`
	ToolResults []ToolResult `json:"tool_results,omitempty"`
}

// Envelope is the channel-agnostic message envelope wrapping multi-turn requests and responses.
type Envelope struct {
	ConversationID string      `json:"conversation_id"`
	Channel        ChannelMeta `json:"channel"`
	Payload        Payload     `json:"payload"`
	Timestamp      time.Time   `json:"timestamp"`
}
