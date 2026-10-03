// Package mcp is a hand-rolled Model Context Protocol server over
// JSON-RPC 2.0 on stdio, with no third-party dependencies.
//
// It is hand-written for the same reason internal/metrics hand-writes
// Prometheus exposition: MCP's wire format is documented plain JSON,
// and implementing it here means every byte on the wire is something
// this project actually does rather than something a library does
// silently. The protocol surface used is small — initialize,
// notifications/initialized, tools/list, tools/call, ping.
//
// Protocol version: 2025-06-18.
//
// # The stdio contract
//
// On stdio transport, stdout carries protocol messages and nothing
// else. A stray fmt.Println anywhere in the process corrupts the
// stream and the client disconnects — which is why this package writes
// every diagnostic to stderr and never touches stdout except through
// the encoder.
package mcp

import (
	"encoding/json"
	"fmt"
)

// JSON-RPC 2.0 standard error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// request is one incoming JSON-RPC message. A message with no ID is a
// notification: it gets no reply, ever, including on error.
type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// isNotification reports whether this message must not be replied to.
func (r *request) isNotification() bool { return len(r.ID) == 0 }

// response is one outgoing JSON-RPC reply. Exactly one of Result and
// Error is set.
type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("jsonrpc %d: %s", e.Code, e.Message) }

func newResult(id json.RawMessage, result any) *response {
	return &response{JSONRPC: "2.0", ID: id, Result: result}
}

func newError(id json.RawMessage, code int, msg string, data any) *response {
	if len(id) == 0 {
		// A reply still needs an id field; JSON null is the
		// spec-sanctioned stand-in when the request's id was
		// unreadable.
		id = json.RawMessage("null")
	}
	return &response{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg, Data: data}}
}
