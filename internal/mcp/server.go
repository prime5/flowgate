package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ProtocolVersion is the MCP revision this server implements.
const ProtocolVersion = "2025-06-18"

// Tool is one callable exposed over tools/list and tools/call.
//
// InputSchema and OutputSchema are JSON Schema objects. OutputSchema is
// optional, but when present the server MUST return structured content
// conforming to it — so Handler's return value and OutputSchema have to
// be kept in step by hand.
type Tool struct {
	Name         string
	Title        string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any

	// Handler runs the tool. A returned error is reported as a tool
	// execution error (isError: true) rather than a protocol error,
	// because a refused experiment is a normal outcome the model
	// should read and reason about, not a transport fault.
	Handler func(ctx context.Context, args json.RawMessage) (any, error)
}

// Server speaks MCP over a pair of streams.
type Server struct {
	name    string
	version string
	// instructions is handed to the client at initialize time. It is
	// the one place a server can tell a model how to use it without
	// relying on the model's priors.
	instructions string

	mu          sync.RWMutex
	tools       map[string]Tool
	order       []string // registration order, so tools/list is stable
	initialized bool

	// logf writes diagnostics. It must never write to stdout.
	logf func(string, ...any)
}

// NewServer returns a server with no tools registered.
func NewServer(name, version, instructions string) *Server {
	return &Server{
		name:         name,
		version:      version,
		instructions: instructions,
		tools:        map[string]Tool{},
		logf:         log.Printf, // the standard logger writes to stderr
	}
}

// Register adds a tool. Registering a duplicate name replaces it and
// keeps the original position in tools/list.
func (s *Server) Register(t Tool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tools[t.Name]; !exists {
		s.order = append(s.order, t.Name)
	}
	s.tools[t.Name] = t
}

// Serve reads JSON-RPC messages from in and writes replies to out
// until in is exhausted. It returns nil on clean EOF, which is how an
// MCP client signals shutdown on stdio: it closes the server's stdin.
//
// Messages are handled one at a time and in order. Tools that take
// real time are expected to start work and return an identifier rather
// than block, so a slow experiment cannot stall the protocol.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	enc := json.NewEncoder(out)

	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			var syn *json.SyntaxError
			if errors.As(err, &syn) {
				// Unparseable input: reply once, then stop. The
				// stream position is no longer trustworthy.
				_ = enc.Encode(newError(nil, CodeParseError, "parse error", err.Error()))
				return err
			}
			return err
		}

		resp := s.handle(ctx, &req)
		if resp == nil {
			continue // notification: no reply, by spec
		}
		if err := enc.Encode(resp); err != nil {
			return err
		}
	}
}

// handle dispatches one message. It returns nil for notifications.
func (s *Server) handle(ctx context.Context, req *request) *response {
	if req.JSONRPC != "2.0" {
		if req.isNotification() {
			return nil
		}
		return newError(req.ID, CodeInvalidRequest, `"jsonrpc" must be "2.0"`, nil)
	}

	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)

	case "notifications/initialized":
		s.mu.Lock()
		s.initialized = true
		s.mu.Unlock()
		return nil

	case "ping":
		// Permitted before initialization; the reply is an empty object.
		if req.isNotification() {
			return nil
		}
		return newResult(req.ID, map[string]any{})

	case "tools/list":
		if req.isNotification() {
			return nil
		}
		return newResult(req.ID, map[string]any{"tools": s.toolList()})

	case "tools/call":
		if req.isNotification() {
			return nil
		}
		return s.handleCall(ctx, req)

	default:
		if req.isNotification() {
			// Unknown notifications are ignored rather than
			// answered — replying to one is itself a protocol error.
			return nil
		}
		return newError(req.ID, CodeMethodNotFound, "unknown method: "+req.Method, nil)
	}
}

type initializeParams struct {
	ProtocolVersion string `json:"protocolVersion"`
	ClientInfo      struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"clientInfo"`
}

func (s *Server) handleInitialize(req *request) *response {
	var p initializeParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return newError(req.ID, CodeInvalidParams, "invalid initialize params", err.Error())
		}
	}

	// Version negotiation: echo the client's version when we speak it,
	// otherwise answer with ours and let the client decide whether to
	// continue or disconnect.
	negotiated := ProtocolVersion
	if p.ProtocolVersion == ProtocolVersion {
		negotiated = p.ProtocolVersion
	} else if p.ProtocolVersion != "" {
		s.logf("mcp: client requested protocol %q, offering %q", p.ProtocolVersion, ProtocolVersion)
	}

	if p.ClientInfo.Name != "" {
		s.logf("mcp: initialize from %s %s", p.ClientInfo.Name, p.ClientInfo.Version)
	}

	return newResult(req.ID, map[string]any{
		"protocolVersion": negotiated,
		"capabilities": map[string]any{
			// listChanged is false: the tool set is fixed at startup,
			// so promising change notifications would be a lie.
			"tools": map[string]any{"listChanged": false},
		},
		"serverInfo": map[string]any{
			"name":    s.name,
			"version": s.version,
		},
		"instructions": s.instructions,
	})
}

func (s *Server) toolList() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]map[string]any, 0, len(s.order))
	for _, name := range s.order {
		t := s.tools[name]
		entry := map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"inputSchema": t.InputSchema,
		}
		if t.Title != "" {
			entry["title"] = t.Title
		}
		if t.OutputSchema != nil {
			entry["outputSchema"] = t.OutputSchema
		}
		out = append(out, entry)
	}
	return out
}

type callParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Meta      struct {
		// Traceparent is the W3C traceparent the caller sent in
		// params._meta, or "" when the caller sent none.
		Traceparent string `json:"traceparent"`
	} `json:"_meta"`
}

func (s *Server) handleCall(ctx context.Context, req *request) *response {
	var p callParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return newError(req.ID, CodeInvalidParams, "invalid tools/call params", err.Error())
	}

	s.mu.RLock()
	t, ok := s.tools[p.Name]
	s.mu.RUnlock()
	if !ok {
		// An unknown tool is a protocol error: the model asked for
		// something that does not exist, which is not a result.
		return newError(req.ID, CodeMethodNotFound, "unknown tool: "+p.Name, nil)
	}

	// The handler runs under the tool span, so anything it starts
	// with this ctx — including the experiment the tool launches —
	// nests inside the caller's trace.
	ctx, span := startToolSpan(ctx, p.Name, p.Meta.Traceparent)
	defer span.End()

	result, err := t.Handler(ctx, p.Arguments)
	if err != nil {
		// Execution errors come back as results with isError, so the
		// model can read the reason and adjust — a refused experiment
		// is information, not a transport failure.
		return newResult(req.ID, toolResult(map[string]any{
			"error": err.Error(),
		}, true))
	}
	return newResult(req.ID, toolResult(result, false))
}

// startToolSpan continues the trace the caller sent in params._meta.
//
// The Python agent generates one trace id per session and a fresh span
// id per request, so a run_experiment and the get_verdict polls that
// follow it land in a single trace. The span is a server span: the
// caller's span id becomes its parent, and the returned ctx carries it
// into the tool handler.
//
// A missing or malformed traceparent starts a new trace instead of an
// error. Tracing is observability — it must never be the reason a tool
// call fails.
func startToolSpan(ctx context.Context, tool, traceparent string) (context.Context, trace.Span) {
	if traceparent != "" {
		ctx = otel.GetTextMapPropagator().Extract(
			ctx, propagation.MapCarrier{"traceparent": traceparent})
	}
	return otel.Tracer("flowgate/mcp").Start(ctx, "mcp.tools/call",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("mcp.tool", tool)))
}

// toolResult builds the dual-form payload MCP expects: structured
// content for programs, and the same JSON serialized into a text block
// for clients that do not read structuredContent.
func toolResult(v any, isError bool) map[string]any {
	text, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		text = []byte(`{"error":"result could not be serialized"}`)
	}
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": string(text)},
		},
		"structuredContent": v,
		"isError":           isError,
	}
}
