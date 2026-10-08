package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// installRecorder points the global tracer provider at an in-memory
// recorder and installs the W3C propagator, mirroring what
// internal/telemetry sets up in production. Each test gets a fresh
// recorder so spans never leak between tests.
func installRecorder() *tracetest.SpanRecorder {
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

// serveOneCall drives Serve with a single tools/call carrying the given
// params JSON, and returns the spans the call produced plus the span
// context the handler observed.
func serveOneCall(t *testing.T, params string) (*tracetest.SpanRecorder, trace.SpanContext) {
	t.Helper()
	sr := installRecorder()

	var handlerSC trace.SpanContext
	ran := false
	s := NewServer("test", "0.0.1", "")
	s.Register(Tool{
		Name:        "probe",
		Description: "test tool",
		InputSchema: map[string]any{"type": "object"},
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			ran = true
			handlerSC = trace.SpanContextFromContext(ctx)
			return map[string]any{"ok": true}, nil
		},
	})

	req := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":` + params + "}\n"
	var out bytes.Buffer
	if err := s.Serve(context.Background(), strings.NewReader(req), &out); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if !ran {
		t.Fatal("tool handler never ran")
	}
	return sr, handlerSC
}

func toolSpans(sr *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "mcp.tools/call" {
			out = append(out, s)
		}
	}
	return out
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if kv.Key == attribute.Key(key) {
			return kv.Value.AsString()
		}
	}
	return ""
}

func TestTrace_ContinuesAgentTrace(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const agentSpan = "00f067aa0ba902b7"

	sr, handlerSC := serveOneCall(t,
		`{"name":"probe","arguments":{},"_meta":{"traceparent":"00-`+traceID+`-`+agentSpan+`-01"}}`)

	spans := toolSpans(sr)
	if len(spans) != 1 {
		t.Fatalf("want 1 mcp.tools/call span, got %d", len(spans))
	}
	span := spans[0]

	if got := span.SpanContext().TraceID().String(); got != traceID {
		t.Errorf("span joined trace %s, want %s", got, traceID)
	}
	if got := span.Parent().SpanID().String(); got != agentSpan {
		t.Errorf("span parent is %s, want the agent's span %s", got, agentSpan)
	}
	if span.SpanContext().SpanID() == span.Parent().SpanID() {
		t.Error("span reused the agent's span id instead of starting its own")
	}
	if got := attrString(span, "mcp.tool"); got != "probe" {
		t.Errorf("mcp.tool attribute = %q, want %q", got, "probe")
	}
	if span.SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want Server", span.SpanKind())
	}

	// The handler must observe the tool span's context, so anything
	// the tool starts — including the experiment — nests under it.
	if !handlerSC.IsValid() {
		t.Fatal("handler ctx carried no span")
	}
	if handlerSC.TraceID() != span.SpanContext().TraceID() ||
		handlerSC.SpanID() != span.SpanContext().SpanID() {
		t.Error("handler ctx span differs from the recorded tool span")
	}
}

func TestTrace_MissingTraceparentStartsNewTrace(t *testing.T) {
	sr, handlerSC := serveOneCall(t, `{"name":"probe","arguments":{}}`)

	spans := toolSpans(sr)
	if len(spans) != 1 {
		t.Fatalf("want 1 mcp.tools/call span, got %d", len(spans))
	}
	span := spans[0]

	if !span.SpanContext().IsValid() {
		t.Error("span without an incoming traceparent must still start a trace")
	}
	if span.Parent().IsValid() {
		t.Error("span without an incoming traceparent must have no parent")
	}
	if !handlerSC.IsValid() {
		t.Error("handler ctx carried no span")
	}
}

func TestTrace_MalformedTraceparentDoesNotFailCall(t *testing.T) {
	sr, _ := serveOneCall(t,
		`{"name":"probe","arguments":{},"_meta":{"traceparent":"not-a-traceparent"}}`)

	// Tracing is observability: a bad traceparent degrades to a fresh
	// trace, it never fails the tool call.
	if spans := toolSpans(sr); len(spans) != 1 {
		t.Fatalf("want 1 mcp.tools/call span despite bad traceparent, got %d", len(spans))
	}
}
