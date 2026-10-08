package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installRecorder points the global tracer provider at an in-memory
// recorder. Call it before Wrap: Wrap takes its tracer from the global
// provider when it is built. Each test gets a fresh recorder, so spans
// never leak between tests.
func installRecorder() *tracetest.SpanRecorder {
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

func spansNamed(sr *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == name {
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

func TestTrace_ContinuesCallersTraceAndNestsEveryStage(t *testing.T) {
	sr := installRecorder()
	h := Wrap(newTestConfig(10, 10, 10, 3), http.HandlerFunc(okHandler))

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const callerSpan = "00f067aa0ba902b7"
	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "1.2.3.4:1111"
	req.Header.Set("traceparent", "00-"+traceID+"-"+callerSpan+"-01")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	roots := spansNamed(sr, "flowgate.request")
	if len(roots) != 1 {
		t.Fatalf("want 1 root span, got %d", len(roots))
	}
	root := roots[0]
	if got := root.SpanContext().TraceID().String(); got != traceID {
		t.Errorf("trace id = %s, want the caller's %s", got, traceID)
	}
	if got := root.Parent().SpanID().String(); got != callerSpan {
		t.Errorf("root parent = %s, want the caller's span %s", got, callerSpan)
	}
	if got := rec.Header().Get("X-Trace-Id"); got != traceID {
		t.Errorf("X-Trace-Id = %q, want %s", got, traceID)
	}
	for _, stage := range []string{"shed", "ratelimit", "breaker", "backend"} {
		found := spansNamed(sr, stage)
		if len(found) != 1 {
			t.Fatalf("stage %q: want 1 span, got %d", stage, len(found))
		}
		if found[0].Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("stage %q is not a child of the request span", stage)
		}
		if found[0].SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Errorf("stage %q is in a different trace", stage)
		}
	}
	if got := attrString(root, "flowgate.outcome"); got != "allowed" {
		t.Errorf("outcome = %q, want allowed", got)
	}
}

func TestTrace_NoIncomingHeaderStartsFreshRoot(t *testing.T) {
	sr := installRecorder()
	h := Wrap(newTestConfig(10, 10, 10, 3), http.HandlerFunc(okHandler))

	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "1.2.3.4:1111"
	h.ServeHTTP(httptest.NewRecorder(), req)

	roots := spansNamed(sr, "flowgate.request")
	if len(roots) != 1 {
		t.Fatalf("want 1 root span, got %d", len(roots))
	}
	if roots[0].Parent().IsValid() {
		t.Error("a request with no traceparent must start a new trace, not join one")
	}
}

func TestTrace_RateLimitedRequestStopsBeforeBreakerAndBackend(t *testing.T) {
	sr := installRecorder()
	h := Wrap(newTestConfig(1, 1, 10, 3), http.HandlerFunc(okHandler))

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/work", nil)
		req.RemoteAddr = "1.2.3.4:1111"
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	outcomes := map[string]int{}
	for _, r := range spansNamed(sr, "flowgate.request") {
		outcomes[attrString(r, "flowgate.outcome")]++
		if attrString(r, "flowgate.outcome") == "rate_limited" && r.Status().Code == codes.Error {
			t.Error("a rate-limited request is a normal outcome and must not be an error span")
		}
	}
	if outcomes["allowed"] != 1 || outcomes["rate_limited"] != 1 {
		t.Fatalf("outcomes = %v, want one allowed and one rate_limited", outcomes)
	}
	// Two requests entered, but only the allowed one got past the limiter.
	if n := len(spansNamed(sr, "ratelimit")); n != 2 {
		t.Errorf("ratelimit spans = %d, want 2", n)
	}
	if n := len(spansNamed(sr, "breaker")); n != 1 {
		t.Errorf("breaker spans = %d, want 1", n)
	}
	if n := len(spansNamed(sr, "backend")); n != 1 {
		t.Errorf("backend spans = %d, want 1", n)
	}
}

func TestTrace_BackendFailureMarksBackendAndRequestSpansAsErrors(t *testing.T) {
	sr := installRecorder()
	failing := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	h := Wrap(newTestConfig(10, 10, 10, 3), failing)

	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "1.2.3.4:1111"
	h.ServeHTTP(httptest.NewRecorder(), req)

	backend := spansNamed(sr, "backend")
	if len(backend) != 1 || backend[0].Status().Code != codes.Error {
		t.Fatalf("backend span should be an error span, got %v", backend)
	}
	root := spansNamed(sr, "flowgate.request")[0]
	if root.Status().Code != codes.Error || attrString(root, "flowgate.outcome") != "error" {
		t.Errorf("request span: status=%v outcome=%q, want error/error",
			root.Status().Code, attrString(root, "flowgate.outcome"))
	}
}
