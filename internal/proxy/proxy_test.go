package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/ratelimit"
	"github.com/prime5/flowgate/internal/shedder"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestProxy_ForwardsPathQueryAndAuthorization(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotAuth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		w.WriteHeader(http.StatusTeapot)
	}))
	defer up.Close()

	h := New(mustURL(t, up.URL), Options{})
	req := httptest.NewRequest("GET", "/alerts/42?verbose=1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status not passed through: %d", rec.Code)
	}
	if gotPath != "/alerts/42" || gotQuery != "verbose=1" {
		t.Fatalf("path/query not preserved: %q ? %q", gotPath, gotQuery)
	}
	if gotAuth != "Bearer abc" {
		t.Fatalf("Authorization must reach the upstream untouched, got %q", gotAuth)
	}
}

func TestProxy_InjectsTraceContextOfCurrentSpan(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "backend")
	defer span.End()
	want := span.SpanContext().TraceID().String()

	var tp1 string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tp1 = r.Header.Get("traceparent")
	}))
	defer up.Close()

	h := New(mustURL(t, up.URL), Options{})
	req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !strings.Contains(tp1, want) {
		t.Fatalf("upstream traceparent %q does not carry trace id %s", tp1, want)
	}
	if !strings.Contains(tp1, span.SpanContext().SpanID().String()) {
		t.Fatalf("upstream traceparent %q should name the backend span as parent", tp1)
	}
}

func TestProxy_UnreachableUpstreamIs502(t *testing.T) {
	up := httptest.NewServer(http.NotFoundHandler())
	u := mustURL(t, up.URL)
	up.Close() // nothing listens there now

	h := New(u, Options{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", rec.Code)
	}
}

func TestProxy_SlowUpstreamIs504(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer up.Close()

	h := New(mustURL(t, up.URL), Options{ResponseHeaderTimeout: 50 * time.Millisecond})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("want 504, got %d", rec.Code)
	}
}

// A dead upstream must trip the breaker through the real middleware
// chain, after which requests fail fast with 503 without touching the
// upstream at all.
func TestProxy_DeadUpstreamTripsBreakerBehindWrap(t *testing.T) {
	up := httptest.NewServer(http.NotFoundHandler())
	u := mustURL(t, up.URL)
	up.Close()

	cfg := ratelimit.Config{
		Limiter: limiter.NewRegistry(1000, 1000),
		Breaker: breaker.New(3, time.Minute),
		Shedder: shedder.New(10),
	}
	h := ratelimit.Wrap(cfg, New(u, Options{}))

	var codes []int
	for i := 0; i < 6; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/x", nil)
		req.RemoteAddr = "9.9.9.9:1"
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	for i := 0; i < 3; i++ {
		if codes[i] != http.StatusBadGateway {
			t.Fatalf("request %d: want 502 while breaker closed, got %v", i, codes)
		}
	}
	for i := 3; i < 6; i++ {
		if codes[i] != http.StatusServiceUnavailable {
			t.Fatalf("request %d: want 503 once breaker open, got %v", i, codes)
		}
	}
}
