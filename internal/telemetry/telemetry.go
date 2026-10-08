// Package telemetry wires OpenTelemetry tracing into flowgate.
//
// Tracing is opt-in. With OTEL_EXPORTER_OTLP_ENDPOINT unset, Setup only
// installs the W3C trace-context propagator and returns: spans created
// by the middleware go to the default no-op provider, so a plain
// `go run` or `go test` behaves exactly as before and needs no collector.
//
// With the variable set (for example http://localhost:4318 for the
// Jaeger in deploy/tracing), spans are batched and sent over OTLP/HTTP.
// Everything else the exporter reads, such as headers and timeouts, comes
// from the standard OTEL_EXPORTER_OTLP_* environment variables.
package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup installs the propagator and, when an OTLP endpoint is
// configured, a tracer provider. The returned function flushes and
// stops the provider; call it before the process exits or the last
// batch of spans is lost.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	// W3C traceparent/tracestate plus baggage: the formats other
	// services and the Python agent speak.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes("",
			attribute.String("service.name", service),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
