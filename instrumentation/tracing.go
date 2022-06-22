package instrumentation

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	service     = "an-dagda"
	environment = "production"
	id          = 1
)

type TracedMessage interface {
	GetTraceID() string
}

var tracer *tracesdk.TracerProvider

func Provider() (*tracesdk.TracerProvider, error) {
	if tracer == nil {
		// Create the Jaeger exporter
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(JaegerURL())))
		if err != nil {
			return nil, err
		}
		tracer = tracesdk.NewTracerProvider(
			// Always be sure to batch in production.
			tracesdk.WithBatcher(exp),
			// Record information about this application in a Resource.
			tracesdk.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(service),
				attribute.String("environment", environment),
				attribute.Int64("ID", id),
			)),
		)
		otel.SetTracerProvider(tracer)
	}

	return tracer, nil
}

func Close(ctx context.Context) error {
	if tracer == nil {
		return nil
	}

	return tracer.Shutdown(ctx)
}

func StartFromRemote(t TracedMessage, tracer trace.Tracer, spanName string) (context.Context, trace.Span) {
	traceID := t.GetTraceID()
	tc := propagation.TraceContext{}
	carrier := propagation.MapCarrier{}
	carrier.Set("traceparent", traceID)
	sctx := tc.Extract(context.Background(), carrier)
	spanContext := trace.SpanContextFromContext(sctx)
	sctx = trace.ContextWithRemoteSpanContext(context.Background(), spanContext)
	return tracer.Start(sctx, spanName)
}
