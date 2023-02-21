package telemetry

import (
	"context"
	"reflect"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	resource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// TracingProvider injects the current request trace-id into logs and error responses
type TracingProvider interface {
	UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error)
	StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
}

type tracingProvider struct{}

// NewTracingProvider creates a new tracing provider designed for GRPC Interceptors (currently only Unary)
// returns the provider and a shutdown function
func NewTracingProvider(ctx context.Context, otelEndpont string) (TracingProvider, func()) {
	var tracerProvider *sdktrace.TracerProvider
	if otelEndpont == "" {
		// when not exporting traces, still provide a generic traceProvider
		// to generate requestIDs, which are needed for logging to work correctly
		tracerProvider = sdktrace.NewTracerProvider()
	} else {
		// hook up exporter, don't block on connection to collector
		traceExporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(otelEndpont),
		)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to create trace exporter")
			return nil, func() {}
		}

		// autodetect resource attributes
		resources, err := resource.New(ctx,
			// ignore the docs, `resource.WithBuiltinDetectors` is deprecated
			// https://opentelemetry.io/docs/go/exporting_data/#resources - not updated as of 2021/08/26
			// https://github.com/open-telemetry/opentelemetry-go/issues/2026
			resource.WithHost(),
			resource.WithOSDescription(),
			resource.WithOSType(),
			resource.WithProcess(),
			// Or specify resource attributes directly
			// resource.WithAttributes(attribute.String("foo", "bar")),
		)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to create resources for trace")
		}

		// Register the trace exporter with a TracerProvider, using a batch
		// span processor to aggregate spans before export.
		bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(resources),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(bsp),
		)
	}
	otel.SetTracerProvider(tracerProvider)

	// set global propagator to tracecontext (the default is no-op).
	propagator := propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{})
	otel.SetTextMapPropagator(propagator)

	shutdownFunction := func() {
		// Shutdown will flush any remaining spans and shut down the exporter.
		err := tracerProvider.Shutdown(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("issue shutting down trace provider")
		} else {
			log.Ctx(ctx).Info().Msg("cleanly shut down trace provider")
		}
	}

	return &tracingProvider{}, shutdownFunction
}

// UnaryInterceptor intercepts unary calls and adds the trace-id to the context logger as request_id
func (tp *tracingProvider) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	traceID := getTraceID(ctx)
	ctx = addTraceIDToContextAndLogs(ctx, traceID)

	// Perform the request
	res, err := handler(ctx, req)

	return addRequestDetailsToResponse(res, traceID), addRequestDetailsToError(err, traceID)
}

// StreamInterceptor intercepts stream calls and adds the trace-id to the context logger as request_id
func (tp *tracingProvider) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	t := newTracingServerStream(ss)
	err := handler(srv, t)
	return addRequestDetailsToError(err, t.traceID)
}

func getTraceID(ctx context.Context) string {
	return trace.SpanFromContext(ctx).SpanContext().TraceID().String()
}

func addTraceIDToContextAndLogs(ctx context.Context, traceID string) context.Context {
	// add trace-id as request_id to tracing interceptor for request & payload logs
	ctx = logging.InjectFields(ctx, logging.Fields{"request_id", traceID})

	// add trace-id as request_id to context logger for all other logs
	contextLogger := log.With().Str("request_id", traceID).Logger()
	return contextLogger.WithContext(ctx)
}

func addRequestDetailsToResponse(res interface{}, traceID string) interface{} {
	// attempt to add the trace-id to the RequestDetails of the response
	if res != nil {
		resVal := reflect.ValueOf(res)
		resVal = reflect.Indirect(resVal)
		if resVal.IsValid() && resVal.Kind() == reflect.Struct {
			requestDetails := resVal.FieldByName("RequestDetails")

			// add a RequestDetails object to the repsonse if one doesn't already exist
			if requestDetails.IsValid() && requestDetails.IsNil() && requestDetails.CanSet() {

				switch {
				case reflect.TypeOf(&v1alpha.RequestDetails{}).AssignableTo(requestDetails.Type()):
					requestDetails.Set(reflect.ValueOf(&v1alpha.RequestDetails{}))
				default:
					log.Error().Msg("unknown type for addRequestDetailsToResponse")
				}
			}

			// add the RequestId to the RequestDetails object
			requestDetails = reflect.Indirect(requestDetails)
			if requestDetails.Kind() == reflect.Struct {
				requestID := requestDetails.FieldByName("RequestId")
				if requestID.IsValid() && requestID.CanSet() {
					requestID.Set(reflect.ValueOf(traceID))
				}
			}
		}
	}
	return res
}

func addRequestDetailsToError(err error, traceID string) error {
	if err != nil {
		s, ok := status.FromError(err)
		if !ok {
			// From: https://pkg.go.dev/google.golang.org/grpc@v1.39.0#UnaryHandler

			// UnaryHandler defines the handler invoked by UnaryServerInterceptor to complete the normal execution
			// of a unary RPC. If a UnaryHandler returns an error, it should be produced by the status package, or
			// else gRPC will use codes.Unknown as the status code and err.Error() as the status message of the RPC.
			s = status.New(codes.Unknown, err.Error())
		}
		s, err = s.WithDetails(&errdetails.RequestInfo{
			RequestId: traceID,
		})
		if err == nil {
			return s.Err()
		}
	}
	return err
}

type tracingServerStream struct {
	ctx     context.Context
	ss      grpc.ServerStream
	traceID string
}

func newTracingServerStream(ss grpc.ServerStream) tracingServerStream {
	traceID := getTraceID(ss.Context())
	return tracingServerStream{
		ctx:     addTraceIDToContextAndLogs(ss.Context(), traceID),
		ss:      ss,
		traceID: traceID,
	}
}

func (t tracingServerStream) SetHeader(m metadata.MD) error {
	return t.ss.SetHeader(m)
}

func (t tracingServerStream) SendHeader(m metadata.MD) error {
	return t.ss.SendHeader(m)
}

func (t tracingServerStream) SetTrailer(m metadata.MD) {
	t.ss.SetTrailer(m)
}

func (t tracingServerStream) Context() context.Context {
	return t.ctx
}

func (t tracingServerStream) SendMsg(m interface{}) error {
	m = addRequestDetailsToResponse(m, t.traceID)
	err := t.ss.SendMsg(m)
	return addRequestDetailsToError(err, t.traceID)
}

func (t tracingServerStream) RecvMsg(m interface{}) error {
	return t.ss.RecvMsg(m)
}
