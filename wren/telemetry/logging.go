package telemetry

import (
	"context"
	"os"
	"time"

	grpc_zerolog "github.com/cheapRoc/grpc-zerolog"
	grpc_middleware_zerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// LoggingProvider injects the current request trace-id into logs and error responses
type LoggingProvider interface {
	UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error)
	UnaryPayloadInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error)
	StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
	StreamPayloadInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
}

type loggingProvider struct {
	loggingOptions []logging.Option
	payloadDecider func(ctx context.Context, fullMethodName string, servingObject interface{}) logging.PayloadDecision
}

// NewLoggingProvider initializes logging for wren
func NewLoggingProvider(debug bool, debugGrpc bool, formatJson bool, logHealthCheck bool, env string) LoggingProvider {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if debugGrpc {
		grpclog.SetLoggerV2(grpc_zerolog.New(log.Logger))
	}

	if !formatJson { // Output in-line & colorized logs instead of json
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
	return &loggingProvider{
		loggingOptions: []logging.Option{
			logging.WithDecider(func(fullMethodName string, err error) logging.Decision {
				switch fullMethodName {
				case "/grpc.health.v1.Health/Check":
					if logHealthCheck {
						return logging.LogFinishCall
					} else {
						return logging.NoLogCall
					}
				default:
					return logging.LogStartAndFinishCall
				}
			}),
		},
		payloadDecider: func(ctx context.Context, fullMethodName string, servingObject interface{}) logging.PayloadDecision {
			switch fullMethodName {
			case "/grpc.health.v1.Health/Check":
				if logHealthCheck {
					return logging.LogPayloadResponse
				} else {
					return logging.NoPayloadLogging
				}
			default:
				return logging.LogPayloadRequestAndResponse
			}
		},
	}
}

// UnaryInterceptor intercepts unary calls and outputs a log about the request
func (lp *loggingProvider) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	interceptor := logging.UnaryServerInterceptor(grpc_middleware_zerolog.InterceptorLogger(log.Logger), lp.loggingOptions...)
	return interceptor(ctx, req, info, handler)
}

// UnaryPayloadInterceptor intercepts unary calls and outputs a log line with the request & response contents
// except it omits the response object for credential calls
func (lp *loggingProvider) UnaryPayloadInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	interceptor := PayloadUnaryServerInterceptor(grpc_middleware_zerolog.InterceptorLogger(log.Logger), lp.payloadDecider, time.RFC3339)
	return interceptor(ctx, req, info, handler)
}

func (lp *loggingProvider) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	interceptor := logging.StreamServerInterceptor(grpc_middleware_zerolog.InterceptorLogger(log.Logger), lp.loggingOptions...)
	return interceptor(srv, ss, info, handler)
}

func (lp *loggingProvider) StreamPayloadInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	interceptor := PayloadStreamServerInterceptor(grpc_middleware_zerolog.InterceptorLogger(log.Logger), lp.payloadDecider, time.RFC3339)
	return interceptor(srv, ss, info, handler)
}
