package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	grpc_zerolog "github.com/cheapRoc/grpc-zerolog"
	grpc_middleware_zerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
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
func NewLoggingProvider(debug bool, debugGrpc bool, formatJson bool, logHealthCheck bool, noColor bool, env string) LoggingProvider {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if debugGrpc {
		grpclog.SetLoggerV2(grpc_zerolog.New(log.Logger))
	}

	if formatJson {
		zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000000Z"
	} else {
		// Output in-line logs instead of json
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:         os.Stdout,
			TimeFormat:  "2006-01-02T15:04:05.000000Z",
			NoColor:     noColor,
			FormatLevel: consoleFormatLevel(noColor),
		})
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

// based on https://github.com/rs/zerolog/blob/8981d80ed338dab0fc9bec56587e0bd0e3c4c40d/console.go#L372
func consoleFormatLevel(noColor bool) zerolog.Formatter {
	return func(i interface{}) string {
		var l string
		if ll, ok := i.(string); ok {
			switch ll {
			case zerolog.LevelTraceValue:
				l = colorize("TRACE", colorMagenta, noColor)
			case zerolog.LevelDebugValue:
				l = colorize("DEBUG", colorBlue, noColor)
			case zerolog.LevelInfoValue:
				l = colorize(" INFO", colorGreen, noColor)
			case zerolog.LevelWarnValue:
				l = colorize(" WARN", colorYellow, noColor)
			case zerolog.LevelErrorValue:
				l = colorize("ERROR", colorRed, noColor)
			case zerolog.LevelFatalValue:
				l = colorize(colorize("FATAL", colorRed, noColor), colorBold, noColor)
			case zerolog.LevelPanicValue:
				l = colorize(colorize("PANIC", colorRed, noColor), colorBold, noColor)
			default:
				l = colorize(ll, colorBold, noColor)
			}
		} else {
			if i == nil {
				l = colorize("???", colorBold, noColor)
			} else {
				l = strings.ToUpper(fmt.Sprintf("%s", i))[0:3]
			}
		}
		return l
	}
}

// colorize returns the string s wrapped in ANSI code c, unless disabled is true.
func colorize(s interface{}, c int, disabled bool) string {
	if disabled {
		return fmt.Sprintf("%s", s)
	}
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}
