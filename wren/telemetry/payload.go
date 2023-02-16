// Based on https://github.com/grpc-ecosystem/go-grpc-middleware/blob/v2.0.0-rc.2/interceptors/logging/payload.go

// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.
package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/kaskada/kaskada-ai/wren/utils"
)

type serverPayloadReporter struct {
	ctx      context.Context
	logger   logging.Logger
	decision logging.PayloadDecision
	grpcType interceptors.GRPCType
}

func getLoggerAndCode(logger logging.Logger, err error, duration time.Duration) (logging.Logger, codes.Code) {
	code := status.Code(err)

	logger = logger.With(
		"grpc.time_ms",
		fmt.Sprintf("%.3f", float64(duration.Microseconds())/1000),
		"grpc.code",
		code.String(),
	)
	if err != nil {
		logger = logger.With("grpc.error", fmt.Sprintf("%v", err))
	}
	return logger, code
}

func (c *serverPayloadReporter) PostCall(error, time.Duration) {}

func (c *serverPayloadReporter) PostMsgSend(req interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case logging.LogPayloadResponse, logging.LogPayloadRequestAndResponse:
	default:
		return
	}

	logger, code := getLoggerAndCode(c.logger, err, duration)

	p, ok := req.(proto.Message)
	if !ok {
		logger.With("req.type", fmt.Sprintf("%T", req)).Log(logging.ERROR, "req is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}
	switch c.grpcType {
	case interceptors.ServerStream, interceptors.BidiStream:
		logProtoMessageAsJson(logger, p, "grpc.response.content", logging.DefaultServerCodeToLevel(code), "stream response")
	default:
		logProtoMessageAsJson(logger, p, "grpc.response.content", logging.DefaultServerCodeToLevel(code), "finished call")
	}
}

func (c *serverPayloadReporter) PostMsgReceive(reply interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case logging.LogPayloadRequest, logging.LogPayloadRequestAndResponse:
	default:
		return
	}

	logger, _ := getLoggerAndCode(c.logger, err, duration)

	p, ok := reply.(proto.Message)
	if !ok {
		logger.With("reply.type", fmt.Sprintf("%T", reply)).Log(logging.ERROR, "reply is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}

	switch c.grpcType {
	case interceptors.ClientStream, interceptors.BidiStream:
		logProtoMessageAsJson(logger, p, "grpc.request.content", logging.INFO, "stream request")
	default:
		logProtoMessageAsJson(logger, p, "grpc.request.content", logging.INFO, "started call")
	}
}

type clientPayloadReporter struct {
	ctx      context.Context
	logger   logging.Logger
	decision logging.PayloadDecision
	grpcType interceptors.GRPCType
}

func (c *clientPayloadReporter) PostCall(error, time.Duration) {}

func (c *clientPayloadReporter) PostMsgSend(req interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case logging.LogPayloadRequest, logging.LogPayloadRequestAndResponse:
	default:
		return
	}

	logger, _ := getLoggerAndCode(c.logger, err, duration)

	p, ok := req.(proto.Message)
	if !ok {
		logger.With("req.type", fmt.Sprintf("%T", req)).Log(logging.ERROR, "req is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}

	switch c.grpcType {
	case interceptors.ClientStream, interceptors.BidiStream:
		logProtoMessageAsJson(logger, p, "grpc.request.content", logging.INFO, "client stream request")
	default:
		logProtoMessageAsJson(logger, p, "grpc.request.content", logging.INFO, "client started call")
	}
}

func (c *clientPayloadReporter) PostMsgReceive(reply interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case logging.LogPayloadResponse, logging.LogPayloadRequestAndResponse:
	default:
		return
	}

	logger, code := getLoggerAndCode(c.logger, err, duration)

	p, ok := reply.(proto.Message)
	if !ok {
		logger.With("reply.type", fmt.Sprintf("%T", reply)).Log(logging.ERROR, "reply is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}

	switch c.grpcType {
	case interceptors.ServerStream, interceptors.BidiStream:
		logProtoMessageAsJson(logger, p, "grpc.response.content", logging.DefaultServerCodeToLevel(code), "client stream response")
	default:
		logProtoMessageAsJson(logger, p, "grpc.response.content", logging.DefaultServerCodeToLevel(code), "client finished call")
	}
}

type payloadReportable struct {
	clientDecider   logging.ClientPayloadLoggingDecider
	serverDecider   logging.ServerPayloadLoggingDecider
	logger          logging.Logger
	timestampFormat string
}

func newCommonFields(kind string, c interceptors.CallMeta) logging.Fields {
	return logging.Fields{
		logging.SystemTag[0], logging.SystemTag[1],
		logging.ComponentFieldKey, kind,
		logging.ServiceFieldKey, c.Service,
		logging.MethodFieldKey, c.Method,
		logging.MethodTypeFieldKey, string(c.Typ),
	}
}

func (r *payloadReportable) ServerReporter(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	decision := r.serverDecider(ctx, c.FullMethod(), c.ReqProtoOrNil)
	if decision == logging.NoPayloadLogging {
		return interceptors.NoopReporter{}, ctx
	}
	fields := newCommonFields(logging.KindServerFieldValue, c)
	fields = fields.AppendUnique(logging.ExtractFields(ctx))
	if peer, ok := peer.FromContext(ctx); ok {
		fields = append(fields, "peer.address", peer.Addr.String())
	}

	/*
		singleUseFields := []string{"grpc.start_time", time.Now().Format(r.timestampFormat)}
		if d, ok := ctx.Deadline(); ok {
			singleUseFields = append(singleUseFields, "grpc.request.deadline", d.Format(r.timestampFormat))
		}
		return &serverPayloadReporter{ctx: ctx, logger: r.logger.With(fields...).With(singleUseFields...), decision: decision}, logging.InjectFields(ctx, fields)
	*/
	return &serverPayloadReporter{ctx: ctx, logger: r.logger.With(fields...), decision: decision, grpcType: c.Typ}, logging.InjectFields(ctx, fields)
}

func (r *payloadReportable) ClientReporter(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	decision := r.clientDecider(ctx, c.FullMethod())
	if decision == logging.NoPayloadLogging {
		return interceptors.NoopReporter{}, ctx
	}
	fields := newCommonFields(logging.KindClientFieldValue, c)
	fields = fields.AppendUnique(logging.ExtractFields(ctx))

	/*
		singleUseFields := []string{"grpc.start_time", time.Now().Format(r.timestampFormat)}
		if d, ok := ctx.Deadline(); ok {
			singleUseFields = append(singleUseFields, "grpc.request.deadline", d.Format(r.timestampFormat))
		}
		return &clientPayloadReporter{ctx: ctx, logger: r.logger.With(fields...).With(singleUseFields...), decision: decision}, logging.InjectFields(ctx, fields)
	*/
	return &clientPayloadReporter{ctx: ctx, logger: r.logger.With(fields...), decision: decision, grpcType: c.Typ}, logging.InjectFields(ctx, fields)
}

// PayloadUnaryServerInterceptor returns a new unary server interceptors that logs the payloads of requests on INFO level.
// Logger tags will be used from tags context.
func PayloadUnaryServerInterceptor(logger logging.Logger, decider logging.ServerPayloadLoggingDecider,
	timestampFormat string) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&payloadReportable{
		logger:          logger,
		serverDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadStreamServerInterceptor returns a new server interceptors that logs the payloads of requests on INFO level.
// Logger tags will be used from tags context.
func PayloadStreamServerInterceptor(logger logging.Logger, decider logging.ServerPayloadLoggingDecider,
	timestampFormat string) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&payloadReportable{
		logger:          logger,
		serverDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadUnaryClientInterceptor returns a new unary client interceptor that logs the payloads of requests and responses on INFO level.
// Logger tags will be used from tags context.
func PayloadUnaryClientInterceptor(logger logging.Logger, decider logging.ClientPayloadLoggingDecider,
	timestampFormat string) grpc.UnaryClientInterceptor {
	return interceptors.UnaryClientInterceptor(&payloadReportable{
		logger:          logger,
		clientDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadStreamClientInterceptor returns a new streaming client interceptor that logs the paylods of requests and responses on INFO level.
// Logger tags will be used from tags context.
func PayloadStreamClientInterceptor(logger logging.Logger, decider logging.ClientPayloadLoggingDecider,
	timestampFormat string) grpc.StreamClientInterceptor {
	return interceptors.StreamClientInterceptor(&payloadReportable{
		logger:          logger,
		clientDecider:   decider,
		timestampFormat: timestampFormat})
}

func logProtoMessageAsJson(logger logging.Logger, pbMsg proto.Message, key string, level logging.Level, msg string) {
	payload, err := protojson.Marshal(utils.RedactCopy(pbMsg))
	if err != nil {
		logger = logger.With(key, err.Error())
	} else {
		// Trim spaces for deterministic output.
		// See: https://github.com/golang/protobuf/issues/1269
		logger = logger.With(key, string(bytes.Replace(payload, []byte{' '}, []byte{}, -1)))
	}
	logger.Log(level, msg)
}
