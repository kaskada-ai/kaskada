package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BlockingClient blocks requests
type BlockingClient interface {
	UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error)
	StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
}

type blockingClient struct{}

// NewBlockingClient creates a new blocking client designed for GRPC Interceptors
func NewBlockingClient() BlockingClient {
	return &blockingClient{}
}

// UnaryInterceptor blocks unary calls
func (a *blockingClient) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	return nil, status.Error(codes.PermissionDenied, "unary calls are not allowed")
}

// StreamInterceptor blocks stream calls
func (a *blockingClient) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return status.Error(codes.PermissionDenied, "streaming calls are not allowed")
}
