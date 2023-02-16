package client

import (
	"google.golang.org/grpc"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

// NewComputeServiceClient creates a new ComputeServiceClient from a gRPC connection
func NewComputeServiceClient(conn *grpc.ClientConn) ComputeServiceClient {
	return computeServiceClient{
		conn:                 conn,
		ComputeServiceClient: v1alpha.NewComputeServiceClient(conn),
	}
}

type computeServiceClient struct {
	conn *grpc.ClientConn
	v1alpha.ComputeServiceClient
}

func (s computeServiceClient) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
