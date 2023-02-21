package client

import (
	"google.golang.org/grpc"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// NewPrepareServiceClient creates a new PrepareServiceClient from a gRPC connection
func NewPrepareServiceClient(conn *grpc.ClientConn) PrepareServiceClient {
	return prepareServiceClient{
		conn:                     conn,
		PreparationServiceClient: v1alpha.NewPreparationServiceClient(conn),
	}
}

type prepareServiceClient struct {
	conn *grpc.ClientConn
	v1alpha.PreparationServiceClient
}

func (s prepareServiceClient) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
