package client

import (
	"google.golang.org/grpc"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

// NewFileServiceClient creates a new FileServiceClient from a gRPC connection
func NewFileServiceClient(conn *grpc.ClientConn) FileServiceClient {
	return fileServiceClient{
		conn:              conn,
		FileServiceClient: v1alpha.NewFileServiceClient(conn),
	}
}

type fileServiceClient struct {
	conn *grpc.ClientConn
	v1alpha.FileServiceClient
}

func (s fileServiceClient) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
