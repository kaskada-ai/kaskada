package api

import (
	"context"
	"io"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type queryClient struct {
	ctx    context.Context
	client apiv1alpha.QueryServiceClient
}

type QueryClient interface {
	Query(request *apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error)
}

func NewQueryServiceClient(ctx context.Context, conn *grpc.ClientConn) QueryClient {
	return &queryClient{
		ctx:    ctx,
		client: apiv1alpha.NewQueryServiceClient(conn),
	}
}

func (c queryClient) Query(request *apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	if request.QueryOptions == nil {
		request.QueryOptions = &apiv1alpha.QueryOptions{
			StreamMetrics: false,
		}
	}

	queryStream, err := c.client.CreateQuery(c.ctx, request)
	if err != nil {
		log.Debug().Err(err).Msg("issue creating query")
		return nil, err
	}
	resp := &apiv1alpha.CreateQueryResponse{}
	for {
		// Start receiving streaming messages
		streamResp, err := queryStream.Recv()

		// if query finished, exit
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Debug().Err(err).Msg("issue getting query response")
			return nil, err
		}
		proto.Merge(resp, streamResp)
	}
	return resp, nil
}
