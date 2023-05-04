package api

import (
	"context"
	"fmt"

	"github.com/kaskada-ai/kaskada/clients/cli/utils"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type viewClient struct {
	ctx    context.Context
	client apiv1alpha.ViewServiceClient
}

type ViewClient interface {
	List(search string, pageSize int32, pageToken string) ([]*apiv1alpha.View, error)
	Get(name string) (*apiv1alpha.View, error)
	Create(item *apiv1alpha.View) (*apiv1alpha.View, error)
	Delete(name string, force bool) error
}

func NewViewServiceClient(ctx context.Context, conn *grpc.ClientConn) ViewClient {
	return &viewClient{
		ctx:    ctx,
		client: apiv1alpha.NewViewServiceClient(conn),
	}
}

func (c viewClient) List(search string, pageSize int32, pageToken string) ([]*apiv1alpha.View, error) {
	resp, err := c.client.ListViews(c.ctx, &apiv1alpha.ListViewsRequest{
		Search:    search,
		PageSize:  pageSize,
		PageToken: pageToken,
	})
	if err != nil {
		log.Debug().Err(err).Msg("issue listing views")
		return nil, err
	}
	// TODO: Pagination
	return resp.Views, nil
}

func (c viewClient) Get(name string) (*apiv1alpha.View, error) {
	resp, err := c.client.GetView(c.ctx, &apiv1alpha.GetViewRequest{ViewName: name})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue getting view")
		return nil, err
	}
	return resp.View, nil
}

func (c viewClient) Create(item *apiv1alpha.View) (*apiv1alpha.View, error) {
	resp, err := c.client.CreateView(c.ctx, &apiv1alpha.CreateViewRequest{View: item})
	if err != nil {
		log.Debug().Err(err).Str("name", item.ViewName).Msg("issue creating view")
		return nil, err
	}
	if resp.Analysis != nil && resp.Analysis.FenlDiagnostics != nil && resp.Analysis.FenlDiagnostics.NumErrors > 0 {
		utils.PrintProtoMessage(resp)
		return nil, fmt.Errorf("found %d errors in view creation", resp.Analysis.FenlDiagnostics.NumErrors)
	}
	return resp.View, nil
}

func (c viewClient) Delete(name string, force bool) error {
	_, err := c.client.DeleteView(c.ctx, &apiv1alpha.DeleteViewRequest{ViewName: name, Force: force})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue deleting view")
		return err
	}
	return nil
}

func ProtoToView(proto protoreflect.ProtoMessage) (*apiv1alpha.View, error) {
	switch t := proto.(type) {
	case *apiv1alpha.View:
		return t, nil
	default:
		return nil, fmt.Errorf("invalid conversion. expected view but got: %T", t)
	}
}
