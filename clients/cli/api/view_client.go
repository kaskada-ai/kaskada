package api

import (
	"context"
	"fmt"

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
	List() ([]*apiv1alpha.View, error)
	Get(name string) (*apiv1alpha.View, error)
	Create(item *apiv1alpha.View) error
	Delete(name string) error
}

func NewViewServiceClient(ctx context.Context, conn *grpc.ClientConn) ViewClient {
	return &viewClient{
		ctx:    ctx,
		client: apiv1alpha.NewViewServiceClient(conn),
	}
}

func (c viewClient) List() ([]*apiv1alpha.View, error) {
	resp, err := c.client.ListViews(c.ctx, &apiv1alpha.ListViewsRequest{})
	if err != nil {
		log.Debug().Err(err).Msg("issue listing views")
		return nil, err
	}
	// TODO: Pagination
	return clearOutputOnlyList(resp.Views), nil
}

func (c viewClient) Get(name string) (*apiv1alpha.View, error) {
	resp, err := c.client.GetView(c.ctx, &apiv1alpha.GetViewRequest{ViewName: name})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue getting view")
		return nil, err
	}
	return clearOutputOnly(resp.View), nil
}

func (c viewClient) Create(item *apiv1alpha.View) error {
	view, err := c.client.CreateView(c.ctx, &apiv1alpha.CreateViewRequest{View: item})
	if err != nil {
		log.Debug().Err(err).Str("name", item.ViewName).Msg("issue creating view")
		return err
	}
	if view.Analysis != nil && view.Analysis.FenlDiagnostics != nil && view.Analysis.FenlDiagnostics.NumErrors > 0 {
		for _, diag := range view.Analysis.FenlDiagnostics.FenlDiagnostics {
			log.Error().Msg(diag.Formatted)
			return fmt.Errorf("found %d errors in view creation", view.Analysis.FenlDiagnostics.NumErrors)
		}
	}
	return nil
}

func (c viewClient) Delete(name string) error {
	_, err := c.client.DeleteView(c.ctx, &apiv1alpha.DeleteViewRequest{ViewName: name, Force: true})
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
