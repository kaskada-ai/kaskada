package api

import (
	"context"
	"fmt"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "listing views")
	}
	// TODO: Pagination
	return clearOutputOnlyList(resp.Views), nil
}

func (c viewClient) Get(name string) (*apiv1alpha.View, error) {
	resp, err := c.client.GetView(c.ctx, &apiv1alpha.GetViewRequest{ViewName: name})
	if err != nil {
		return nil, errors.Wrapf(err, "getting view: `%s`", name)
	}
	return clearOutputOnly(resp.View), nil
}

func (c viewClient) Create(item *apiv1alpha.View) error {
	_, err := c.client.CreateView(c.ctx, &apiv1alpha.CreateViewRequest{View: item})
	if err != nil {
		return errors.Wrapf(err, "creating view: `%s`", item.ViewName)
	}
	return nil
}

func (c viewClient) Delete(name string) error {
	_, err := c.client.DeleteView(c.ctx, &apiv1alpha.DeleteViewRequest{ViewName: name, Force: true})
	if err != nil {
		return errors.Wrapf(err, "deleting view: `%s`", name)
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
