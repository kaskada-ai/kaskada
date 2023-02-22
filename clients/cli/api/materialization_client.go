package api

import (
	"context"
	"fmt"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type materializationClient struct {
	ctx    context.Context
	client apiv1alpha.MaterializationServiceClient
}

type MaterializationClient interface {
	List() ([]*apiv1alpha.Materialization, error)
	Get(name string) (*apiv1alpha.Materialization, error)
	Create(item *apiv1alpha.Materialization) error
	Delete(name string) error
}

func NewMaterializationServiceClient(ctx context.Context, conn *grpc.ClientConn) MaterializationClient {
	return &materializationClient{
		ctx:    ctx,
		client: apiv1alpha.NewMaterializationServiceClient(conn),
	}
}

func (c materializationClient) List() ([]*apiv1alpha.Materialization, error) {
	resp, err := c.client.ListMaterializations(c.ctx, &apiv1alpha.ListMaterializationsRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "listing materializations")
	}
	// TODO: Pagination
	return clearOutputOnlyList(resp.Materializations), nil
}

func (c materializationClient) Get(name string) (*apiv1alpha.Materialization, error) {
	resp, err := c.client.GetMaterialization(c.ctx, &apiv1alpha.GetMaterializationRequest{MaterializationName: name})
	if err != nil {
		return nil, errors.Wrapf(err, "getting materialization: `%s`", name)
	}
	return clearOutputOnly(resp.Materialization), nil
}

func (c materializationClient) Create(item *apiv1alpha.Materialization) error {
	_, err := c.client.CreateMaterialization(c.ctx, &apiv1alpha.CreateMaterializationRequest{Materialization: item})
	if err != nil {
		return errors.Wrapf(err, "creating materialization: `%s`", item.MaterializationName)
	}
	return nil
}

func (c materializationClient) Delete(name string) error {
	_, err := c.client.DeleteMaterialization(c.ctx, &apiv1alpha.DeleteMaterializationRequest{MaterializationName: name})
	if err != nil {
		return errors.Wrapf(err, "deleting materialization: `%s`", name)
	}
	return nil
}

func ProtoToMaterialization(proto protoreflect.ProtoMessage) (*apiv1alpha.Materialization, error) {
	switch t := proto.(type) {
	case *apiv1alpha.Materialization:
		return t, nil
	default:
		return nil, fmt.Errorf("invalid conversion. expected materialization but got: %T", t)
	}
}
