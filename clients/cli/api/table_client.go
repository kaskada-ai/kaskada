package api

import (
	"context"
	"fmt"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type tableClient struct {
	ctx    context.Context
	client apiv1alpha.TableServiceClient
}

type TableClient interface {
	List() ([]*apiv1alpha.Table, error)
	Get(name string) (*apiv1alpha.Table, error)
	Create(item *apiv1alpha.Table) error
	Delete(name string) error
}

func NewTableServiceClient(ctx context.Context, conn *grpc.ClientConn) TableClient {
	return &tableClient{
		ctx:    ctx,
		client: apiv1alpha.NewTableServiceClient(conn),
	}
}

func (c tableClient) List() ([]*apiv1alpha.Table, error) {
	resp, err := c.client.ListTables(c.ctx, &apiv1alpha.ListTablesRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "listing tables")
	}
	// TODO: Pagination
	return clearOutputOnlyList(resp.Tables), nil
}

func (c tableClient) Get(name string) (*apiv1alpha.Table, error) {
	resp, err := c.client.GetTable(c.ctx, &apiv1alpha.GetTableRequest{TableName: name})
	if err != nil {
		return nil, errors.Wrapf(err, "getting table: `%s`", name)
	}
	return clearOutputOnly(resp.Table), nil
}

func (c tableClient) Create(item *apiv1alpha.Table) error {
	_, err := c.client.CreateTable(c.ctx, &apiv1alpha.CreateTableRequest{Table: item})
	if err != nil {
		return errors.Wrapf(err, "creating table: `%s`", item.TableName)
	}
	return nil
}

func (c tableClient) Delete(name string) error {
	_, err := c.client.DeleteTable(c.ctx, &apiv1alpha.DeleteTableRequest{TableName: name, Force: true})
	if err != nil {
		return errors.Wrapf(err, "deleting table: `%s`", name)
	}
	return nil
}

func ProtoToTable(proto protoreflect.ProtoMessage) (*apiv1alpha.Table, error) {
	switch t := proto.(type) {
	case *apiv1alpha.Table:
		return t, nil
	default:
		return nil, fmt.Errorf("invalid conversion. expected table but got: %T", t)
	}
}
