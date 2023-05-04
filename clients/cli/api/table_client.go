package api

import (
	"context"
	"fmt"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/rs/zerolog/log"
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
	Delete(name string, force bool) error
	LoadFile(name string, fileInput *apiv1alpha.FileInput) error
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
		log.Debug().Err(err).Msg("issue listing tables")
		return nil, err
	}
	// TODO: Pagination
	return resp.Tables, nil
}

func (c tableClient) Get(name string) (*apiv1alpha.Table, error) {
	resp, err := c.client.GetTable(c.ctx, &apiv1alpha.GetTableRequest{TableName: name})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue getting table")
		return nil, err
	}
	return resp.Table, nil
}

func (c tableClient) Create(item *apiv1alpha.Table) error {
	_, err := c.client.CreateTable(c.ctx, &apiv1alpha.CreateTableRequest{Table: item})
	if err != nil {
		log.Debug().Err(err).Str("name", item.TableName).Msg("issue creating table")
		return err
	}
	return nil
}

func (c tableClient) Delete(name string, force bool) error {
	_, err := c.client.DeleteTable(c.ctx, &apiv1alpha.DeleteTableRequest{TableName: name, Force: force})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue deleting table")
		return err
	}
	return nil
}

func (c tableClient) LoadFile(name string, fileInput *apiv1alpha.FileInput) error {
	_, err := c.client.LoadData(c.ctx, &apiv1alpha.LoadDataRequest{TableName: name, SourceData: &apiv1alpha.LoadDataRequest_FileInput{fileInput}})
	if err != nil {
		log.Debug().Err(err).Msg("issue loading data")
		return err
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
