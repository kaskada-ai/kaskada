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

type materializationClient struct {
	ctx    context.Context
	client apiv1alpha.MaterializationServiceClient
}

type MaterializationClient interface {
	List(search string, pageSize int32, pageToken string) ([]*apiv1alpha.Materialization, error)
	Get(name string) (*apiv1alpha.Materialization, error)
	Create(item *apiv1alpha.Materialization) (*apiv1alpha.Materialization, error)
	Delete(name string, force bool) error
}

func NewMaterializationServiceClient(ctx context.Context, conn *grpc.ClientConn) MaterializationClient {
	return &materializationClient{
		ctx:    ctx,
		client: apiv1alpha.NewMaterializationServiceClient(conn),
	}
}

func (c materializationClient) List(search string, pageSize int32, pageToken string) ([]*apiv1alpha.Materialization, error) {
	resp, err := c.client.ListMaterializations(c.ctx, &apiv1alpha.ListMaterializationsRequest{
		Search:     search,
		PageSize:   pageSize,
		PageToken:  pageToken,
	})
	if err != nil {
		log.Debug().Err(err).Msg("issue listing materializations")
		return nil, err
	}
	// TODO: Pagination
	return resp.Materializations, nil
}

func (c materializationClient) Get(name string) (*apiv1alpha.Materialization, error) {
	resp, err := c.client.GetMaterialization(c.ctx, &apiv1alpha.GetMaterializationRequest{MaterializationName: name})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue getting materialization")
		return nil, err
	}
	return resp.Materialization, nil
}

func (c materializationClient) Create(item *apiv1alpha.Materialization) (*apiv1alpha.Materialization, error) {
	resp, err := c.client.CreateMaterialization(c.ctx, &apiv1alpha.CreateMaterializationRequest{Materialization: item})
	if err != nil {
		log.Debug().Err(err).Str("name", item.MaterializationName).Msg("issue creating materialization")
		return nil, err
	}
	if resp.Analysis != nil && resp.Analysis.FenlDiagnostics != nil && resp.Analysis.FenlDiagnostics.NumErrors > 0 {
		utils.PrintProtoMessage(resp)
		return nil, fmt.Errorf("found %d errors in materialization creation", resp.Analysis.FenlDiagnostics.NumErrors)
	}
	return resp.Materialization, nil
}

func (c materializationClient) Delete(name string, force bool) error {
	_, err := c.client.DeleteMaterialization(c.ctx, &apiv1alpha.DeleteMaterializationRequest{MaterializationName: name})
	if err != nil {
		log.Debug().Err(err).Str("name", name).Msg("issue deleting materialization")
		return err
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
