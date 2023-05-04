package api

import (
	"context"
	"fmt"

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
	List() ([]*apiv1alpha.Materialization, error)
	Get(name string) (*apiv1alpha.Materialization, error)
	Create(item *apiv1alpha.Materialization) error
	Delete(name string, force bool) error
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

func (c materializationClient) Create(item *apiv1alpha.Materialization) error {
	mat, err := c.client.CreateMaterialization(c.ctx, &apiv1alpha.CreateMaterializationRequest{Materialization: item})
	if err != nil {
		log.Debug().Err(err).Str("name", item.MaterializationName).Msg("issue creating materialization")
		return err
	}
	if mat.Analysis != nil && mat.Analysis.FenlDiagnostics != nil && mat.Analysis.FenlDiagnostics.NumErrors > 0 {
		for _, diag := range mat.Analysis.FenlDiagnostics.FenlDiagnostics {
			log.Error().Msg(diag.Formatted)
			return fmt.Errorf("found %d errors in materialization creation", mat.Analysis.FenlDiagnostics.NumErrors)
		}
	}
	return nil
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
