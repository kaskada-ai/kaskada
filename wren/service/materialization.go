package service

import (
	"context"
	"encoding/base64"
	"strconv"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

type MaterializationService interface {
	ListMaterializations(ctx context.Context, request *v1alpha.ListMaterializationsRequest) (*v1alpha.ListMaterializationsResponse, error)
	GetMaterialization(ctx context.Context, request *v1alpha.GetMaterializationRequest) (*v1alpha.GetMaterializationResponse, error)
	CreateMaterialization(ctx context.Context, request *v1alpha.CreateMaterializationRequest) (*v1alpha.CreateMaterializationResponse, error)
	DeleteMaterialization(ctx context.Context, request *v1alpha.DeleteMaterializationRequest) (*v1alpha.DeleteMaterializationResponse, error)
}

type materializationService struct {
	v1alpha.UnimplementedMaterializationServiceServer
	kaskadaTableClient    internal.KaskadaTableClient
	kaskadaViewClient     internal.KaskadaViewClient
	materializationClient internal.MaterializationClient
	computeManager        *compute.Manager
}

// NewMaterializationService creates a new materialization service
func NewMaterializationService(computeManager *compute.Manager, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient, materializationClient *internal.MaterializationClient) *materializationService {
	return &materializationService{
		kaskadaTableClient:    *kaskadaTableClient,
		kaskadaViewClient:     *kaskadaViewClient,
		materializationClient: *materializationClient,
		computeManager:        computeManager,
	}
}

func (s *materializationService) ListMaterializations(ctx context.Context, request *v1alpha.ListMaterializationsRequest) (*v1alpha.ListMaterializationsResponse, error) {
	resp, err := s.listMaterializations(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "materializationService.ListMaterializations").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *materializationService) listMaterializations(ctx context.Context, owner *ent.Owner, request *v1alpha.ListMaterializationsRequest) (*v1alpha.ListMaterializationsResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "materializationService.listMaterializations").Logger()
	var (
		pageSize, offset int
	)

	if request.PageToken != "" {
		// decode and unmarshal request object from page token
		// to use instead of the initially passed request object
		data, err := base64.URLEncoding.DecodeString(request.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}

		innerRequest := &v1alpha.ListMaterializationsRequest{}

		err = proto.Unmarshal(data, innerRequest)
		if err != nil {
			subLogger.Info().AnErr("unmarshal", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
		pageSize = int(innerRequest.PageSize)
		offset, err = strconv.Atoi(innerRequest.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
	} else {
		pageSize = int(request.PageSize)
		offset = 0
	}

	materializations, err := s.materializationClient.ListMaterializations(ctx, owner, request.Search, pageSize, offset)
	if err != nil {
		return nil, err
	}

	response := &v1alpha.ListMaterializationsResponse{
		Materializations: make([]*v1alpha.Materialization, 0, len(materializations)),
	}
	for _, materialization := range materializations {
		response.Materializations = append(response.Materializations, s.getProtoFromDB(materialization))
	}

	if len(materializations) > 0 && len(materializations) == pageSize {
		nextRequest := &v1alpha.ListMaterializationsRequest{
			Search:    request.Search,
			PageSize:  int32(pageSize),
			PageToken: strconv.Itoa(offset + pageSize),
		}

		data, err := proto.Marshal(nextRequest)
		if err != nil {
			subLogger.Err(err).Msg("issue listing materializations")
			return nil, customerrors.NewInternalError("issue listing materializations")
		}

		response.NextPageToken = base64.URLEncoding.EncodeToString(data)
	}

	return response, nil
}

func (s *materializationService) GetMaterialization(ctx context.Context, request *v1alpha.GetMaterializationRequest) (*v1alpha.GetMaterializationResponse, error) {
	resp, err := s.getMaterialization(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "materializationService.GetMaterialization").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *materializationService) getMaterialization(ctx context.Context, owner *ent.Owner, request *v1alpha.GetMaterializationRequest) (*v1alpha.GetMaterializationResponse, error) {
	materialization, err := s.materializationClient.GetMaterializationByName(ctx, owner, request.MaterializationName)
	if err != nil {
		return nil, err
	}
	return &v1alpha.GetMaterializationResponse{Materialization: s.getProtoFromDB(materialization)}, nil
}

func (s *materializationService) CreateMaterialization(ctx context.Context, request *v1alpha.CreateMaterializationRequest) (*v1alpha.CreateMaterializationResponse, error) {
	resp, err := s.createMaterialization(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "materializationService.CreateMaterialization").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *materializationService) createMaterialization(ctx context.Context, owner *ent.Owner, request *v1alpha.CreateMaterializationRequest) (*v1alpha.CreateMaterializationResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "materializationService.createMaterialization").Str("expression", request.Materialization.Query).Logger()

	compileResp, err := s.computeManager.CompileQuery(ctx, owner, request.Materialization.Query, request.Materialization.WithViews, false, false, request.Materialization.Slice, v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue compiling materialization")
		return nil, err
	}

	// if cannot execute
	if compileResp.Plan == nil || request.DryRun {
		// then return materialization response without saving to DB...
		subLogger.Debug().Msg("not creating materialization due to diagnostic errors or dry-run")
		return &v1alpha.CreateMaterializationResponse{
			Materialization: request.Materialization,
			Analysis:        getAnalysisFromCompileResponse(compileResp),
		}, nil
	}

	tableMap, err := s.kaskadaTableClient.GetKaskadaTablesFromNames(ctx, owner, compileResp.FreeNames)
	if err != nil {
		return nil, err
	}
	viewMap, err := s.kaskadaViewClient.GetKaskadaViewsFromNames(ctx, owner, compileResp.FreeNames)
	if err != nil {
		return nil, err
	}

	dependencies := make([]*ent.MaterializationDependency, 0, len(compileResp.FreeNames))

	for _, freeName := range compileResp.FreeNames {
		if table, found := tableMap[freeName]; found {
			dependencies = append(dependencies, &ent.MaterializationDependency{
				DependencyType: schema.DependencyType_Table,
				DependencyName: table.Name,
				DependencyID:   &table.ID,
			})
		} else if view, found := viewMap[freeName]; found {
			dependencies = append(dependencies, &ent.MaterializationDependency{
				DependencyType: schema.DependencyType_View,
				DependencyName: view.Name,
				DependencyID:   &view.ID,
			})
		} else {
			subLogger.Error().Str("free_name", freeName).Msg("unable to locate free_name dependency when creating materialization")
			return nil, customerrors.NewInvalidArgumentErrorWithCustomText("materialization expression contains unknown dependency")
		}
	}

	newMaterialization := &ent.Materialization{
		Name:         request.Materialization.MaterializationName,
		Expression:   request.Materialization.Query,
		WithViews:    &v1alpha.WithViews{Views: request.Materialization.WithViews},
		Destination:  request.Materialization.Destination,
		Schema:       compileResp.ResultType.GetStruct(),
		SliceRequest: request.Materialization.Slice,
		Analysis:     getAnalysisFromCompileResponse(compileResp),
	}

	materialization, err := s.materializationClient.CreateMaterialization(ctx, owner, newMaterialization, dependencies)
	if err != nil {
		return nil, err
	}

	subLogger.Debug().Msg("running materializations")
	s.computeManager.RunMaterializations(ctx, owner)

	return &v1alpha.CreateMaterializationResponse{Materialization: s.getProtoFromDB(materialization), Analysis: materialization.Analysis}, nil
}

func (s *materializationService) DeleteMaterialization(ctx context.Context, request *v1alpha.DeleteMaterializationRequest) (*v1alpha.DeleteMaterializationResponse, error) {
	resp, err := s.deleteMaterialization(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "materializationService.DeleteMaterialization").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *materializationService) deleteMaterialization(ctx context.Context, owner *ent.Owner, request *v1alpha.DeleteMaterializationRequest) (*v1alpha.DeleteMaterializationResponse, error) {
	materialization, err := s.materializationClient.GetMaterializationByName(ctx, owner, request.MaterializationName)
	if err != nil {
		return nil, err
	}

	err = s.materializationClient.DeleteMaterialization(ctx, owner, materialization)
	if err != nil {
		return nil, err
	}

	return &v1alpha.DeleteMaterializationResponse{}, nil
}

func (s *materializationService) getProtoFromDB(materialization *ent.Materialization) *v1alpha.Materialization {
	return &v1alpha.Materialization{
		MaterializationId:   materialization.ID.String(),
		MaterializationName: materialization.Name,
		CreateTime:          timestamppb.New(materialization.CreatedAt),
		Query:               materialization.Expression,
		WithViews:           materialization.WithViews.Views,
		Destination:         materialization.Destination,
		Schema:              materialization.Schema,
		Slice:               materialization.SliceRequest,
		Analysis:            materialization.Analysis,
	}
}
