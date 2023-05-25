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
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
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
	kaskadaTableClient     internal.KaskadaTableClient
	kaskadaViewClient      internal.KaskadaViewClient
	dataTokenClient        internal.DataTokenClient
	materializationClient  internal.MaterializationClient
	computeManager         compute.ComputeManager
	materializationManager compute.MaterializationManager
}

// NewMaterializationService creates a new materialization service
func NewMaterializationService(computeManager *compute.ComputeManager, materializationManager *compute.MaterializationManager, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient, dataTokenClient *internal.DataTokenClient, materializationClient *internal.MaterializationClient) *materializationService {
	return &materializationService{
		kaskadaTableClient:     *kaskadaTableClient,
		kaskadaViewClient:      *kaskadaViewClient,
		dataTokenClient:        *dataTokenClient,
		materializationClient:  *materializationClient,
		computeManager:         *computeManager,
		materializationManager: *materializationManager,
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
		materializationProto, err := s.getProtoFromDB(ctx, owner, materialization)
		if err != nil {
			return nil, err
		}
		response.Materializations = append(response.Materializations, materializationProto)
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
	materializationProto, err := s.getProtoFromDB(ctx, owner, materialization)
	if err != nil {
		return nil, err
	}
	return &v1alpha.GetMaterializationResponse{Materialization: materializationProto}, nil
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
	if request.Materialization == nil {
		return nil, customerrors.NewInvalidArgumentErrorWithCustomText("missing materialization definition")
	}

	if request.Materialization.Expression == "" {
		return nil, customerrors.NewInvalidArgumentErrorWithCustomText("missing materialization expression")
	}

	if request.Materialization.Destination == nil {
		return nil, customerrors.NewInvalidArgumentErrorWithCustomText("missing materialization destination")
	}

	subLogger := log.Ctx(ctx).With().Str("method", "materializationService.createMaterialization").Str("expression", request.Materialization.Expression).Logger()
	compileResp, _, err := s.materializationManager.CompileV1Materialization(ctx, owner, request.Materialization)
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

	sourceType := materialization.SourceTypeUnspecified
	for _, table := range tableMap {
		var newSourceType materialization.SourceType
		switch table.Source.Source.(type) {
		case *v1alpha.Source_Kaskada:
			newSourceType = materialization.SourceTypeFiles
		case *v1alpha.Source_Pulsar:
			newSourceType = materialization.SourceTypeStreams
		default:
			log.Error().Msgf("unknown source type %T", table.Source.Source)
			return nil, customerrors.NewInternalError("unknown table source type")
		}
		if sourceType == materialization.SourceTypeUnspecified {
			sourceType = newSourceType
		} else if sourceType != newSourceType {
			return nil, customerrors.NewInvalidArgumentErrorWithCustomText("cannot materialize tables from different source types")
		}
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

	sliceRequest := &v1alpha.SliceRequest{}
	if request.Materialization.Slice != nil {
		sliceRequest = request.Materialization.Slice
	}

	// The first time we compute a materialization, we want the
	// `changed_since_time` to be from the time of the earliest event
	// in any file. The `DataVersionID` is used to determine the time
	// from new files, so setting it to 0 ensures we evaluate all files
	// to find the earliest event time.
	var dataVersionID int64 = 0
	var materializationVersion int64 = 0
	newMaterialization := &ent.Materialization{
		Name:          request.Materialization.MaterializationName,
		Version:       materializationVersion,
		Expression:    request.Materialization.Expression,
		WithViews:     &v1alpha.WithViews{Views: request.Materialization.WithViews},
		Destination:   request.Materialization.Destination,
		Schema:        compileResp.ResultType.GetStruct(),
		SliceRequest:  sliceRequest,
		Analysis:      getAnalysisFromCompileResponse(compileResp),
		DataVersionID: dataVersionID,
		SourceType:    sourceType,
	}

	createdMaterialization, err := s.materializationClient.CreateMaterialization(ctx, owner, newMaterialization, dependencies)
	if err != nil {
		return nil, err
	}

	switch sourceType {
	case materialization.SourceTypeFiles:
		subLogger.Debug().Msg("running materializations")
		s.computeManager.RunMaterializations(ctx, owner)
	case materialization.SourceTypeStreams:
		subLogger.Debug().Msg("adding materialization to compute")
		err := s.materializationManager.StartMaterialization(ctx, owner, createdMaterialization.ID.String(), compileResp, createdMaterialization.Destination)
		if err != nil {
			return nil, err
		}
	default:
		log.Error().Msgf("unknown source type %T", sourceType)
		return nil, customerrors.NewInternalError("unknown table source type")
	}

	// Get the newly computed materialization and its associated data token.
	//
	// Note that we can't just return the "current data token", as it's possible a newer data token
	// than the one the materialization is run on is the "current token".
	//
	// We could also store the `data_token_id` (or DataToken itself) on the materialization,
	// which would allow us to skip the secondary lookup of the token from version.
	computedMaterialization, err := s.materializationClient.GetMaterialization(ctx, owner, createdMaterialization.ID)
	if err != nil {
		return nil, err
	}
	materializationProto, err := s.getProtoFromDB(ctx, owner, computedMaterialization)
	if err != nil {
		return nil, err
	}
	return &v1alpha.CreateMaterializationResponse{Materialization: materializationProto, Analysis: createdMaterialization.Analysis}, nil
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
	foundMaterialization, err := s.materializationClient.GetMaterializationByName(ctx, owner, request.MaterializationName)
	if err != nil {
		return nil, err
	}

	if foundMaterialization.SourceType == materialization.SourceTypeStreams {
		err := s.materializationManager.StopMaterialization(ctx, foundMaterialization.ID.String())
		if err != nil {
			subLogger := log.Ctx(ctx).With().Str("method", "materializationService.deleteMaterialization").Logger()
			subLogger.Warn().Err(err).Str("materialization_id", foundMaterialization.ID.String()).Msg("unable to stop materialization on engine")
		}
	}

	err = s.materializationClient.DeleteMaterialization(ctx, owner, foundMaterialization)
	if err != nil {
		return nil, err
	}

	return &v1alpha.DeleteMaterializationResponse{}, nil
}

func (s *materializationService) getProtoFromDB(ctx context.Context, owner *ent.Owner, materialization *ent.Materialization) (*v1alpha.Materialization, error) {
	dataTokenId := ""
	dataToken, err := s.dataTokenClient.GetDataTokenFromVersion(ctx, owner, materialization.DataVersionID)
	if err != nil {
		return nil, err
	}

	// Materializations are created with a data version id of 0,
	// which corresponds to an empty token id.
	if dataToken != nil {
		dataTokenId = dataToken.ID.String()
	}

	return &v1alpha.Materialization{
		MaterializationId:   materialization.ID.String(),
		MaterializationName: materialization.Name,
		CreateTime:          timestamppb.New(materialization.CreatedAt),
		Expression:          materialization.Expression,
		WithViews:           materialization.WithViews.Views,
		Destination:         materialization.Destination,
		Schema:              materialization.Schema,
		Slice:               materialization.SliceRequest,
		Analysis:            materialization.Analysis,
		DataTokenId:         dataTokenId,
	}, nil
}
