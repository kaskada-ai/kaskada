package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	apiv2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/property"
)

type queryV2Service struct {
	apiv2alpha.UnimplementedQueryServiceServer

	computeManager     *compute.Manager
	dataTokenClient    internal.DataTokenClient
	kaskadaQueryClient internal.KaskadaQueryClient
}

// NewQueryV2Service creates a new query service
func NewQueryV2Service(computeManager *compute.Manager, dataTokenClient *internal.DataTokenClient, kaskadaQueryClient *internal.KaskadaQueryClient) apiv2alpha.QueryServiceServer {
	return &queryV2Service{
		computeManager:     computeManager,
		dataTokenClient:    *dataTokenClient,
		kaskadaQueryClient: *kaskadaQueryClient,
	}
}

// gets the current dataToken, or the specified dataToken.  Returns the dataTokenID and an updated DataToken config.
func (q *queryV2Service) getDataToken(ctx context.Context, owner *ent.Owner, queryDataToken *apiv2alpha.DataToken) (*ent.DataToken, *apiv2alpha.DataToken, error) {
	var dataToken *ent.DataToken
	var err error

	if queryDataToken == nil {
		queryDataToken = &apiv2alpha.DataToken{
			DataToken: &apiv2alpha.DataToken_LatestDataToken{},
		}
	}

	switch dt := queryDataToken.DataToken.(type) {
	case *apiv2alpha.DataToken_LatestDataToken:
		dataToken, err = q.dataTokenClient.GetCurrentDataToken(ctx, owner)
		queryDataToken.DataToken = &apiv2alpha.DataToken_LatestDataToken{
			LatestDataToken: &apiv2alpha.LatestDataToken{
				DataTokenId: dataToken.ID.String(),
			},
		}

	case *apiv2alpha.DataToken_SpecificDataToken:
		id, parseErr := uuid.Parse(dt.SpecificDataToken.DataTokenId)
		if parseErr != nil {
			return nil, nil, customerrors.NewInvalidArgumentError("data_token")
		} else {
			dataToken, err = q.dataTokenClient.GetDataToken(ctx, owner, id)
		}
	default:

	}
	if err != nil {
		return nil, nil, err
	}
	return dataToken, queryDataToken, nil
}

// CreateQuery calls createQuery and wraps errors with status.
func (q *queryV2Service) CreateQuery(ctx context.Context, request *apiv2alpha.CreateQueryRequest) (*apiv2alpha.CreateQueryResponse, error) {
	resp, err := q.createQuery(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.CreateQuery").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

// createQuery creates a query for a user if it compiles and it isn't a dry-run
func (q *queryV2Service) createQuery(ctx context.Context, owner *ent.Owner, request *apiv2alpha.CreateQueryRequest) (*apiv2alpha.CreateQueryResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.createQuery").Logger()

	queryViews := request.Views
	if queryViews == nil {
		queryViews = &apiv2alpha.QueryViews{
			Views: []*apiv2alpha.QueryView{},
		}
	}

	queryConfig := request.Config

	if queryConfig == nil {
		queryConfig = &apiv2alpha.QueryConfig{}
	}

	if queryConfig.DataToken == nil {
		queryConfig.DataToken = &apiv2alpha.DataToken{
			DataToken: &apiv2alpha.DataToken_LatestDataToken{},
		}
	}

	if queryConfig.Destination == nil {
		queryConfig.Destination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			},
		}
	}

	if queryConfig.ResultBehavior == nil {
		queryConfig.ResultBehavior = &apiv2alpha.ResultBehavior{
			ResultBehavior: &apiv2alpha.ResultBehavior_AllResults{},
		}
	}

	formulas, err := q.computeManager.GetFormulas(ctx, owner, queryViews)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting formulas")
	}

	compileResp, err := q.computeManager.CompileQueryV2(ctx, owner, request.Expression, formulas, queryConfig)
	if err != nil {
		subLogger.Debug().Msg("returning from CompileQueryV2")
		return nil, err
	}

	newKaskadaQuery := &ent.KaskadaQuery{
		Config:          queryConfig,
		CompileResponse: compileResp,
		Expression:      request.Expression,
		Metrics:         &apiv2alpha.QueryMetrics{},
		Views:           q.computeManager.GetUsedViews(formulas, compileResp),
	}

	if compileResp.FenlDiagnostics.NumErrors == 0 {
		newKaskadaQuery.State = property.QueryStateCompiled
	} else {
		newKaskadaQuery.State = property.QueryStateFailure
	}

	if newKaskadaQuery.State == property.QueryStateFailure || request.DryRun {
		response := &apiv2alpha.CreateQueryResponse{
			Query: q.getProtoFromDB(ctx, newKaskadaQuery),
		}
		return response, nil
	}

	dataToken, queryDataToken, err := q.getDataToken(ctx, owner, queryConfig.DataToken)
	if err != nil {
		return nil, err
	}

	newKaskadaQuery.DataTokenID = dataToken.ID
	newKaskadaQuery.Config.DataToken = queryDataToken

	kaskadaQuery, err := q.kaskadaQueryClient.CreateKaskadaQuery(ctx, owner, newKaskadaQuery, true)
	if err != nil {
		return nil, err
	}

	response := &apiv2alpha.CreateQueryResponse{
		Query: q.getProtoFromDB(ctx, kaskadaQuery),
	}
	return response, nil
}

// DeleteQuery calls deleteQuery and wraps errors with status.
func (q *queryV2Service) DeleteQuery(ctx context.Context, request *apiv2alpha.DeleteQueryRequest) (*apiv2alpha.DeleteQueryResponse, error) {
	resp, err := q.deleteQuery(ctx, auth.APIOwnerFromContext(ctx), request.QueryId)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.DeleteQuery").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

// deleteQuery fetches a query by owner and query ID
func (q *queryV2Service) deleteQuery(ctx context.Context, owner *ent.Owner, queryId string) (*apiv2alpha.DeleteQueryResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.deleteQuery").Str("query_id", queryId).Logger()
	queryUUID, err := uuid.Parse(queryId)
	if err != nil {
		subLogger.Warn().Err(err).Msg("invalid query_id")
		return nil, customerrors.NewInvalidArgumentError("query_id")
	}
	err = q.kaskadaQueryClient.DeleteKaskadaQuery(ctx, owner, queryUUID, true)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue finding query in DB")
		return nil, err
	}

	return &apiv2alpha.DeleteQueryResponse{}, nil
}

// GetQuery calls getQuery and wraps errors with status.
func (q *queryV2Service) GetQuery(ctx context.Context, request *apiv2alpha.GetQueryRequest) (*apiv2alpha.GetQueryResponse, error) {
	resp, err := q.getQuery(ctx, auth.APIOwnerFromContext(ctx), request.QueryId)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.GetQuery").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

// getQuery fetches a query by owner and query ID
func (q *queryV2Service) getQuery(ctx context.Context, owner *ent.Owner, queryId string) (*apiv2alpha.GetQueryResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.getQuery").Str("query_id", queryId).Logger()
	queryUUID, err := uuid.Parse(queryId)
	if err != nil {
		subLogger.Warn().Err(err).Msg("invalid query_id")
		return nil, customerrors.NewInvalidArgumentError("query_id")
	}
	kaskadaQuery, err := q.kaskadaQueryClient.GetKaskadaQuery(ctx, owner, queryUUID, true)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue finding query in DB")
		return nil, err
	}

	return &apiv2alpha.GetQueryResponse{
		Query: q.getProtoFromDB(ctx, kaskadaQuery),
	}, nil
}

// ListQuery calls listQueries
func (q *queryV2Service) ListQueries(ctx context.Context, request *apiv2alpha.ListQueriesRequest) (*apiv2alpha.ListQueriesResponse, error) {
	resp, err := q.listQueries(ctx, auth.APIOwnerFromContext(ctx), request.Search, request.PageToken, int(request.PageSize))
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.ListQueries").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

// listQueries searches for queries that contain the search string with pagination
func (q *queryV2Service) listQueries(ctx context.Context, owner *ent.Owner, search string, pageToken string, pageSize int) (*apiv2alpha.ListQueriesResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.listQueries").Logger()
	offset := 0

	if pageToken != "" {
		// decode and unmarshal request object from page token
		// to use instead of the initially passed request object
		data, err := base64.URLEncoding.DecodeString(pageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("page_token")
		}

		innerRequest := &apiv2alpha.ListQueriesRequest{}

		err = proto.Unmarshal(data, innerRequest)
		if err != nil {
			subLogger.Info().AnErr("unmarshal", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("page_token")
		}
		pageSize = int(innerRequest.PageSize)
		offset, err = strconv.Atoi(innerRequest.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("page_token")
		}
	}

	kaskadaQueries, err := q.kaskadaQueryClient.ListKaskadaQueries(ctx, owner, search, pageSize, offset, true)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue listing queries in DB")
		return nil, err
	}

	response := &apiv2alpha.ListQueriesResponse{
		Queries: make([]*apiv2alpha.Query, 0, len(kaskadaQueries)),
	}

	for _, kaskadaQuery := range kaskadaQueries {
		response.Queries = append(response.Queries, q.getProtoFromDB(ctx, kaskadaQuery))
	}

	if len(kaskadaQueries) > 0 && len(kaskadaQueries) == pageSize {
		nextRequest := &apiv2alpha.ListQueriesRequest{
			Search:    search,
			PageSize:  int32(pageSize),
			PageToken: strconv.Itoa(offset + pageSize),
		}

		data, err := proto.Marshal(nextRequest)
		if err != nil {
			subLogger.Err(err).Msg("issue marshaling nextListRequest proto")
			return nil, customerrors.NewInternalError("issue listing queries")
		}

		response.NextPageToken = base64.URLEncoding.EncodeToString(data)
	}

	return response, nil
}

func (q *queryV2Service) getProtoFromDB(ctx context.Context, query *ent.KaskadaQuery) *apiv2alpha.Query {
	responseQuery := &apiv2alpha.Query{
		QueryId:    query.ID.String(),
		CreateTime: timestamppb.New(query.CreatedAt),
		UpdateTime: timestamppb.New(*query.UpdatedAt),
		Expression: query.Expression,
		Views:      query.Views,
		Config:     query.Config,
		Results: &apiv2alpha.QueryResults{
			Output:          &apiv2alpha.QueryOutput{},
			FenlDiagnostics: query.CompileResponse.FenlDiagnostics,
			Schema:          query.CompileResponse.ResultType.GetStruct(),
		},
		Metrics: query.Metrics,
	}

	switch kind := query.Config.Destination.Destination.(type) {
	case *v1alpha.Destination_ObjectStore:
		responseQuery.Results.Output.FileResults = &apiv1alpha.FileResults{
			FileType: kind.ObjectStore.FileType,
			Paths:    []string{},
		}
	}

	switch query.State {
	case property.QueryStateCompiled:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_COMPILED
	case property.QueryStateComputing:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_COMPUTING
	case property.QueryStateFailure:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_FAILURE
	case property.QueryStatePrepared:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_PREPARED
	case property.QueryStatePreparing:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_PREPARING
	case property.QueryStateSuccess:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_SUCCESS
	case property.QueryStateUnspecified:
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_UNSPECIFIED
	default:
		subLogger := log.Ctx(ctx).With().Str("method", "queryV2Service.getProtoFromDB").Logger()
		subLogger.Error().Str("query_state_type", fmt.Sprintf("%T", query.State)).Msg("unknown query state")
		responseQuery.State = apiv2alpha.QueryState_QUERY_STATE_UNSPECIFIED
	}

	return responseQuery
}
