package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

const (
	keyColumnName                 = "key"
	progressReportingSeconds      = 10
	secondsBeforeCancelingCompute = 300
)

type queryV1Service struct {
	v1alpha.UnimplementedQueryServiceServer

	computeManager     compute.ComputeManager
	dataTokenClient    internal.DataTokenClient
	kaskadaQueryClient internal.KaskadaQueryClient
	objectStoreClient  client.ObjectStoreClient
}

// NewQueryV1Service creates a new query service
func NewQueryV1Service(computeManager *compute.ComputeManager, dataTokenClient *internal.DataTokenClient, kaskadaQueryClient *internal.KaskadaQueryClient, objectStoreClient *client.ObjectStoreClient) v1alpha.QueryServiceServer {
	return &queryV1Service{
		computeManager:     *computeManager,
		dataTokenClient:    *dataTokenClient,
		kaskadaQueryClient: *kaskadaQueryClient,
		objectStoreClient:  *objectStoreClient,
	}
}

func (q *queryV1Service) getDataToken(ctx context.Context, owner *ent.Owner, dataTokenId string) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryservice.getDataToken").Logger()
	if dataTokenId == "" {
		dataToken, err := q.dataTokenClient.GetCurrentDataToken(ctx, owner)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting current data_token")
			return nil, err
		}
		return dataToken, nil
	} else {
		id, err := uuid.Parse(dataTokenId)
		if err != nil {
			return nil, customerrors.NewInvalidArgumentError("data_token")
		} else {
			dataToken, err := q.dataTokenClient.GetDataToken(ctx, owner, id)
			if err != nil {
				subLogger.Error().Err(err).Msg("issue getting data_token")
				return nil, err
			}
			return dataToken, nil
		}
	}
}

// CreateQuery streams a query request and creates a query resource
func (q *queryV1Service) CreateQuery(request *v1alpha.CreateQueryRequest, responseStream v1alpha.QueryService_CreateQueryServer) error {
	ctx := responseStream.Context()
	subLogger := log.Ctx(ctx).With().Str("method", "queryservice.CreateQuery").Logger()
	owner := auth.APIOwnerFromContext(ctx)

	if err := q.validateSliceRequest(request.Query.Slice); err != nil {
		subLogger.Debug().Msg("returning from validateSliceRequest")
		return wrapErrorWithStatus(err, subLogger)
	}

	if err := q.validateOutputTo(ctx, request.Query); err != nil {
		return wrapErrorWithStatus(err, subLogger)
	}

	if request.Query.ResultBehavior == v1alpha.Query_RESULT_BEHAVIOR_UNSPECIFIED {
		request.Query.ResultBehavior = v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS
	}

	queryRequest := compute.QueryRequest{
		Query:          request.Query.Expression,
		RequestViews:   make([]*v1alpha.WithView, 0),
		SliceRequest:   request.Query.Slice,
		ResultBehavior: request.Query.ResultBehavior,
	}

	queryOptions := compute.QueryOptions{
		IsFormula:      false,
		IsExperimental: false,
	}

	if request.QueryOptions != nil && request.QueryOptions.ExperimentalFeatures {
		queryOptions.IsExperimental = true
	}

	previousQueryId, err := uuid.Parse(request.Query.QueryId)
	if err == nil {
		previousQuery, err := q.kaskadaQueryClient.GetKaskadaQuery(ctx, owner, previousQueryId, false)
		if err != nil {
			subLogger.Debug().Msg("returning from GetKaskadaQuery")
			return wrapErrorWithStatus(err, subLogger)
		}
		queryRequest.Query = previousQuery.Expression
		for _, view := range previousQuery.Query.Views {
			queryRequest.RequestViews = append(queryRequest.RequestViews, &v1alpha.WithView{
				Name:       view.ViewName,
				Expression: view.Expression,
			})
		}
		queryRequest.SliceRequest = previousQuery.Query.Slice
		queryRequest.ResultBehavior = previousQuery.Query.ResultBehavior
	}

	compileRequest, err := q.computeManager.CreateCompileRequest(ctx, owner, &queryRequest, &queryOptions)
	if err != nil {
		subLogger.Debug().Msg("returning from CreateCompileRequest")
		return wrapErrorWithStatus(err, subLogger)
	}

	compileResponse, err := q.computeManager.RunCompileRequest(ctx, owner, compileRequest)
	if err != nil {
		subLogger.Debug().Msg("returning from RunCompileRequest")
		return wrapErrorWithStatus(err, subLogger)
	}

	// Update the request views with only the views required for the query.
	request.Query.Views = compileResponse.Views

	analysisResponse := &v1alpha.CreateQueryResponse{
		State: v1alpha.CreateQueryResponse_STATE_ANALYSIS,
		Config: &v1alpha.CreateQueryResponse_Config{
			SliceRequest: request.Query.Slice,
		},
		Analysis: &v1alpha.CreateQueryResponse_Analysis{
			CanExecute: compileResponse.ComputeResponse.Plan != nil,
			Schema:     compileResponse.ComputeResponse.ResultType.GetStruct(),
		},
		FenlDiagnostics: compileResponse.ComputeResponse.FenlDiagnostics,
	}

	dataTokenId := ""
	if request.Query.DataTokenId != nil {
		dataTokenId = request.Query.DataTokenId.GetValue()
	}
	dataToken, err := q.getDataToken(ctx, owner, dataTokenId)
	if err != nil {
		subLogger.Debug().Err(err).Msg("returning from getDataToken")
	}

	if dataToken != nil {
		request.Query.DataTokenId = &wrapperspb.StringValue{
			Value: dataToken.ID.String(),
		}
		analysisResponse.Config.DataTokenId = dataToken.ID.String()
	}

	metrics := &v1alpha.CreateQueryResponse_Metrics{}
	if request.QueryOptions != nil && request.QueryOptions.DryRun {
		analysisResponse.Metrics = metrics
		responseStream.Send(analysisResponse)
		subLogger.Debug().Msg("returning from DryRun")
		return nil
	} else {
		responseStream.Send(q.addMetricsIfRequested(request, analysisResponse, metrics))
	}

	if compileResponse.ComputeResponse.Plan == nil {
		responseStream.Send(&v1alpha.CreateQueryResponse{
			State:   v1alpha.CreateQueryResponse_STATE_FAILURE,
			Metrics: metrics,
		})
		subLogger.Debug().Msg("returning because compile plan is nil")
		return nil
	}

	query, err := q.kaskadaQueryClient.CreateKaskadaQuery(ctx, owner, &ent.KaskadaQuery{
		Expression:  request.Query.Expression,
		DataTokenID: dataToken.ID,
		Query:       request.Query,
	}, false)

	if err != nil {
		subLogger.Debug().Msg("returning from CreateKaskadaQuery")
		return wrapErrorWithStatus(err, subLogger)
	}

	queryIDResponse := &v1alpha.CreateQueryResponse{
		QueryId: query.ID.String(),
	}
	responseStream.Send(queryIDResponse)

	limits := &v1alpha.ExecuteRequest_Limits{}
	if request.Query.Limits != nil {
		limits.PreviewRows = request.Query.Limits.PreviewRows
	}

	// send message about starting prepare
	prepareStartTime := time.Now()
	prepareResponse := &v1alpha.CreateQueryResponse{
		State: v1alpha.CreateQueryResponse_STATE_PREPARING,
	}
	responseStream.Send(q.addMetricsIfRequested(request, prepareResponse, metrics))

	// while prepare is on-going:
	// * send progress messages every progressReportingSeconds interval
	prepareTimerContext, prepareTimerContextCancel := context.WithCancel(ctx)
	defer prepareTimerContextCancel()
	if request.QueryOptions != nil && request.QueryOptions.StreamMetrics {
		go func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					prepareResponse.Metrics.TimePreparing = durationpb.New(time.Since(prepareStartTime))
					responseStream.Send(prepareResponse)
				}
				time.Sleep(time.Second * progressReportingSeconds)
			}
		}(prepareTimerContext)
	}

	// do prepare
	tables, err := q.computeManager.GetTablesForCompute(ctx, owner, dataToken, compileResponse.ComputeResponse.TableSlices)
	if err != nil {
		subLogger.Error().Err(err).Str("data_token", dataToken.ID.String()).Msg("issue getting tables for compute")
		return wrapErrorWithStatus(err, subLogger)
	}

	// stop sending prepare progress messages
	prepareTimerContextCancel()
	metrics.TimePreparing = durationpb.New(time.Since(prepareStartTime))

	// send message about starting compute
	computeStartTime := time.Now()
	lastComputeRespnseTime := time.Now()
	computeResponse := &v1alpha.CreateQueryResponse{
		State: v1alpha.CreateQueryResponse_STATE_COMPUTING,
	}
	responseStream.Send(q.addMetricsIfRequested(request, computeResponse, metrics))

	destination := &v1alpha.Destination{}
	if request.Query.Destination != nil {
		outputURI := q.computeManager.GetOutputURI(owner, compileResponse.ComputeResponse.PlanHash.Hash)
		switch kind := request.Query.Destination.Destination.(type) {
		case *v1alpha.Destination_ObjectStore:
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType:        kind.ObjectStore.FileType,
					OutputPrefixUri: outputURI,
				},
			}
		default:
			subLogger.Error().Interface("kind", kind).Msg("unsupported output")
			return status.Errorf(codes.Unimplemented, "output %s is not supported for queries", kind)
		}
	}

	// while compute is on-going:
	// * check to make sure we have heard from the compute engine in the secondsBeforeCancelingCompute window
	// * send progress messages every progressReportingSeconds interval
	computeTimerContext, computeTimerContextCancel := context.WithCancel(ctx)
	defer computeTimerContextCancel()
	queryContext, queryContextCancel := compute.GetNewQueryContext(ctx, owner, request.Query.ChangedSinceTime, compileResponse.ComputeResponse, dataToken, request.Query.FinalResultTime, dataTokenId == "", limits, destination, request.Query.Slice, tables)
	defer queryContextCancel()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if time.Since(lastComputeRespnseTime) > time.Second*secondsBeforeCancelingCompute {
					//if we haven't heard from compute inside the window, cancel the query
					queryContextCancel()
				}

				if request.QueryOptions != nil && request.QueryOptions.StreamMetrics {
					computeResponse.Metrics.TimeComputing = durationpb.New(time.Since(computeStartTime))
					responseStream.Send(computeResponse)
				}
			}
			time.Sleep(time.Second * progressReportingSeconds)
		}
	}(computeTimerContext)

	// start compute
	client, computeStream, err := q.computeManager.InitiateQuery(queryContext)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue initiating query")
		return wrapErrorWithStatus(err, subLogger)
	}
	defer client.Close()

	success := true
	for {
		// if compute canceled, exit with failure
		if queryContext.Cancelled() {
			subLogger.Warn().Msg("streaming query heartbeat missing, ending query")
			success = false
			break
		}

		// Start receiving streaming messages
		computeResponse, err := computeStream.Recv()

		// if compute finished, exit with success
		if err == io.EOF {
			break
		}

		// if compute response error, exit with failure
		if err != nil {
			subLogger.Warn().Err(err).Msg("issue receiving streaming query compute response")
			success = false
			break
		}

		// otherwise process compute response message...
		log.Debug().Interface("computeResponse", computeResponse).Msg("received response from compute")
		lastComputeRespnseTime = time.Now()
		queryResponse := &v1alpha.CreateQueryResponse{
			State: v1alpha.CreateQueryResponse_STATE_COMPUTING,
		}

		switch computeResponse.State {
		case v1alpha.LongQueryState_LONG_QUERY_STATE_INITIAL:
			subLogger.Info().Msg("received initial message from execute request")
		case v1alpha.LongQueryState_LONG_QUERY_STATE_RUNNING:
			subLogger.Info().Interface("progress", computeResponse.Progress).Msg("received progress from execute request")
			// TODO: set query progress metrics here after sparrow starts reporting progress info
		case v1alpha.LongQueryState_LONG_QUERY_STATE_FINAL:
			subLogger.Info().Bool("query_done", computeResponse.IsQueryDone).Msg("received final message from execute request")
		default:
			subLogger.Error().Str("state", computeResponse.State.String()).Msg("unexpected long query state")
		}

		if computeResponse.Destination != nil {
			switch kind := request.Query.Destination.Destination.(type) {
			case *v1alpha.Destination_ObjectStore:
				outputPaths := []string{}
				outputPaths = append(outputPaths, computeResponse.Destination.GetObjectStore().GetOutputPaths().Paths...)

				if request.QueryOptions != nil && request.QueryOptions.PresignResults {
					outputPaths, err = q.presignResults(ctx, owner, outputPaths)
					if err != nil {
						return fmt.Errorf("error signing results")
					}
				}

				outputURI := q.computeManager.GetOutputURI(owner, compileResponse.ComputeResponse.PlanHash.Hash)
				queryResponse.Destination = &v1alpha.Destination{
					Destination: &v1alpha.Destination_ObjectStore{
						ObjectStore: &v1alpha.ObjectStoreDestination{
							FileType:        kind.ObjectStore.FileType,
							OutputPrefixUri: outputURI,
							OutputPaths: &v1alpha.ObjectStoreDestination_ResultPaths{
								Paths: outputPaths,
							},
						},
					},
				}

				metrics.OutputFiles += int64(len(computeResponse.Destination.GetObjectStore().GetOutputPaths().Paths))
			default:
				subLogger.Error().Interface("kind", kind).Msg("unknown output type")
				return fmt.Errorf("query output type %s is not supported", kind)
			}
		}

		if computeResponse.Progress != nil {
			metrics.TotalInputRows = computeResponse.Progress.TotalInputRows
			metrics.ProcessedInputRows = computeResponse.Progress.ProcessedInputRows
			metrics.ProducedOutputRows = computeResponse.Progress.ProducedOutputRows
		}

		q.computeManager.SaveComputeSnapshots(queryContext, computeResponse.ComputeSnapshots)

		metrics.TimeComputing = durationpb.New(time.Since(computeStartTime))
		responseStream.Send(q.addMetricsIfRequested(request, queryResponse, metrics))
	}

	// stop monitoring compute
	computeTimerContextCancel()

	// send final message
	if success {
		responseStream.Send(&v1alpha.CreateQueryResponse{
			State:   v1alpha.CreateQueryResponse_STATE_SUCCESS,
			Metrics: metrics,
		})
	} else {
		responseStream.Send(&v1alpha.CreateQueryResponse{
			State:   v1alpha.CreateQueryResponse_STATE_FAILURE,
			Metrics: metrics,
		})
	}

	subLogger.Debug().Msg("complete")
	return nil
}

// validates the OutputTo field of the query, defaulting if unspecified or unknown.
func (q *queryV1Service) validateOutputTo(ctx context.Context, query *v1alpha.Query) error {
	subLogger := log.Ctx(ctx).With().Str("method", "queryservice.validateOutputTo").Logger()
	if query.Destination == nil {
		subLogger.Warn().Msg("mssing output_to, defaulting to 'ObjectStore->Parquet'")
	} else {
		switch kind := query.Destination.Destination.(type) {
		case *v1alpha.Destination_ObjectStore:
			switch kind.ObjectStore.FileType {
			case v1alpha.FileType_FILE_TYPE_PARQUET, v1alpha.FileType_FILE_TYPE_CSV:
				return nil
			default:
				subLogger.Warn().Interface("kind", kind).Interface("type", kind.ObjectStore.FileType).Msg("unknown output_to file_type, defaulting to 'ObjectStore->Parquet'")
			}
		case *v1alpha.Destination_Pulsar, *v1alpha.Destination_Redis:
			return fmt.Errorf("query output type: %s is only valid for materializations", kind)
		default:
			subLogger.Warn().Interface("kind", kind).Msg("unknown output_to, defaulting to 'ObjectStore->Parquet'")
		}
	}

	// set default if haven't yet returned from this method
	query.Destination = &v1alpha.Destination{
		Destination: &v1alpha.Destination_ObjectStore{
			ObjectStore: &v1alpha.ObjectStoreDestination{
				FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
			},
		},
	}

	return nil
}

func (q *queryV1Service) validateSliceRequest(sliceRequest *v1alpha.SliceRequest) error {
	// Request did not contain any slicing request. Only perform validation when slices were provided
	if sliceRequest != nil {
		switch t := sliceRequest.Slice.(type) {
		case *v1alpha.SliceRequest_Percent:
			if t.Percent.Percent < 0.1 {
				return status.Errorf(codes.InvalidArgument, "slice: %f is too small ", t.Percent.Percent)
			}

			if t.Percent.Percent > 100 {
				return status.Errorf(codes.InvalidArgument, "slice: %f is too large", t.Percent.Percent)
			}
		case *v1alpha.SliceRequest_EntityKeys:
			for _, k := range t.EntityKeys.EntityKeys {
				if len(strings.TrimSpace(k)) == 0 {
					return status.Errorf(codes.InvalidArgument, "slice: %s is invalid", k)
				}
			}
		default:
			log.Error().Interface("slice_request", sliceRequest).Msg("unknown slice_request is not a supported slice")
			return status.Errorf(codes.InvalidArgument, "slice: %v is not a supported slice", t)
		}
	}
	return nil
}

// if the request has `stream_metrics` set to `true`, then add the metrics object to the response object before returning
func (q *queryV1Service) addMetricsIfRequested(request *v1alpha.CreateQueryRequest, response *v1alpha.CreateQueryResponse, metrics *v1alpha.CreateQueryResponse_Metrics) *v1alpha.CreateQueryResponse {
	if request.QueryOptions != nil && request.QueryOptions.StreamMetrics {
		response.Metrics = metrics
	}
	return response
}

func (q *queryV1Service) presignResults(ctx context.Context, owner *ent.Owner, URIs []string) ([]string, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryservice.presignResults").Logger()
	presignedURIs := []string{}
	for _, fromURI := range URIs {
		presignedURI, err := q.objectStoreClient.GetPresignedDownloadURL(ctx, fromURI)
		if err != nil {
			subLogger.Error().Err(err).Str("from_uri", fromURI).Msg("error presigning download url")
			return nil, err
		}
		presignedURIs = append(presignedURIs, presignedURI)
	}
	return presignedURIs, nil
}

// GetQuery calls getQuery and wraps errors with status.
func (q *queryV1Service) GetQuery(ctx context.Context, request *v1alpha.GetQueryRequest) (*v1alpha.GetQueryResponse, error) {
	resp, err := q.getQuery(ctx, auth.APIOwnerFromContext(ctx), request.QueryId)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "query.GetQuery").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

// getQuery fetches a query by owner and query ID
func (q *queryV1Service) getQuery(ctx context.Context, owner *ent.Owner, queryId string) (*v1alpha.GetQueryResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryservice.getQuery").Logger()
	queryUUID, err := uuid.Parse(queryId)
	if err != nil {
		return nil, customerrors.NewInvalidArgumentError("query_id")
	}
	query, err := q.kaskadaQueryClient.GetKaskadaQuery(ctx, owner, queryUUID, false)
	if err != nil {
		subLogger.Error().Err(err).Str("query_id", queryId).Msg("issue getting query from db table")
		return nil, err
	}
	return &v1alpha.GetQueryResponse{
		Query: query.Query,
	}, nil
}

// ListQuery calls listQueries
func (q *queryV1Service) ListQueries(ctx context.Context, request *v1alpha.ListQueriesRequest) (*v1alpha.ListQueriesResponse, error) {
	return q.listQueries(ctx, auth.APIOwnerFromContext(ctx), request.Search, request.PageToken, int(request.PageSize))
}

// listQueries searches for queries that contain the search string with pagination
func (q *queryV1Service) listQueries(ctx context.Context, owner *ent.Owner, search string, pageToken string, pageSize int) (*v1alpha.ListQueriesResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "queryService.listQueries").Logger()
	offset := 0

	if pageToken != "" {
		// decode and unmarshal request object from page token
		// to use instead of the initially passed request object
		data, err := base64.URLEncoding.DecodeString(pageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}

		innerRequest := &v1alpha.ListQueriesRequest{}

		err = proto.Unmarshal(data, innerRequest)
		if err != nil {
			subLogger.Info().AnErr("unmarshal", err).Msg("invalid page token")
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}
		pageSize = int(innerRequest.PageSize)
		offset, err = strconv.Atoi(innerRequest.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}
	}

	queries, err := q.kaskadaQueryClient.ListKaskadaQueries(ctx, owner, search, pageSize, offset, false)
	if err != nil {
		return nil, err
	}

	response := &v1alpha.ListQueriesResponse{
		Queries: make([]*v1alpha.Query, 0, len(queries)),
	}

	for _, query := range queries {
		response.Queries = append(response.Queries, query.Query)
	}

	if len(queries) > 0 && len(queries) == pageSize {
		nextRequest := &v1alpha.ListQueriesRequest{
			Search:    search,
			PageSize:  int32(pageSize),
			PageToken: strconv.Itoa(offset + pageSize),
		}

		data, err := proto.Marshal(nextRequest)
		if err != nil {
			subLogger.Err(err).Msg("issue listing queries")
			return nil, status.Error(codes.Internal, "issue listing queries")
		}

		response.NextPageToken = base64.URLEncoding.EncodeToString(data)
	}

	return response, nil
}
