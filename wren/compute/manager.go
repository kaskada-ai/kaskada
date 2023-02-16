package compute

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/kaskada/kaskada-ai/wren/auth"
	"github.com/kaskada/kaskada-ai/wren/client"
	"github.com/kaskada/kaskada-ai/wren/customerrors"
	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadafile"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v2alpha"
	"github.com/kaskada/kaskada-ai/wren/internal"
	"github.com/kaskada/kaskada-ai/wren/store"
	"github.com/kaskada/kaskada-ai/wren/utils"
)

const (
	keyColumnName         = "key"
	compileTimeoutSeconds = 10
)

type Manager struct {
	computeClients        *client.ComputeClients
	errGroup              *errgroup.Group
	dataTokenClient       internal.DataTokenClient
	kaskadaTableClient    internal.KaskadaTableClient
	kaskadaViewClient     internal.KaskadaViewClient
	materializationClient internal.MaterializationClient
	prepareJobClient      internal.PrepareJobClient
	parallelizeConfig     utils.ParallelizeConfig
	store                 client.ObjectStoreClient
	tableStore            store.TableStore
	tr                    trace.Tracer
}

// NewManager creates a new compute manager
func NewManager(errGroup *errgroup.Group, computeClients *client.ComputeClients, dataTokenClient *internal.DataTokenClient, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient, materializationClient *internal.MaterializationClient, prepareJobClient internal.PrepareJobClient, objectStoreClient *client.ObjectStoreClient, tableStore store.TableStore, parallelizeConfig utils.ParallelizeConfig) *Manager {
	return &Manager{
		computeClients:        computeClients,
		errGroup:              errGroup,
		dataTokenClient:       *dataTokenClient,
		kaskadaTableClient:    *kaskadaTableClient,
		kaskadaViewClient:     *kaskadaViewClient,
		materializationClient: *materializationClient,
		parallelizeConfig:     parallelizeConfig,
		prepareJobClient:      prepareJobClient,
		store:                 *objectStoreClient,
		tableStore:            tableStore,
		tr:                    otel.Tracer("ComputeManager"),
	}
}

func (m *Manager) CompileQuery(ctx context.Context, owner *ent.Owner, query string, requestViews []*v1alpha.WithView, isFormula bool, isExperimental bool, sliceRequest *v1alpha.SliceRequest, resultBehavior v1alpha.Query_ResultBehavior) (*v1alpha.CompileResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.CompileQuery").Logger()
	formulas, err := m.getFormulas(ctx, owner, requestViews)
	if err != nil {
		return nil, errors.WithMessage(err, "getting views")
	}

	tables, err := m.getTablesForCompile(ctx, owner)
	if err != nil {
		return nil, errors.WithMessage(err, "getting tables for compile")
	}

	var perEntityBehavior v1alpha.PerEntityBehavior

	switch resultBehavior {
	case v1alpha.Query_RESULT_BEHAVIOR_UNSPECIFIED:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_UNSPECIFIED
	case v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_ALL
	case v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL
	case v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS_AT_TIME:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL_AT_TIME
	default:
		subLogger.Error().Str("resultBehavior", resultBehavior.String()).Msg("unexpeted resultBehavior")
		return nil, fmt.Errorf("unexpected resultBehavior: %s", resultBehavior.String())
	}

	compileRequest := &v1alpha.CompileRequest{
		Experimental: isExperimental,
		FeatureSet: &v1alpha.FeatureSet{
			Formulas: formulas,
			Query:    query,
		},
		PerEntityBehavior: perEntityBehavior,
		SliceRequest:      sliceRequest,
		Tables:            tables,
	}

	if isFormula {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_FORMULA
	} else {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_COMPLETE
	}

	queryClient := m.computeClients.ComputeServiceClient(ctx)
	subLogger.Info().Interface("request", compileRequest).Msg("sending compile request")
	compileTimeoutCtx, compileTimeoutCancel := context.WithTimeout(ctx, time.Second*compileTimeoutSeconds)
	defer compileTimeoutCancel()
	compileResponse, err := queryClient.Compile(compileTimeoutCtx, compileRequest)
	subLogger.Info().Err(err).Interface("response", compileResponse).Msg("received compile respone")

	return compileResponse, err
}

// gets the set of passed views and system views available for a query
func (m *Manager) GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error) {
	requestViews := []*v1alpha.WithView{}
	for _, queryView := range views.Views {
		requestViews = append(requestViews, &v1alpha.WithView{
			Name:       queryView.ViewName,
			Expression: queryView.Expression,
		})
	}

	formulas, err := m.getFormulas(ctx, owner, requestViews)
	if err != nil {
		return nil, errors.WithMessage(err, "getting views")
	}
	return formulas, nil
}

// gets the actual set of views used in a compiled query
func (m *Manager) GetUsedViews(formulas []*v1alpha.Formula, compileResponse *v1alpha.CompileResponse) *v2alpha.QueryViews {
	formulaMap := make(map[string]string, len(formulas))

	for _, formula := range formulas {
		formulaMap[formula.Name] = formula.Formula
	}

	views := []*v2alpha.QueryView{}

	for _, freeName := range compileResponse.FreeNames {
		if formula, found := formulaMap[freeName]; found {
			views = append(views, &v2alpha.QueryView{ViewName: freeName, Expression: formula})
		}
	}

	return &v2alpha.QueryViews{Views: views}
}

func (m *Manager) CompileQueryV2(ctx context.Context, owner *ent.Owner, expression string, formulas []*v1alpha.Formula, config *v2alpha.QueryConfig) (*v1alpha.CompileResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.CompileQueryV2").Logger()

	tables, err := m.getTablesForCompile(ctx, owner)
	if err != nil {
		return nil, errors.WithMessage(err, "getting tables for compile")
	}

	var perEntityBehavior v1alpha.PerEntityBehavior
	switch config.ResultBehavior.ResultBehavior.(type) {
	case *v2alpha.ResultBehavior_AllResults:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_ALL
	case *v2alpha.ResultBehavior_FinalResults:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL
	case *v2alpha.ResultBehavior_FinalResultsAtTime:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL_AT_TIME
	default:
		subLogger.Error().Str("resultBehavior", fmt.Sprintf("%T", config.ResultBehavior.ResultBehavior)).Msg("unexpeted resultBehavior")
		return nil, customerrors.NewInternalError("unexpected resultBehavior")
	}

	sliceRequest := &v1alpha.SliceRequest{}
	if config.Slice != nil && config.Slice.Slice != nil {
		switch s := config.Slice.Slice.(type) {
		case *v1alpha.SliceRequest_EntityKeys:
			sliceRequest.Slice = &v1alpha.SliceRequest_EntityKeys{
				EntityKeys: &v1alpha.SliceRequest_EntityKeysSlice{
					EntityKeys: s.EntityKeys.EntityKeys,
				},
			}
		case *v1alpha.SliceRequest_Percent:
			sliceRequest.Slice = &v1alpha.SliceRequest_Percent{
				Percent: &v1alpha.SliceRequest_PercentSlice{
					Percent: s.Percent.Percent,
				},
			}
		default:
			subLogger.Error().Str("sliceRequest", fmt.Sprintf("%T", config.Slice.Slice)).Msg("unexpeted sliceRequest")
			return nil, customerrors.NewInternalError("unexpected sliceRequest")
		}
	}

	incrementalQueryExperiment := false
	for _, experimentalFeature := range config.ExperimentalFeatures {
		switch {
		case strings.EqualFold("incremental", experimentalFeature):
			incrementalQueryExperiment = true
		}
	}

	compileRequest := &v1alpha.CompileRequest{
		Experimental: incrementalQueryExperiment,
		FeatureSet: &v1alpha.FeatureSet{
			Formulas: formulas,
			Query:    expression,
		},
		PerEntityBehavior: perEntityBehavior,
		SliceRequest:      sliceRequest,
		Tables:            tables,
	}

	isFormula := false

	if isFormula {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_FORMULA
	} else {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_COMPLETE
	}

	queryClient := m.computeClients.ComputeServiceClient(ctx)
	subLogger.Info().Interface("request", compileRequest).Msg("sending compile request")
	compileTimeoutCtx, compileTimeoutCancel := context.WithTimeout(ctx, time.Second*compileTimeoutSeconds)
	defer compileTimeoutCancel()
	compileResponse, err := queryClient.Compile(compileTimeoutCtx, compileRequest)
	subLogger.Info().Err(err).
		Interface("fenl_diagnostics", compileResponse.FenlDiagnostics).
		Bool("incremental_enabled", compileResponse.IncrementalEnabled).
		Strs("free_names", compileResponse.FreeNames).
		Strs("missing_names", compileResponse.MissingNames).
		Interface("plan_hash", compileResponse.PlanHash).
		Interface("result_type", compileResponse.ResultType).
		Interface("slices", compileResponse.TableSlices).Msg("received compile response")

	return compileResponse, err
}

type QueryRequest struct {
	Query          string
	RequestViews   []*v1alpha.WithView
	SliceRequest   *v1alpha.SliceRequest
	ResultBehavior v1alpha.Query_ResultBehavior
}
type QueryOptions struct {
	IsFormula      bool
	IsExperimental bool
}

type CompileQueryResponse struct {
	ComputeResponse *v1alpha.CompileResponse
	Views           []*v1alpha.View
}

func (m *Manager) CreateCompileRequest(ctx context.Context, owner *ent.Owner, request *QueryRequest, options *QueryOptions) (*v1alpha.CompileRequest, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.CreateCompileRequest").Logger()
	formulas, err := m.getFormulas(ctx, owner, request.RequestViews)
	if err != nil {
		return nil, errors.WithMessage(err, "getting views")
	}

	tables, err := m.getTablesForCompile(ctx, owner)
	if err != nil {
		return nil, errors.WithMessage(err, "getting tables for compile")
	}

	var perEntityBehavior v1alpha.PerEntityBehavior

	switch request.ResultBehavior {
	case v1alpha.Query_RESULT_BEHAVIOR_UNSPECIFIED:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_UNSPECIFIED
	case v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_ALL
	case v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL

	default:
		subLogger.Error().Str("resultBehavior", request.ResultBehavior.String()).Msg("unexpeted resultBehavior")
		return nil, fmt.Errorf("unexpected resultBehavior: %s", request.ResultBehavior.String())
	}

	compileRequest := &v1alpha.CompileRequest{
		Experimental: options.IsExperimental,
		FeatureSet: &v1alpha.FeatureSet{
			Formulas: formulas,
			Query:    request.Query,
		},
		PerEntityBehavior: perEntityBehavior,
		SliceRequest:      request.SliceRequest,
		Tables:            tables,
	}

	if options.IsFormula {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_FORMULA
	} else {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_COMPLETE
	}

	return compileRequest, nil
}

func (m *Manager) RunCompileRequest(ctx context.Context, owner *ent.Owner, compileRequest *v1alpha.CompileRequest) (*CompileQueryResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.RunCompileRequest").Logger()
	computeClient := m.computeClients.ComputeServiceClient(ctx)
	subLogger.Info().Interface("request", compileRequest).Msg("sending compile request")
	compileTimeoutCtx, compileTimeoutCancel := context.WithTimeout(ctx, time.Second*compileTimeoutSeconds)
	defer compileTimeoutCancel()
	computeCompileResponse, err := computeClient.Compile(compileTimeoutCtx, compileRequest)
	subLogger.Info().Err(err).
		Interface("fenl_diagnostics", computeCompileResponse.FenlDiagnostics).
		Bool("incremental_enabled", computeCompileResponse.IncrementalEnabled).
		Strs("free_names", computeCompileResponse.FreeNames).
		Strs("missing_names", computeCompileResponse.MissingNames).
		Interface("plan_hash", computeCompileResponse.PlanHash).
		Interface("result_type", computeCompileResponse.ResultType).
		Interface("slices", computeCompileResponse.TableSlices).Msg("received compile response")
	if err != nil {
		return nil, err
	}

	compileResponse := CompileQueryResponse{
		ComputeResponse: computeCompileResponse,
		Views:           make([]*v1alpha.View, 0),
	}

	for _, formula := range compileRequest.FeatureSet.Formulas {
		for _, freeName := range computeCompileResponse.FreeNames {
			if freeName == formula.Name {
				compileResponse.Views = append(compileResponse.Views, &v1alpha.View{
					ViewName:   formula.Name,
					Expression: formula.Formula,
				})
			}
		}
	}

	return &compileResponse, err
}

func (m *Manager) GetDataToken(ctx context.Context, owner *ent.Owner, dataTokenId string) (*ent.DataToken, error) {
	if dataTokenId == "" {
		dataToken, err := m.dataTokenClient.GetCurrentDataToken(ctx, owner)
		if err != nil {
			return nil, errors.WithMessage(err, "getting current data_token")
		}
		return dataToken, nil
	} else {
		id, err := uuid.Parse(dataTokenId)
		if err != nil {
			return nil, customerrors.NewInvalidArgumentError("data_token")
		} else {
			dataToken, err := m.dataTokenClient.GetDataToken(ctx, owner, id)
			if err != nil {
				return nil, errors.WithMessagef(err, "getting data_token: %s", dataTokenId)
			}
			return dataToken, nil
		}
	}
}

func (m *Manager) InitiateQuery(queryContext *QueryContext) (v1alpha.ComputeService_ExecuteClient, error) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.InitiateQuery").Logger()

	// if files are being returned, set the results path for the query
	switch destination := queryContext.outputTo.Destination.(type) {
	case *v1alpha.ExecuteRequest_OutputTo_ObjectStore:
		subPath := path.Join("results", queryContext.owner.ID.String(), base64.RawURLEncoding.EncodeToString(queryContext.compileResp.PlanHash.Hash))
		destination.ObjectStore.OutputPrefixUri = m.store.GetDataPathURI(subPath)
	}

	executeRequest := &v1alpha.ExecuteRequest{
		ChangedSince:    queryContext.changedSinceTime,
		FinalResultTime: queryContext.finalResultTime,
		Plan:            queryContext.compileResp.Plan,
		Limits:          queryContext.limits,
		OutputTo:        queryContext.outputTo,
		Tables:          queryContext.GetComputeTables(),
	}

	snapshotCacheBuster, err := m.getSnapshotCacheBuster(queryContext.ctx)
	if err != nil {
		return nil, err
	}
	prepareCacheBuster, err := m.getPrepareCacheBuster(queryContext.ctx)
	if err != nil {
		return nil, err
	}

	queryClient := m.computeClients.ComputeServiceClient(queryContext.ctx)

	subLogger.Info().Bool("incremental_enabled", queryContext.compileResp.IncrementalEnabled).Bool("is_current_data_token", queryContext.isCurrentDataToken).Msg("Populating snapshot config if needed")
	if queryContext.compileResp.IncrementalEnabled && queryContext.isCurrentDataToken && queryContext.compileResp.PlanHash != nil {
		executeRequest.ComputeSnapshotConfig = &v1alpha.ExecuteRequest_ComputeSnapshotConfig{
			OutputPrefix: ConvertURIForCompute(m.getComputeSnapshotDataURI(queryContext.owner, *snapshotCacheBuster, queryContext.compileResp.PlanHash.Hash, queryContext.dataToken.DataVersionID)),
		}
		subLogger.Info().Str("SnapshotPrefix", executeRequest.ComputeSnapshotConfig.OutputPrefix).Msg("Snapshot output prefix")

		bestSnapshot, err := m.kaskadaTableClient.GetBestComputeSnapshot(queryContext.ctx, queryContext.owner, queryContext.compileResp.PlanHash.Hash, *snapshotCacheBuster, queryContext.GetSlices(), *prepareCacheBuster)
		if err != nil {
			log.Warn().Err(err).Msg("issue getting existing snapshot. query will execute from scratch")
		} else if bestSnapshot != nil {
			executeRequest.ComputeSnapshotConfig.ResumeFrom = &wrapperspb.StringValue{Value: ConvertURIForCompute(bestSnapshot.Path)}
			subLogger.Info().Str("ResumeFrom", executeRequest.ComputeSnapshotConfig.ResumeFrom.Value).Msg("Found snapshot to resume compute from")
		} else {
			subLogger.Info().Msg("no valid snapshot to resume from")
		}
	}

	subLogger.Info().
		Interface("compute_snapshot_config", executeRequest.ComputeSnapshotConfig).
		Interface("tables", executeRequest.Tables).
		Interface("limits", executeRequest.Limits).
		Interface("final_result_time", executeRequest.FinalResultTime).
		Interface("changed_since_time", executeRequest.ChangedSince).
		Interface("output_to", executeRequest.OutputTo).Msg("sending streaming query request to compute backend")

	executeClient, err := queryClient.Execute(queryContext.ctx, executeRequest)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue initiating streaming query compute request")
		return nil, customerrors.NewComputeError(reMapSparrowError(queryContext.ctx, err))
	}
	return executeClient, nil
}

func (m *Manager) runMaterializationQuery(queryContext *QueryContext) (*QueryResult, error) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.runMaterializationQuery").Logger()

	stream, err := m.InitiateQuery(queryContext)
	if err != nil {
		return nil, err
	}

	result := &QueryResult{
		DataTokenId: queryContext.dataToken.ID.String(),
		Paths:       []string{},
	}

	for {
		// Start receiving streaming messages
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			subLogger.Warn().Err(err).Msg("issue receiving execute response")
			return result, customerrors.NewComputeError(reMapSparrowError(queryContext.ctx, err))
		}
		if res.OutputPaths != nil {
			result.Paths = append(result.Paths, res.OutputPaths.Paths...)
		}
		switch res.State {
		case v1alpha.LongQueryState_LONG_QUERY_STATE_INITIAL:
			subLogger.Info().Msg("recieved initial message from execute request")
		case v1alpha.LongQueryState_LONG_QUERY_STATE_RUNNING:
			subLogger.Info().Interface("progress", res.Progress).Msg("received progress from execute request")
		case v1alpha.LongQueryState_LONG_QUERY_STATE_FINAL:
			subLogger.Info().Bool("query_done", res.IsQueryDone).Msg("recieved final message from execute request")
		default:
			subLogger.Error().Str("state", res.State.String()).Msg("unexpected long query state")
		}

		m.SaveComputeSnapshots(queryContext, res.ComputeSnapshots)
	}

	subLogger.Info().Interface("result", result).Msg("final query result")
	return result, nil
}

func (m *Manager) SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ExecuteResponse_ComputeSnapshot) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.SaveComputeSnapshots").Logger()
	for _, computeSnapshot := range computeSnapshots {
		if err := m.kaskadaTableClient.SaveComputeSnapshot(queryContext.ctx, queryContext.owner, computeSnapshot.PlanHash.Hash, computeSnapshot.SnapshotVersion, queryContext.dataToken, ConvertURIForManager(computeSnapshot.Path), computeSnapshot.MaxEventTime.AsTime(), queryContext.GetTableIDs()); err != nil {
			subLogger.Error().Err(err).Str("data_token_id", queryContext.dataToken.ID.String()).Msg("issue saving compute snapshot")
		}
	}
}

// Runs all saved materializations on current data inside a go-routine that attempts to finish before shutdown
func (m *Manager) RunMaterializations(requestCtx context.Context, owner *ent.Owner) {
	// do nothing for now since materialization output is disabled
	// m.errGroup.Go(func() error { return m.processMaterializations(requestCtx, owner) })
}

// Runs all saved materializations on current data
// Note: any errors returned from this method will cause wren to start its safe-shutdown routine
// so be careful to only return errors that truly warrent a shutdown.
func (m *Manager) processMaterializations(requestCtx context.Context, owner *ent.Owner) error {
	ctx, cancel, err := auth.NewBackgroundContextWithAPIClient(requestCtx)
	if err != nil {
		log.Ctx(requestCtx).Error().Err(err).Msg("error creating background context for processing materializations")
		return err
	}
	defer cancel()

	subLogger := log.Ctx(ctx).With().Str("method", "manager.processMaterializations").Logger()

	dataToken, err := m.dataTokenClient.GetCurrentDataToken(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("getting current data_token")
		return nil
	}

	materializations, err := m.materializationClient.GetAllMaterializations(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("error listing materializations")
		return nil
	}

	for _, materialization := range materializations {
		matLogger := subLogger.With().Str("materialization_name", materialization.Name).Logger()

		// TODO: changed_since time should be computed from file metadata.
		// By default, all entities will be included in the materialization, regardless of
		// whether their feature values have updated.
		compileResp, err := m.CompileQuery(ctx, owner, materialization.Expression, materialization.WithViews.Views, false, false, materialization.SliceRequest, v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS)
		if err != nil {
			matLogger.Error().Err(err).Msg("analyzing materialization")
			return nil
		}

		if compileResp.Plan == nil {
			matLogger.Error().Interface("missing_names", compileResp.MissingNames).Interface("diagnostics", compileResp.FenlDiagnostics).Msg("analysis determined query is not executable. This is unexpected, as the materialization was previously able to compile.")
			return nil
		}

		tables, err := m.GetTablesForCompute(ctx, owner, dataToken, compileResp.TableSlices)
		if err != nil {
			matLogger.Error().Err(err).Str("data_token_id", dataToken.ID.String()).Msg("getting tables for data_token")
			return nil
		}

		//TODO: get an "actual" outputTo config from the materialization definition.
		outputTo := &v1alpha.ExecuteRequest_OutputTo{
			Destination: &v1alpha.ExecuteRequest_OutputTo_Redis{},
		}

		queryContext, queryContextCancel := GetNewQueryContext(ctx, owner, nil, compileResp, dataToken, nil, true, nil, tables, outputTo)
		defer queryContextCancel()

		err = m.computeMaterialization(materialization, queryContext)
		if err != nil {
			matLogger.Error().Err(err).Str("name", materialization.Name).Msg("error computing materialization")
			return nil
		}
	}

	return nil
}

func (m *Manager) computeMaterialization(materialization *ent.Materialization, queryContext *QueryContext) error {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.computeMaterialization").Str("materialization", materialization.Name).Logger()
	switch kind := materialization.Destination.Destination.(type) {
	//case *v1alpha.Materialization_Destination_RedisAI:
	default:
		subLogger.Error().Interface("type", kind).Str("when", "pre-compute").Msg("materialization output type not implemented")
		return fmt.Errorf("materialization output type %s is not implemented", kind)
	}

	/*
		queryResult, err := m.runMaterializationQuery(queryContext)
		if err != nil {
			subLogger.Error().Err(err).Msg("invalid compute backend response")
			return err
		}

		switch kind := materialization.Destination.Destination.(type) {
		case *v1alpha.Materialization_Destination_RedisAI:
			redisAI := materialization.Destination.GetRedisAI()
			redisConfig := redis.RedisConfig{
				Host: redisAI.Host,
				Port: redisAI.Port,
				DB:   redisAI.Db,
			}
			return m.UploadResultsToRedisAI(queryContext.ctx, *queryContext.owner, queryResult.Paths, redisConfig)
		default:
			subLogger.Error().Interface("type", kind).Str("when", "post-compute").Msg("materialization output type not implemented")
			return fmt.Errorf("materialization output type %s is not implemented", kind)
		}

		subLogger.Info().Msg("successfully exported materialization")


		return nil
	*/
}

func reMapSparrowError(ctx context.Context, err error) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.reMapSparrowError").Logger()
	inStatus, ok := status.FromError(err)
	if !ok {
		subLogger.Error().Msg("unexpected: compute error did not include error-status")
		return err
	}
	outStatus := status.New(inStatus.Code(), inStatus.Message())

	for _, detail := range inStatus.Details() {
		switch t := detail.(type) {
		case protoiface.MessageV1:
			outStatus, err = outStatus.WithDetails(t)
			if err != nil {
				subLogger.Error().Err(err).Interface("detail", t).Msg("unable to add detail to re-mapped error details")
			}
		default:
			subLogger.Error().Err(err).Interface("detail", t).Msg("unexpected: detail from compute doesn't implement the protoifam.MessageV1 interface")
		}
	}
	return outStatus.Err()
}

// converts diagnostics in non-executable responses into error details to preserve legacy behavior
// TODO: update the python client to be able to handle non-executable responses in the response body
// TODO: remove this and pass back non-executable responses in the response body instead of as an error
func (m *Manager) ReMapAnalysisError(ctx context.Context, analysis *v1alpha.Analysis) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.reMapAnalysisError").Logger()
	if analysis != nil {
		if !analysis.CanExecute {
			if analysis.FenlDiagnostics != nil {
				diagCount := len(analysis.FenlDiagnostics.FenlDiagnostics)
				if diagCount > 0 {
					outStatus := status.New(codes.InvalidArgument, fmt.Sprintf("%d errors in Fenl statements; see error details", diagCount))
					outStatus, err := outStatus.WithDetails(analysis.FenlDiagnostics)
					if err != nil {
						subLogger.Error().Err(err).Interface("fenl_diagnostics", analysis.FenlDiagnostics).Msg("unable to add diagnostic to re-mapped error details")
					}
					return outStatus.Err()
				}
			}
			return status.Error(codes.Internal, "internal compute error")
		}
	}
	return nil
}

func (m *Manager) getTablesForCompile(ctx context.Context, owner *ent.Owner) ([]*v1alpha.ComputeTable, error) {
	computeTables := []*v1alpha.ComputeTable{}

	kaskadaTables, err := m.kaskadaTableClient.GetAllKaskadaTables(ctx, owner)
	if err != nil {
		return nil, errors.WithMessagef(err, "getting all tables")
	}

	for _, kaskadaTable := range kaskadaTables {
		// if merged schema not set, table still contains no data
		if kaskadaTable.MergedSchema != nil {
			computeTables = append(computeTables, convertKaskadaTableToComputeTable(kaskadaTable))
		}
	}
	return computeTables, nil
}

func (m *Manager) getTablesForQuery(ctx context.Context, owner *ent.Owner, slicePlans []*v1alpha.SlicePlan) (map[string]*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getTablesForQuery").Logger()

	tableMap := map[string]*ent.KaskadaTable{}
	for _, slicePlan := range slicePlans {
		tableName := slicePlan.TableName
		sliceLoggger := subLogger.With().Str("table_name", tableName).Interface("slice_plan", slicePlan.Slice).Logger()

		if _, found := tableMap[tableName]; !found {
			kaskadaTable, err := m.kaskadaTableClient.GetKaskadaTableByName(ctx, owner, tableName)
			if err != nil {
				sliceLoggger.Error().Err(err).Msg("issue getting kaskada table")
				return nil, err
			}
			tableMap[tableName] = kaskadaTable
		}
	}
	return tableMap, nil
}

// converts a dataToken into a map of of tableIDs to internal.SliceTable
// prepares data as needed
func (m *Manager) GetTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetTablesForCompute").Logger()

	kaskadaTableMap, err := m.getTablesForQuery(ctx, owner, slicePlans)
	if err != nil {
		return nil, err
	}

	sliceTableMap := make(map[uuid.UUID]*internal.SliceTable, len(kaskadaTableMap))
	for _, kaskadaTable := range kaskadaTableMap {
		sliceTableMap[kaskadaTable.ID] = internal.GetNewSliceTable(kaskadaTable)
	}

	for _, slicePlan := range slicePlans {
		tableName := slicePlan.TableName
		sliceLoggger := subLogger.With().Str("table_name", tableName).Interface("slice_plan", slicePlan.Slice).Logger()

		kaskadaTable, found := kaskadaTableMap[slicePlan.TableName]
		if !found {
			sliceLoggger.Error().Msg("unexpected; missing kaskadaTable")
			return nil, fmt.Errorf("unexpected; missing kaskadaTable")
		}

		sliceInfo, err := internal.GetNewSliceInfo(slicePlan, kaskadaTable)
		if err != nil {
			sliceLoggger.Error().Err(err).Msg("issue gettting slice info")
		}

		prepareJobs, err := m.getOrCreatePrepareJobs(ctx, owner, dataToken, sliceInfo)
		if err != nil {
			sliceLoggger.Error().Err(err).Msg("issue getting and/or creating prepare jobs")
			return nil, err
		}

		sliceTableMap[kaskadaTable.ID].FileSetMap[&sliceInfo.PlanHash] = internal.GetNewFileSet(sliceInfo, prepareJobs)
	}

	err = m.parallelPrepare(ctx, owner, sliceTableMap)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue preparing tables")
		return nil, errors.WithMessagef(err, "preparing tables")
	}

	//refresh prepareJobs after prepare complete
	// for each job
	for _, sliceTable := range sliceTableMap {
		for _, fileSet := range sliceTable.FileSetMap {
			refreshedPrepareJobs := make([]*ent.PrepareJob, len(fileSet.PrepareJobs))
			for i, prepareJob := range fileSet.PrepareJobs {
				refreshedPrepareJobs[i], err = m.prepareJobClient.GetPrepareJob(ctx, prepareJob.ID)
				subLogger.Debug().Interface("prepare_job", refreshedPrepareJobs[i]).Msg("refreshed job")
				if err != nil {
					subLogger.Error().Err(err).Msg("issue refreshing prepare jobs")
					return nil, fmt.Errorf("issue refreshing prepare jobs")
				}
			}
			fileSet.PrepareJobs = refreshedPrepareJobs
		}
	}

	return sliceTableMap, nil
}

// converts request views and persisted views to formulas.  if a request view has the same name as a persisted view
// the request view is used.
func (m *Manager) getFormulas(ctx context.Context, owner *ent.Owner, requestViews []*v1alpha.WithView) ([]*v1alpha.Formula, error) {
	persistedViews, err := m.kaskadaViewClient.GetAllKaskadaViews(ctx, owner)
	if err != nil {
		return nil, errors.WithMessage(err, "listing all views")
	}

	formulas := []*v1alpha.Formula{}

	requestViewNames := make(map[string]struct{}, len(requestViews))
	for _, requestView := range requestViews {
		requestViewNames[requestView.Name] = struct{}{}

		formulas = append(formulas, &v1alpha.Formula{
			Name:           requestView.Name,
			Formula:        requestView.Expression,
			SourceLocation: fmt.Sprintf("View %s", requestView.Name),
		})
	}

	for _, persistedView := range persistedViews {
		if _, found := requestViewNames[persistedView.Name]; !found {
			formulas = append(formulas, &v1alpha.Formula{
				Name:           persistedView.Name,
				Formula:        persistedView.Expression,
				SourceLocation: fmt.Sprintf("View %s", persistedView.Name),
			})
		}
	}

	return formulas, nil
}

// gets the current snapshot cache buster
func (m *Manager) getSnapshotCacheBuster(ctx context.Context) (*int32, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getSnapshotCacheBuster").Logger()
	queryClient := m.computeClients.ComputeServiceClient(ctx)
	res, err := queryClient.GetCurrentSnapshotVersion(ctx, &v1alpha.GetCurrentSnapshotVersionRequest{})
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting snapshot_cache_buster")
		return nil, err
	}
	return &res.SnapshotVersion, nil
}

// returns s3://root/computeSnapshots/<snapshot_cache_buster>/<owner_id>/<plan_hash>/<data_version>
func (m *Manager) getComputeSnapshotDataURI(owner *ent.Owner, snapshotCacheBuster int32, planHash []byte, dataVersion int64) string {
	subPath := path.Join("computeSnapshots", strconv.Itoa(int(snapshotCacheBuster)), owner.ID.String(), base64.RawURLEncoding.EncodeToString(planHash), utils.Int64ToString(dataVersion))
	return ConvertURIForCompute(m.store.GetDataPathURI(subPath))
}

func (m *Manager) GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetFileSchema").Str("uri", fileInput.GetURI()).Str("type", fileInput.GetExtension()).Logger()
	// Send the metadata request to the FileService

	var filePath *v1alpha.FilePath

	switch fileInput.GetType() {
	case kaskadafile.TypeCsv:
		filePath = &v1alpha.FilePath{Path: &v1alpha.FilePath_CsvPath{CsvPath: ConvertURIForCompute(fileInput.GetURI())}}
	case kaskadafile.TypeParquet:
		filePath = &v1alpha.FilePath{Path: &v1alpha.FilePath_ParquetPath{ParquetPath: ConvertURIForCompute(fileInput.GetURI())}}
	default:
		subLogger.Warn().Msg("user didn't specifiy file type, defaulting to parquet for now, but will error in the future")
		filePath = &v1alpha.FilePath{Path: &v1alpha.FilePath_ParquetPath{ParquetPath: ConvertURIForCompute(fileInput.GetURI())}}
	}

	fileClient := m.computeClients.FileServiceClient(ctx)
	metadataReq := &v1alpha.GetMetadataRequest{
		FilePaths: []*v1alpha.FilePath{filePath},
	}

	subLogger.Debug().Interface("request", metadataReq).Msg("sending get_metadata request to file service")
	metadataRes, err := fileClient.GetMetadata(ctx, metadataReq)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting file schema from file_service")
		return nil, err
	}

	if len(metadataRes.FileMetadatas) != 1 {
		subLogger.Error().Int("response_count", len(metadataRes.FileMetadatas)).Msg("issue getting file schema from file_service")
		return nil, fmt.Errorf("issue getting file schema from file_service")
	}

	return metadataRes.FileMetadatas[0].Schema, nil
}

func ConvertURIForCompute(URI string) string {
	return strings.TrimPrefix(URI, "file://")
}

func ConvertURIForManager(URI string) string {
	if strings.HasPrefix(URI, "/") {
		return fmt.Sprintf("file://%s", URI)
	}
	return URI
}
