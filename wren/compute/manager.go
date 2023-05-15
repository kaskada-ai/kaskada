package compute

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadafile"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/store"
	"github.com/kaskada-ai/kaskada/wren/utils"
)

const (
	keyColumnName         = "key"
	compileTimeoutSeconds = 10
)

//go:generate mockery --name=ComputeManger
type ComputeManager interface {
	CompileQuery(ctx context.Context, owner *ent.Owner, query string, requestViews []*v1alpha.WithView, isFormula bool, isExperimental bool, sliceRequest *v1alpha.SliceRequest, resultBehavior v1alpha.Query_ResultBehavior) (*v1alpha.CompileResponse, error)
	GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error)
	GetUsedViews(formulas []*v1alpha.Formula, compileResponse *v1alpha.CompileResponse) *v2alpha.QueryViews
	CompileQueryV2(ctx context.Context, owner *ent.Owner, expression string, formulas []*v1alpha.Formula, config *v2alpha.QueryConfig) (*v1alpha.CompileResponse, error)
	CreateCompileRequest(ctx context.Context, owner *ent.Owner, request *QueryRequest, options *QueryOptions) (*v1alpha.CompileRequest, error)
	RunCompileRequest(ctx context.Context, owner *ent.Owner, compileRequest *v1alpha.CompileRequest) (*CompileQueryResponse, error)
	GetOutputURI(owner *ent.Owner, planHash []byte) string
	InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error)
	SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot)
	RunMaterializations(requestCtx context.Context, owner *ent.Owner)
	GetTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error)
	GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error)
}

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
func NewManager(errGroup *errgroup.Group, computeClients *client.ComputeClients, dataTokenClient *internal.DataTokenClient, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient, materializationClient *internal.MaterializationClient, prepareJobClient internal.PrepareJobClient, objectStoreClient *client.ObjectStoreClient, tableStore store.TableStore, parallelizeConfig utils.ParallelizeConfig) ComputeManager {
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
		subLogger.Error().Err(err).Msg("issue getting formulas")
		return nil, err
	}

	tables, err := m.getTablesForCompile(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting tables for compile")
		return nil, err
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
		subLogger.Error().Str("resultBehavior", resultBehavior.String()).Msg("unexpected resultBehavior")
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
	defer queryClient.Close()

	subLogger.Info().Interface("request", compileRequest).Msg("sending compile request")
	compileTimeoutCtx, compileTimeoutCancel := context.WithTimeout(ctx, time.Second*compileTimeoutSeconds)
	defer compileTimeoutCancel()
	compileResponse, err := queryClient.Compile(compileTimeoutCtx, compileRequest)
	subLogger.Info().Err(err).Interface("response", compileResponse).Msg("received compile respone")

	return compileResponse, err
}

// gets the set of passed views and system views available for a query
func (m *Manager) GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetFormulas").Logger()
	requestViews := []*v1alpha.WithView{}
	for _, queryView := range views.Views {
		requestViews = append(requestViews, &v1alpha.WithView{
			Name:       queryView.ViewName,
			Expression: queryView.Expression,
		})
	}

	formulas, err := m.getFormulas(ctx, owner, requestViews)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting formulas")
		return nil, err
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
		subLogger.Error().Err(err).Msg("issue getting tables for compile")
		return nil, err
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
		subLogger.Error().Str("resultBehavior", fmt.Sprintf("%T", config.ResultBehavior.ResultBehavior)).Msg("unexpected resultBehavior")
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
			subLogger.Error().Str("sliceRequest", fmt.Sprintf("%T", config.Slice.Slice)).Msg("unexpected sliceRequest")
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
	defer queryClient.Close()

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
		subLogger.Error().Err(err).Msg("issue getting formulas")
		return nil, err
	}

	tables, err := m.getTablesForCompile(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting tables for compile")
		return nil, err
	}

	var perEntityBehavior v1alpha.PerEntityBehavior

	switch request.ResultBehavior {
	case v1alpha.Query_RESULT_BEHAVIOR_UNSPECIFIED:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_UNSPECIFIED
	case v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_ALL
	case v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL
	case v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS_AT_TIME:
		perEntityBehavior = v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL_AT_TIME
	default:
		subLogger.Error().Str("resultBehavior", request.ResultBehavior.String()).Msg("unexpected resultBehavior")
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
	defer computeClient.Close()

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

func (m *Manager) GetOutputURI(owner *ent.Owner, planHash []byte) string {
	subPath := path.Join("results", owner.ID.String(), base64.RawURLEncoding.EncodeToString(planHash))
	return m.store.GetDataPathURI(subPath)
}

func (m *Manager) InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.InitiateQuery").Logger()

	executeRequest := &v1alpha.ExecuteRequest{
		ChangedSince:    queryContext.changedSinceTime,
		FinalResultTime: queryContext.finalResultTime,
		Plan:            queryContext.compileResp.Plan,
		Limits:          queryContext.limits,
		Destination:     queryContext.destination,
		Tables:          queryContext.GetComputeTables(),
	}

	snapshotCacheBuster, err := m.getSnapshotCacheBuster(queryContext.ctx)
	if err != nil {
		return nil, nil, err
	}
	prepareCacheBuster, err := m.getPrepareCacheBuster(queryContext.ctx)
	if err != nil {
		return nil, nil, err
	}

	queryClient := m.computeClients.ComputeServiceClient(queryContext.ctx)

	subLogger.Info().Bool("incremental_enabled", queryContext.compileResp.IncrementalEnabled).Bool("is_current_data_token", queryContext.isCurrentDataToken).Msg("Populating snapshot config if needed")
	if queryContext.compileResp.IncrementalEnabled && queryContext.isCurrentDataToken && queryContext.compileResp.PlanHash != nil {
		executeRequest.ComputeSnapshotConfig = &v1alpha.ComputeSnapshotConfig{
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
		Interface("destination", executeRequest.Destination).Msg("sending streaming query request to compute backend")

	executeClient, err := queryClient.Execute(queryContext.ctx, executeRequest)
	if err != nil {
		subLogger.Warn().Err(err).Msg("issue initiating streaming query compute request")
		return nil, nil, customerrors.NewComputeError(reMapSparrowError(queryContext.ctx, err))
	}
	return queryClient, executeClient, nil
}

func (m *Manager) runMaterializationQuery(queryContext *QueryContext) (*QueryResult, error) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.runMaterializationQuery").Logger()

	client, stream, err := m.InitiateQuery(queryContext)
	if err != nil {
		return nil, err
	}
	defer client.Close()

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

		// Note: this does nothing visible to the user at the moment, as
		// running materializations are currently opaque to the user.
		// Eventually, we'll want to provide useful metadata for all destination types.
		if res.Destination != nil {
			switch kind := res.Destination.Destination.(type) {
			case *v1alpha.Destination_ObjectStore:
				result.Paths = append(result.Paths, kind.ObjectStore.OutputPaths.Paths...)
			}
		}

		switch res.State {
		case v1alpha.LongQueryState_LONG_QUERY_STATE_INITIAL:
			subLogger.Info().Msg("received initial message from execute request")
		case v1alpha.LongQueryState_LONG_QUERY_STATE_RUNNING:
			subLogger.Info().Interface("progress", res.Progress).Msg("received progress from execute request")
		case v1alpha.LongQueryState_LONG_QUERY_STATE_FINAL:
			subLogger.Info().Bool("query_done", res.IsQueryDone).Msg("received final message from execute request")
		default:
			subLogger.Error().Str("state", res.State.String()).Msg("unexpected long query state")
		}

		m.SaveComputeSnapshots(queryContext, res.ComputeSnapshots)
	}

	subLogger.Info().Interface("result", result).Msg("final query result")
	return result, nil
}

func (m *Manager) SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.SaveComputeSnapshots").Logger()
	for _, computeSnapshot := range computeSnapshots {
		if err := m.kaskadaTableClient.SaveComputeSnapshot(queryContext.ctx, queryContext.owner, computeSnapshot.PlanHash.Hash, computeSnapshot.SnapshotVersion, queryContext.dataToken, ConvertURIForManager(computeSnapshot.Path), computeSnapshot.MaxEventTime.AsTime(), queryContext.GetTableIDs()); err != nil {
			subLogger.Error().Err(err).Str("data_token_id", queryContext.dataToken.ID.String()).Msg("issue saving compute snapshot")
		}
	}
}

// Runs all saved materializations on current data inside a go-routine that attempts to finish before shutdown
func (m *Manager) RunMaterializations(requestCtx context.Context, owner *ent.Owner) {
	m.errGroup.Go(func() error { return m.processMaterializations(requestCtx, owner) })
}

// Runs all saved materializations on current data
// Note: any errors returned from this method will cause wren to start its safe-shutdown routine
// so be careful to only return errors that truly warrant a shutdown.
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

		prepareCacheBuster, err := m.getPrepareCacheBuster(ctx)
		if err != nil {
			matLogger.Error().Err(err).Msg("issue getting current prepare cache buster")
		}

		isExperimental := false
		compileResp, err := m.CompileQuery(ctx, owner, materialization.Expression, materialization.WithViews.Views, false, isExperimental, materialization.SliceRequest, v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS)
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

		destination := &v1alpha.Destination{}
		if materialization.Destination == nil {
			matLogger.Error().Str("materialization", materialization.Name).Msg("materialization has no destination")
			return nil
		}
		switch kind := materialization.Destination.Destination.(type) {
		case *v1alpha.Destination_ObjectStore:
			matLogger.Info().Interface("type", kind).Str("when", "pre-compute").Msg("materializating to object store")

			// Append the materialization version to the output prefix so result files
			// for specific datatokens are grouped together.
			outputPrefixUri := kind.ObjectStore.GetOutputPrefixUri()
			outputPrefixUri = path.Join(outputPrefixUri, strconv.FormatInt(materialization.Version, 10))

			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType:        kind.ObjectStore.GetFileType(),
					OutputPrefixUri: outputPrefixUri,
				},
			}
		case *v1alpha.Destination_Pulsar:
			matLogger.Info().Interface("type", kind).Str("when", "pre-compute").Msg("materializating to pulsar")
			destination.Destination = kind
		case *v1alpha.Destination_Redis:
			matLogger.Info().Interface("type", kind).Str("when", "pre-compute").Msg("materializing to redis")
			destination.Destination = kind
		default:
			matLogger.Error().Interface("type", kind).Str("when", "pre-compute").Msg("materialization output type not implemented")
			return fmt.Errorf("materialization output type %s is not implemented", kind)
		}

		queryContext, _ := GetNewQueryContext(ctx, owner, nil, compileResp, dataToken, nil, true, nil, destination, materialization.SliceRequest, tables)

		dataVersionID := materialization.DataVersionID
		var minTimeInNewFiles int64 = math.MaxInt64
		for _, slice := range queryContext.GetSlices() {
			minTime, err := m.kaskadaTableClient.GetMinTimeOfNewPreparedFiles(ctx, *prepareCacheBuster, slice, dataVersionID)
			if ent.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("could not get min time of new files for slice %s. Not materializing results", slice)
			}

			if *minTime < minTimeInNewFiles {
				minTimeInNewFiles = *minTime
			}
		}

		// Interpret the int64 (as nanos since epoch) as a proto timestamp
		changedSinceTime := &timestamppb.Timestamp{
			Seconds: minTimeInNewFiles / 1_000_000_000,
			Nanos:   (int32)(minTimeInNewFiles % 1_000_000_000),
		}

		// Remakes the query context with the changed since time.
		//
		// Not a great pattern, since we're recreating the context. If we're able
		// to pull out the relevant code that converts `SlicePlans` to `SliceInfo`
		// for the table client to get the min time of files, we can clean this up.
		queryContext, queryContextCancel := GetNewQueryContext(ctx, owner, changedSinceTime, compileResp, dataToken, nil, true, nil, destination, materialization.SliceRequest, tables)
		defer queryContextCancel()

		err = m.computeMaterialization(materialization, queryContext)
		if err != nil {
			matLogger.Error().Err(err).Str("name", materialization.Name).Msg("error computing materialization")
			return nil
		}

		// Update materializations that have run with the current data version id, so on
		// subsequent runs only the updated values will be produced.
		_, err = m.materializationClient.UpdateDataVersion(ctx, materialization, queryContext.dataToken.DataVersionID)
		if err != nil {
			matLogger.Error().Err(err).Str("name", materialization.Name).Int64("previousDataVersion", dataVersionID).Int64("newDataVersion", queryContext.dataToken.DataVersionID).Msg("error updating materialization with new data version")
			return nil
		}
	}

	return nil
}

func (m *Manager) computeMaterialization(materialization *ent.Materialization, queryContext *QueryContext) error {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.computeMaterialization").Str("materialization", materialization.Name).Logger()
	_, err := m.runMaterializationQuery(queryContext)
	if err != nil {
		subLogger.Error().Err(err).Msg("invalid compute backend response")
		return err
	}

	subLogger.Info().Msg("successfully exported materialization")
	return nil
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

func (m *Manager) getTablesForCompile(ctx context.Context, owner *ent.Owner) ([]*v1alpha.ComputeTable, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getTablesForCompile").Logger()
	computeTables := []*v1alpha.ComputeTable{}

	kaskadaTables, err := m.kaskadaTableClient.GetAllKaskadaTables(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("error getting all tables")
		return nil, err
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
		return nil, err
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
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getFormulas").Logger()
	persistedViews, err := m.kaskadaViewClient.GetAllKaskadaViews(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting persisted views")
		return nil, err
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
	defer queryClient.Close()

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
	return m.store.GetDataPathURI(subPath)
}

func (m *Manager) GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetFileSchema").Str("uri", fileInput.GetURI()).Str("type", fileInput.GetExtension()).Logger()
	// Send the metadata request to the FileService

	var sourceData *v1alpha.SourceData

	switch fileInput.GetType() {
	case kaskadafile.TypeCsv:
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_CsvPath{CsvPath: fileInput.GetURI()}}
	case kaskadafile.TypeParquet:
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_ParquetPath{ParquetPath: fileInput.GetURI()}}
	default:
		subLogger.Warn().Msg("user didn't specifiy file type, defaulting to parquet for now, but will error in the future")
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_ParquetPath{ParquetPath: fileInput.GetURI()}}
	}

	fileClient := m.computeClients.FileServiceClient(ctx)
	defer fileClient.Close()

	metadataReq := &v1alpha.GetMetadataRequest{
		SourceData: sourceData,
	}

	subLogger.Debug().Interface("request", metadataReq).Msg("sending get_metadata request to file service")
	metadataRes, err := fileClient.GetMetadata(ctx, metadataReq)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting file schema from file_service")
		return nil, err
	}

	if metadataRes.SourceMetadata == nil {
		subLogger.Error().Msg("issue getting file schema from file_service")
		return nil, fmt.Errorf("issue getting file schema from file_service")
	}

	return metadataRes.SourceMetadata.Schema, nil
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
