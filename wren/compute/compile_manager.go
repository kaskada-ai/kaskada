package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/maps"
)

type compileRequest struct {
	Expression     string
	Views          []*v1alpha.WithView
	SliceRequest   *v1alpha.SliceRequest
	ResultBehavior v1alpha.Query_ResultBehavior
}

type compileOptions struct {
	IsFormula      bool
	IsExperimental bool
}

type CompileManager interface {
	CompileEntMaterialization(ctx context.Context, owner *ent.Owner, materialization *ent.Materialization) (*v1alpha.CompileResponse, []*v1alpha.View, error)
	CompileV1Materialization(ctx context.Context, owner *ent.Owner, materialization *v1alpha.Materialization) (*v1alpha.CompileResponse, []*v1alpha.View, error)
	CompileV1Query(ctx context.Context, owner *ent.Owner, query *v1alpha.Query, queryOptions *v1alpha.QueryOptions) (*v1alpha.CompileResponse, []*v1alpha.View, error)
	CompileV2Query(ctx context.Context, owner *ent.Owner, expression string, views []*v2alpha.QueryView, queryConfig *v2alpha.QueryConfig) (*v1alpha.CompileResponse, []*v1alpha.View, error)
	CompileV1View(ctx context.Context, owner *ent.Owner, view *v1alpha.View) (*v1alpha.CompileResponse, error)
}

type compileManager struct {
	computeClients     client.ComputeClients
	kaskadaTableClient internal.KaskadaTableClient
	kaskadaViewClient  internal.KaskadaViewClient
}

func NewCompileManager(computeClients *client.ComputeClients, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient) CompileManager {
	return &compileManager{
		computeClients:     *computeClients,
		kaskadaTableClient: *kaskadaTableClient,
		kaskadaViewClient:  *kaskadaViewClient,
	}
}
func (m *compileManager) CompileEntMaterialization(ctx context.Context, owner *ent.Owner, materialization *ent.Materialization) (*v1alpha.CompileResponse, []*v1alpha.View, error) {
	compileRequest := &compileRequest{
		Expression:     materialization.Expression,
		Views:          materialization.WithViews.Views,
		SliceRequest:   materialization.SliceRequest,
		ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
	}

	compileOptions := &compileOptions{
		IsFormula:      false,
		IsExperimental: false,
	}
	return m.compile(ctx, owner, compileRequest, compileOptions)
}

func (m *compileManager) CompileV1Materialization(ctx context.Context, owner *ent.Owner, materialization *v1alpha.Materialization) (*v1alpha.CompileResponse, []*v1alpha.View, error) {
	compileRequest := &compileRequest{
		Expression:     materialization.Expression,
		Views:          materialization.WithViews,
		SliceRequest:   materialization.Slice,
		ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
	}

	compileOptions := &compileOptions{
		IsFormula:      false,
		IsExperimental: false,
	}
	return m.compile(ctx, owner, compileRequest, compileOptions)
}

func (m *compileManager) CompileV1Query(ctx context.Context, owner *ent.Owner, query *v1alpha.Query, queryOptions *v1alpha.QueryOptions) (*v1alpha.CompileResponse, []*v1alpha.View, error) {
	compileRequest := &compileRequest{
		Expression:     query.Expression,
		Views:          []*v1alpha.WithView{},
		SliceRequest:   query.Slice,
		ResultBehavior: query.ResultBehavior,
	}

	compileOptions := &compileOptions{
		IsFormula:      false,
		IsExperimental: queryOptions != nil && queryOptions.ExperimentalFeatures,
	}

	return m.compile(ctx, owner, compileRequest, compileOptions)
}

func (m *compileManager) CompileV2Query(ctx context.Context, owner *ent.Owner, expression string, views []*v2alpha.QueryView, queryConfig *v2alpha.QueryConfig) (*v1alpha.CompileResponse, []*v1alpha.View, error) {
	
	compileRequest := &compileRequest{
		Expression:   expression,
		Views:        make([]*v1alpha.WithView, len(views)),
		SliceRequest: queryConfig.Slice,
	}

	for i, view := range views {
		compileRequest.Views[i] = &v1alpha.WithView{
			Name:       view.ViewName,
			Expression: view.Expression,
		}
	}

	switch queryConfig.ResultBehavior.ResultBehavior.(type) {
	case *v2alpha.ResultBehavior_AllResults:
		compileRequest.ResultBehavior = v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS
	case *v2alpha.ResultBehavior_FinalResults:
		compileRequest.ResultBehavior = v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS
	case *v2alpha.ResultBehavior_FinalResultsAtTime:
		compileRequest.ResultBehavior = v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS_AT_TIME
	default:
		subLogger := log.Ctx(ctx).With().Str("method", "compileManager.CompileV2Query").Logger()
		subLogger.Error().Str("resultBehavior", fmt.Sprintf("%T", queryConfig.ResultBehavior.ResultBehavior)).Msg("unexpected resultBehavior")
		return nil, nil, fmt.Errorf("unexpected resultBehavior: %T", queryConfig.ResultBehavior.ResultBehavior)
	}

	incrementalQueryExperiment := false
	for _, experimentalFeature := range queryConfig.ExperimentalFeatures {
		switch {
		case strings.EqualFold("incremental", experimentalFeature):
			incrementalQueryExperiment = true
		}
	}

	compileOptions := &compileOptions{
		IsFormula:      false,
		IsExperimental: incrementalQueryExperiment,
	}
	return m.compile(ctx, owner, compileRequest, compileOptions)
}

func (m *compileManager) CompileV1View(ctx context.Context, owner *ent.Owner, view *v1alpha.View) (*v1alpha.CompileResponse, error) {
	compileRequest := &compileRequest{
		Expression:     view.Expression,
		Views:          []*v1alpha.WithView{},
		ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
	}

	compileOptions := &compileOptions{
		IsFormula:      true,
		IsExperimental: false,
	}

	compileResponse, _, err := m.compile(ctx, owner, compileRequest, compileOptions)
	return compileResponse, err
}

func (m *compileManager) compile(ctx context.Context, owner *ent.Owner, request *compileRequest, options *compileOptions) (*v1alpha.CompileResponse, []*v1alpha.View, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "compileManager.compileQuery").Logger()

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
		return nil, nil, fmt.Errorf("unexpected resultBehavior: %s", request.ResultBehavior.String())
	}

	formulaMap, err := m.getFormulaMap(ctx, owner, request.Views)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting formulas")
		return nil, nil, err
	}

	computeTables := []*v1alpha.ComputeTable{}
	kaskadaTables, err := m.kaskadaTableClient.GetAllKaskadaTables(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("error getting all tables")
		return nil, nil, err
	}

	for _, kaskadaTable := range kaskadaTables {
		// if merged schema not set, table still contains no data
		if kaskadaTable.MergedSchema != nil {
			computeTables = append(computeTables, convertKaskadaTableToComputeTable(kaskadaTable))
		}
	}

	compileRequest := &v1alpha.CompileRequest{
		Experimental: options.IsExperimental,
		FeatureSet: &v1alpha.FeatureSet{
			Formulas: maps.Values(formulaMap),
			Query:    request.Expression,
		},
		PerEntityBehavior: perEntityBehavior,
		SliceRequest:      request.SliceRequest,
		Tables:            computeTables,
	}

	if options.IsFormula {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_FORMULA
	} else {
		compileRequest.ExpressionKind = v1alpha.CompileRequest_EXPRESSION_KIND_COMPLETE
	}

	computeClient := m.computeClients.ComputeServiceClient(ctx)
	defer computeClient.Close()

	subLogger.Info().Interface("request", compileRequest).Msg("sending compile request")
	compileTimeoutCtx, compileTimeoutCancel := context.WithTimeout(ctx, time.Second*compileTimeoutSeconds)
	defer compileTimeoutCancel()

	compileResponse, err := computeClient.Compile(compileTimeoutCtx, compileRequest)
	subLogger.Info().Err(err).
		Interface("fenl_diagnostics", compileResponse.FenlDiagnostics).
		Bool("incremental_enabled", compileResponse.IncrementalEnabled).
		Strs("free_names", compileResponse.FreeNames).
		Strs("missing_names", compileResponse.MissingNames).
		Interface("plan_hash", compileResponse.PlanHash).
		Interface("result_type", compileResponse.ResultType).
		Interface("slices", compileResponse.TableSlices).Msg("received compile response")
	if err != nil {
		return nil, nil, err
	}

	views := []*v1alpha.View{}
	for _, freeName := range compileResponse.FreeNames {
		if formula, ok := formulaMap[freeName]; ok {
			views = append(views, &v1alpha.View{
				ViewName:   formula.Name,
				Expression: formula.Formula,
			})
		}
	}

	return compileResponse, views, nil
}

// returns map of formulaName to formula, including all persisted views in the owner, and all requested views
// if a requestView and a persisted view have the same name, the requestView will be used
func (m *compileManager) getFormulaMap(ctx context.Context, owner *ent.Owner, requestViews []*v1alpha.WithView) (map[string]*v1alpha.Formula, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getFormulas").Logger()
	persistedViews, err := m.kaskadaViewClient.GetAllKaskadaViews(ctx, owner)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting persisted views")
		return nil, err
	}

	formulaMap := map[string]*v1alpha.Formula{}

	for _, persistedView := range persistedViews {
		formulaMap[persistedView.Name] = &v1alpha.Formula{
			Name:           persistedView.Name,
			Formula:        persistedView.Expression,
			SourceLocation: fmt.Sprintf("Persisted View: %s", persistedView.Name),
		}
	}

	for _, requestView := range requestViews {
		formulaMap[requestView.Name] = &v1alpha.Formula{
			Name:           requestView.Name,
			Formula:        requestView.Expression,
			SourceLocation: fmt.Sprintf("Requested View %s", requestView.Name),
		}
	}

	return formulaMap, nil
}
