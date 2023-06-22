package compute

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/utils"
)

const (
	keyColumnName         = "key"
	compileTimeoutSeconds = 10
)

type ComputeManager interface {
	CompileManager

	// execute related
	GetOutputURI(owner *ent.Owner, planHash []byte) string
	InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error)
	SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot)

	// Runs all existing file-based materializations for the given owner
	// Note: this exists in the ComputeManager interface instead of the MaterializationManager interface because
	// it runs materializations in a similar way to InitiateQuery
	RunMaterializations(ctx context.Context, owner *ent.Owner)
}

type computeManager struct {
	CompileManager

	prepareManager        PrepareManager
	computeClients        client.ComputeClients
	errGroup              *errgroup.Group
	dataTokenClient       internal.DataTokenClient
	kaskadaTableClient    internal.KaskadaTableClient
	materializationClient internal.MaterializationClient
	objectStore           client.ObjectStoreClient
	tr                    trace.Tracer
}

// NewComputeManager creates a new compute manager
func NewComputeManager(errGroup *errgroup.Group, compileManager *CompileManager, computeClients *client.ComputeClients, dataTokenClient *internal.DataTokenClient, kaskadaTableClient *internal.KaskadaTableClient, materializationClient *internal.MaterializationClient, objectStoreClient *client.ObjectStoreClient, prepareManager *PrepareManager) ComputeManager {
	return &computeManager{
		CompileManager:        *compileManager,
		computeClients:        *computeClients,
		errGroup:              errGroup,
		dataTokenClient:       *dataTokenClient,
		kaskadaTableClient:    *kaskadaTableClient,
		materializationClient: *materializationClient,
		objectStore:           *objectStoreClient,
		prepareManager:        *prepareManager,
		tr:                    otel.Tracer("ComputeManager"),
	}
}

type QueryResult struct {
	DataTokenId string
	Paths       []string
}

func (m *computeManager) GetOutputURI(owner *ent.Owner, planHash []byte) string {
	subPath := path.Join("results", owner.ID.String(), base64.RawURLEncoding.EncodeToString(planHash))
	return m.objectStore.GetDataPathURI(subPath)
}

func (m *computeManager) InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error) {
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
	prepareCacheBuster, err := m.prepareManager.GetPrepareCacheBuster(queryContext.ctx)
	if err != nil {
		return nil, nil, err
	}

	queryClient := m.computeClients.NewComputeServiceClient(queryContext.ctx)

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

func (m *computeManager) runMaterializationQuery(queryContext *QueryContext) (*QueryResult, error) {
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

func (m *computeManager) SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot) {
	subLogger := log.Ctx(queryContext.ctx).With().Str("method", "manager.SaveComputeSnapshots").Logger()
	for _, computeSnapshot := range computeSnapshots {
		if err := m.kaskadaTableClient.SaveComputeSnapshot(queryContext.ctx, queryContext.owner, computeSnapshot.PlanHash.Hash, computeSnapshot.SnapshotVersion, queryContext.dataToken, ConvertURIForManager(computeSnapshot.Path), computeSnapshot.MaxEventTime.AsTime(), queryContext.GetTableIDs()); err != nil {
			subLogger.Error().Err(err).Str("data_token_id", queryContext.dataToken.ID.String()).Msg("issue saving compute snapshot")
		}
	}
}

// Runs all saved materializations on current data inside a go-routine that attempts to finish before shutdown
// TODO: After sparrow supports long-running materializations from file-based sources
// remove all the code related to this method
func (m *computeManager) RunMaterializations(requestCtx context.Context, owner *ent.Owner) {
	m.errGroup.Go(func() error { return m.processMaterializations(requestCtx, owner) })
}

// Runs all saved materializations on current data
// Note: any errors returned from this method will cause wren to start its safe-shutdown routine
// so be careful to only return errors that truly warrant a shutdown.
func (m *computeManager) processMaterializations(requestCtx context.Context, owner *ent.Owner) error {
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

	prepareCacheBuster, err := m.prepareManager.GetPrepareCacheBuster(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting current prepare cache buster")
	}

	materializations, err := m.materializationClient.GetMaterializationsBySourceType(ctx, owner, materialization.SourceTypeFiles)
	if err != nil {
		subLogger.Error().Err(err).Msg("error listing materializations")
		return nil
	}

	for _, materialization := range materializations {
		matLogger := subLogger.With().Str("materialization_name", materialization.Name).Logger()

		compileResp, _, err := m.CompileEntMaterialization(ctx, owner, materialization)
		if err != nil {
			matLogger.Error().Err(err).Msg("issue compiling materialization")
			return nil
		}

		if compileResp.Plan == nil {
			matLogger.Error().Interface("missing_names", compileResp.MissingNames).Interface("diagnostics", compileResp.FenlDiagnostics).Msg("analysis determined the materialization is not executable. This is unexpected, as it was previously able to compile.")
			return nil
		}

		tables, err := m.prepareManager.PrepareTablesForCompute(ctx, owner, dataToken, compileResp.TableSlices)
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

		_, err = m.runMaterializationQuery(queryContext)
		if err != nil {
			matLogger.Error().Err(err).Msg("error computing materialization")
			return nil
		}
		subLogger.Info().Msg("successfully exported materialization")

		// Update materializations that have run with the current data version id, so on
		// subsequent runs only the updated values will be produced.
		_, err = m.materializationClient.UpdateDataVersion(ctx, materialization, queryContext.dataToken.DataVersionID)
		if err != nil {
			matLogger.Error().Err(err).Int64("previousDataVersion", dataVersionID).Int64("newDataVersion", queryContext.dataToken.DataVersionID).Msg("error updating materialization with new data version")
			return nil
		}
		// Update the version for this materialization.
		_, err = m.materializationClient.IncrementVersion(ctx, materialization)
		if err != nil {
			matLogger.Error().Err(err).Msg("error updating materialization version")
			return nil
		}
	}

	return nil
}

// gets the current snapshot cache buster
func (m *computeManager) getSnapshotCacheBuster(ctx context.Context) (*int32, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getSnapshotCacheBuster").Logger()
	queryClient := m.computeClients.NewComputeServiceClient(ctx)
	defer queryClient.Close()

	res, err := queryClient.GetCurrentSnapshotVersion(ctx, &v1alpha.GetCurrentSnapshotVersionRequest{})
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting snapshot_cache_buster")
		return nil, err
	}
	return &res.SnapshotVersion, nil
}

// returns s3://root/computeSnapshots/<snapshot_cache_buster>/<owner_id>/<plan_hash>/<data_version>
func (m *computeManager) getComputeSnapshotDataURI(owner *ent.Owner, snapshotCacheBuster int32, planHash []byte, dataVersion int64) string {
	subPath := path.Join("computeSnapshots", strconv.Itoa(int(snapshotCacheBuster)), owner.ID.String(), base64.RawURLEncoding.EncodeToString(planHash), utils.Int64ToString(dataVersion))
	return m.objectStore.GetDataPathURI(subPath)
}
