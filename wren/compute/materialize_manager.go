package compute

import (
	"context"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/rs/zerolog/log"
)

type MaterializationManager interface {
	CompileManager

	// StartMaterialization starts a materialization on the compute backend
	StartMaterialization(ctx context.Context, owner *ent.Owner, materializationID string, compileResp *v1alpha.CompileResponse, destination *v1alpha.Destination) error

	// StopMaterialization stops a materialization on the compute backend
	StopMaterialization(ctx context.Context, materializationID string) error

	// GetMaterializationStatus gets the status of a materialization on the compute backend
	GetMaterializationStatus(ctx context.Context, materializationID string) (*v1alpha.ProgressInformation, error)

	// ReconcileMaterializations reconciles the materializations in the database with the materializations on the compute backend
	ReconcileMaterializations(ctx context.Context) error
}

type materializationManager struct {
	CompileManager

	computeClients        client.ComputeClients
	kaskadaTableClient    internal.KaskadaTableClient
	materializationClient internal.MaterializationClient

	runningMaterailizations map[string]struct{}
}

func NewMaterializationManager(compileManager *CompileManager, computeClients *client.ComputeClients, kaskadaTableClient *internal.KaskadaTableClient, materializationClient *internal.MaterializationClient) MaterializationManager {
	return &materializationManager{
		CompileManager:          *compileManager,
		computeClients:          *computeClients,
		kaskadaTableClient:      *kaskadaTableClient,
		materializationClient:   *materializationClient,
		runningMaterailizations: map[string]struct{}{},
	}
}

func (m *materializationManager) StartMaterialization(ctx context.Context, owner *ent.Owner, materializationID string, compileResp *v1alpha.CompileResponse, destination *v1alpha.Destination) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.StartMaterialization").Str("materialization_id", materializationID).Logger()

	tables, err := m.getMaterializationTables(ctx, owner, compileResp)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting materialization tables")
	}

	startRequest := &v1alpha.StartMaterializationRequest{
		MaterializationId: materializationID,
		Plan:              compileResp.Plan,
		Tables:            tables,
		Destination:       destination,
	}

	computeClient := m.computeClients.ComputeServiceClient(ctx)
	defer computeClient.Close()

	subLogger.Info().
		Interface("tables", startRequest.Tables).
		Interface("destination", startRequest.Destination).Msg("sending start materialization request to compute backend")

	_, err = computeClient.StartMaterialization(ctx, startRequest)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting materialization")
		return customerrors.NewComputeError(reMapSparrowError(ctx, err))
	}

	return nil
}

func (m *materializationManager) StopMaterialization(ctx context.Context, materializationID string) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.StopMaterialization").Str("materialization_id", materializationID).Logger()

	stopRequest := &v1alpha.StopMaterializationRequest{
		MaterializationId: materializationID,
	}

	computeClient := m.computeClients.ComputeServiceClient(ctx)
	defer computeClient.Close()

	_, err := computeClient.StopMaterialization(ctx, stopRequest)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue stopping materialization")
		return customerrors.NewComputeError(reMapSparrowError(ctx, err))
	}

	return nil
}

func (m *materializationManager) GetMaterializationStatus(ctx context.Context, materializationID string) (*v1alpha.ProgressInformation, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetMaterializationStatus").Str("materialization_id", materializationID).Logger()

	statusRequest := &v1alpha.GetMaterializationStatusRequest{
		MaterializationId: materializationID,
	}

	computeClient := m.computeClients.ComputeServiceClient(ctx)
	defer computeClient.Close()

	statusResponse, err := computeClient.GetMaterializationStatus(ctx, statusRequest)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting materialization status")
		return nil, customerrors.NewComputeError(reMapSparrowError(ctx, err))
	}

	return statusResponse.Progress, nil
}

func (m *materializationManager) ReconcileMaterializations(ctx context.Context) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.ReconcileMaterialzations").Logger()

	allStreamMaterializations, err := m.materializationClient.GetAllMaterializationsBySourceType(ctx, materialization.SourceTypeStreams)
	if err != nil {
		subLogger.Error().Err(err).Msg("failed to get all stream materializations")
		return err
	}

	newRunningMaterailizations := make(map[string]struct{})
	// find all materializations in the database and ensure they are running
	for _, streamMaterialization := range allStreamMaterializations {
		materializationID := streamMaterialization.ID.String()
		owner := streamMaterialization.Edges.Owner

		isRunning := false
		// check to see if the materailization was running in the previous iteration
		if _, found := m.runningMaterailizations[materializationID]; found {
			//verify that the materialization is still running
			progressInfo, err := m.GetMaterializationStatus(ctx, materializationID)
			if err != nil {
				log.Error().Err(err).Str("id", materializationID).Msg("failed to get materialization status")
			}
			isRunning = progressInfo != nil
		}

		if isRunning {
			newRunningMaterailizations[materializationID] = struct{}{}
		} else {
			log.Debug().Str("id", materializationID).Msg("found materialization that is not running, attempting to start it")

			compileResp, _, err := m.CompileEntMaterialization(ctx, owner, streamMaterialization)
			if err != nil {
				log.Error().Err(err).Str("id", materializationID).Msg("issue compiling materialization")
			} else {
				err = m.StartMaterialization(ctx, owner, materializationID, compileResp, streamMaterialization.Destination)
				if err != nil {
					log.Error().Err(err).Str("id", materializationID).Msg("failed to start materialization")
				} else {
					log.Debug().Str("id", materializationID).Msg("started materialization")
					newRunningMaterailizations[materializationID] = struct{}{}
				}
			}
		}
	}

	// find all materializations that were running in the previous iteration but don't exist anymore
	for materializationID := range m.runningMaterailizations {
		if _, found := newRunningMaterailizations[materializationID]; !found {
			log.Debug().Str("id", materializationID).Msg("found materialization that no longer exists, attempting to stop it")
			err := m.StopMaterialization(ctx, materializationID)
			if err != nil {
				log.Error().Err(err).Str("id", materializationID).Msg("failed to stop materialization")
				newRunningMaterailizations[materializationID] = struct{}{}
			} else {
				log.Debug().Str("id", materializationID).Msg("stopped materialization")
			}
		}
	}
	m.runningMaterailizations = newRunningMaterailizations
	return nil
}

func (m *materializationManager) getMaterializationTables(ctx context.Context, owner *ent.Owner, compileResp *v1alpha.CompileResponse) ([]*v1alpha.ComputeTable, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "materializationManager.getMaterializationTables").Logger()

	// map of tableName to a list of slice plans
	slicePlanMap := map[string][]*v1alpha.SlicePlan{}
	for _, slicePlan := range compileResp.TableSlices {
		if _, found := slicePlanMap[slicePlan.TableName]; !found {
			slicePlanMap[slicePlan.TableName] = []*v1alpha.SlicePlan{}
		}
		slicePlanMap[slicePlan.TableName] = append(slicePlanMap[slicePlan.TableName], slicePlan)
	}

	computeTables := make([]*v1alpha.ComputeTable, len(slicePlanMap))
	i := 0

	for tableName, slicePlanList := range slicePlanMap {

		kaskadaTable, err := m.kaskadaTableClient.GetKaskadaTableByName(ctx, owner, tableName)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting kaskada table")
			return nil, err
		}

		computeTables[i] = convertKaskadaTableToComputeTable(kaskadaTable)
		computeTables[i].FileSets = make([]*v1alpha.ComputeTable_FileSet, len(slicePlanList))

		for j, slicePlan := range slicePlanList {
			computeTables[i].FileSets[j] = &v1alpha.ComputeTable_FileSet{SlicePlan: slicePlan}
		}
		i++
	}

	return computeTables, nil
}
