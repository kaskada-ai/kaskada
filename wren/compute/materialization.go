package compute

import (
	"context"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/rs/zerolog/log"
)

func (m *Manager) StartMaterialization(ctx context.Context, owner *ent.Owner, materializationID string, compileResp *v1alpha.CompileResponse, destination *v1alpha.Destination) error {
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

func (m *Manager) StopMaterialization(ctx context.Context, materializationID string) error {
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

func (m *Manager) GetMaterializationStatus(ctx context.Context, materializationID string) (*v1alpha.ProgressInformation, error) {
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

func (m *Manager) ReconcileMaterialzations(ctx context.Context) error {
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

			isExperimental := false
			compileResp, err := m.CompileQuery(ctx, owner, streamMaterialization.Expression, streamMaterialization.WithViews.Views, false, isExperimental, streamMaterialization.SliceRequest, v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS)
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

func (m *Manager) getMaterializationTables(ctx context.Context, owner *ent.Owner, compileResp *v1alpha.CompileResponse) ([]*v1alpha.ComputeTable, error) {
	slicePlanMap := map[string][]*v1alpha.SlicePlan{}
	for _, slicePlan := range compileResp.TableSlices {
		if _, found := slicePlanMap[slicePlan.TableName]; !found {
			slicePlanMap[slicePlan.TableName] = []*v1alpha.SlicePlan{}
		}
		slicePlanMap[slicePlan.TableName] = append(slicePlanMap[slicePlan.TableName], slicePlan)
	}

	kaskadaTableMap, err := m.getTablesForQuery(ctx, owner, compileResp.TableSlices)
	if err != nil {
		return nil, err
	}

	computeTables := make([]*v1alpha.ComputeTable, len(kaskadaTableMap))
	i := 0

	for _, kaskadaTable := range kaskadaTableMap {

		computeTables[i] = convertKaskadaTableToComputeTable(kaskadaTable)
		computeTables[i].FileSets = []*v1alpha.ComputeTable_FileSet{}

		for _, slicePlan := range slicePlanMap[kaskadaTable.Name] {
			computeTables[i].FileSets = append(computeTables[i].FileSets, &v1alpha.ComputeTable_FileSet{SlicePlan: slicePlan})
		}
		i++
	}

	return computeTables, nil
}
