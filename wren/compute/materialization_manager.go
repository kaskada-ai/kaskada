package compute

import (
	"context"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
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
}

type materializationManager struct {
	CompileManager

	computeClients        client.ComputeClients
	kaskadaTableClient    internal.KaskadaTableClient
	materializationClient internal.MaterializationClient
}

func NewMaterializationManager(compileManager *CompileManager, computeClients *client.ComputeClients, kaskadaTableClient *internal.KaskadaTableClient, materializationClient *internal.MaterializationClient) MaterializationManager {
	return &materializationManager{
		CompileManager:        *compileManager,
		computeClients:        *computeClients,
		kaskadaTableClient:    *kaskadaTableClient,
		materializationClient: *materializationClient,
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
