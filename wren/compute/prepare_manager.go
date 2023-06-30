package compute

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadafile"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/property"
	"github.com/kaskada-ai/kaskada/wren/store"
	"github.com/kaskada-ai/kaskada/wren/utils"
)

const (
	prepareFilePrefix     = "part"
	prepareTimeoutSeconds = 1800 //30 mins
)

type PrepareManager interface {
	PrepareTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error)
	GetPrepareCacheBuster(ctx context.Context) (*int32, error)
}

type prepareManager struct {
	computeClients     client.ComputeClients
	kaskadaTableClient internal.KaskadaTableClient
	prepareJobClient   internal.PrepareJobClient
	parallelizeConfig  utils.ParallelizeConfig
	tableStore         store.TableStore
	tr                 trace.Tracer
}

func NewPrepareManager(computeClients *client.ComputeClients, kaskadaTableClient *internal.KaskadaTableClient, prepareJobClient *internal.PrepareJobClient, parallelizeConfig *utils.ParallelizeConfig, tableStore *store.TableStore) PrepareManager {
	return &prepareManager{
		computeClients:     *computeClients,
		kaskadaTableClient: *kaskadaTableClient,
		prepareJobClient:   *prepareJobClient,
		parallelizeConfig:  *parallelizeConfig,
		tableStore:         *tableStore,
		tr:                 otel.Tracer("prepareManager"),
	}
}

// converts a dataToken into a map of of tableIDs to internal.SliceTable
// prepares data as needed
func (m *prepareManager) PrepareTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error) {
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

// parallelly prepares files and downloads them after prepare.  starts downloading as soon as files are available.
func (m *prepareManager) parallelPrepare(ctx context.Context, owner *ent.Owner, sliceTableMap map[uuid.UUID]*internal.SliceTable) error {
	subLogger := log.Ctx(ctx).With().Str("method", "compute.parallelPrepare").Logger()
	ctx, span := m.tr.Start(ctx, "compute.parallelPrepare")
	defer span.End()

	// create 2 wait groups.  the prepare group inherits from the main group.
	mainGroup, mainCtx := errgroup.WithContext(ctx)
	prepareGroup, prepareCtx := errgroup.WithContext(mainCtx)

	// create 2 channels. new jobs are pushed into the prepare channel
	// pulled from prepare channel for prepare, and pushed into the results channel
	// pulled from results channel and returned
	prepareCh := make(chan *ent.PrepareJob)
	resultCh := make(chan *ent.PrepareJob)

	// initiate work in the prepare group
	prepareGroup.Go(func() error {
		// after loading all jobs into the prepare channel, close it
		// after closing, remaining jobs can still be pulled, but no more work can be added.
		defer close(prepareCh)

		// for each job
		for _, sliceTable := range sliceTableMap {
			for _, fileSet := range sliceTable.FileSetMap {
				for _, prepareJob := range fileSet.PrepareJobs {
					select {
					case <-prepareCtx.Done(): // if prepare context closed, stop loading jobs
						err := prepareCtx.Err()
						subLogger.Warn().Err(err).Msg("unexpected: prepare context canceled, exiting from input go routine")
						return err
					case prepareCh <- prepareJob: // otherwise load job
					}
				}
			}
		}
		return nil // after loading jobs successfully, return no errors
	})

	// spin up parallel prepare workers
	for i := 0; i < m.parallelizeConfig.PrepareFactor; i++ {
		// launch workers inside the prepare group.  all the workers must return before the group closes.
		prepareGroup.Go(func() error {
			// pull jobs from the prepare channel
			// this for-loop will only exit after the prepare channel is closed
			for prepareJob := range prepareCh {
				if prepareJob == nil {
					subLogger.Error().Msg("unexpected; prepare_job is nil")
					continue
				}

				err := m.executePrepare(ctx, owner, prepareJob)
				if err != nil {
					subLogger.Error().Err(err).Str("prepare_job_id", prepareJob.ID.String()).Msg("unable to execute prepare")
					return err
				}

				select {
				case <-prepareCtx.Done(): // if prepare context closed, stop working on jobs
					err := prepareCtx.Err()
					subLogger.Warn().Err(err).Msg("unexpected: prepare context canceled, exiting from prepare worker go routine")
					return err
				case resultCh <- prepareJob: // otherwise send job to results
				}
			}
			subLogger.Debug().Msg("expected: no more jobs to process, prepare channel closed, exiting from prepare worker go routine")
			return nil
		})
	}

	// initiate work inside the main group.
	mainGroup.Go(func() error {
		// wait for the job loader and all the prepare workers to finish
		err := prepareGroup.Wait()
		// then close the result channel
		close(resultCh)
		// pass any errors through
		return err
	})

	// pull results off the results channel
	// note: this is essentially the only code in this method running outside of a go routine.
	//       execution will block here until the results channel is closed
	for result := range resultCh {
		subLogger.Debug().Interface("prepare_job", result).Msg("prepared")
	}

	// Check whether any of the goroutines failed. Since g is accumulating the
	// errors, we don't need to send them (or check for them) in the individual
	// results sent on the channel.
	if err := mainGroup.Wait(); err != nil {
		return err
	}

	subLogger.Debug().Msg("finished preparing")
	return nil
}

// executePrepare will prepare files via the Compute Prepare API
// when successful, updates the the `prepareJob`
func (m *prepareManager) executePrepare(ctx context.Context, owner *ent.Owner, prepareJob *ent.PrepareJob) error {
	if prepareJob == nil {
		log.Ctx(ctx).Error().Msg("unexpected; got nil prepare_job")
		return fmt.Errorf("unexpected; got nil prepare_job")
	}

	subLogger := log.Ctx(ctx).With().
		Str("method", "compute.executePrepare").
		Int32("prepare_cache_buster", prepareJob.PrepareCacheBuster).
		Interface("slice_plan", prepareJob.SlicePlan).Logger()

	// first test if job already complete
	if prepareJob.State == property.PrepareJobStateFinished {
		return nil
	}

	// next test if job already complete from query v1 prepare
	if prepareJob.State == property.PrepareJobStateUnspecified {
		if len(prepareJob.Edges.PreparedFiles) > 0 {
			err := m.prepareJobClient.UpdatePrepareJobState(ctx, prepareJob, property.PrepareJobStateFinished)
			if err != nil {
				subLogger.Error().Err(err).Msg("issue updating prepare_job state")
				return err
			}
			return nil
		}
	}

	kaskadaTable := prepareJob.Edges.KaskadaTable
	for _, kaskadaFile := range prepareJob.Edges.KaskadaFiles {
		prepareOutputURI := m.tableStore.GetPrepareOutputURI(owner, kaskadaTable, kaskadaFile, prepareJob.PrepareCacheBuster, prepareJob.SliceHash)

		computeTable := convertKaskadaTableToComputeTable(kaskadaTable)

		var sourceData *v1alpha.SourceData
		switch kaskadaFile.Type {
		case kaskadafile.TypeCsv:
			sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_CsvPath{CsvPath: kaskadaFile.Path}}
		case kaskadafile.TypeParquet:
			sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_ParquetPath{ParquetPath: kaskadaFile.Path}}
		default:
			subLogger.Error().Str("file_type", kaskadaFile.Type.String()).Msg("unsupported file_type for prepare")
			return fmt.Errorf("unsupported file_type for prepare")
		}

		// Send the preparation request to the prepare client
		prepareClient := m.computeClients.NewPrepareServiceClient(ctx)
		defer prepareClient.Close()
		prepareReq := &v1alpha.PrepareDataRequest{
			Source: &v1alpha.PrepareDataRequest_SourceData{
				SourceData: sourceData,
			},
			Config:           computeTable.Config,
			OutputPathPrefix: prepareOutputURI,
			FilePrefix:       prepareFilePrefix,
			SlicePlan:        prepareJob.SlicePlan,
		}

		subLogger.Debug().Interface("request", prepareReq).Msg("sending prepare request")
		prepareTimeoutCtx, prepareTimeoutCancel := context.WithTimeout(ctx, time.Second*prepareTimeoutSeconds)
		defer prepareTimeoutCancel()

		prepareRes, err := prepareClient.PrepareData(prepareTimeoutCtx, prepareReq)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue preparing files")
			return err
		}
		subLogger.Debug().Interface("response", prepareRes).Msg("received prepare response")

		err = m.prepareJobClient.AddFilesToPrepareJob(ctx, prepareJob, prepareRes.PreparedFiles, kaskadaFile)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue adding prepared_files to prepare_job")
			return err
		}
	}

	err := m.prepareJobClient.UpdatePrepareJobState(ctx, prepareJob, property.PrepareJobStateFinished)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue updating prepare_job state")
		return err
	}
	return nil
}

func (m *prepareManager) getOrCreatePrepareJobs(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, sliceInfo *internal.SliceInfo) ([]*ent.PrepareJob, error) {
	kaskadaTable := sliceInfo.KaskadaTable
	slicePlan := sliceInfo.Plan
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getOrCreatePrepareJobs").Str("table_name", kaskadaTable.Name).Interface("slice_plan", slicePlan.Slice).Logger()

	prepareCacheBuster, err := m.GetPrepareCacheBuster(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting current prepare cache buster")
	}

	kaskadaFiles, err := m.kaskadaTableClient.GetKaskadaFiles(ctx, owner, kaskadaTable, dataToken)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting kaskada table files")
		return nil, err
	}

	foundPrepareJobs, err := m.prepareJobClient.ListPrepareJobs(ctx, kaskadaFiles, sliceInfo, *prepareCacheBuster)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing prepare jobs")
		return nil, err
	}

	kaskadaFileIDsWithExistingPrepareJobs := map[uuid.UUID]interface{}{}

	//find list of kaskadaFiles related to the existing prepareJobs
	for _, prepareJob := range foundPrepareJobs {
		relatedKaskadaFileIDs, err := prepareJob.QueryKaskadaFiles().IDs(ctx)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue querying kaskada_file_ids for prepare_job")
			return nil, err
		}
		for _, relatedKaskadaFileID := range relatedKaskadaFileIDs {
			kaskadaFileIDsWithExistingPrepareJobs[relatedKaskadaFileID] = nil
		}
	}

	newPrepareJobs := []*ent.PrepareJob{}

	//currently prepare can only handle one kaskadaFile at a time
	for _, kaskadaFile := range kaskadaFiles {
		_, found := kaskadaFileIDsWithExistingPrepareJobs[kaskadaFile.ID]
		if !found {
			newPrepareJob, err := m.prepareJobClient.CreatePrepareJob(ctx, []*ent.KaskadaFile{kaskadaFile}, sliceInfo, *prepareCacheBuster, property.PrepareJobStateUnspecified)
			if err != nil {
				subLogger.Error().Err(err).Msg("issue creating prepare_job")
			}
			newPrepareJobs = append(newPrepareJobs, newPrepareJob)
		}
	}

	subLogger.Debug().Interface("merged", append(foundPrepareJobs, newPrepareJobs...)).Msg("done getting prepare")

	return append(foundPrepareJobs, newPrepareJobs...), nil
}

// gets the current prepare cache buster
func (m *prepareManager) GetPrepareCacheBuster(ctx context.Context) (*int32, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.getPrepareCacheBuster").Logger()
	prepareClient := m.computeClients.NewPrepareServiceClient(ctx)
	defer prepareClient.Close()
	res, err := prepareClient.GetCurrentPrepID(ctx, &v1alpha.GetCurrentPrepIDRequest{})
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting prepare_cache_buster")
		return nil, err
	}
	return &res.PrepId, nil
}

func (m *prepareManager) getTablesForQuery(ctx context.Context, owner *ent.Owner, slicePlans []*v1alpha.SlicePlan) (map[string]*ent.KaskadaTable, error) {
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
