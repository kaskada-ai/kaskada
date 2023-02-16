package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada/kaskada-ai/wren/customerrors"
	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadafile"
	"github.com/kaskada/kaskada-ai/wren/ent/predicate"
	"github.com/kaskada/kaskada-ai/wren/ent/preparejob"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	"github.com/kaskada/kaskada-ai/wren/property"
)

type prepareJobClient struct {
	entClient *ent.Client
}

// NewPrepareJobClient creates a new PrepareJobClient from an ent client
func NewPrepareJobClient(entClient *ent.Client) PrepareJobClient {
	return &prepareJobClient{
		entClient: entClient,
	}
}

// creates a preparejob
func (c *prepareJobClient) CreatePrepareJob(ctx context.Context, kaskadaFiles []*ent.KaskadaFile, sliceInfo *SliceInfo, prepareCacheBuster int32, state property.PrepareJobState) (*ent.PrepareJob, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.CreatePrepareJob").
		Str("table_name", sliceInfo.KaskadaTable.Name).
		Interface("slice_plan", sliceInfo.Plan).
		Int32("prepare_cache_buster", prepareCacheBuster).
		Logger()

	prepareJob, err := c.entClient.PrepareJob.Create().
		AddKaskadaFiles(kaskadaFiles...).
		SetKaskadaTable(sliceInfo.KaskadaTable).
		SetPrepareCacheBuster(prepareCacheBuster).
		SetSliceHash(sliceInfo.PlanHash).
		SetSlicePlan(sliceInfo.Plan).
		SetState(state).Save(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue creating prepareJob")
		return nil, err
	}

	// return job with expected edges set.
	prepareJob.Edges.KaskadaFiles = kaskadaFiles
	prepareJob.Edges.KaskadaTable = sliceInfo.KaskadaTable

	return prepareJob, nil
}

func (c *prepareJobClient) GetPrepareJob(ctx context.Context, id uuid.UUID) (*ent.PrepareJob, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetPrepareJob").
		Str("preapre_job_id", id.String()).
		Logger()

	prepareJob, err := c.includeEdges(c.entClient.PrepareJob.Query().Where(preparejob.ID(id))).First(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("prepare_job")
		}
		subLogger.Error().Err(err).Msg("issue getting prepare_job")
		return nil, err
	}
	return prepareJob, nil
}

// finds all the prepare jobs for the passed files, in the prepareStates
func (c *prepareJobClient) ListPrepareJobs(ctx context.Context, kaskadaFiles []*ent.KaskadaFile, sliceInfo *SliceInfo, prepareCacheBuster int32, additonalFilters ...predicate.PrepareJob) ([]*ent.PrepareJob, error) {
	fileIds := []uuid.UUID{}
	for _, kaskadaFile := range kaskadaFiles {
		fileIds = append(fileIds, kaskadaFile.ID)
	}

	wherePredicates := []predicate.PrepareJob{
		preparejob.PrepareCacheBuster(prepareCacheBuster),
		preparejob.SliceHash(sliceInfo.PlanHash),
		preparejob.HasKaskadaFilesWith(kaskadafile.IDIn(fileIds...)),
	}

	wherePredicates = append(wherePredicates, additonalFilters...)

	return c.includeEdges(sliceInfo.KaskadaTable.QueryPrepareJobs().Where(wherePredicates...)).All(ctx)
}

func (c *prepareJobClient) UpdatePrepareJobState(ctx context.Context, prepareJob *ent.PrepareJob, newState property.PrepareJobState) error {
	return prepareJob.Update().SetState(newState).Exec(ctx)
}

func (c *prepareJobClient) AddFilesToPrepareJob(ctx context.Context, prepareJob *ent.PrepareJob, preparedFilesToCreateAndAttach []*v1alpha.PreparedFile, relatedKaskadaFile *ent.KaskadaFile) error {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.AddFilesToPrepareJob").
		Str("prepare_job_id", prepareJob.ID.String()).
		Logger()

	tx, err := c.entClient.Tx(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting transaction")
		return err
	}

	rollbackCleanup := func(tx *ent.Tx, err error, reason string) error {
		rollbackErr := rollback(tx, err)
		subLogger.Error().Err(rollbackErr).Msg(reason)
		return rollbackErr
	}

	for _, preparedFile := range preparedFilesToCreateAndAttach {
		newPreparedFileCreate := tx.PreparedFile.Create().
			SetKaskadaTable(prepareJob.Edges.KaskadaTable).
			SetPrepareJob(prepareJob).
			SetMaxEventTime(preparedFile.MaxEventTime.AsTime().UnixNano()).
			SetMinEventTime(preparedFile.MinEventTime.AsTime().UnixNano()).
			SetRowCount(preparedFile.NumRows).
			SetPath(preparedFile.Path).
			SetValidFromVersion(relatedKaskadaFile.ValidFromVersion).
			SetMetadataPath(preparedFile.MetadataPath)

		if relatedKaskadaFile.ValidToVersion != nil {
			newPreparedFileCreate.SetValidToVersion(*relatedKaskadaFile.ValidToVersion)
		}

		_, err := newPreparedFileCreate.Save(ctx)
		if err != nil {
			return rollbackCleanup(tx, err, "issue creating new prepared_file")
		}
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return err
	}

	return nil
}

func (c *prepareJobClient) includeEdges(query *ent.PrepareJobQuery) *ent.PrepareJobQuery {
	return query.
		WithKaskadaFiles().
		WithKaskadaTable(func(q *ent.KaskadaTableQuery) { q.WithOwner() }).
		WithPreparedFiles()
}
