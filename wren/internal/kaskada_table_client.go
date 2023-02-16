package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada/kaskada-ai/wren/customerrors"
	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/computesnapshot"
	"github.com/kaskada/kaskada-ai/wren/ent/dataversion"
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadafile"
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadatable"
	"github.com/kaskada/kaskada-ai/wren/ent/materializationdependency"
	"github.com/kaskada/kaskada-ai/wren/ent/predicate"
	"github.com/kaskada/kaskada-ai/wren/ent/preparedfile"
	"github.com/kaskada/kaskada-ai/wren/ent/preparejob"
	"github.com/kaskada/kaskada-ai/wren/ent/schema"
	"github.com/kaskada/kaskada-ai/wren/ent/viewdependency"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

type kaskadaTableClient struct {
	entClient *ent.Client
}

// NewKaskadaTableClient creates a new KaskadaTableClient from an ent client
func NewKaskadaTableClient(entClient *ent.Client) KaskadaTableClient {
	return &kaskadaTableClient{
		entClient: entClient,
	}
}

func (c *kaskadaTableClient) CreateKaskadaTable(ctx context.Context, owner *ent.Owner, newTable *ent.KaskadaTable) (*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.CreateKaskadaTable").
		Interface("new_table", newTable).
		Logger()

	createTable := c.entClient.KaskadaTable.Create().
		SetOwner(owner).
		SetName(newTable.Name).
		SetEntityKeyColumnName(newTable.EntityKeyColumnName).
		SetGroupingID(newTable.GroupingID).
		SetTimeColumnName(newTable.TimeColumnName).
		SetSource(newTable.Source)

	if newTable.SubsortColumnName != nil {
		createTable.SetSubsortColumnName(*newTable.SubsortColumnName)
	}

	if newTable.MergedSchema != nil {
		createTable.SetMergedSchema(newTable.MergedSchema)
	}

	kaskadaTable, err := createTable.Save(ctx)
	if err != nil {
		if violatesUniqueConstraint(err) {
			return nil, customerrors.NewAlreadyExistsError("table")
		}
		subLogger.Error().Err(err).Msg("issue creating kaskada_table")
		return nil, err
	}

	return kaskadaTable, nil
}

func (c *kaskadaTableClient) DeleteKaskadaTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.DeleteKaskadaTable").
		Str("table_name", kaskadaTable.Name).
		Logger()

	tx, err := c.entClient.Tx(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting transaction")
		return nil, err
	}

	rollbackCleanup := func(tx *ent.Tx, err error, reason string) error {
		rollbackErr := rollback(tx, err)
		subLogger.Error().Err(rollbackErr).Msg(reason)
		return rollbackErr
	}

	_, err = tx.KaskadaFile.Delete().Where(kaskadafile.HasKaskadaTableWith(kaskadatable.ID(kaskadaTable.ID))).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting kaskada_file")
	}

	_, err = tx.PreparedFile.Delete().Where(preparedfile.HasKaskadaTableWith(kaskadatable.ID(kaskadaTable.ID))).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting prepared_files")
	}

	_, err = tx.PrepareJob.Delete().Where(preparejob.HasKaskadaTableWith(kaskadatable.ID(kaskadaTable.ID))).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting prepare_jobs")
	}

	_, err = tx.DataVersion.Delete().Where(dataversion.HasKaskadaTableWith(kaskadatable.ID(kaskadaTable.ID))).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting data_versions")
	}

	_, err = tx.ComputeSnapshot.Delete().Where(computesnapshot.HasKaskadaTablesWith(kaskadatable.ID(kaskadaTable.ID))).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting compute_snapshots")
	}

	kaskadaTableID := kaskadaTable.ID
	err = tx.KaskadaTable.DeleteOneID(kaskadaTableID).Exec(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue deleting kaskada_table")
	}

	// update dependency links to nil where this table is used by views
	_, err = tx.ViewDependency.Update().
		Where(
			viewdependency.DependencyTypeEQ(schema.DependencyType_Table),
			viewdependency.ID(kaskadaTableID),
		).
		ClearDependencyID().
		Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue clearing table dependencies for views")
	}

	// update dependency links to nil where this table is used by materializations
	_, err = tx.MaterializationDependency.Update().
		Where(
			materializationdependency.DependencyTypeEQ(schema.DependencyType_Table),
			materializationdependency.ID(kaskadaTableID),
		).
		ClearDependencyID().
		Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue clearing table dependencies for materializations")
	}

	newDataVersion, err := tx.DataVersion.Create().
		SetOwner(owner).
		Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue creating new data_version")
	}

	newDataToken, err := tx.DataToken.Create().
		SetOwner(owner).
		SetKaskadaTableID(kaskadaTableID).
		SetDataVersionID(newDataVersion.ID).
		Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue creating new data_token")
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return nil, err
	}

	return newDataToken.Unwrap(), nil
}

func (c *kaskadaTableClient) GetAllKaskadaTables(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetAllKaskadaTables").
		Logger()

	kaskadaTables, err := owner.QueryKaskadaTables().All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting kaskada_tables")
		return nil, err
	}
	return kaskadaTables, nil
}

func (c *kaskadaTableClient) ListKaskadaTables(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.ListKaskadaTables").
		Logger()

	tables, err := owner.QueryKaskadaTables().
		Where(
			kaskadatable.Or(
				kaskadatable.NameContainsFold(searchTerm),
				kaskadatable.DescriptionContainsFold(searchTerm),
				kaskadatable.EntityKeyColumnNameContainsFold(searchTerm),
				kaskadatable.TimeColumnNameContainsFold(searchTerm),
				kaskadatable.SubsortColumnNameContainsFold(searchTerm),
			),
		).
		Limit(pageSize).
		Offset(offset).
		Order(ent.Asc(kaskadatable.FieldName)).
		All(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("table")
		}
		subLogger.Error().Err(err).Msg("issue listing kaskada_tables")
		return nil, err
	}
	return tables, nil
}

func (c *kaskadaTableClient) GetKaskadaTable(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetKaskadaTable").
		Str("table_id", id.String()).
		Logger()

	kaskadaTable, err := owner.QueryKaskadaTables().Where(kaskadatable.ID(id)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("table")
		}
		subLogger.Error().Err(err).Msg("issue getting kaskada_table")
		return nil, err
	}
	return kaskadaTable, nil
}

func (c *kaskadaTableClient) GetKaskadaTableByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetKaskadaTableByName").
		Str("table_name", name).
		Logger()

	kaskadaTable, err := owner.QueryKaskadaTables().Where(kaskadatable.Name(name)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("table")
		}
		subLogger.Error().Err(err).Msg("issue getting kaskada_table")
		return nil, err
	}
	return kaskadaTable, nil
}

func (c *kaskadaTableClient) GetKaskadaTablesFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaTable, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetKaskadaTablesFromNames").
		Logger()

	predicates := make([]predicate.KaskadaTable, 0, len(names))

	for _, name := range names {
		predicates = append(predicates, kaskadatable.Name(name))
	}

	kaskadaTables, err := owner.QueryKaskadaTables().Where(kaskadatable.Or(predicates...)).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting tables")
		return nil, err
	}

	tableMap := map[string]*ent.KaskadaTable{}

	for _, kaskadaTable := range kaskadaTables {
		tableMap[kaskadaTable.Name] = kaskadaTable
	}

	return tableMap, nil
}

func (c *kaskadaTableClient) GetKaskadaFiles(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, dataToken *ent.DataToken) ([]*ent.KaskadaFile, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.GetKaskadaFiles").
		Str("table_name", kaskadaTable.Name).
		Str("data_token", dataToken.ID.String()).
		Logger()

	tableFiles, err := kaskadaTable.QueryKaskadaFiles().Where(kaskadafile.ValidFromVersionLTE(dataToken.DataVersionID)).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting table files for data token")
	}
	return tableFiles, err
}

type AddFileProps struct {
	// the URI of where to find the file
	URI string

	// a md5 hash or other identifier that can help determine if the file is unique
	Identifier string

	// the schema of the file
	Schema *v1alpha.Schema

	// the type of file
	FileType kaskadafile.Type
}

func (c *kaskadaTableClient) AddFilesToTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, newFiles []AddFileProps, newMergedSchema *v1alpha.Schema, newExternalRevision *string, cleanupOnError func() error) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.AddFilesToTable").
		Str("table_name", kaskadaTable.Name).
		Int("file_count", len(newFiles)).
		Logger()

	tx, err := c.entClient.Tx(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting transaction")
		return nil, err
	}

	rollbackCleanup := func(tx *ent.Tx, err error, reason string) error {
		rollbackErr := rollback(tx, err)
		if cleanupErr := cleanupOnError(); cleanupErr != nil {
			subLogger.Error().Err(cleanupErr).Msg("issue cleaning up after transaction rollback")
		}
		if err != nil && violatesUniqueConstraint(err) {
			return customerrors.NewAlreadyExistsInError("file", "table")
		}
		subLogger.Error().Err(rollbackErr).Msg(reason)
		return rollbackErr
	}

	createDataVersion := tx.DataVersion.Create().
		SetOwner(owner).
		SetKaskadaTable(kaskadaTable)

	if newExternalRevision != nil {
		createDataVersion.SetExternalRevision(*newExternalRevision)
	}

	newDataVersion, err := createDataVersion.Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue creating new kaskada_table_version")
	}

	for _, newFile := range newFiles {
		_, err = tx.KaskadaFile.Create().
			SetKaskadaTable(kaskadaTable).
			SetPath(newFile.URI).
			SetIdentifier(newFile.Identifier).
			SetSchema(newFile.Schema).
			SetType(newFile.FileType).
			SetValidFromVersion(newDataVersion.ID).
			Save(ctx)
		if err != nil {
			return nil, rollbackCleanup(tx, err, "issue creating new kaskada_table_file")
		}
	}

	newDataToken, err := tx.DataToken.Create().
		SetOwner(owner).
		SetKaskadaTableID(kaskadaTable.ID).
		SetDataVersionID(newDataVersion.ID).
		Save(ctx)
	if err != nil {
		return nil, rollbackCleanup(tx, err, "issue creating new data_token")
	}

	if kaskadaTable.MergedSchema != newMergedSchema {
		var n int
		// optimistically update the kaskadaTable MergedSchema.  See https://entgo.io/blog/2021/07/22/database-locking-techniques-with-ent/
		if kaskadaTable.MergedSchema == nil {
			n, err = tx.KaskadaTable.Update().
				// limit the update to only work on the correct record and existing schema
				Where(kaskadatable.ID(kaskadaTable.ID), kaskadatable.MergedSchemaIsNil()).
				SetMergedSchema(newMergedSchema).Save(ctx)
		} else {
			n, err = tx.KaskadaTable.Update().
				// limit the update to only work on the correct record and existing schema
				Where(kaskadatable.ID(kaskadaTable.ID), kaskadatable.MergedSchema(kaskadaTable.MergedSchema)).
				SetMergedSchema(newMergedSchema).Save(ctx)
		}
		if err != nil {
			return nil, rollbackCleanup(tx, err, "issue updating table merged_schema")
		}
		if n != 1 {
			return nil, rollbackCleanup(tx, fmt.Errorf("expected one updated row, but got: %d", n), "merged_schema was updated by another process")
		}
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return nil, err
	}

	return newDataToken.Unwrap(), nil
}

func (c *kaskadaTableClient) SaveComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, dataToken *ent.DataToken, path string, maxEventTime time.Time, relatedTableIDs []uuid.UUID) error {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.CreateComputeSnapshot").
		Bytes("compile_plan_hash", complilePlanHash).
		Logger()

	_, err := c.entClient.ComputeSnapshot.Create().
		SetOwner(owner).
		SetDataVersionID(dataToken.DataVersionID).
		SetMaxEventTime(maxEventTime.UnixNano()).
		SetPath(path).
		SetPlanHash(complilePlanHash).
		SetSnapshotCacheBuster(snapshotCacheBuster).
		AddKaskadaTableIDs(relatedTableIDs...).
		Save(ctx)

	if err != nil {
		if violatesUniqueConstraint(err) {
			return customerrors.NewAlreadyExistsError("compute_snapshot")
		}
		subLogger.Error().Err(err).Msg("issue creating compute_snapshot")
		return err
	}

	return nil
}

func (c *kaskadaTableClient) GetBestComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, slices []*SliceInfo, prepareCacheBuster int32) (*ent.ComputeSnapshot, error) {
	// Current assumptions:
	// 1) Only one snapshot per data token is created
	// 2) Only Final results are supported, meaning we can start at anytime to produce the full result set
	// 3) Incremental (resume from snapshot) is only used when a query is ran at the "current" dataToken

	// Handling late data in snapshots
	//
	// find most recent snapshot, and its associated dataToken.
	// find the find the min_event_time of new files since this snapshot by
	//    finding all preparedFiles that are newer than this dataToken:
	//        by finding preparedFiles that have a ValidFrom greater than this dataToken
	// then, loop over snapshots from most recent to oldest.
	//    If: the snapshot has a max_event_time less than the min_event_time
	//    Then: we can use it, return it
	//    Else: it's now invalid, so delete it.
	//
	// Note that we do not need to find the min_event_time of new files again.
	// If we have a valid snapshot, we can assume that all previous snapshots are
	// also valid in regard to the most recent valid snapshot. Therefore, only the
	// new files between the most recent snapshot and the current data token can
	// affect the validity of snapshots.

	mostRecentSnapshot, err := getNewestSnapshot(ctx, owner, snapshotCacheBuster, complilePlanHash)
	if err != nil {
		if ent.IsNotFound(err) {
			// It is a valid use case to not have an existing snapshot yet.
			return nil, nil
		}
		return nil, err
	}

	// Initialize the min_event_time to the snapshot's max_event_time.
	// If a new file has a minimum time less than this, the snapshot
	// is invalid.
	minTimeInNewFiles := mostRecentSnapshot.MaxEventTime

	for _, slice := range slices {
		minTime, err := getMinTimeOfNewPreparedFiles(ctx, prepareCacheBuster, slice, mostRecentSnapshot.DataVersionID)
		if ent.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		if *minTime < minTimeInNewFiles {
			minTimeInNewFiles = *minTime
		}
	}

	// cleanup any invalid snapshots

	/* TODO: figure out how to delete old snapshot data from S3
	invalidSnapshots, err := owner.QueryComputeSnapshots().Where(
		computesnapshot.PlanHash(complilePlanHash),
		computesnapshot.SnapshotVersion(snapshotVersion),
		computesnapshot.MaxEventTimeGT(minTimeInNewFiles),
	).All(ctx)
	*/

	c.entClient.ComputeSnapshot.Delete().Where(
		computesnapshot.PlanHash(complilePlanHash),
		computesnapshot.SnapshotCacheBuster(snapshotCacheBuster),
		computesnapshot.MaxEventTimeGT(minTimeInNewFiles),
	).Exec(ctx)

	// get the newest of the remaining snapshots

	return getNewestSnapshot(ctx, owner, snapshotCacheBuster, complilePlanHash)
}

func getNewestSnapshot(ctx context.Context, owner *ent.Owner, snapshotCacheBuster int32, complilePlanHash []byte) (*ent.ComputeSnapshot, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.getNewestSnapshot").
		Bytes("compile_plan_hash", complilePlanHash).
		Logger()

	newestSnapshot, err := owner.QueryComputeSnapshots().
		Where(computesnapshot.PlanHash(complilePlanHash), computesnapshot.SnapshotCacheBuster(snapshotCacheBuster)).
		Order(ent.Desc(computesnapshot.FieldDataVersionID)).
		First(ctx)
	if err != nil {
		if !ent.IsNotFound(err) {
			subLogger.Error().Err(err).Msg("issue getting newest compute_snapshot")
		}
		return nil, err
	}
	return newestSnapshot, nil
}

func getMinTimeOfNewPreparedFiles(ctx context.Context, prepareCacheBuster int32, sliceInfo *SliceInfo, dataVersion int64) (*int64, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaTableClient.getMinTimeOfNewPreparedFiles").
		Str("table_name", sliceInfo.KaskadaTable.Name).
		Interface("slice_info", sliceInfo).
		Logger()

	preparedFileWithMinEventTime, err := sliceInfo.KaskadaTable.QueryPreparedFiles().
		Where(
			preparedfile.HasPrepareJobWith(preparejob.PrepareCacheBuster(prepareCacheBuster), preparejob.SliceHash(sliceInfo.PlanHash)),
			preparedfile.ValidFromVersionGT(dataVersion),
		).Order(ent.Asc(preparedfile.FieldMinEventTime)).
		First(ctx)

	if err != nil {
		if !ent.IsNotFound(err) {
			subLogger.Error().Err(err).Msg("issue getting min_event_time of new files")
		}
		return nil, err
	}
	return &preparedFileWithMinEventTime.MinEventTime, nil
}

func (c *kaskadaTableClient) GetKaskadaTableVersion(ctx context.Context, kaskadaTable *ent.KaskadaTable) (*ent.DataVersion, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "kaskadaTableClient.GetKaskadaTableVersion").Str("table_id", kaskadaTable.ID.String()).Logger()
	version, err := kaskadaTable.QueryDataVersions().Order(ent.Desc(dataversion.FieldID)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		} else {
			subLogger.Error().Err(err).Msg("issue getting current table version")
		}
	}
	return version, nil
}
