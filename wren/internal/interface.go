package internal

import (
	"context"
	"time"

	"github.com/google/uuid"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadafile"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/ent/predicate"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/property"
)

type DataTokenClient interface {
	// gets the current dataToken for an owner
	GetCurrentDataToken(ctx context.Context, owner *ent.Owner) (*ent.DataToken, error)

	// gets a specific dataToken for an owner
	GetDataToken(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.DataToken, error)

	// gets a specific dataToken at the specified version
	GetDataTokenFromVersion(ctx context.Context, owner *ent.Owner, version int64) (*ent.DataToken, error)

	// at a specific dataToken, returns a map of tableIDs to their most recent dataVersion
	GetTableVersions(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken) (map[uuid.UUID]*ent.DataVersion, error)
}

// DataTokenClientProvider creates DataTokenClients
type DataTokenClientProvider func(entClient *ent.Client) DataTokenClient

type OwnerClient interface {
	GetOwner(ctx context.Context, id uuid.UUID) (*ent.Owner, error)
	GetOwnerFromClientID(ctx context.Context, clientID string) (*ent.Owner, error)
}

// OwnerClientProvider creates OwnerClients
type OwnerClientProvider func(entClient *ent.Client) OwnerClient

type KaskadaTableClient interface {
	CreateKaskadaTable(ctx context.Context, owner *ent.Owner, newTable *ent.KaskadaTable) (*ent.KaskadaTable, error)
	DeleteKaskadaTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable) (*ent.DataToken, error)
	GetAllKaskadaTables(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaTable, error)
	GetKaskadaTable(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaTable, error)
	GetKaskadaTableByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaTable, error)
	GetKaskadaTablesFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaTable, error)
	ListKaskadaTables(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaTable, error)

	GetKaskadaTableVersion(ctx context.Context, kaskadaTable *ent.KaskadaTable) (*ent.DataVersion, error)

	GetMinTimeOfNewPreparedFiles(ctx context.Context, prepareCacheBuster int32, sliceInfo *SliceInfo, dataVersion int64) (*int64, error)

	AddFilesToTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, newFiles []AddFileProps, newMergedSchema *v1alpha.Schema, newExternalVersion *string, cleanupOnError func() error) (*ent.DataToken, error)
	GetKaskadaFiles(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, dataToken *ent.DataToken) ([]*ent.KaskadaFile, error)

	SaveComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, dataToken *ent.DataToken, path string, maxEventTime time.Time, relatedTablesIDs []uuid.UUID) error
	GetBestComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, slices []*SliceInfo, prepareCacheBuster int32) (*ent.ComputeSnapshot, error)
}

// KaskadaTableClientProvider creates KaskadaTableClients
type KaskadaTableClientProvider func(entClient *ent.Client) KaskadaTableClient

type KaskadaViewClient interface {
	CreateKaskadaView(ctx context.Context, owner *ent.Owner, newView *ent.KaskadaView, dependencies []*ent.ViewDependency) (*ent.KaskadaView, error)
	DeleteKaskadaView(ctx context.Context, owner *ent.Owner, view *ent.KaskadaView) error
	GetAllKaskadaViews(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaView, error)
	GetKaskadaView(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaView, error)
	GetKaskadaViewByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaView, error)
	GetKaskadaViewsFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaView, error)
	GetKaskadaViewsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.KaskadaView, error)
	ListKaskadaViews(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaView, error)
}

// KaskadaViewClientProvider creates ViewClients
type KaskadaViewClientProvider func(entClient *ent.Client) KaskadaViewClient

type MaterializationClient interface {
	CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error)
	DeleteMaterialization(ctx context.Context, owner *ent.Owner, view *ent.Materialization) error
	GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error)
	GetAllMaterializationsBySourceType(ctx context.Context, sourceType materialization.SourceType) ([]*ent.Materialization, error)
	GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error)
	GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error)
	GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error)
	GetMaterializationsBySourceType(ctx context.Context, owner *ent.Owner, sourceType materialization.SourceType) ([]*ent.Materialization, error)
	ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error)
	UpdateDataVersion(ctx context.Context, materialization *ent.Materialization, newDataVersion int64) (*ent.Materialization, error)
	IncrementVersion(ctx context.Context, materialization *ent.Materialization) (*ent.Materialization, error)
}

// MaterializationClientProvider creates MaterializationClients
type MaterializationClientProvider func(entClient *ent.Client) MaterializationClient

type PrepareJobClient interface {
	// creates a new prepareJob, with the passed params
	CreatePrepareJob(ctx context.Context, kaskadaFiles []*ent.KaskadaFile, sliceInfo *SliceInfo, prepareCacheBuster int32, state property.PrepareJobState) (*ent.PrepareJob, error)

	// gets an existing prepareJob. includes the following edges: KaskdaTable, KaskadaTable.Owner, KaskadaFiles, PreparedFiles
	GetPrepareJob(ctx context.Context, id uuid.UUID) (*ent.PrepareJob, error)

	// lists existing prepareJobs that match the params.  all params required execpt `additionalFilters`
	// results include the following edges for each prepareJob: KaskdaTable, KaskadaTable.Owner, KaskadaFiles, PreparedFiles
	ListPrepareJobs(ctx context.Context, kaskadaFiles []*ent.KaskadaFile, sliceInfo *SliceInfo, prepareCacheBuster int32, additonalFilters ...predicate.PrepareJob) ([]*ent.PrepareJob, error)

	// updates the prepareJob state.  if the state is the same, still updates the `update` time.
	UpdatePrepareJobState(ctx context.Context, prepareJob *ent.PrepareJob, newState property.PrepareJobState) error

	// creates preparedFiles and adds them to the prepareJob
	AddFilesToPrepareJob(ctx context.Context, prepareJob *ent.PrepareJob, preparedFilesToCreateAndAttach []*v1alpha.PreparedFile, relatedKaskadaFile *ent.KaskadaFile) error
}

// PrepareJobClientProvider creates PrepareJobClientClients
type PrepareJobClientProvider func(entClient *ent.Client) PrepareJobClient

type KaskadaQueryClient interface {
	CreateKaskadaQuery(ctx context.Context, owner *ent.Owner, newQuery *ent.KaskadaQuery, isV2 bool) (*ent.KaskadaQuery, error)
	DeleteKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) error
	GetAllKaskadaQueries(ctx context.Context, owner *ent.Owner, isV2 bool) ([]*ent.KaskadaQuery, error)
	GetKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) (*ent.KaskadaQuery, error)
	ListKaskadaQueries(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int, isV2 bool) ([]*ent.KaskadaQuery, error)
}

// KaskadaQueryClientProvider creates QueryClients
type KaskadaQueryClientProvider func(entClient *ent.Client) KaskadaQueryClient

type FileInput interface {
	GetURI() string
	GetType() kaskadafile.Type
	GetExtension() string
}
