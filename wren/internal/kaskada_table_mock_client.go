package internal

import (
	"context"
	"time"

	"github.com/google/uuid"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
)

type KaskadaTableMockClientConfig struct {
	CreateKaskadaTableResponse           func() (*ent.KaskadaTable, error)
	DeleteKaskadaTableResponse           func() (*ent.DataToken, error)
	GetAllKaskadaTablesResponse          func() ([]*ent.KaskadaTable, error)
	ListKaskadaTablesResponse            func() ([]*ent.KaskadaTable, error)
	GetKaskadaTableResponse              func() (*ent.KaskadaTable, error)
	GetKaskadaTableByNameResponse        func() (*ent.KaskadaTable, error)
	GetKaskadaTablesFromNamesResponse    func() (map[string]*ent.KaskadaTable, error)
	GetKaskadaFilesResponse              func() ([]*ent.KaskadaFile, error)
	AddFilesToTableResponse              func() (*ent.DataToken, error)
	SaveComputeSnapshotResponse          func() error
	GetBestComputeSnapshotResponse       func() (*ent.ComputeSnapshot, error)
	GetMinTimeOfNewPreparedFilesResponse func() (*int64, error)
	GetKaskadaTableVersionResponse       func() (*ent.DataVersion, error)
}

// GetKaskadaTableMockClientConfig returns a default KaskadaTableMockClientConfig with all methods returning nil
func GetKaskadaTableMockClientConfig() KaskadaTableMockClientConfig {
	return KaskadaTableMockClientConfig{
		CreateKaskadaTableResponse: func() (*ent.KaskadaTable, error) {
			return nil, nil
		},
		DeleteKaskadaTableResponse: func() (*ent.DataToken, error) {
			return nil, nil
		},
		GetAllKaskadaTablesResponse: func() ([]*ent.KaskadaTable, error) {
			return nil, nil
		},
		ListKaskadaTablesResponse: func() ([]*ent.KaskadaTable, error) {
			return nil, nil
		},
		GetKaskadaTableResponse: func() (*ent.KaskadaTable, error) {
			return nil, nil
		},
		GetKaskadaTableByNameResponse: func() (*ent.KaskadaTable, error) {
			return nil, nil
		},
		GetKaskadaTablesFromNamesResponse: func() (map[string]*ent.KaskadaTable, error) {
			return nil, nil
		},
		GetKaskadaFilesResponse: func() ([]*ent.KaskadaFile, error) {
			return nil, nil
		},
		AddFilesToTableResponse: func() (*ent.DataToken, error) {
			return nil, nil
		},
		SaveComputeSnapshotResponse: func() error {
			return nil
		},
		GetBestComputeSnapshotResponse: func() (*ent.ComputeSnapshot, error) {
			return nil, nil
		},
		GetMinTimeOfNewPreparedFilesResponse: func() (*int64, error) {
			return nil, nil
		},
		GetKaskadaTableVersionResponse: func() (*ent.DataVersion, error) {
			return nil, nil
		},
	}
}

type kaskadaTableMockClient struct {
	config KaskadaTableMockClientConfig
}

// NewKaskadaTableMockClient creates a new KaskadaTableClient for testing purposes
func NewKaskadaTableMockClient(config KaskadaTableMockClientConfig) KaskadaTableClient {
	return &kaskadaTableMockClient{
		config: config,
	}
}

func (c *kaskadaTableMockClient) CreateKaskadaTable(ctx context.Context, owner *ent.Owner, newTable *ent.KaskadaTable) (*ent.KaskadaTable, error) {
	return c.config.CreateKaskadaTableResponse()
}

func (c *kaskadaTableMockClient) DeleteKaskadaTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable) (*ent.DataToken, error) {
	return c.config.DeleteKaskadaTableResponse()
}

func (c *kaskadaTableMockClient) GetAllKaskadaTables(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaTable, error) {
	return c.config.GetAllKaskadaTablesResponse()
}

func (c *kaskadaTableMockClient) ListKaskadaTables(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaTable, error) {
	return c.config.ListKaskadaTablesResponse()
}

func (c *kaskadaTableMockClient) GetKaskadaTable(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaTable, error) {
	return c.config.GetKaskadaTableResponse()
}

func (c *kaskadaTableMockClient) GetKaskadaTableByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaTable, error) {
	return c.config.GetKaskadaTableByNameResponse()
}

func (c *kaskadaTableMockClient) GetKaskadaTablesFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaTable, error) {
	return c.config.GetKaskadaTablesFromNamesResponse()
}

func (c *kaskadaTableMockClient) GetKaskadaFiles(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, dataToken *ent.DataToken) ([]*ent.KaskadaFile, error) {
	return c.config.GetKaskadaFilesResponse()
}

func (c *kaskadaTableMockClient) AddFilesToTable(ctx context.Context, owner *ent.Owner, kaskadaTable *ent.KaskadaTable, newFiles []AddFileProps, newMergedSchema *v1alpha.Schema, newExternalRevision *string, cleanupOnError func() error) (*ent.DataToken, error) {
	return c.config.AddFilesToTableResponse()
}

func (c *kaskadaTableMockClient) SaveComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, dataToken *ent.DataToken, path string, maxEventTime time.Time, relatedTableIDs []uuid.UUID) error {
	return c.config.SaveComputeSnapshotResponse()
}

func (c *kaskadaTableMockClient) GetBestComputeSnapshot(ctx context.Context, owner *ent.Owner, complilePlanHash []byte, snapshotCacheBuster int32, slices []*SliceInfo, prepareCacheBuster int32) (*ent.ComputeSnapshot, error) {
	return c.config.GetBestComputeSnapshotResponse()
}

func (c *kaskadaTableMockClient) GetMinTimeOfNewPreparedFiles(ctx context.Context, prepareCacheBuster int32, sliceInfo *SliceInfo, dataVersion int64) (*int64, error) {
	return c.config.GetMinTimeOfNewPreparedFilesResponse()
}

func (c *kaskadaTableMockClient) GetKaskadaTableVersion(ctx context.Context, kaskadaTable *ent.KaskadaTable) (*ent.DataVersion, error) {
	return c.config.GetKaskadaTableVersionResponse()
}
