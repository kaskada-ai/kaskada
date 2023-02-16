package store

import (
	"fmt"
	"path"

	"github.com/google/uuid"

	"github.com/kaskada/kaskada-ai/wren/client"
	"github.com/kaskada/kaskada-ai/wren/ent"
)

// TableStore is a wrapper around the store
type TableStore struct {
	objectStoreClient client.ObjectStoreClient
}

// NewTableStore returns a new table store
func NewTableStore(objectStoreClient *client.ObjectStoreClient) *TableStore {
	return &TableStore{
		objectStoreClient: *objectStoreClient,
	}
}

// returns tables/<owner_id>/table-id
func (s *TableStore) GetTableSubPath(owner *ent.Owner, kaskadaTable *ent.KaskadaTable) string {
	return path.Join("tables", owner.ID.String(), kaskadaTable.ID.String())
}

// returns tables/<owner_id>/table-id/data/file-id.file-type
func (s *TableStore) GetFileSubPath(owner *ent.Owner, kaskadaTable *ent.KaskadaTable, fileType string) string {
	return path.Join(s.GetTableSubPath(owner, kaskadaTable), fmt.Sprintf("data/%s.%s", uuid.NewString(), fileType))
}

// returns the tables/<owner_id>/table-id/prepared/prepare-version/hash(slice_plan)/file_id/
func (s *TableStore) GetPrepareOutputURI(owner *ent.Owner, kaskadaTable *ent.KaskadaTable, KaskadaFile *ent.KaskadaFile, prepareVersion int32, slicePlanHash []byte) string {
	subPath := path.Join(s.GetTableSubPath(owner, kaskadaTable), fmt.Sprintf("prepared/prep_%d/%x/%s", prepareVersion, slicePlanHash, KaskadaFile.ID.String()))
	return s.objectStoreClient.GetDataPathURI(subPath)
}
