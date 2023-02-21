package internal

import (
	"context"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/datatoken"
	"github.com/kaskada-ai/kaskada/wren/ent/dataversion"
)

type dataTokenClient struct {
	entClient *ent.Client
}

// NewDataTokenClient creates a new DataTokenClient from an ent client
func NewDataTokenClient(entClient *ent.Client) DataTokenClient {
	return &dataTokenClient{
		entClient: entClient,
	}
}

func (d *dataTokenClient) GetDataToken(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "dataTokenClient.GetDataToken").
		Str("data_token_id", id.String()).
		Logger()

	dataToken, err := owner.QueryDataTokens().Where(datatoken.ID(id)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("data_token")
		}
		subLogger.Error().Err(err).Msg("issue getting data_token")
		return nil, err
	}
	return dataToken, nil
}

func (d *dataTokenClient) GetCurrentDataToken(ctx context.Context, owner *ent.Owner) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "dataTokenClient.GetCurrentDataToken").
		Logger()

	dataToken, err := owner.QueryDataTokens().Order(ent.Desc(datatoken.FieldDataVersionID)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("data_token")
		}
		subLogger.Error().Err(err).Msg("issue getting current data_token")
		return nil, err
	}
	return dataToken, nil
}

// at a specific dataToken, returns a map of tableIDs to their most recent dataVersion
func (d *dataTokenClient) GetTableVersions(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken) (map[uuid.UUID]*ent.DataVersion, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "dataTokenClient.GetDataVersionsForExistingTables").
		Str("data_token_id", dataToken.ID.String()).
		Logger()

	dataVersions, err := d.entClient.DataVersion.Query().
		Modify(func(s *sql.Selector) {
			table_versions := sql.Select(sql.As(sql.Max(s.C(dataversion.FieldID)), "id"), sql.As(s.C(dataversion.KaskadaTableColumn), "table_id")).
				From(sql.Table(dataversion.Table)).
				Where(sql.And(
					sql.EQ(s.C(dataversion.OwnerColumn), owner.ID),
					sql.LTE(s.C(dataversion.FieldID), dataToken.DataVersionID),
					sql.NotNull(dataversion.KaskadaTableColumn),
				)).
				GroupBy("table_id").
				As("table_versions")

			s.Join(table_versions).On(s.C(dataversion.FieldID), table_versions.C("id"))
		}).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting table versions")
		return nil, err
	}

	tableVersionMap := map[uuid.UUID]*ent.DataVersion{}

	for _, dataVersion := range dataVersions {
		tableID, err := dataVersion.QueryKaskadaTable().FirstID(ctx)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting table id from data_version")
			return nil, err
		}
		tableVersionMap[tableID] = dataVersion
	}

	subLogger.Debug().Interface("table_version_map", tableVersionMap).Msg("did tricky table version query")
	return tableVersionMap, nil
}
