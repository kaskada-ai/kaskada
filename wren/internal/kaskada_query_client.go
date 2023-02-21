package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadaquery"
	"github.com/kaskada-ai/kaskada/wren/ent/predicate"
)

func NewKaskadaQueryClient(entClient *ent.Client) KaskadaQueryClient {
	return &kaskadaQueryClient{
		entClient: entClient,
	}
}

type kaskadaQueryClient struct {
	entClient *ent.Client
}

// CreateKaskadaQuery implements KaskadaQueryClient
func (k *kaskadaQueryClient) CreateKaskadaQuery(ctx context.Context, owner *ent.Owner, newQuery *ent.KaskadaQuery, isV2 bool) (*ent.KaskadaQuery, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaQueryClient.CreateKaskadaQuery").
		Interface("new_query", newQuery).
		Logger()

	var createQuery *ent.KaskadaQueryCreate
	if isV2 {
		createQuery = k.entClient.KaskadaQuery.Create().
			SetCompileResponse(newQuery.CompileResponse).
			SetConfig(newQuery.Config).
			SetDataTokenID(newQuery.DataTokenID).
			SetExpression(newQuery.Expression).
			SetMetrics(newQuery.Metrics).
			SetOwner(owner).
			SetState(newQuery.State).
			SetViews(newQuery.Views)
	} else {
		queryId := uuid.New()
		newQuery.Query.QueryId = queryId.String()
		newQuery.Query.CreateTime = timestamppb.Now()
		createQuery = k.entClient.KaskadaQuery.Create().
			SetID(queryId).
			SetOwner(owner).
			SetExpression(newQuery.Expression).
			SetQuery(newQuery.Query).
			SetDataTokenID(newQuery.DataTokenID)
	}

	kaskadaQuery, err := createQuery.Save(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue creating kaskada_query")
		return nil, err
	}
	return kaskadaQuery, nil
}

// DeleteKaskadaQuery implements KaskadaQueryClient
func (k *kaskadaQueryClient) DeleteKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) error {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaQueryClient.DeleteKaskadaQuery").
		Str("query_id", id.String()).
		Logger()

	// make sure owner owns query
	_, err := k.GetKaskadaQuery(ctx, owner, id, isV2)
	if err != nil {
		return err
	}

	err = k.entClient.KaskadaQuery.DeleteOneID(id).Exec(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return customerrors.NewNotFoundError("query")
		}
		subLogger.Error().Err(err).Msg("issue deleteting kaskada_query")
		return err
	}

	return nil
}

// GetAllKaskadaQueries implements KaskadaQueryClient
func (k *kaskadaQueryClient) GetAllKaskadaQueries(ctx context.Context, owner *ent.Owner, isV2 bool) ([]*ent.KaskadaQuery, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaQueryClient.GetAllKaskadaQueries").
		Logger()

	kaskadaQueries, err := owner.QueryKaskadaQueries().Where(k.getVersionPredicate(isV2)).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting kaskada_queries")
		return nil, err
	}
	return kaskadaQueries, nil
}

// GetKaskadaQuery implements KaskadaQueryClient
func (k *kaskadaQueryClient) GetKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) (*ent.KaskadaQuery, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaQueryClient.GetKaskadaQuery").
		Str("query_id", id.String()).
		Logger()
	kaskadaQuery, err := owner.QueryKaskadaQueries().Where(kaskadaquery.ID(id), k.getVersionPredicate(isV2)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("query")
		}
		subLogger.Error().Err(err).Msg("issue getting kaskada_query")
		return nil, err
	}
	return kaskadaQuery, nil
}

// ListKaskadaQueries implements KaskadaQueryClient
func (k *kaskadaQueryClient) ListKaskadaQueries(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int, isV2 bool) ([]*ent.KaskadaQuery, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaQueryClient.ListKaskadaQueries").
		Logger()

	tables, err := owner.QueryKaskadaQueries().
		Where(
			kaskadaquery.ExpressionContains(searchTerm),
			k.getVersionPredicate(isV2),
		).
		Limit(pageSize).
		Offset(offset).
		Order(ent.Desc(kaskadaquery.FieldCreatedAt)).
		All(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("query")
		}
		subLogger.Error().Err(err).Msg("issue listing kaskada_queries")
		return nil, err
	}
	return tables, nil
}

func (k *kaskadaQueryClient) getVersionPredicate(isV2 bool) predicate.KaskadaQuery {
	if isV2 {
		return kaskadaquery.QueryIsNil()
	} else {
		return kaskadaquery.Not(kaskadaquery.QueryIsNil())
	}
}
