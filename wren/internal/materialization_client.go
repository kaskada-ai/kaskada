package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/ent/materializationdependency"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
)

type materializationClient struct {
	entClient *ent.Client
}

// NewMaterializationClient creates a new materializationClient from an ent client
func NewMaterializationClient(entClient *ent.Client) MaterializationClient {
	return &materializationClient{
		entClient: entClient,
	}
}

func (c *materializationClient) CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.CreateMaterialization").
		Interface("new_materialization", newMaterialization).
		Logger()

	tx, err := c.entClient.Tx(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting transaction")
		return nil, err
	}

	rollbackCleanup := func(tx *ent.Tx, err error, reason string) error {
		rollbackErr := rollback(tx, err)
		if reason != "" {
			subLogger.Error().Err(rollbackErr).Msg(reason)
		}
		return rollbackErr
	}

	materialization, err := tx.Materialization.Create().
		SetOwner(owner).
		SetName(newMaterialization.Name).
		SetDescription(newMaterialization.Description).
		SetExpression(newMaterialization.Expression).
		SetWithViews(newMaterialization.WithViews).
		SetDestination(newMaterialization.Destination).
		SetSchema(newMaterialization.Schema).
		SetSliceRequest(newMaterialization.SliceRequest).
		SetAnalysis(newMaterialization.Analysis).
		SetDataVersionID(newMaterialization.DataVersionID).
		SetVersion(newMaterialization.Version).
		SetSourceType(newMaterialization.SourceType).
		Save(ctx)

	if err != nil {
		if violatesUniqueConstraint(err) {
			return nil, rollbackCleanup(tx, customerrors.NewAlreadyExistsError("materialization"), "")
		}
		return nil, rollbackCleanup(tx, err, "issue creating materialization")
	}

	for _, dependency := range dependencies {
		_, err := tx.MaterializationDependency.Create().
			SetDependencyType(dependency.DependencyType).
			SetDependencyName(dependency.DependencyName).
			SetDependencyID(*dependency.DependencyID).
			SetMaterialization(materialization).
			Save(ctx)
		if err != nil {
			return nil, rollbackCleanup(tx, err, "issue creating materialization dependency")
		}
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return nil, err
	}

	return materialization.Unwrap(), nil
}

func (c *materializationClient) DeleteMaterialization(ctx context.Context, owner *ent.Owner, materialization *ent.Materialization) error {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.DeleteMaterialization").
		Str("materialization_name", materialization.Name).
		Logger()

	materializationID := materialization.ID

	err := c.entClient.Materialization.DeleteOneID(materializationID).Exec(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return customerrors.NewNotFoundError("materialization")
		}
		subLogger.Error().Err(err).Msg("issue deleting materialization")
		return err
	}

	return nil
}

func (c *materializationClient) GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.GetAllMaterializations").
		Logger()

	materializations, err := owner.QueryMaterializations().All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting materializations")
		return nil, err
	}
	return materializations, nil
}

func (c *materializationClient) GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.GetMaterialization").
		Str("materialization_id", id.String()).
		Logger()

	materialization, err := owner.QueryMaterializations().Where(materialization.ID(id)).First(ctx)

	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("materialization")
		}
		subLogger.Error().Err(err).Msg("issue getting materialization")
		return nil, err
	}
	return materialization, nil
}

func (c *materializationClient) GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.GetMaterializationByName").
		Str("materialization_name", name).
		Logger()

	materialization, err := owner.QueryMaterializations().Where(materialization.Name(name)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("materialization")
		}
		subLogger.Error().Err(err).Msg("issue getting materialization")
		return nil, err
	}
	return materialization, nil
}

func (c *materializationClient) GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.GetMaterializationsWithDependency").
		Str("name", name).
		Str("type", string(dependencyType)).
		Logger()

	materializations, err := owner.QueryMaterializations().
		Where(
			materialization.HasDependenciesWith(
				materializationdependency.And(
					materializationdependency.DependencyName(name),
					materializationdependency.DependencyTypeEQ(dependencyType),
				),
			),
		).
		All(ctx)

	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing materializations")
		return nil, err
	}
	return materializations, nil
}

func (c *materializationClient) GetMaterializationsBySourceType(ctx context.Context, owner *ent.Owner, sourceType materialization.SourceType) ([]*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.GetMaterializationsBySourceType").
		Str("source_type", string(sourceType)).
		Logger()

	materializations, err := owner.QueryMaterializations().Where(materialization.SourceTypeEQ(sourceType)).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing materializations")
		return nil, err
	}
	return materializations, nil
}

func (c *materializationClient) ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "materializationClient.ListMaterializations").
		Logger()

	materializations, err := owner.QueryMaterializations().
		Where(
			materialization.Or(
				materialization.NameContainsFold(searchTerm),
				materialization.DescriptionContainsFold(searchTerm),
				materialization.ExpressionContainsFold(searchTerm),
			),
		).
		Limit(pageSize).
		Offset(offset).
		Order(ent.Asc(materialization.FieldName)).
		All(ctx)

	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing materializations")
		return nil, err
	}
	return materializations, nil
}

func (c *materializationClient) UpdateDataVersion(ctx context.Context, materialization *ent.Materialization, newDataVersion int64) (*ent.Materialization, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "materializationClient.UpdateDateVersion").Logger()

	updated_materialization, err := materialization.Update().SetDataVersionID(newDataVersion).SetVersion(materialization.Version + 1).Save(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue updating materialization data version id")
		return nil, err
	}

	return updated_materialization, nil
}

