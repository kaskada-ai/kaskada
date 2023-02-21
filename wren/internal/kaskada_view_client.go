package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadaview"
	"github.com/kaskada-ai/kaskada/wren/ent/materializationdependency"
	"github.com/kaskada-ai/kaskada/wren/ent/predicate"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/ent/viewdependency"
)

type kaskadaViewClient struct {
	entClient *ent.Client
}

// NewKaskadaViewClient creates a new kaskadaViewClient from an ent client
func NewKaskadaViewClient(entClient *ent.Client) KaskadaViewClient {
	return &kaskadaViewClient{
		entClient: entClient,
	}
}

func (c *kaskadaViewClient) CreateKaskadaView(ctx context.Context, owner *ent.Owner, newView *ent.KaskadaView, dependencies []*ent.ViewDependency) (*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.CreateKaskadaView").
		Interface("new_view", newView).
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

	kaskadaView, err := tx.KaskadaView.Create().
		SetOwner(owner).
		SetName(newView.Name).
		SetDescription(newView.Description).
		SetExpression(newView.Expression).
		SetDataType(newView.DataType).
		SetAnalysis(newView.Analysis).
		Save(ctx)

	if err != nil {
		if violatesUniqueConstraint(err) {
			return nil, rollbackCleanup(tx, customerrors.NewAlreadyExistsError("view"), "")
		}
		return nil, rollbackCleanup(tx, err, "issue creating view")
	}

	for _, dependency := range dependencies {
		_, err := tx.ViewDependency.Create().
			SetDependencyType(dependency.DependencyType).
			SetDependencyName(dependency.DependencyName).
			SetDependencyID(*dependency.DependencyID).
			SetKaskadaView(kaskadaView).
			Save(ctx)
		if err != nil {
			return nil, rollbackCleanup(tx, err, "issue creating view dependency")
		}
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return nil, err
	}

	return kaskadaView.Unwrap(), nil
}

func (c *kaskadaViewClient) DeleteKaskadaView(ctx context.Context, owner *ent.Owner, kaskadaView *ent.KaskadaView) error {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.DeleteKaskadaView").
		Str("kaskadaView_name", kaskadaView.Name).
		Logger()

	viewID := kaskadaView.ID

	// start a transaction
	tx, err := c.entClient.Tx(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue starting transaction")
		return err
	}

	rollbackCleanup := func(tx *ent.Tx, err error, reason string) error {
		rollbackErr := rollback(tx, err)
		if reason != "" {
			subLogger.Error().Err(rollbackErr).Msg(reason)
		}
		return rollbackErr
	}

	err = tx.KaskadaView.DeleteOneID(viewID).Exec(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return rollbackCleanup(tx, customerrors.NewNotFoundError("view"), "")
		}
		return rollbackCleanup(tx, err, "issue deleting view")
	}

	// update dependency links to nil where this view is used by other views
	_, err = tx.ViewDependency.Update().
		Where(
			viewdependency.DependencyTypeEQ(schema.DependencyType_View),
			viewdependency.ID(viewID),
		).
		ClearDependencyID().
		Save(ctx)
	if err != nil {
		return rollbackCleanup(tx, err, "issue clearing view dependencies for other views")
	}

	// update dependency links to nil where this view is used by materializations
	_, err = tx.MaterializationDependency.Update().
		Where(
			materializationdependency.DependencyTypeEQ(schema.DependencyType_View),
			materializationdependency.ID(viewID),
		).
		ClearDependencyID().
		Save(ctx)
	if err != nil {
		return rollbackCleanup(tx, err, "issue clearing view dependencies for materializations")
	}

	err = tx.Commit()
	if err != nil {
		subLogger.Error().Err(err).Msg("issue committing transaction")
		return err
	}

	return nil
}

func (c *kaskadaViewClient) GetAllKaskadaViews(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.GetAllKaskadaViews").
		Logger()

	kaskadaViews, err := owner.QueryKaskadaViews().All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting views")
		return nil, err
	}
	return kaskadaViews, nil
}

func (c *kaskadaViewClient) GetKaskadaView(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.GetKaskadaView").
		Str("kaskadaView_id", id.String()).
		Logger()

	kaskadaView, err := owner.QueryKaskadaViews().Where(kaskadaview.ID(id)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("view")
		}
		subLogger.Error().Err(err).Msg("issue getting view")
		return nil, err
	}
	return kaskadaView, nil
}

func (c *kaskadaViewClient) GetKaskadaViewByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.GetKaskadaViewByName").
		Str("kaskadaView_name", name).
		Logger()

	kaskadaView, err := owner.QueryKaskadaViews().Where(kaskadaview.Name(name)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("view")
		}
		subLogger.Error().Err(err).Msg("issue getting view")
		return nil, err
	}
	return kaskadaView, nil
}

func (c *kaskadaViewClient) GetKaskadaViewsFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.GetKaskadaViewsFromNames").
		Logger()

	predicates := make([]predicate.KaskadaView, 0, len(names))

	for _, name := range names {
		predicates = append(predicates, kaskadaview.Name(name))
	}

	kaskadaViews, err := owner.QueryKaskadaViews().Where(kaskadaview.Or(predicates...)).All(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting views")
		return nil, err
	}

	viewMap := map[string]*ent.KaskadaView{}

	for _, kaskadaView := range kaskadaViews {
		viewMap[kaskadaView.Name] = kaskadaView
	}

	return viewMap, nil
}

func (c *kaskadaViewClient) GetKaskadaViewsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.GetKaskadaViewsWithDependency").
		Str("name", name).
		Str("type", string(dependencyType)).
		Logger()

	kaskadaViews, err := owner.QueryKaskadaViews().
		Where(
			kaskadaview.HasDependenciesWith(
				viewdependency.And(
					viewdependency.DependencyName(name),
					viewdependency.DependencyTypeEQ(dependencyType),
				),
			),
		).
		All(ctx)

	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing views")
		return nil, err
	}
	return kaskadaViews, nil
}

func (c *kaskadaViewClient) ListKaskadaViews(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaView, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "kaskadaViewClient.ListKaskadaViews").
		Logger()

	kaskadaViews, err := owner.QueryKaskadaViews().
		Where(
			kaskadaview.Or(
				kaskadaview.NameContainsFold(searchTerm),
				kaskadaview.DescriptionContainsFold(searchTerm),
				kaskadaview.ExpressionContainsFold(searchTerm),
			),
		).
		Limit(pageSize).
		Offset(offset).
		Order(ent.Asc(kaskadaview.FieldName)).
		All(ctx)

	if err != nil {
		subLogger.Error().Err(err).Msg("issue listing views")
		return nil, err
	}
	return kaskadaViews, nil
}
