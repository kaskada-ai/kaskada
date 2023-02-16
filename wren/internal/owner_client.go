package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/kaskada/kaskada-ai/wren/customerrors"
	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/owner"
)

type ownerClient struct {
	entClient *ent.Client
}

// NewOwnerClient creates a new OwnerClient from an ent client
func NewOwnerClient(entClient *ent.Client) OwnerClient {
	return &ownerClient{
		entClient: entClient,
	}
}

func (d *ownerClient) GetOwner(ctx context.Context, id uuid.UUID) (*ent.Owner, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "ownerClient.GetOwner").
		Str("owner_id", id.String()).
		Logger()

	owner, err := d.entClient.Owner.Get(ctx, id)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, customerrors.NewNotFoundError("owner")
		}
		subLogger.Error().Err(err).Msg("issue getting owner")
		return nil, err
	}
	return owner, nil
}

func (d *ownerClient) GetOwnerFromClientID(ctx context.Context, clientID string) (*ent.Owner, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "ownerClient.GetOwnerFromClientID").
		Str("client_ID", clientID).
		Logger()

	owner, err := d.entClient.Owner.Query().Where(owner.ClientID(clientID)).First(ctx)
	if err == nil {
		return owner, nil
	} else if !ent.IsNotFound(err) {
		subLogger.Error().Err(err).Msg("issue getting owner")
		return nil, err
	}
	owner, err = d.entClient.Owner.Create().SetClientID(clientID).Save(ctx)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue creating owner")
		return nil, err
	}
	return owner, nil
}
