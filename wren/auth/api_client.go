// based on egret/auth/client.go
package auth

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/kaskada-ai/kaskada/wren/ent"
)

type contextKeyAPIClient struct{}

// APIClient provides access to info about the client
type APIClient interface {
	GetOwner() *ent.Owner
}

type apiClient struct {
	owner *ent.Owner
}

// NewAPIClient constructs a API Client
func NewAPIClient(owner *ent.Owner) APIClient {
	return &apiClient{
		owner: owner,
	}
}

// GetOwner returns the Owner
func (u *apiClient) GetOwner() *ent.Owner {
	return u.owner
}

// AddAPIClientToContext returns a context with the client attached
func AddAPIClientToContext(ctx context.Context, client APIClient) context.Context {
	// Add the client-id to the context logger
	contextLogger := log.Ctx(ctx).With().
		Str("owner_id", client.GetOwner().ID.String()).
		Logger()
	ctx = contextLogger.WithContext(ctx)
	// Finally add the full client object
	return context.WithValue(ctx, contextKeyAPIClient{}, client)
}

// APIClientFromContext returns the api client on context or an error if the client is missing
func APIClientFromContext(ctx context.Context) (APIClient, error) {
	client, ok := ctx.Value(contextKeyAPIClient{}).(APIClient)
	if !ok {
		return nil, fmt.Errorf("no api client on context")
	}

	return client, nil
}

// APIOwnerFromContext returns the api owner on context or panics if the client is missing
func APIOwnerFromContext(ctx context.Context) *ent.Owner {
	client, err := APIClientFromContext(ctx)
	if err != nil {
		panic("no api client on context, this should never happen")
	}
	return client.GetOwner()
}

// Returns a new background context with the api client set from the passed context.
// The returned context won't end when the passed context ends.
func NewBackgroundContextWithAPIClient(inCtx context.Context) (context.Context, context.CancelFunc, error) {
	client, err := APIClientFromContext(inCtx)
	if err != nil {
		return nil, nil, err
	}
	newCtx, cancel := context.WithCancel(context.Background())

	outCtx := AddAPIClientToContext(newCtx, client)

	return outCtx, cancel, nil
}
