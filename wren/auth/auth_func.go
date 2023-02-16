package auth

import (
	"context"
	"errors"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kaskada/kaskada-ai/wren/internal"
)

const (
	clientIDHeader = "client-id"
)

// ClientIDHeaderAuthFunc returns the authorization function that reads the client id from the metadata.
// Authorized Flow (Overrides)
// 1. Default Client ID provided at Server runtime
// 2. Client provides Client-ID
func ClientIDHeaderAuthFunc(defaultClientID string, ownerClient internal.OwnerClient) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		subLogger := log.Ctx(ctx).With().Str("method", "auth function").Logger()

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			subLogger.Warn().Msg("malformed metadata")
			return nil, status.Errorf(codes.Unknown, "malformed metadata")
		}

		subLogger.Debug().Interface("metadata", md).Send()

		// Check to see if the request was from the public facing endpoint
		clientIDMetadata := md.Get(clientIDHeader)
		// No client ID is provided in the header
		if len(clientIDMetadata) == 0 {
			// Case 1 - No server client ID and no request client ID are unauthorized
			if len(defaultClientID) == 0 {
				return nil, status.Error(codes.Unauthenticated, "unauthorized")
			}
			// Case 2 - No request client ID but default to server client ID
			apiClient, err := createAPIClient(ctx, ownerClient, defaultClientID)
			if err != nil {
				return nil, status.Error(codes.Unauthenticated, err.Error())
			}
			return AddAPIClientToContext(ctx, apiClient), nil
		}
		// Client ID is provided in the header
		reqClientID := clientIDMetadata[0]
		apiClient, err := createAPIClient(ctx, ownerClient, reqClientID)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		return AddAPIClientToContext(ctx, apiClient), nil
	}
}

func createAPIClient(ctx context.Context, ownerClient internal.OwnerClient, clientID string) (APIClient, error) {
	owner, err := ownerClient.GetOwnerFromClientID(ctx, clientID)
	if err != nil {
		return nil, errors.New("invalid client-id")
	}
	apiClient := NewAPIClient(owner)
	return apiClient, nil
}
