package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kaskada/kaskada-ai/wren/auth"
	"github.com/kaskada/kaskada-ai/wren/customerrors"
	"github.com/kaskada/kaskada-ai/wren/ent"
	pb "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	"github.com/kaskada/kaskada-ai/wren/internal"
)

type dataTokenService struct {
	pb.UnimplementedDataTokenServiceServer
	client internal.DataTokenClient
}

// NewDataTokenService creates a new user service
func NewDataTokenService(client *internal.DataTokenClient) pb.DataTokenServiceServer {
	return &dataTokenService{
		client: *client,
	}
}

func (d *dataTokenService) GetDataToken(ctx context.Context, req *pb.GetDataTokenRequest) (*pb.GetDataTokenResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "dataTokenService.GetDataToken").Logger()

	owner := auth.APIOwnerFromContext(ctx)

	var (
		id        uuid.UUID
		dataToken *ent.DataToken
		err       error
	)

	if req.DataTokenId == "current_data_token" {
		dataToken, err = d.client.GetCurrentDataToken(ctx, owner)
	} else {
		id, err = uuid.Parse(req.DataTokenId)
		if err != nil {
			subLogger.Debug().Err(err).Msg("unable to parse data_token_id")
			return nil, customerrors.NewInvalidArgumentError("data_token_id")
		}
		dataToken, err = d.client.GetDataToken(ctx, owner, id)
	}
	if err != nil {
		return nil, wrapErrorWithStatus(errors.Wrapf(err, "getting data_token: %s", req.DataTokenId), subLogger)
	}

	tableVersionMap, err := d.client.GetTableVersions(ctx, owner, dataToken)
	if err != nil {
		return nil, wrapErrorWithStatus(errors.Wrapf(err, "getting data_version for data_token: %s", req.DataTokenId), subLogger)
	}

	outputMap := map[string]int64{}

	for tableID, dataVersion := range tableVersionMap {
		outputMap[tableID.String()] = dataVersion.ID
	}

	return &pb.GetDataTokenResponse{
		DataToken: &pb.DataToken{
			CreateTime:    timestamppb.New(dataToken.CreatedAt),
			DataTokenId:   dataToken.ID.String(),
			TableVersions: outputMap,
		},
	}, nil
}
