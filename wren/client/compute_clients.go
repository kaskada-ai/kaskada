package client

import (
	"context"

	"github.com/rs/zerolog/log"
)

type ComputeClients interface {
	NewFileServiceClient(ctx context.Context) FileServiceClient
	NewPrepareServiceClient(ctx context.Context) PrepareServiceClient
	NewComputeServiceClient(ctx context.Context) ComputeServiceClient
}

// ComputeClients is the container to hold client for communicating with compute services
type computeClients struct {
	fileServiceConfig    *HostConfig
	prepareServiceConfig *HostConfig
	computeServiceConfig *HostConfig
}

// CreateComputeClients initializes the computeClients
func CreateComputeClients(fileServiceConfig *HostConfig, prepareServiceConfig *HostConfig, computeServiceConfig *HostConfig) ComputeClients {
	return &computeClients{
		fileServiceConfig:    fileServiceConfig,
		prepareServiceConfig: prepareServiceConfig,
		computeServiceConfig: computeServiceConfig,
	}
}

// FileServiceClient creates a new FileServiceClient from the configuration and context
func (c *computeClients) NewFileServiceClient(ctx context.Context) FileServiceClient {
	conn, err := connection(ctx, c.fileServiceConfig)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Interface("host_config", c.fileServiceConfig).Msg("unable to dial FileServiceClient")
	}
	if conn == nil {
		return nil
	}
	return NewFileServiceClient(conn)
}

// PrepareServiceClient creates a new PrepareServiceClient from the configuration and context
func (c *computeClients) NewPrepareServiceClient(ctx context.Context) PrepareServiceClient {
	conn, err := connection(ctx, c.prepareServiceConfig)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Interface("host_config", c.prepareServiceConfig).Msg("unable to dial PrepareServiceClient")
	}
	if conn == nil {
		return nil
	}
	return NewPrepareServiceClient(conn)
}

// ComputeServiceClient creates a new ComputeServiceClient from the configuration and context
func (c *computeClients) NewComputeServiceClient(ctx context.Context) ComputeServiceClient {
	conn, err := connection(ctx, c.computeServiceConfig)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Interface("host_config", c.computeServiceConfig).Msg("unable to dial ComputeServiceClient")
	}
	if conn == nil {
		return nil
	}
	return NewComputeServiceClient(conn)
}
