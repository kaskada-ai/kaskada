package telemetry

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

// MetricsProvider sets up everything needed to serve metrics for prometheus
type MetricsProvider interface {
	RegisterGrpc(server *grpc.Server)
}

type metricsProvider struct{}

// NewMetricsProvider initializes logging for wren
func NewMetricsProvider() MetricsProvider {
	mp := &metricsProvider{}
	return mp
}

// Initializes grpc metrics
func (mp *metricsProvider) RegisterGrpc(server *grpc.Server) {
	grpc_prometheus.Register(server)
}
