package client

import (
	"context"
	"crypto/tls"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// HostConfig holds the data needed to connect to a particular grpc server
type HostConfig struct {
	Host   string
	Port   int
	UseTLS bool
}

func connection(ctx context.Context, hostConfig *HostConfig) (*grpc.ClientConn, error) {
	if hostConfig == nil {
		return nil, nil
	}

	// default to disabling TLS for running in k8s
	tlsOpt := grpc.WithInsecure()

	// Override with TLS (usually for running locally against k8s)
	if hostConfig.UseTLS {
		tlsOpt = grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: false,
			}))
	}

	// get a connection
	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("%s:%d", hostConfig.Host, hostConfig.Port),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			grpc_prometheus.UnaryClientInterceptor,
			otelgrpc.UnaryClientInterceptor(),
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			grpc_prometheus.StreamClientInterceptor,
			otelgrpc.StreamClientInterceptor(),
		)),
		tlsOpt,
	)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
