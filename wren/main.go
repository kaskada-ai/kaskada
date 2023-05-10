package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kouhin/envflag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/service"
	"github.com/kaskada-ai/kaskada/wren/store"
	"github.com/kaskada-ai/kaskada/wren/telemetry"
	"github.com/kaskada-ai/kaskada/wren/utils"
)

var (
	// db configuration
	dbDialect  = flag.String("db-dialect", "sqlite", "database dialect to use.  should be `postgres` or `sqlite` (default)")
	dbHost     = flag.String("db-host", "postgres", "database hostname (only for postgres)")
	dbInMemory = flag.Bool("db-in-memory", true, "use an in-memory database (only for sqlite)")
	dbName     = flag.String("db-name", "wren", "database name (only for postgres)")
	dbPath     = flag.String("db-path", "./data/wren.db", "the path database file, when `db-in-memory` is false (only for sqlite)")
	dbPass     = flag.String("db-pass", "wren123", "database password")
	dbPort     = flag.Int("db-port", 5432, "database port (only for postgres)")
	dbUseSSL   = flag.Bool("db-use-ssl", false, "the ssl mode to use when connection to the database (only for postgres)")
	dbUser     = flag.String("db-user", "wren", "database username")

	objectStoreType           = flag.String("object-store-type", "local", "the type of object store to use.  should be `local` (default), `s3`, `gcs`, or `azure`.")
	objectStoreBucket         = flag.String("object-store-bucket", "", "the bucket or container to use for storing objects. required if the `object-store-type` is not `local`.")
	objectStoreDisableSSL     = flag.Bool("object-store-disable-ssl", false, "set true to disable SSL when connecting to the object store")
	objectStoreEndpoint       = flag.String("object-store-endpoint", "", "the endpoint for accessing the object store.  will use the object store default if this is not defined.")
	objectStoreForcePathStyle = flag.Bool("object-store-force-path-style", false, "set to true to access the bucket as a path instead via a sub-domain. required when using minio as the object store.")
	objectStorePath           = flag.String("object-store-path", "~/.cache/kaskada/data", "the path or prefix for storing data objects. can be a relative path if the `object-store-type` is `local`.")

	debug                    = flag.Bool("debug", false, "sets log level to debug")
	debugGrpc                = flag.Bool("debug-grpc", false, "logs grpc connection debug info")
	defaultClientID          = flag.String("default-client-id", "default-client-id", "the default client-id to use when one isn't passed in the request")
	env                      = flag.String("env", "prod", "the environment the service is running in")
	grpcHealthPort           = flag.Int("grpc-health-port", 6666, "the port of the gRPC health listener")
	grpcPort                 = flag.Int("grpc-port", 50051, "the port of the gRPC listener")
	otelEndpoint             = flag.String("otel-exporter-otlp-endpoint", "", "the host and port of the grpc otel otlp exporter")
	fileServiceHost          = flag.String("file-service-host", "localhost", "the hostname of the file service")
	fileServicePort          = flag.Int("file-service-port", 50052, "the port of the file service")
	fileServiceUseTLS        = flag.Bool("file-service-use-tls", false, "should TLS be used to connect to the file service")
	logFormatJson            = flag.Bool("log-format-json", false, "if enabled, logs will be outputted in json")
	logHealthCheck           = flag.Bool("log-health-check", false, "if enabled, output logs for health-check API calls")
	prepareParallelizeFactor = flag.Int("prepare-parallelize-factor", 2, "the number of parallel prepare requests that are made per api request")
	prepareServiceHost       = flag.String("prepare-service-host", "localhost", "the hostname of the prepare service")
	prepareServicePort       = flag.Int("prepare-service-port", 50052, "the port of the prepare service")
	prepareServiceUseTLS     = flag.Bool("prepare-service-use-tls", false, "should TLS be used to connect to the prepare service")
	promPort                 = flag.Int("prom-port", 9100, "the port for pulling prometheus metrics")
	queryParallelizeFactor   = flag.Int("query-parallelize-factor", 1, "the number of parallel query requests that are made per api request")
	queryServiceHost         = flag.String("query-service-host", "localhost", "the hostname of the query service")
	queryServicePort         = flag.Int("query-service-port", 50052, "the port of the query service")
	queryServiceUseTLS       = flag.Bool("query-service-use-tls", false, "should TLS be used to connect to the query service")
	restPort                 = flag.Int("rest-port", 3365, "the port of the REST listener")
	safeShutdownSeconds      = flag.Int("safe-shutdown-seconds", 120, "the maximum duration in seconds to try to perform a safe shutdown before killing in-flight requests")
	serviceName              = flag.String("service-name", "api", "the name of this service")

	license = flag.Bool("license", false, "prints the software's license")
	version = flag.Bool("version", false, "prints the software's version")

	grpcServer                *grpc.Server
	grpcHealthServer          *grpc.Server
	grpcGatewayServer         *http.Server
	metricsServer             *http.Server
	fileServiceConfig         *client.HostConfig
	icebergTableServiceConfig *client.HostConfig
	prepareServiceConfig      *client.HostConfig
	queryServiceConfig        *client.HostConfig
)

// ServerConfig contains the config to startup the server
type ServerConfig struct {
	GrpcPort int
}

//go:embed .version
var service_version string

//go:embed NOTICE
var service_license string

func main() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "starts the service")
		flag.PrintDefaults()
	}

	if err := envflag.Parse(); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse flags")
	}

	if license != nil && *license {
		fmt.Println(service_license)
		return
	}

	if version != nil && *version {
		fmt.Println(service_version)

		return
	}

	log.Info().Msgf("Starting %s...", *serviceName)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	logProvider := telemetry.NewLoggingProvider(*debug, *debugGrpc, *logFormatJson, *logHealthCheck, *env)
	metricsProvider := telemetry.NewMetricsProvider()

	traceProvider, shutdownTrace := telemetry.NewTracingProvider(ctx, *otelEndpoint)
	defer shutdownTrace()

	g, ctx := errgroup.WithContext(ctx)

	// setup compute clients
	if *fileServiceHost != "" {
		fileServiceConfig = &client.HostConfig{
			Host:   *fileServiceHost,
			Port:   *fileServicePort,
			UseTLS: *fileServiceUseTLS,
		}
	}

	if *prepareServiceHost != "" {
		prepareServiceConfig = &client.HostConfig{
			Host:   *prepareServiceHost,
			Port:   *prepareServicePort,
			UseTLS: *prepareServiceUseTLS,
		}
	}

	if *queryServiceHost != "" {
		queryServiceConfig = &client.HostConfig{
			Host:   *queryServiceHost,
			Port:   *queryServicePort,
			UseTLS: *queryServiceUseTLS,
		}
	}

	entConfig := client.NewEntConfig(*dbDialect, dbName, dbHost, dbInMemory, dbPath, dbPass, dbPort, dbUser, dbUseSSL)
	entClient := client.NewEntClient(ctx, entConfig)
	if *debug {
		entClient = entClient.Debug()
	}
	defer entClient.Close()

	objectStoreClient := client.NewObjectStoreClient(*env, *objectStoreType, *objectStoreBucket, *objectStorePath, *objectStoreEndpoint, *objectStoreDisableSSL, *objectStoreForcePathStyle)

	ownerClient := internal.NewOwnerClient(entClient)
	dataTokenClient := internal.NewDataTokenClient(entClient)
	kaskadaTableClient := internal.NewKaskadaTableClient(entClient)
	kaskadaViewClient := internal.NewKaskadaViewClient(entClient)
	materializationClient := internal.NewMaterializationClient(entClient)
	kaskadaQueryClient := internal.NewKaskadaQueryClient(entClient)
	prepareJobClient := internal.NewPrepareJobClient(entClient)

	computeClients := client.CreateComputeClients(fileServiceConfig, prepareServiceConfig, queryServiceConfig)
	// Defines the authorization function as a client ID header
	authFunc := auth.ClientIDHeaderAuthFunc(*defaultClientID, ownerClient)

	// populate parallize config
	// see: https://github.com/kaskada-ai/kaskada/issues/69
	var parallelizeConfig *utils.ParallelizeConfig
	if entConfig.DriverName() == "sqlite3" {
		if *prepareParallelizeFactor > 1 || *queryParallelizeFactor > 1 {
			log.Warn().Msg("sqlite backing store currently doesn't support parallel operation.  setting all parallization factors to 1.")
		}
		parallelizeConfig = &utils.ParallelizeConfig{
			PrepareFactor: 1,
			QueryFactor:   1,
		}
	} else {
		parallelizeConfig = &utils.ParallelizeConfig{
			PrepareFactor: *prepareParallelizeFactor,
			QueryFactor:   *queryParallelizeFactor,
		}
	}

	// connect to stores
	tableStore := store.NewTableStore(&objectStoreClient)

	computeManager := compute.NewManager(g, computeClients, &dataTokenClient, &kaskadaTableClient, &kaskadaViewClient, &materializationClient, prepareJobClient, &objectStoreClient, *tableStore, *parallelizeConfig)

	// gRPC Health Server
	healthServer := health.NewServer()
	g.Go(func() error {
		unaryServerChain := grpcmiddleware.WithUnaryServerChain(
			logProvider.UnaryPayloadInterceptor,
		)
		grpcHealthServer = grpc.NewServer(unaryServerChain)

		healthpb.RegisterHealthServer(grpcHealthServer, healthServer)

		hln, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcHealthPort))
		if err != nil {
			log.Fatal().Int("port", *grpcHealthPort).Err(err).Msg("not able to start gRPC-health listener")
		}
		log.Info().Int("port", *grpcHealthPort).Msg("gRPC-health server is online")
		return grpcHealthServer.Serve(hln)
	})

	// metrics http server
	g.Go(func() error {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		// Start HTTP metrics server
		metricsServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", *promPort),
			Handler: mux,
		}
		log.Info().Int("port", *promPort).Msg("metrics is online")

		if err := metricsServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Error().Err(err).Int("port", *promPort).Msg("not able to start metrics listener")
			return err
		}

		return nil
	})

	// gRPC server
	g.Go(func() error {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
		if err != nil {
			log.Fatal().Err(err).Int("grpcPort", *grpcPort).Msg("not able to start gRPC listener")
		}

		// Unary Interceptors
		unaryServerChain := grpcmiddleware.WithUnaryServerChain(
			grpc_prometheus.UnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(),
			traceProvider.UnaryInterceptor,
			logProvider.UnaryInterceptor,
			logProvider.UnaryPayloadInterceptor,
			grpc_auth.UnaryServerInterceptor(authFunc),
			validator.UnaryServerInterceptor(true),
		)
		// Stream Interceptors
		streamServerChain := grpcmiddleware.WithStreamServerChain(
			grpc_prometheus.StreamServerInterceptor,
			otelgrpc.StreamServerInterceptor(),
			traceProvider.StreamInterceptor,
			logProvider.StreamInterceptor,
			logProvider.StreamPayloadInterceptor,
			grpc_auth.StreamServerInterceptor(authFunc),
			validator.StreamServerInterceptor(true),
		)

		grpcServer = grpc.NewServer(unaryServerChain, streamServerChain)
		metricsProvider.RegisterGrpc(grpcServer)

		dependencyAnalyzerService := service.NewDependencyAnalyzer(&kaskadaViewClient, &materializationClient)
		tableService := service.NewTableService(computeManager, &kaskadaTableClient, &objectStoreClient, tableStore, &dependencyAnalyzerService)
		viewService := service.NewViewService(computeManager, &kaskadaTableClient, &kaskadaViewClient, &dependencyAnalyzerService)
		materializationService := service.NewMaterializationService(computeManager, &kaskadaTableClient, &kaskadaViewClient, &dataTokenClient, &materializationClient)
		queryV1Service := service.NewQueryV1Service(computeManager, &kaskadaQueryClient, &objectStoreClient)

		// Register the grpc services
		v1alpha.RegisterDataTokenServiceServer(grpcServer, service.NewDataTokenService(&dataTokenClient))
		v1alpha.RegisterTableServiceServer(grpcServer, tableService)
		v1alpha.RegisterViewServiceServer(grpcServer, viewService)
		v1alpha.RegisterMaterializationServiceServer(grpcServer, materializationService)
		v1alpha.RegisterQueryServiceServer(grpcServer, queryV1Service)

		queryV2Service := service.NewQueryV2Service(computeManager, &dataTokenClient, &kaskadaQueryClient)
		v2alpha.RegisterQueryServiceServer(grpcServer, queryV2Service)

		// Register reflection service on gRPC server.
		reflection.Register(grpcServer)

		log.Info().Int("grpcPort", *grpcPort).Msg("gRPC server is online")

		healthServer.SetServingStatus(fmt.Sprintf("grpc.health.v1.%s", *serviceName), healthpb.HealthCheckResponse_SERVING)

		return grpcServer.Serve(lis)
	})

	// grpc-gateway REST server
	g.Go(func() error {
		serviceHandlerMap := map[string]func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error{
			"materialization": v1alpha.RegisterMaterializationServiceHandlerFromEndpoint,
			"query":           v2alpha.RegisterQueryServiceHandlerFromEndpoint,
			"table":           v1alpha.RegisterTableServiceHandlerFromEndpoint,
			"view":            v1alpha.RegisterViewServiceHandlerFromEndpoint,
		}

		// Note: Make sure the gRPC server is running properly and accessible
		mux := runtime.NewServeMux()
		opts := []grpc.DialOption{grpc.WithInsecure()}
		wrenGrpcEndpoint := fmt.Sprintf("localhost:%d", *grpcPort)

		for service, handler := range serviceHandlerMap {
			if err := handler(ctx, mux, wrenGrpcEndpoint, opts); err != nil {
				log.Fatal().Err(err).Msgf("failed to register %s service", service)
			}
		}

		// Start HTTP server (and proxy calls to gRPC server endpoint)
		grpcGatewayServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", *restPort),
			Handler: mux,
		}
		log.Info().Int("port", *restPort).Msg("gRPC-gateway is online")

		if err := grpcGatewayServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal().Err(err).Int("port", *restPort).Msg("not able to start gRPC-gateway listener")
			return err
		}

		return nil
	})

	// wait until shutdown signal occurs
	select {
	case <-interrupt:
		log.Info().Msg("interrupt!")
		break
	case <-ctx.Done():
		log.Info().Msg("context done!")
		break
	}

	log.Info().Msg("received shutdown signal")

	cancel()
	log.Info().Msg("canceled the main context")

	healthServer.SetServingStatus(fmt.Sprintf("grpc.health.v1.%s", *serviceName), healthpb.HealthCheckResponse_NOT_SERVING)
	log.Info().Msg("set gRPC-health status to not-serving")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(*safeShutdownSeconds)*time.Second)
	defer shutdownCancel()
	log.Info().Msg("created a shutdownCancel context")

	gracefulShutdownHttpServer(shutdownCtx, grpcGatewayServer, "gRPC-gateway")
	gracefulStopGrpcServer(grpcServer, "gRPC")
	gracefulShutdownHttpServer(shutdownCtx, metricsServer, "metrics")
	gracefulStopGrpcServer(grpcHealthServer, "gRPC-health")

	if err := g.Wait(); err != nil {
		log.Fatal().Err(err).Msgf("%s terminated!", *serviceName)
	}
}

func gracefulShutdownHttpServer(ctx context.Context, server *http.Server, name string) {
	if server != nil {
		log.Info().Msgf("initiating graceful shutdown of the %s server", name)
		if err := server.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msgf("error shutting down the %s server", name)
		} else {
			log.Info().Msgf("graceful shutdown of the %s server is complete", name)
		}
	}
}

func gracefulStopGrpcServer(server *grpc.Server, name string) {
	if server != nil {
		log.Info().Msgf("initiating graceful stop of the %s server", name)
		server.GracefulStop()
		log.Info().Msgf("graceful stop of the %s server is complete", name)
	}
}
