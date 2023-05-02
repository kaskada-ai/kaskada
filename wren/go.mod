module github.com/kaskada-ai/kaskada/wren

go 1.19

replace github.com/kaskada-ai/kaskada/gen/proto/go => ../gen/proto/go

require (
	github.com/aws/aws-sdk-go v1.44.122
	github.com/cheapRoc/grpc-zerolog v0.0.0-20180425150930-27ca9d023ead
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2 v2.0.0-rc.2.0.20211203031854-ed43ab3fa4f8
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20211203031854-ed43ab3fa4f8
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.0
	github.com/kouhin/envflag v0.0.0-20150818174321-0e9a86061649
	github.com/mattn/go-sqlite3 v1.14.14
	github.com/onsi/ginkgo/v2 v2.1.4
	github.com/onsi/gomega v1.19.0
	github.com/prometheus/client_golang v1.14.0
	github.com/rabbitmq/amqp091-go v1.5.0
	github.com/rs/zerolog v1.26.0
	github.com/stretchr/testify v1.8.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.26.1
	go.opentelemetry.io/otel v1.2.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.2.0
	go.opentelemetry.io/otel/sdk v1.2.0
	go.opentelemetry.io/otel/trace v1.2.0
	golang.org/x/sync v0.1.0
	google.golang.org/genproto v0.0.0-20230221151758-ace64dc21148
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.28.1
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/compute v1.18.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.11.0 // indirect
	cloud.google.com/go/storage v1.27.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.15.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.28 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.21 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.9.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20210107165309-348f09dbbbc0 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/mattn/go-ieproxy v0.0.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/oauth2 v0.5.0 // indirect
	golang.org/x/tools v0.3.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.110.0 // indirect
)

require (
	ariga.io/atlas v0.5.0
	entgo.io/ent v0.11.1
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/c2fo/vfs/v6 v6.6.0
	github.com/cenkalti/backoff/v4 v4.1.2 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/hcl/v2 v2.10.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kaskada-ai/kaskada/gen/proto/go v0.0.0-00010101000000-000000000000
	github.com/lib/pq v1.10.6
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-wordwrap v0.0.0-20150314170334-ad45545899c7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/zclconf/go-cty v1.8.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.2.0 // indirect
	go.opentelemetry.io/proto/otlp v0.11.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
