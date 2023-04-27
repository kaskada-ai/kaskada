module github.com/kaskada-ai/kaskada/tests/integration/api

go 1.19

replace github.com/kaskada-ai/kaskada/gen/proto/go => ../../../gen/proto/go

replace github.com/kaskada-ai/kaskada/wren => ../../../wren

replace github.com/kaskada-ai/kaskada/tests/integration/shared => ../shared

require (
	github.com/apache/pulsar-client-go v0.9.0
	github.com/gomodule/redigo v1.8.9
	github.com/google/uuid v1.3.0
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/onsi/ginkgo/v2 v2.9.2
	github.com/onsi/gomega v1.27.6
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
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.15.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.28 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.21 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/aws/aws-sdk-go v1.44.122 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.9.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kaskada-ai/kaskada/wren v0.0.0-00010101000000-000000000000 // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/linkedin/goavro/v2 v2.9.8 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/mattn/go-ieproxy v0.0.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.49 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/secure-io/sio-go v0.3.1 // indirect
	github.com/shirou/gopsutil/v3 v3.22.9 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/stretchr/testify v1.8.1 // indirect
	github.com/tinylib/msgp v1.1.6 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.5.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/oauth2 v0.5.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/tools v0.7.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.110.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)

require (
	ariga.io/atlas v0.5.0 // indirect
	entgo.io/ent v0.11.1 // indirect
	github.com/RedisAI/redisai-go v1.0.1
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/c2fo/vfs v2.1.4+incompatible // indirect
	github.com/c2fo/vfs/v6 v6.6.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/hashicorp/hcl/v2 v2.10.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kaskada-ai/kaskada/gen/proto/go v0.0.0-00010101000000-000000000000
	github.com/kaskada-ai/kaskada/tests/integration/shared v0.0.0-00010101000000-000000000000
	github.com/lib/pq v1.10.6
	github.com/minio/madmin-go v1.7.5 // indirect
	github.com/mitchellh/go-wordwrap v0.0.0-20150314170334-ad45545899c7 // indirect
	github.com/namsral/flag v1.7.4-pre
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/xitongsys/parquet-go v1.6.2 // indirect
	github.com/xitongsys/parquet-go-source v0.0.0-20221025031416-9877e685ef65 // indirect
	github.com/zclconf/go-cty v1.8.0 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/mod v0.9.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
