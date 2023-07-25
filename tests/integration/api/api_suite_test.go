package api_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/RedisAI/redisai-go/redisai"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"

	_ "github.com/mattn/go-sqlite3"
	"github.com/namsral/flag"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
)

var (
	objectStoreType   = flag.String("object-store-type", "local", "the type of object store to use.  should be `local` (default), `s3`, `gcs`, or `azure`.")
	objectStoreBucket = flag.String("object-store-bucket", "integration", "the bucket or container to use for storing objects. required if the `object-store-type` is not `local`.")
	objectStorePath   = flag.String("object-store-path", "../data/", "the path or prefix for storing data objects. can be a relative path if the `object-store-type` is `local`.")

	dbDialect            = flag.String("db-driver", "sqlite", "should be `postgres` or `sqlite`")
	integrationClientID  = flag.String("integration-client-id", "client-id-integration", "integration client-id")
	minioKaskadaRegion   = flag.String("minio-kaskada-region", "us-west-2", "region for the bucket in minio")
	minioKaskadaUser     = flag.String("minio-kaskada-user", "kaskada", "kaskada username for connecting to minio")
	minioKaskadaPassword = flag.String("minio-kaskada-password", "kaskada123", "kaskada password for connecting to minio")
	minioRootUser        = flag.String("minio-root-user", "minio", "root username for connecting to minio")
	minioRootPassword    = flag.String("minio-root-password", "minio123", "root password for connecting to minio")
	minioEndpoint        = flag.String("minio-endpoint", "127.0.0.1:9000", "endpoint for connecting to minio")
	redisAIPort          = flag.Int("redis-ai-port", 6379, "Port to connect to the redis-ai integration instance. Note that this should be a specific instance for integration tests only, as the test cleanup will wipe any existing data from the redis instance.")
	redisAIHost          = flag.String("redis-ai-host", "127.0.0.1", "Host to connect to the redis-ai integration instance. Note that this should be a specific instance for integration tests only, as the test cleanup will wipe any existing data from the redis instance.")
	kaskadaHostname      = flag.String("hostname", "127.0.0.1", "hostname of Kaskada to connect")
	kaskadaGrpcPort      = flag.Int("grpc-port", 50051, "Kaskada's gRPC port to connect")
	kaskadaRestPort      = flag.Int("rest-port", 3365, "Kaskada's REST port to connect")
	kaskadaUseTLS        = flag.Bool("use-tls", false, "protocol for connecting to Kaskada")

	externalBucket   = "external-bucket"
	externalFile     = "purchases/purchases_part2.parquet"
	externalRegion   = "eu-central-1" //frankfurt
	externalUser     = "external-user"
	externalPassword = "external-password"

	grpcConfig helpers.HostConfig
)

// Before starting tests, delete all tables associated with the Integration clientID.  Also completely wipes connected RedisAI instance.
var _ = BeforeSuite(func() {
	flag.Parse()

	if *objectStoreType == helpers.Object_store_type_s3 {
		proxyMinioRequests()

		minioHelper := helpers.MinioHelper{
			Endpoint:     *minioEndpoint,
			RootUser:     *minioRootUser,
			RootPassword: *minioRootPassword,
		}

		err := minioHelper.InitBucketAndUser(*objectStoreBucket, *minioKaskadaRegion, *minioKaskadaUser, *minioKaskadaPassword, false, true)
		Expect(err).ShouldNot(HaveOccurred())

		err = minioHelper.InitBucketAndUser(externalBucket, externalRegion, externalUser, externalPassword, true, true)
		Expect(err).ShouldNot(HaveOccurred())

		err = minioHelper.AddFileToBucket(fmt.Sprintf("../../../testdata/%s", externalFile), externalBucket, externalRegion, externalFile)
		Expect(err).ShouldNot(HaveOccurred())
	}

	helpers.DeleteAllExistingObjects(*objectStoreType, *objectStoreBucket, *objectStorePath)

	helpers.CleanDatabase(dbDialect)

	grpcConfig = helpers.HostConfig{
		Hostname: *kaskadaHostname,
		Port:     *kaskadaGrpcPort,
		UseTLS:   *kaskadaUseTLS,
	}
})

func TestApi(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Api Suite")
}

func proxyMinioRequests() {
	http.DefaultTransport.(*http.Transport).Proxy = func(r *http.Request) (*url.URL, error) {
		url := r.URL
		if strings.Contains(r.URL.Host, "minio") {
			url.Host = strings.Replace(url.Host, "minio", "127.0.0.1", 1)
			return url, nil
		}
		return nil, nil
	}
}

func isARM() bool {
	return strings.Contains(runtime.GOARCH, "arm")
}

func getRedisAIClient(db int) *redisai.Client {
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", fmt.Sprintf("%s:%d", *redisAIHost, *redisAIPort), redis.DialDatabase(db))
	}}

	return redisai.Connect("", pool)
}

func wipeRedisDatabase(db int) {
	//Cleanup all existing data in RedisAI
	redisAIClient := getRedisAIClient(db)
	defer redisAIClient.Close()
	redisAIClient.ActiveConnNX()
	err := redisAIClient.ActiveConn.Send("FLUSHALL", "SYNC")
	Expect(err).ShouldNot(HaveOccurred())
}

func getRestRequest(ctx context.Context, method, endpoint string, jsonBody []byte) *http.Request {
	var (
		req *http.Request
		err error
		url string
	)

	if *kaskadaUseTLS {
		url = fmt.Sprintf("https://%s:%v/%s", *kaskadaHostname, *kaskadaRestPort, endpoint)
	} else {
		url = fmt.Sprintf("http://%s:%v/%s", *kaskadaHostname, *kaskadaRestPort, endpoint)
	}

	if jsonBody != nil {
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(jsonBody))
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
		Expect(err).ShouldNot(HaveOccurred())
	}
	req.Header.Add("client-id", *integrationClientID)
	return req
}

func getBody(res *http.Response) string {
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	return string(b)
}

func convertDetails(errStatus *status.Status) []string {
	details := []string{}
	for _, detail := range errStatus.Proto().GetDetails() {
		// the `detail.String()` method uses the global type registry to resolve the proto
		// message type and unmarshal it into a string. In order for a message type to appear
		// in the global registry, the Go type representing that protobuf message type must
		// be linked into the Go binary. This is achieved through an import of the generated
		// Go package representing a .proto file.
		//
		// The following imports are added with an underscore (see: https://tinyurl.com/n5rxvbky)
		// to make sure they are included in the build:
		//
		// "google.golang.org/genproto/googleapis/rpc/errdetails"
		// "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
		details = append(details, detail.String())
	}
	return details
}

// verifies RequestDetails are not empty, and logs the requestID for easier debugging of test failures
func VerifyRequestDetails(requestDetails *v1alpha.RequestDetails) {
	Expect(requestDetails.RequestId).ShouldNot(BeEmpty())
	helpers.LogLn(fmt.Sprintf("RequestID: %s", requestDetails.RequestId))
}

// wait for minio to get things in order
func waitForMinio() {
	if *objectStoreType == helpers.Object_store_type_s3 {
		time.Sleep(250 * time.Millisecond)
	}
}

func primitiveSchemaField(name string, primitiveType v1alpha.DataType_PrimitiveType) *v1alpha.Schema_Field {
	return &v1alpha.Schema_Field{
		Name: name,
		DataType: &v1alpha.DataType{
			Kind: &v1alpha.DataType_Primitive{
				Primitive: primitiveType,
			},
		},
		Nullable: true,
	}
}

func getRemotePulsarHostname() string {
	if os.Getenv("ENV") == "local-local" {
		return "localhost"
	} else {
		return "pulsar"
	}
}

func getPulsarConfig(topicName string) *v1alpha.PulsarConfig {
	return &v1alpha.PulsarConfig{
		BrokerServiceUrl: fmt.Sprintf("pulsar://%s:6650", getRemotePulsarHostname()),
		AdminServiceUrl:  fmt.Sprintf("http://%s:8080", getRemotePulsarHostname()),
		AuthPlugin:       "",
		AuthParams:       "",
		Tenant:           "public",
		Namespace:        "default",
		TopicName:        topicName,
	}
}

func getKafkaConfig(topicName string) *v1alpha.KafkaConfig {
	return &v1alpha.KafkaConfig{
		Hosts: []string{"localhost:29092"},
		Topic: topicName,
		Schema: &v1alpha.KafkaConfig_AvroSchema{
			AvroSchema: "{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"time\", \"type\":\"long\"}, {\"name\": \"id\", \"type\": \"long\"}, {\"name\": \"my_val\", \"type\": \"long\"}]}",
		},
	}
}

func receivePulsarMessageWithTimeout(pulsarConsumer pulsar.Consumer, ctx context.Context) pulsar.Message {
	timeout, timeoutCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer timeoutCancel()
	msg, err := pulsarConsumer.Receive(timeout)
	if err != nil {
		helpers.LogLn("timed out waiting for puslar message")
		return nil
	} else {
		helpers.LogLn("recieved pulsar response: %v", msg)
		return msg
	}
}

func getUniqueName(tableNamePrefix string) string {
	return fmt.Sprintf("%s_%d", tableNamePrefix, time.Now().Unix())
}
