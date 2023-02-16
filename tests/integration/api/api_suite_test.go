/*
Copyright (C) 2021 Kaskada Inc. All rights reserved.

This package cannot be used, copied or distributed without the express
written permission of Kaskada Inc.

For licensing inquiries, please contact us at info@kaskada.com.
*/

package api_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/RedisAI/redisai-go/redisai"
	"github.com/c2fo/vfs"
	"github.com/c2fo/vfs/backend"
	"github.com/c2fo/vfs/backend/gs"
	vfsos "github.com/c2fo/vfs/backend/os"
	"github.com/c2fo/vfs/backend/s3"
	vfs_utils "github.com/c2fo/vfs/utils"
	"github.com/c2fo/vfs/v6/backend/azure"
	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/namsral/flag"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/kaskada/kaskada-ai/tests/integration/api/helpers"
	. "github.com/kaskada/kaskada-ai/tests/integration/api/matchers"
	"github.com/kaskada/kaskada-ai/wren/ent"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
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
	kaskadaRestPort      = flag.Int("rest-port", 8080, "Kaskada's REST port to connect")
	kaskadaUseTLS        = flag.Bool("use-tls", false, "protocol for connecting to Kaskada")

	externalBucket   = "external-bucket"
	externalFile     = "purchases/purchases_part2.parquet"
	externalRegion   = "eu-central-1" //frankfurt
	externalUser     = "external-user"
	externalPassword = "external-password"
)

// Before starting tests, delete all tables associated with the Integration clientID.  Also completely wipes connected RedisAI instance.
var _ = BeforeSuite(func() {
	flag.Parse()

	if *objectStoreType == object_store_type_s3 {
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

	DeleteAllExistingObjects(*objectStoreType, *objectStoreBucket, *objectStorePath)

	CleanDatabase()
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

// HostConfig holds the data needed to connect to a particular grpc server
type HostConfig struct {
	Hostname string
	Port     int
	UseTLS   bool
}

func getGrpcConnection(ctx context.Context) *grpc.ClientConn {
	// default to disabling TLS for running in k8s
	tlsOpt := grpc.WithInsecure()

	// Override with TLS (usually for running locally against k8s)
	if *kaskadaUseTLS {
		tlsOpt = grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: false,
			}))
	}

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", *kaskadaHostname, *kaskadaGrpcPort), tlsOpt)
	Expect(err).ShouldNot(HaveOccurred())
	return conn
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

func getContextCancelConnection(seconds int) (context.Context, context.CancelFunc, *grpc.ClientConn) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	conn := getGrpcConnection(ctx)
	return ctx, cancel, conn
}

func downloadCSV(url string) [][]string {
	localPath, cleanup := downloadFile(url)
	defer cleanup()

	file, err := os.Open(localPath)
	Expect(err).ShouldNot(HaveOccurred(), "can't open file")

	r := csv.NewReader(file)

	results, err := r.ReadAll()
	Expect(err).ShouldNot(HaveOccurred(), "can't read csv file")
	return results
}

func downloadParquet(url string) []interface{} {
	localPath, cleanup := downloadFile(url)
	defer cleanup()

	fr, err := local.NewLocalFileReader(localPath)
	Expect(err).ShouldNot(HaveOccurred(), "can't open file")

	pr, err := reader.NewParquetReader(fr, nil, 4)
	Expect(err).ShouldNot(HaveOccurred(), "Can't create parquet reader")

	num := int(pr.GetNumRows())
	rows, err := pr.ReadByNumber(num)
	Expect(err).ShouldNot(HaveOccurred(), "Can't read")

	pr.ReadStop()
	fr.Close()

	return rows
}

func downloadFile(url string) (localPath string, cleanup func()) {
	if strings.HasPrefix(url, "http://") {
		// download to temp file
		tempFile, err := os.CreateTemp("", "*.parquet")
		Expect(err).ShouldNot(HaveOccurred(), "Can't create temp file")
		defer tempFile.Close()
		resp, err := http.Get(url)
		Expect(err).ShouldNot(HaveOccurred(), "Can't download url")
		Expect(resp.StatusCode).Should(Equal(200))
		defer resp.Body.Close()
		io.Copy(tempFile, resp.Body)

		localPath = tempFile.Name()
		cleanup = func() {
			err = os.Remove(tempFile.Name())
			Expect(err).ShouldNot(HaveOccurred(), "Can't remove temp file")
		}
	} else if strings.HasPrefix(url, "file:///") {
		localPath = fmt.Sprintf("..%s", strings.TrimPrefix(url, "file:///"))
		cleanup = func() {}
	} else {
		localPath = fmt.Sprintf("..%s", url)
		cleanup = func() {}
	}
	return
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
		// "github.com/kaskada/kaskada-ai/kaskada/gen/kaskada/kaskada/v1alpha"
		details = append(details, detail.String())
	}
	return details
}

func getFileURI(fileName string) string {
	return fmt.Sprintf("file:///testdata/%s", fileName)
}

// loads files from testdata/ into a table.
func loadTestFilesIntoTable(ctx context.Context, conn *grpc.ClientConn, table *v1alpha.Table, fileNames ...string) []*v1alpha.LoadDataResponse {
	tableClient := v1alpha.NewTableServiceClient(conn)

	responses := []*v1alpha.LoadDataResponse{}
	for _, fileName := range fileNames {
		var fileType v1alpha.FileType
		switch path.Ext(fileName) {
		case ".csv":
			fileType = v1alpha.FileType_FILE_TYPE_CSV
		case ".parquet":
			fileType = v1alpha.FileType_FILE_TYPE_PARQUET
		default:
			fileType = v1alpha.FileType_FILE_TYPE_UNSPECIFIED
		}

		loadReq := v1alpha.LoadDataRequest{
			TableName: table.TableName,
			SourceData: &v1alpha.LoadDataRequest_FileInput{
				FileInput: &v1alpha.FileInput{
					FileType: fileType,
					Uri:      getFileURI(fileName),
				},
			},
		}
		res, err := tableClient.LoadData(ctx, &loadReq)
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(res).ShouldNot(BeNil())
		responses = append(responses, res)
	}
	if *objectStoreType == object_store_type_s3 {
		waitForMinio()
	}
	return responses
}

// loads a file from testdata/ into a table.
func loadTestFileIntoTable(ctx context.Context, conn *grpc.ClientConn, table *v1alpha.Table, fileURI string) *v1alpha.LoadDataResponse {
	return loadTestFilesIntoTable(ctx, conn, table, fileURI)[0]
}

// verifies RequestDetails are not empty, and logs the requestID for easier debugging of test failures
func VerifyRequestDetails(requestDetails *v1alpha.RequestDetails) {
	Expect(requestDetails.RequestId).ShouldNot(BeEmpty())
	logLn(fmt.Sprintf("RequestID: %s", requestDetails.RequestId))
}

// helper to log to test output
func logLn(line string) {
	fmt.Fprintln(GinkgoWriter, line)
}

// wait for minio to get things in order
func waitForMinio() {
	time.Sleep(250 * time.Millisecond)
}

func primitiveSchemaField(name string, primitiveType v1alpha.DataType_PrimitiveType) *v1alpha.Schema_Field {
	return &v1alpha.Schema_Field{
		Name: name,
		DataType: &v1alpha.DataType{
			Kind: &v1alpha.DataType_Primitive{
				Primitive: primitiveType,
			},
		},
	}
}

func getCreateQueryResponses(stream v1alpha.QueryService_CreateQueryClient) ([]*v1alpha.CreateQueryResponse, error) {
	responses := []*v1alpha.CreateQueryResponse{}
	for {
		queryResponse, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return responses, err
		}
		responses = append(responses, queryResponse)
	}
	return responses, nil
}

func getMergedCreateQueryResponse(stream v1alpha.QueryService_CreateQueryClient) (*v1alpha.CreateQueryResponse, error) {
	mergedResponse := &v1alpha.CreateQueryResponse{}
	for {
		queryResponse, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return mergedResponse, err
		}
		if queryResponse.Analysis != nil {
			mergedResponse.Analysis = queryResponse.Analysis
		}
		if queryResponse.Config != nil {
			mergedResponse.Config = queryResponse.Config
		}
		if queryResponse.FenlDiagnostics != nil {
			mergedResponse.FenlDiagnostics = queryResponse.FenlDiagnostics
		}
		if queryResponse.Metrics != nil {
			mergedResponse.Metrics = queryResponse.Metrics
		}
		if queryResponse.QueryId != "" {
			mergedResponse.QueryId = queryResponse.QueryId
		}
		if queryResponse.RequestDetails != nil {
			mergedResponse.RequestDetails = queryResponse.RequestDetails
		}
		if queryResponse.GetRedisAI() != nil {
			mergedResponse.Results = &v1alpha.CreateQueryResponse_RedisAI_{
				RedisAI: queryResponse.GetRedisAI(),
			}
		}
		if queryResponse.GetFileResults() != nil {
			newPaths := queryResponse.GetFileResults().Paths
			existingPaths := []string{}

			if mergedResponse.GetFileResults() != nil {
				existingPaths = mergedResponse.GetFileResults().Paths
			}
			mergedResponse.Results = &v1alpha.CreateQueryResponse_FileResults{
				FileResults: &v1alpha.FileResults{
					FileType: queryResponse.GetFileResults().FileType,
					Paths:    append(existingPaths, newPaths...),
				},
			}
		}

		mergedResponse.State = queryResponse.State
	}
	return mergedResponse, nil
}

// NewEntClient creates a new EntClient
func newEntClient() *ent.Client {
	if dbDialect == nil {
		panic("Flag `db-dialect` must be set to either `postgres` or `sqlite`")
	} else if *dbDialect == "postgres" {
		client, err := ent.Open("postgres", "host=localhost port=5432 user=kaskada dbname=kaskada password=kaskada123 sslmode=disable")
		Expect(err).ShouldNot(HaveOccurred())
		return client
	} else if *dbDialect == "sqlite" {
		client, err := ent.Open("sqlite3", "file:../data/kaskada.db?mode=rw&_fk=1&_auth&_auth_user=kaskada&_auth_pass=kaskada123")
		Expect(err).ShouldNot(HaveOccurred())
		return client
	} else {
		panic("Flag `db-dialect` must be set to either `postgres` or `sqlite`")
	}
}

const (
	object_store_type_local = "local"
	object_store_type_s3    = "s3"
	object_store_type_gcs   = "gcs"
	object_store_type_azure = "azure"
)

func CleanDatabase() {
	entClient := newEntClient()

	Expect(entClient.Materialization.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.KaskadaView.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.PreparedFile.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.KaskadaFile.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.DataVersion.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.KaskadaTable.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.DataToken.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.KaskadaQuery.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.ComputeSnapshot.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
	Expect(entClient.Owner.Delete().Exec(context.Background())).Error().ShouldNot(HaveOccurred())
}

func DeleteAllExistingObjects(objectStoreType string, objectStoreBucket string, objectStorePath string) {
	objectStoreType = strings.ToLower(objectStoreType)

	switch objectStoreType {
	case object_store_type_local:
		absPath, err := filepath.Abs(objectStorePath)
		if err != nil {
			panic(fmt.Sprintf("could not locate local data path: %s", objectStorePath))
		}
		objectStorePath = vfs_utils.EnsureTrailingSlash(absPath)
	case object_store_type_azure, object_store_type_gcs, object_store_type_s3:
		if objectStoreBucket == "" {
			panic(fmt.Sprintf("when using %s for the `object-store-type`, `object-store-bucket` is requried.", objectStoreType))
		} else if !filepath.IsAbs(objectStorePath) {
			panic(fmt.Sprintf("when using %s for the `object-store-type`, `object-store-path` cannot be a relative path or prefix", objectStoreType))
		}
	default:
		panic("invalid value set for `object-store-type`. Should be  `local`, `s3`, `gcs`, or `azure`")
	}

	var rootObjectStore vfs.FileSystem
	switch objectStoreType {
	case object_store_type_azure:
		rootObjectStore = backend.Backend(azure.Scheme)
	case object_store_type_gcs:
		rootObjectStore = backend.Backend(gs.Scheme)
	case object_store_type_local:
		rootObjectStore = backend.Backend(vfsos.Scheme)
	case object_store_type_s3:
		rootObjectStore = backend.Backend(s3.Scheme)
		// currently this cleanup is done by the init minio stuff in the `BeforeSuite` code above
		return
	default:
		panic("invalid value set for `object-store-type`. Should be  `local`, `s3`, `gcs`, or `azure`")
	}

	dataLocation, err := rootObjectStore.NewLocation(objectStoreBucket, objectStorePath)
	Expect(err).ShouldNot(HaveOccurred(), "unable to initialize object store")
	if objectStoreType == object_store_type_local {
		logLn(fmt.Sprintf("removing everything at: %s", dataLocation.Path()))
		files, err := ioutil.ReadDir(dataLocation.Path())
		Expect(err).ShouldNot(HaveOccurred())
		for _, file := range files {
			switch file.Name() {
			case "tmp", "kaskada.db":
				continue
			default:
				path := dataLocation.Path() + file.Name()
				logLn(fmt.Sprintf("Deleting path: %s", path))
				os.RemoveAll(path)
			}
		}
	} else {
		logLn(fmt.Sprintf("dataPath: %s", dataLocation.URI()))
		objectList, err := dataLocation.ListByPrefix("")
		Expect(err).ShouldNot(HaveOccurred(), "issue listing objects")
		logLn(fmt.Sprintf("objectList: %v", objectList))
		for _, object := range objectList {
			logLn(fmt.Sprintf("Deleting object: %s", object))
			err = dataLocation.DeleteFile(object)
			Expect(err).ShouldNot(HaveOccurred(), "issue deleting object")
		}
		time.Sleep(5 * time.Second)
	}
}
