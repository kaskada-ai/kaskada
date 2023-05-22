package helpers

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/c2fo/vfs"
	"github.com/c2fo/vfs/backend"
	"github.com/c2fo/vfs/backend/gs"
	vfsos "github.com/c2fo/vfs/backend/os"
	"github.com/c2fo/vfs/backend/s3"
	vfs_utils "github.com/c2fo/vfs/utils"
	"github.com/c2fo/vfs/v6/backend/azure"
	"github.com/kaskada-ai/kaskada/wren/ent"

	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// HostConfig holds the data needed to connect to a particular grpc server
type HostConfig struct {
	Hostname string
	Port     int
	UseTLS   bool
}

func (c HostConfig) GetGrpcConnection(ctx context.Context) *grpc.ClientConn {
	// default to disabling TLS for running in k8s
	tlsOpt := grpc.WithInsecure()

	// Override with TLS (usually for running locally against k8s)
	if c.UseTLS {
		tlsOpt = grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: false,
			}))
	}

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", c.Hostname, c.Port), tlsOpt)
	Expect(err).ShouldNot(HaveOccurred())
	return conn
}

func (c HostConfig) GetContextCancelConnection(seconds int) (context.Context, context.CancelFunc, *grpc.ClientConn) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	conn := c.GetGrpcConnection(ctx)
	return ctx, cancel, conn
}

func DownloadCSV(url string) [][]string {
	localPath, cleanup := downloadFile(url)
	defer cleanup()

	file, err := os.Open(localPath)
	Expect(err).ShouldNot(HaveOccurred(), "can't open file")

	r := csv.NewReader(file)

	results, err := r.ReadAll()
	Expect(err).ShouldNot(HaveOccurred(), "can't read csv file")
	return results
}

func DownloadParquet(url string) []interface{} {
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
		return
	}
	localPath = strings.TrimPrefix(url, "file://")
	cleanup = func() {}
	if os.Getenv("ENV") != "local-local" {
		localPath = fmt.Sprintf("../%s", localPath)
	}
	return
}

func GetFileURI(fileName string) string {
	if os.Getenv("ENV") == "local-local" {
		workDir, err := os.Getwd()
		Expect(err).ShouldNot(HaveOccurred())
		return fmt.Sprintf("file://%s/../../../testdata/%s", workDir, fileName)
	}
	return fmt.Sprintf("file:///testdata/%s", fileName)
}

// loads files from testdata/ into a table.
func LoadTestFilesIntoTable(ctx context.Context, conn *grpc.ClientConn, table *v1alpha.Table, fileNames ...string) []*v1alpha.LoadDataResponse {
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
					Uri:      GetFileURI(fileName),
				},
			},
		}
		res, err := tableClient.LoadData(ctx, &loadReq)
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(res).ShouldNot(BeNil())
		responses = append(responses, res)
	}
	return responses
}

// loads a file from testdata/ into a table.
func LoadTestFileIntoTable(ctx context.Context, conn *grpc.ClientConn, table *v1alpha.Table, fileURI string) *v1alpha.LoadDataResponse {
	return LoadTestFilesIntoTable(ctx, conn, table, fileURI)[0]
}

// NewEntClient creates a new EntClient
func newEntClient(dbDialect *string) *ent.Client {
	if dbDialect == nil {
		panic("Flag `db-dialect` must be set to either `postgres` or `sqlite`")
	} else if *dbDialect == "postgres" {
		client, err := ent.Open("postgres", "host=localhost port=5432 user=kaskada dbname=kaskada password=kaskada123 sslmode=disable")
		Expect(err).ShouldNot(HaveOccurred())
		return client
	} else if *dbDialect == "sqlite" {
		client, err := ent.Open("sqlite3", "file:../data/kaskada.db?mode=rwc&_fk=1&_auth&_auth_user=kaskada&_auth_pass=kaskada123")
		Expect(err).ShouldNot(HaveOccurred())
		return client
	} else {
		panic("Flag `db-dialect` must be set to either `postgres` or `sqlite`")
	}
}

func CleanDatabase(dbDialect *string) {
	entClient := newEntClient(dbDialect)

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

const (
	Object_store_type_local = "local"
	Object_store_type_s3    = "s3"
	Object_store_type_gcs   = "gcs"
	Object_store_type_azure = "azure"
)

func DeleteAllExistingObjects(objectStoreType string, objectStoreBucket string, objectStorePath string) {
	objectStoreType = strings.ToLower(objectStoreType)

	switch objectStoreType {
	case Object_store_type_local:
		absPath, err := filepath.Abs(objectStorePath)
		if err != nil {
			panic(fmt.Sprintf("could not locate local data path: %s", objectStorePath))
		}
		objectStorePath = vfs_utils.EnsureTrailingSlash(absPath)
	case Object_store_type_azure, Object_store_type_gcs, Object_store_type_s3:
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
	case Object_store_type_azure:
		rootObjectStore = backend.Backend(azure.Scheme)
	case Object_store_type_gcs:
		rootObjectStore = backend.Backend(gs.Scheme)
	case Object_store_type_local:
		rootObjectStore = backend.Backend(vfsos.Scheme)
	case Object_store_type_s3:
		rootObjectStore = backend.Backend(s3.Scheme)
		// currently this cleanup is done by the init minio stuff in the `BeforeSuite` code above
		return
	default:
		panic("invalid value set for `object-store-type`. Should be  `local`, `s3`, `gcs`, or `azure`")
	}

	dataLocation, err := rootObjectStore.NewLocation(objectStoreBucket, objectStorePath)
	Expect(err).ShouldNot(HaveOccurred(), "unable to initialize object store")
	if objectStoreType == Object_store_type_local {
		LogLn(fmt.Sprintf("removing everything at: %s", dataLocation.Path()))
		files, err := ioutil.ReadDir(dataLocation.Path())
		Expect(err).ShouldNot(HaveOccurred())
		for _, file := range files {
			switch file.Name() {
			case "tmp", "kaskada.db":
				continue
			default:
				path := dataLocation.Path() + file.Name()
				LogLn(fmt.Sprintf("Deleting path: %s", path))
				os.RemoveAll(path)
			}
		}
	} else {
		LogLn(fmt.Sprintf("dataPath: %s", dataLocation.URI()))
		objectList, err := dataLocation.ListByPrefix("")
		Expect(err).ShouldNot(HaveOccurred(), "issue listing objects")
		LogLn(fmt.Sprintf("objectList: %v", objectList))
		for _, object := range objectList {
			LogLn(fmt.Sprintf("Deleting object: %s", object))
			err = dataLocation.DeleteFile(object)
			Expect(err).ShouldNot(HaveOccurred(), "issue deleting object")
		}
		time.Sleep(5 * time.Second)
	}
}

// helper to log to test output
func LogLn(line string) {
	fmt.Fprintln(GinkgoWriter, line)
}

func GetCreateQueryResponses(stream v1alpha.QueryService_CreateQueryClient) ([]*v1alpha.CreateQueryResponse, error) {
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

func GetMergedCreateQueryResponse(stream v1alpha.QueryService_CreateQueryClient) (*v1alpha.CreateQueryResponse, error) {
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
		if queryResponse.GetDestination().GetRedis() != nil {
			mergedResponse.Destination = &v1alpha.Destination{
				Destination: &v1alpha.Destination_Redis{Redis: queryResponse.GetDestination().GetRedis()},
			}
		}
		if queryResponse.GetDestination().GetObjectStore().GetOutputPaths() != nil {
			newPaths := queryResponse.GetDestination().GetObjectStore().GetOutputPaths().Paths
			existingPaths := []string{}

			if mergedResponse.GetDestination().GetObjectStore().GetOutputPaths() != nil {
				existingPaths = mergedResponse.GetDestination().GetObjectStore().GetOutputPaths().Paths
			}
			mergedResponse.Destination = &v1alpha.Destination{
				Destination: &v1alpha.Destination_ObjectStore{
					ObjectStore: &v1alpha.ObjectStoreDestination{
						FileType: queryResponse.GetDestination().GetObjectStore().FileType,
						OutputPaths: &v1alpha.ObjectStoreDestination_ResultPaths{
							Paths: append(existingPaths, newPaths...),
						},
					},
				},
			}
		}

		mergedResponse.State = queryResponse.State
	}
	return mergedResponse, nil
}
