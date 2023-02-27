package cli_test

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/namsral/flag"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
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
	kaskadaHostname      = flag.String("hostname", "127.0.0.1", "hostname of Kaskada to connect")
	kaskadaGrpcPort      = flag.Int("grpc-port", 50051, "Kaskada's gRPC port to connect")
	kaskadaRestPort      = flag.Int("rest-port", 8080, "Kaskada's REST port to connect")
	kaskadaUseTLS        = flag.Bool("use-tls", false, "protocol for connecting to Kaskada")

	grpcConfig helpers.HostConfig
)

// Before starting tests, delete all tables associated with the Integration clientID.  Also completely wipes connected RedisAI instance.
var _ = BeforeSuite(func() {
	flag.Parse()

	if *objectStoreType == helpers.Object_store_type_s3 {
		minioHelper := helpers.MinioHelper{
			Endpoint:     *minioEndpoint,
			RootUser:     *minioRootUser,
			RootPassword: *minioRootPassword,
		}

		err := minioHelper.InitBucketAndUser(*objectStoreBucket, *minioKaskadaRegion, *minioKaskadaUser, *minioKaskadaPassword, false, true)
		Expect(err).ShouldNot(HaveOccurred())
	}

	helpers.DeleteAllExistingObjects(*objectStoreType, *objectStoreBucket, *objectStorePath)

	helpers.CleanDatabase(dbDialect)
	time.Sleep(time.Second)

	grpcConfig = helpers.HostConfig{
		Hostname: *kaskadaHostname,
		Port:     *kaskadaGrpcPort,
		UseTLS:   *kaskadaUseTLS,
	}
})

func TestCLI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "CLI Suite")
}

func getExpectedResults(localPath string) string {
	file, err := os.Open(localPath)
	Expect(err).ShouldNot(HaveOccurred(), "can't open file")
	defer file.Close()

	data, err := io.ReadAll(file)
	Expect(err).ShouldNot(HaveOccurred())
	return string(data)
}

func getExpectedCSVResults(localPath string) [][]string {
	file, err := os.Open(localPath)
	Expect(err).ShouldNot(HaveOccurred(), "can't open file")

	r := csv.NewReader(file)

	results, err := r.ReadAll()
	Expect(err).ShouldNot(HaveOccurred(), "can't read csv file")
	return results
}

type cliResults struct {
	stdErr   string
	stdOut   string
	exitCode int
}

// runs a kaskada CLI command and returns the results sent to stdOut and stdErr
func runCliCommand(args ...string) cliResults {
	args = append(args, "--kaskada-client-id", *integrationClientID, "--debug")
	cmd := exec.Command("./kaskada_cli", args...)

	var outBytes, errBtyes bytes.Buffer
	cmd.Stdout = &outBytes
	cmd.Stderr = &errBtyes
	err := cmd.Run()

	exitCode := 0
	if exiterr, ok := err.(*exec.ExitError); ok {
		exitCode = exiterr.ExitCode()
	}

	return cliResults{
		stdErr:   errBtyes.String(),
		stdOut:   outBytes.String(),
		exitCode: exitCode,
	}
}
