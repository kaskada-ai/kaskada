package api_test

import (
	"context"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = FDescribe("Graceful Shutdown test", Ordered, Label("docker"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		materializationClient v1alpha.MaterializationServiceClient
		tableClient           v1alpha.TableServiceClient
		queryClient           v1alpha.QueryServiceClient
		outputSubPath         string
		table                 *v1alpha.Table
		tableName             string
		matName               string
	)

	outputSubPath = "graceful_shutdown"

	tableName = "graceful_shutdown_table"
	matName = "graceful_shutdown_mat"

	query := `
{
time: graceful_shutdown_table.transaction_time,
key: graceful_shutdown_table.id,
max_price: graceful_shutdown_table.price | max(),
min_spent_in_single_transaction: min(graceful_shutdown_table.price * graceful_shutdown_table.quantity),
max_spent_in_single_transaction: max(graceful_shutdown_table.price * graceful_shutdown_table.quantity)
}`

	kaskadaIsDown := false

	terminateKaskada := func() {
		defer GinkgoRecover()

		cmd := exec.Command("docker", "kill", "-s", "SIGTERM", "kaskada")
		err := cmd.Run()
		Expect(err).ShouldNot(HaveOccurred(), "Unable to terminate kaskada")

		Eventually(func(g Gomega) {
			_, err = tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			g.Expect(err).Should(HaveOccurred())
		}, "30s", "1s").Should(Succeed())

		kaskadaIsDown = true
	}

	restartKaskada := func() {
		Eventually(func(g Gomega) {
			if kaskadaIsDown {
				helpers.LogLn("Trying to restart kaskada....")
				cmd := exec.Command("docker", "start", "kaskada")
				err := cmd.Run()
				g.Expect(err).ShouldNot(HaveOccurred(), "Unable to start kaskada")

				time.Sleep(500 * time.Millisecond) //wait for kaskada to start
			}

			//get connection to kaskada
			ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
			ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

			// get a grpc client for the table service
			tableClient = v1alpha.NewTableServiceClient(conn)

			_, err := tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			g.Expect(err).ShouldNot(HaveOccurred())

			materializationClient = v1alpha.NewMaterializationServiceClient(conn)
			queryClient = v1alpha.NewQueryServiceClient(conn)
		}, "30s", "1s").Should(Succeed())

		kaskadaIsDown = false
	}

	BeforeAll(func() {
		if helpers.TestsAreRunningLocally() {
			Skip("tests running locally, skipping gracefull shutdown test")
		}

		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// delete the table and materialization if not cleaned up in the previous run
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: matName})

		// create a table
		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "transaction_time",
			EntityKeyColumnName: "id",
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		// load the first file into the table
		helpers.LoadTestFileIntoTable(ctx, conn, table, "transactions/transactions_part1.parquet")
	})

	AfterAll(func() {
		// clean up items created
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurred())

		cancel()
		conn.Close()
	})

	BeforeEach(func() {
		restartKaskada()
	})

	Context("When the table schema is created correctly", func() {
		Describe("Start a query, and then send a termination signal to Kaskada", func() {
			It("should return query results before exiting", func() {
				destination := &v1alpha.Destination{
					Destination: &v1alpha.Destination_ObjectStore{
						ObjectStore: &v1alpha.ObjectStoreDestination{
							FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						},
					},
				}

				go terminateKaskada()
				stream, err := queryClient.CreateQuery(ctx, &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:     query,
						Destination:    destination,
						ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					},
					QueryOptions: &v1alpha.QueryOptions{
						PresignResults: true,
					},
				})

				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				res, err := helpers.GetMergedCreateQueryResponse(stream)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(res.GetDestination()).ShouldNot(BeNil())
				Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

				resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
				results := helpers.DownloadParquet(resultsUrl)

				Expect(len(results)).Should(Equal(50000))
			})
		})

		Describe("Add a materialization, and then send a termination signal to Kaskada", func() {
			It("create the materialzation without error", func() {
				helpers.EmptyOutputPath(outputSubPath)

				go terminateKaskada()

				destination := &v1alpha.Destination{
					Destination: &v1alpha.Destination_ObjectStore{
						ObjectStore: &v1alpha.ObjectStoreDestination{
							FileType:        v1alpha.FileType_FILE_TYPE_PARQUET,
							OutputPrefixUri: helpers.GetOutputPathURI(outputSubPath),
						},
					},
				}

				res, err := materializationClient.CreateMaterialization(ctx, &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						MaterializationName: "transaction_details",
						Expression:          query,
						Destination:         destination,
					},
				})
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(res).ShouldNot(BeNil())
			})

			It("Should output results to a file before terminating", func() {
				Eventually(func(g Gomega) {
					files := helpers.EventuallyListOutputFiles(outputSubPath, g)
					g.Expect(files).Should(HaveLen(1))

					results := helpers.DownloadParquet(files[0])
					g.Expect(len(results)).Should(Equal(100000))
				}, "30s", "1s").Should(Succeed())
			})
		})

		Describe("Load the second file into the table, and then send a termination signal to Kaskada", func() {
			It("Should work without error", func() {
				helpers.EmptyOutputPath(outputSubPath)

				go terminateKaskada()

				helpers.LoadTestFileIntoTable(ctx, conn, table, "transactions/transactions_part2.parquet")
			})

			It("Should output results to a file before terminating", func() {
				Eventually(func(g Gomega) {
					files := helpers.EventuallyListOutputFiles(outputSubPath, g)
					g.Expect(files).Should(HaveLen(1))

					results := helpers.DownloadParquet(files[0])
					g.Expect(len(results)).Should(Equal(100000))
				}, "30s", "1s").Should(Succeed())
			})
		})

		Describe("Cleeanup the materialization used in the test", func() {
			It("Should work without error", func() {
				_, err := materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: matName})
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
})
