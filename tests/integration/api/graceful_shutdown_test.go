package api_test

import (
	"context"
	"os/exec"
	"time"

	"github.com/RedisAI/redisai-go/redisai"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = PDescribe("Graceful Shutdown test", Ordered, Label("redis"), Label("redis-ai"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		key1                  string
		key2                  string
		redisAIClient         *redisai.Client
		materializationClient v1alpha.MaterializationServiceClient
		tableClient           v1alpha.TableServiceClient
		queryClient           v1alpha.QueryServiceClient
		table                 *v1alpha.Table
	)

	query := `
{
time: transactions.transaction_time,
key: transactions.id,
max_price: transactions.price | max(),
min_spent_in_single_transaction: min(transactions.price * transactions.quantity)
max_spent_in_single_transaction: max(transactions.price * transactions.quantity)
}`

	redisDb := 4
	kaskadaIsDown := false

	terminateKaskada := func() {
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
		}, "30s", "1s").Should(Succeed())

		kaskadaIsDown = false

		// get a grpc client for the materialization & compute services
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)
	}

	BeforeAll(func() {
		// get a redis connections for verifying results
		redisAIClient = getRedisAIClient(redisDb)

		wipeRedisDatabase(redisDb)

		// declare the keys we are testing for
		key1 = "Symdt3HKIYEFyzRCgdQl2/OKVBzjl7aO1XcKd7o70wM="
		key2 = "c5obkiyX5gof2EdzWlYbXZ98xfu+cpjxxvANgTfRNzM="

		// delete the table and materialization if not cleaned up in the previous run
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "transactions"})
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "transaction_details"})

		// create a table
		table = &v1alpha.Table{
			TableName:           "transactions",
			TimeColumnName:      "transaction_time",
			EntityKeyColumnName: "id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "idx",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		// load the first file into the table
		helpers.LoadTestFileIntoTable(ctx, conn, table, "transactions/transactions_part1.parquet")
	})

	AfterAll(func() {
		// clean up items created
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "transaction_details"})
		// this materialization might not have been created if test had an issue, so we don't check error here
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "transactions"})
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
				go terminateKaskada()

				stream, err := queryClient.CreateQuery(ctx, &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:     query,
						ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
						ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					},
				})

				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				res, err := helpers.GetMergedCreateQueryResponse(stream)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(res.GetFileResults()).ShouldNot(BeNil())
				Expect(res.GetFileResults().Paths).Should(HaveLen(1))

				resultsUrl := res.GetFileResults().Paths[0]
				results := helpers.DownloadParquet(resultsUrl)

				Expect(len(results)).Should(Equal(100000))
				Expect(results).Should(ContainElement(MatchFields(IgnoreExtras, Fields{
					"Time":           PointTo(BeEquivalentTo(20150106)),
					"Key":            PointTo(Equal(key1)),
					"Max_list_price": PointTo(BeEquivalentTo(149)),
					"Min_paid":       PointTo(BeEquivalentTo(149)),
				})))

				Expect(results).Should(ContainElement(MatchFields(IgnoreExtras, Fields{
					"Time":           PointTo(BeEquivalentTo(20150104)),
					"Key":            PointTo(Equal(key2)),
					"Max_list_price": PointTo(BeEquivalentTo(149)),
					"Min_paid":       PointTo(BeEquivalentTo(149)),
				})))
			})
		})

		Describe("Add a materialization, and then send a termination signal to Kaskada", func() {
			It("create the materialzation without error", func() {
				go terminateKaskada()

				res, err := materializationClient.CreateMaterialization(ctx, &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						MaterializationName: "transaction_details",
						Query:               query,
						Destination: &v1alpha.Materialization_Destination{
							Destination: &v1alpha.Materialization_Destination_Redis{
								Redis: &v1alpha.RedisDestination{
									HostName:       "redis",
									Port:           6379,
									DatabaseNumber: int32(redisDb),
								},
							},
						},
					},
				})
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(res).ShouldNot(BeNil())
			})

			It("Should upload results to redis before terminating", func() {
				Eventually(func(g Gomega) {
					dataType, shape, values, err := redisAIClient.TensorGetValues(key1)
					g.Expect(err).ShouldNot(HaveOccurred())
					g.Expect(dataType).Should(Equal("INT64"))
					g.Expect(shape).Should(Equal([]int64{1, 3}))
					g.Expect(values).Should(Equal([]int64{20150106, 149, 149}))

					dataType, shape, values, err = redisAIClient.TensorGetValues(key2)
					g.Expect(err).ShouldNot(HaveOccurred())
					g.Expect(dataType).Should(Equal("INT64"))
					g.Expect(shape).Should(Equal([]int64{1, 3}))
					g.Expect(values).Should(Equal([]int64{20150104, 149, 149}))
				}, "30s", "1s").Should(Succeed())
			})
		})

		Describe("Load the second file into the table, and then send a termination signal to Kaskada", func() {
			It("Should work without error", func() {
				go terminateKaskada()

				helpers.LoadTestFileIntoTable(ctx, conn, table, "transactions/transactions_part2.parquet")
			})

			It("Should upload new results to redis before terminating", func() {
				Eventually(func(g Gomega) {
					dataType, shape, values, err := redisAIClient.TensorGetValues(key1)
					g.Expect(err).ShouldNot(HaveOccurred())
					g.Expect(dataType).Should(Equal("INT64"))
					g.Expect(shape).Should(Equal([]int64{1, 3}))
					g.Expect(values).Should(Equal([]int64{20150109, 149, 100}))

					dataType, shape, values, err = redisAIClient.TensorGetValues(key2)
					g.Expect(err).ShouldNot(HaveOccurred())
					g.Expect(dataType).Should(Equal("INT64"))
					g.Expect(shape).Should(Equal([]int64{1, 3}))
					g.Expect(values).Should(Equal([]int64{20150111, 149, 149}))
				}, "30s", "1s").Should(Succeed())
			})
		})
	})
})
