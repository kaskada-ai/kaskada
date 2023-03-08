package api_test

import (
	"context"

	"github.com/RedisAI/redisai-go/redisai"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = PDescribe("Materialization with redis AI upload", Ordered, Label("redis"), Label("redis-ai"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		redisAIClient         *redisai.Client
		table                 *v1alpha.Table
		tableClient           v1alpha.TableServiceClient
		tableName             string
		materializationClient v1alpha.MaterializationServiceClient
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & materialization services
		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)

		// get a redis connections for verifying results
		redisAIClient = getRedisAIClient(2)

		wipeRedisDatabase(2)

		tableName = "mat_redis_ai"

		// create a table
		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		// load data into the table
		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
	})

	AfterAll(func() {
		// clean up items from the test
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "purchase_min_and_max"})
		// this materialization might not have been created if test had an issue, so we don't check error here
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
		redisAIClient.Close()
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: "purchase_min_and_max",
					Expression: `
{
time: mat_redis_ai.purchase_time,
key: mat_redis_ai.customer_id,
max_amount: mat_redis_ai.amount | max(),
min_amount: mat_redis_ai.amount | min(),
}`,
					Destination: &v1alpha.Materialization_Destination{
						Destination: &v1alpha.Materialization_Destination_Redis{
							Redis: &v1alpha.RedisDestination{
								HostName:       "redis",
								Port:           6379,
								DatabaseNumber: 2,
							},
						},
					},
				},
			}
			res, err := materializationClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
		})

		It("Should upload results to redis", func() {
			Eventually(func(g Gomega) {
				dataType, shape, values, err := redisAIClient.TensorGetValues("karen")
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(dataType).Should(Equal("INT64"))
				g.Expect(shape).Should(Equal([]int64{1, 3}))
				g.Expect(values).Should(Equal([]int64{1578182400000000000, 9, 2}))

				dataType, shape, values, err = redisAIClient.TensorGetValues("patrick")
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(dataType).Should(Equal("INT64"))
				g.Expect(shape).Should(Equal([]int64{1, 3}))
				g.Expect(values).Should(Equal([]int64{1578182400000000000, 5000, 3}))

				_, _, _, err = redisAIClient.TensorGetValues("spongebob")
				g.Expect(err).Should(HaveOccurred())
				g.Expect(err.Error()).Should(ContainSubstring("tensor key is empty"))
			}, "30s", "1s").Should(Succeed())
		})
	})

	Describe("Load the second file into the table", func() {
		It("Should work without error", func() {
			helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
		})

		It("Should upload results to redis", func() {
			Eventually(func(g Gomega) {
				dataType, shape, values, err := redisAIClient.TensorGetValues("karen")
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(dataType).Should(Equal("INT64"))
				g.Expect(shape).Should(Equal([]int64{1, 3}))
				g.Expect(values).Should(Equal([]int64{1578441600000000000, 9, 2}))

				dataType, shape, values, err = redisAIClient.TensorGetValues("patrick")
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(dataType).Should(Equal("INT64"))
				g.Expect(shape).Should(Equal([]int64{1, 3}))
				g.Expect(values).Should(Equal([]int64{1578441600000000000, 5000, 2}))

				dataType, shape, values, err = redisAIClient.TensorGetValues("spongebob")
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(dataType).Should(Equal("INT64"))
				g.Expect(shape).Should(Equal([]int64{1, 3}))
				g.Expect(values).Should(Equal([]int64{1578355200000000000, 34, 7}))
			}, "30s", "1s").Should(Succeed())
		})
	})
})
