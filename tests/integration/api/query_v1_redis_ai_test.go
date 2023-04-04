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

var _ = PDescribe("Query V1 gRPC with redis AI upload", Ordered, Label("redis"), Label("redis-ai"), func() {
	var (
		ctx                context.Context
		cancel             context.CancelFunc
		conn               *grpc.ClientConn
		createQueryRequest *v1alpha.CreateQueryRequest
		redisAIClient      *redisai.Client
		table              *v1alpha.Table
		tableClient        v1alpha.TableServiceClient
		queryClient        v1alpha.QueryServiceClient
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// get a redis connections for verifying results
		redisAIClient = getRedisAIClient(1)

		wipeRedisDatabase(1)

		// create a table, load data
		table = &v1alpha.Table{
			TableName:           "purchases_redis_ai",
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")

		// define a query to run on the table
		destination := &v1alpha.Destination{}
		destination.Destination = &v1alpha.Destination_Redis{
			Redis: &v1alpha.RedisDestination{
				HostName:       "redis",
				Port:           6379,
				DatabaseNumber: 1,
			},
		}

		createQueryRequest = &v1alpha.CreateQueryRequest{
			Query: &v1alpha.Query{
				Expression: `
				{
				time: purchases_redis_ai.purchase_time,
				key: purchases_redis_ai.customer_id,
				max_amount: purchases_redis_ai.amount | max(),
				min_amount: purchases_redis_ai.amount | min(),
				}`,
				Destination: destination,
			},
		}
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "purchases_redis_ai"})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
		redisAIClient.Close()
	})

	Describe("Run a simple query with redis-ai upload on the first file", func() {
		It("should return an empty message and write the results to redis-ai", func() {
			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.GetDestination().GetRedis()).ShouldNot(BeNil())

			//verify results
			dataType, shape, values, err := redisAIClient.TensorGetValues("karen")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dataType).Should(Equal("INT64"))
			Expect(shape).Should(Equal([]int64{1, 3}))
			Expect(values).Should(Equal([]int64{1578182400000000000, 9, 2}))

			dataType, shape, values, err = redisAIClient.TensorGetValues("patrick")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dataType).Should(Equal("INT64"))
			Expect(shape).Should(Equal([]int64{1, 3}))
			Expect(values).Should(Equal([]int64{1578182400000000000, 5000, 3}))

			_, _, _, err = redisAIClient.TensorGetValues("spongebob")
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("tensor key is empty"))
		})
	})

	Describe("Load the second file into the table", func() {
		It("Should work without error", func() {
			helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
		})
	})

	Describe("Run a simple query with redis-ai upload on the both files", func() {
		It("should return an empty message and write the results to redis-ai", func() {
			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.GetDestination().GetRedis()).ShouldNot(BeNil())

			//verify results
			dataType, shape, values, err := redisAIClient.TensorGetValues("karen")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dataType).Should(Equal("INT64"))
			Expect(shape).Should(Equal([]int64{1, 3}))
			Expect(values).Should(Equal([]int64{1578441600000000000, 9, 2}))

			dataType, shape, values, err = redisAIClient.TensorGetValues("patrick")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dataType).Should(Equal("INT64"))
			Expect(shape).Should(Equal([]int64{1, 3}))
			Expect(values).Should(Equal([]int64{1578441600000000000, 5000, 2}))

			dataType, shape, values, err = redisAIClient.TensorGetValues("spongebob")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dataType).Should(Equal("INT64"))
			Expect(shape).Should(Equal([]int64{1, 3}))
			Expect(values).Should(Equal([]int64{1578355200000000000, 34, 7}))
		})
	})
})
