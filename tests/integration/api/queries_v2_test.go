package api_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Queries V2", Ordered, func() {
	var (
		ctx           context.Context
		cancel        context.CancelFunc
		conn          *grpc.ClientConn
		queryV2Client v2alpha.QueryServiceClient
		tableClient   v1alpha.TableServiceClient
		tableName     string
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table service
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryV2Client = v2alpha.NewQueryServiceClient(conn)

		// list and delete any existing v2 queries
		res, err := queryV2Client.ListQueries(ctx, &v2alpha.ListQueriesRequest{})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(res).ShouldNot(BeNil())
		for _, query := range res.Queries {
			Expect(queryV2Client.DeleteQuery(ctx, &v2alpha.DeleteQueryRequest{QueryId: query.QueryId})).Error().ShouldNot(HaveOccurredGrpc())
		}

		//setup other vars
		tableName = "purchases_queries_v2"

		//initialize table to query against
		createTableRequest := &v1alpha.CreateTableRequest{
			Table: &v1alpha.Table{
				TableName:           tableName,
				TimeColumnName:      "purchase_time",
				EntityKeyColumnName: "customer_id",
				SubsortColumnName: &wrapperspb.StringValue{
					Value: "subsort_id",
				},
			},
		}
		createResponse, err := tableClient.CreateTable(ctx, createTableRequest)
		Expect(err).ShouldNot(HaveOccurredGrpc())
		helpers.LoadTestFileIntoTable(ctx, conn, createResponse.Table, "purchases/purchases_part1.parquet")
	})

	AfterAll(func() {
		Expect(tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})).Error().ShouldNot(HaveOccurredGrpc())
		cancel()
		conn.Close()
	})

	Context("before creating queries", func() {
		Describe("LIST Query", func() {
			It("should work and return no results", func() {
				res, err := queryV2Client.ListQueries(ctx, &v2alpha.ListQueriesRequest{})
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(res).ShouldNot(BeNil())
				Expect(len(res.Queries)).Should(Equal(0))
			})
		})
	})

	Describe("CREATE Query", func() {
		Context("with only an expression", func() {
			It("should create a query resource assuming defaults for all config values", func() {
				createQueryRequest := &v2alpha.CreateQueryRequest{
					Expression: tableName,
				}
				res, err := queryV2Client.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(res).ShouldNot(BeNil())

				Expect(res.RequestDetails).ShouldNot(BeNil())
				Expect(res.RequestDetails.RequestId).Should(HaveLen(32))

				query := res.Query
				Expect(query).ShouldNot(BeNil())
				Expect(query.Expression).Should(Equal(tableName))
				Expect(query.Views.Views).Should(HaveLen(0))
				Expect(query.CreateTime.AsTime()).Should(BeTemporally("~", time.Now(), time.Second))
				Expect(query.UpdateTime.AsTime()).Should(BeTemporally("~", time.Now(), time.Second))
				Expect(query.State).Should(Equal(v2alpha.QueryState_QUERY_STATE_COMPILED))

				Expect(query.Metrics).ShouldNot(BeNil())
				Expect(query.Results).ShouldNot(BeNil())

				config := query.Config
				Expect(config).ShouldNot(BeNil())
				Expect(config.DataToken.GetLatestDataToken().DataTokenId).ShouldNot(BeNil())
				Expect(config.ExperimentalFeatures).Should(BeNil())
				Expect(config.Limits).Should(BeNil())
				Expect(config.Destination.GetObjectStore()).ShouldNot(BeNil())
				Expect(config.Destination.GetRedis()).Should(BeNil())
				Expect(config.ResultBehavior.GetAllResults()).ShouldNot(BeNil())
				Expect(config.ResultBehavior.GetFinalResults()).Should(BeNil())
				Expect(config.Slice).Should(BeNil())

				metrics := query.Metrics
				Expect(metrics).ShouldNot(BeNil())
				Expect(metrics.OutputFiles).Should(Equal(int64(0)))
				Expect(metrics.TimePreparing.AsDuration().Seconds()).Should(BeNumerically("==", 0))
				Expect(metrics.TimeComputing.AsDuration().Seconds()).Should(BeNumerically("==", 0))

				results := query.Results
				Expect(results).ShouldNot(BeNil())
				Expect(results.FenlDiagnostics.FenlDiagnostics).Should(HaveLen(0))
				Expect(results.FenlDiagnostics.NumErrors).Should(BeNumerically("==", 0))
				Expect(results.Output.GetFileResults().Paths).Should(HaveLen(0))
				Expect(results.Schema).ShouldNot(BeNil())
			})
		})
	})

	Context("after creating queries", func() {
		Describe("LIST Query", func() {
			It("should work with some results", func() {
				res, err := queryV2Client.ListQueries(ctx, &v2alpha.ListQueriesRequest{})
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(res).ShouldNot(BeNil())
				Expect(len(res.Queries)).Should(BeNumerically(">=", 0))
			})
		})

		Describe("GET Query", func() {
			It("invalid query id should return friendly error message", func() {
				res, err := queryV2Client.GetQuery(ctx, &v2alpha.GetQueryRequest{
					QueryId: "some-non-existing-query",
				})
				Expect(err).Should(HaveOccurredGrpc())
				Expect(res).Should(BeNil())
				errStatus, ok := status.FromError(err)

				Expect(ok).Should(BeTrue())
				Expect(errStatus.Code()).Should(Equal(codes.InvalidArgument))
				Expect(errStatus.Message()).Should(ContainSubstring("query_id"))
			})
			It("non-existent query id should return friendly error message", func() {
				res, err := queryV2Client.GetQuery(ctx, &v2alpha.GetQueryRequest{
					QueryId: uuid.NewString(),
				})
				Expect(err).Should(HaveOccurredGrpc())
				Expect(res).Should(BeNil())
				errStatus, ok := status.FromError(err)

				Expect(ok).Should(BeTrue())
				Expect(errStatus.Code()).Should(Equal(codes.NotFound))
				Expect(errStatus.Message()).Should(ContainSubstring("query not found"))
			})
		})
	})
})
