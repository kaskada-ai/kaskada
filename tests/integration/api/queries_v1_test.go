package api_test

import (
	"context"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	. "github.com/kaskada-ai/kaskada/tests/integration/api/matchers"
)

var _ = Describe("Queries V1", Ordered, func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		conn   *grpc.ClientConn

		queryClient v1alpha.QueryServiceClient
		tableClient v1alpha.TableServiceClient
		tableName   string
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = getContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table service
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "queries_v1_test"

		// create table, load data
		table := &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		uploadRes := loadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
		Expect(uploadRes).ShouldNot(BeNil())
	})

	AfterAll(func() {
		_, _ = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{
			TableName: tableName,
		})
		cancel()
		conn.Close()
	})

	Describe("LIST Query", func() {
		It("should work with some or none results", func() {
			res, err := queryClient.ListQueries(ctx, &v1alpha.ListQueriesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(len(res.Queries)).Should(BeNumerically(">=", 0))
		})
	})

	Context("When a query resource is created", func() {
		queryId := ""
		It("should create a query resource", func() {
			query := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     tableName,
					ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
				QueryOptions: &v1alpha.QueryOptions{},
			}
			stream, err := queryClient.CreateQuery(ctx, query)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())
			queryResponses, err := getCreateQueryResponses(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

			lastResponse, responses := queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
			Expect(lastResponse).ShouldNot(BeNil())
			Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
			var validUUID = false
			for _, queryResponse := range responses {
				if queryResponse.GetQueryId() != "" {
					_, err := uuid.Parse(queryResponse.GetQueryId())
					Expect(err).Should(BeNil())
					validUUID = true
					queryId = queryResponse.GetQueryId()
				}
			}
			Expect(validUUID).Should(BeTrue())
		})
		It("should get an existing query resource", func() {
			queryUUID, err := uuid.Parse(queryId)
			Expect(err).Should(BeNil())
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
				QueryId: queryUUID.String(),
			})
			Expect(err).Should(BeNil())
			Expect(res.Query.QueryId).Should(Equal(queryUUID.String()))
		})
		It("should list the queries with newly created resource", func() {
			res, err := queryClient.ListQueries(ctx, &v1alpha.ListQueriesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			var found = false
			for _, query := range res.Queries {
				if query.QueryId == queryId {
					found = true
				}
			}
			Expect(found).Should(BeTrue())
		})
	})

	Describe("GET Query", func() {
		It("invalid query id should return friendly error message", func() {
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
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
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
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
