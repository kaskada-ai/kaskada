package api_test

import (
	"context"

	"github.com/google/uuid"
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

var _ = Describe("Query V1 gRPC with multiple tables", Ordered, func() {
	var (
		ctx                context.Context
		cancel             context.CancelFunc
		conn               *grpc.ClientConn
		tableClient        v1alpha.TableServiceClient
		queryClient        v1alpha.QueryServiceClient
		memberTable        *v1alpha.Table
		transactionTable   *v1alpha.Table
		createQueryRequest *v1alpha.CreateQueryRequest
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// create the tables
		transactionTable = &v1alpha.Table{
			TableName:           "Transaction",
			TimeColumnName:      "transaction_time",
			EntityKeyColumnName: "id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "idx",
			},
			GroupingId: "User",
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: transactionTable})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		memberTable = &v1alpha.Table{
			TableName:           "Member",
			TimeColumnName:      "registration_date",
			EntityKeyColumnName: "id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "idx",
			},
			GroupingId: "User",
		}
		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: memberTable})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		//load data into the tables
		helpers.LoadTestFileIntoTable(ctx, conn, memberTable, "transactions/members.parquet")
		helpers.LoadTestFileIntoTable(ctx, conn, transactionTable, "transactions/transactions_part1.parquet")

		// define a query to run on the table
		query := &v1alpha.Query{
			Expression: `
# 1. Data Cleaning

let meaningful_txns = Transaction 
| when(Transaction.price > 100)
| when(time_of(Transaction.transaction_time) > ("2000-01-01T00:00:00Z" as timestamp_ns))

let membership = lookup(meaningful_txns.purchaser_id | last(), Member | first())

in {
price: meaningful_txns.price,
quantity: meaningful_txns.quantity,
membership_date : membership.registration_date,
member_name : membership.name
}`,
			ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
			ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
		}
		queryOptions := &v1alpha.QueryOptions{
			ExperimentalFeatures: false,
		}
		createQueryRequest = &v1alpha.CreateQueryRequest{
			Query:        query,
			QueryOptions: queryOptions,
		}
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "Member"})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "Transaction"})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Context("When the table schema is created correctly", Ordered, func() {
		Describe("Run the streaming query using dry-run", func() {
			It("should return a single response with schema info, but no results", func() {
				createQueryRequest.QueryOptions.DryRun = true
				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				createQueryRequest.QueryOptions.DryRun = false
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(queryResponses)).Should(Equal(1))

				firstResponse := queryResponses[0]

				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Config.DataTokenId).ShouldNot(BeEmpty())
				Expect(firstResponse.GetFileResults()).Should(BeNil())
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					primitiveSchemaField("price", v1alpha.DataType_PRIMITIVE_TYPE_F64),
					primitiveSchemaField("quantity", v1alpha.DataType_PRIMITIVE_TYPE_I64),
					primitiveSchemaField("membership_date", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
					primitiveSchemaField("member_name", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
				))
			})
		})

		Describe("Run the streaming query without specifying a dataToken", func() {
			It("should return at least 3 responses, with results from the first file", func() {
				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

				var firstResponse, secondResponse, thirdResponse, lastResponse *v1alpha.CreateQueryResponse

				firstResponse, queryResponses = queryResponses[0], queryResponses[1:]
				secondResponse, queryResponses = queryResponses[0], queryResponses[1:]
				thirdResponse, queryResponses = queryResponses[0], queryResponses[1:]

				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					primitiveSchemaField("price", v1alpha.DataType_PRIMITIVE_TYPE_F64),
					primitiveSchemaField("quantity", v1alpha.DataType_PRIMITIVE_TYPE_I64),
					primitiveSchemaField("membership_date", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
					primitiveSchemaField("member_name", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
				))

				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(75000))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(75000))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(32699))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetFileResults() != nil {
						resultUrls = append(resultUrls, queryResponse.GetFileResults().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])

				Expect(results).Should(HaveLen(32699))
				Expect(results[30005]).Should(MatchFields(IgnoreExtras, Fields{
					"Price":           PointTo(BeEquivalentTo(211.91)),
					"Quantity":        PointTo(BeEquivalentTo(8)),
					"Membership_date": PointTo(Equal("1993-02-21")),
					"Member_name":     PointTo(BeEquivalentTo("Ryan Leonard")),
				}))
			})
		})
	})
})
