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

	. "github.com/kaskada/kaskada-ai/tests/integration/api/matchers"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

var _ = Describe("Incremental query V1 with late data", Ordered, func() {
	var (
		ctx                context.Context
		cancel             context.CancelFunc
		conn               *grpc.ClientConn
		tableClient        v1alpha.TableServiceClient
		queryClient        v1alpha.QueryServiceClient
		table              *v1alpha.Table
		tableName          string
		createQueryRequest *v1alpha.CreateQueryRequest
		firstDataTokenId   string
		secondDataTokenId  string
		firstResults       []interface{}
		secondResults      []interface{}
	)

	tableName = "purchases_late_data"

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = getContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

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

		// define a basic (single-pass) query to run on the table
		query := &v1alpha.Query{
			Expression: `
						{
						time: purchases_late_data.purchase_time,
						entity: purchases_late_data.customer_id,
						max_amount: purchases_late_data.amount | max(),
						min_amount: purchases_late_data.amount | min(),
						}`,
			ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
			ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
		}
		queryOptions := &v1alpha.QueryOptions{
			ExperimentalFeatures: true,
		}
		createQueryRequest = &v1alpha.CreateQueryRequest{
			Query:        query,
			QueryOptions: queryOptions,
		}

		// load data into the table
		res := loadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part3.parquet")
		Expect(res.DataTokenId).ShouldNot(BeEmpty())
		firstDataTokenId = res.DataTokenId
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Context("When the table schema is created correctly", Ordered, func() {
		Describe("Run the query", func() {
			It("should return query results", func() {
				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := getCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

				var firstResponse, secondResponse, thirdResponse, lastResponse *v1alpha.CreateQueryResponse

				firstResponse, queryResponses = queryResponses[0], queryResponses[1:]
				secondResponse, queryResponses = queryResponses[0], queryResponses[1:]
				thirdResponse, queryResponses = queryResponses[0], queryResponses[1:]

				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Config.DataTokenId).Should(Equal(firstDataTokenId))
				// Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
				// 	primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_MICROSECOND),
				// 	primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
				// 	primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
				// 	primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
				// ))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(3))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(3))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(1))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetFileResults() != nil {
						resultUrls = append(resultUrls, queryResponse.GetFileResults().GetPaths()...)
					}
				}
				Expect(len(resultUrls)).Should(Equal(1))
				firstResults = downloadParquet(resultUrls[0])
				Expect(firstResults).Should(HaveLen(1))
				Expect(firstResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1610409600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(55)),
					"Min_amount": PointTo(BeEquivalentTo(1)),
				}))
			})
		})

		Describe("Load the second file with late date into the table", func() {
			It("Should work without error and return a dataToken", func() {
				res := loadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part4_late.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				secondDataTokenId = res.DataTokenId
				Expect(secondDataTokenId).ShouldNot(Equal(firstDataTokenId))
			})
		})

		Describe("Query does not resume from snapshot", func() {
			// This tests that we don't have panics or unexpected results when using late data.
			// However, the integration tests can't currently test that we use the correct snapshot
			// or delete invalid snapshots correctly, since an incremental query should return
			// equivalent results to a non-incremental one.
			It("should not use existing snapshot due to late data", func() {
				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := getCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

				var firstResponse, secondResponse, thirdResponse, lastResponse *v1alpha.CreateQueryResponse

				firstResponse, queryResponses = queryResponses[0], queryResponses[1:]
				secondResponse, queryResponses = queryResponses[0], queryResponses[1:]
				thirdResponse, queryResponses = queryResponses[0], queryResponses[1:]
				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Config.DataTokenId).Should(Equal(secondDataTokenId))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(6))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(6))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(1))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetFileResults() != nil {
						resultUrls = append(resultUrls, queryResponse.GetFileResults().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				secondResults = downloadParquet(resultUrls[0])
				Expect(secondResults).Should(HaveLen(1))
				Expect(secondResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1611273600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(555)),
					"Min_amount": PointTo(BeEquivalentTo(1)),
				}))
			})
		})
	})
})
