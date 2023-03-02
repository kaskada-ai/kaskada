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

var _ = Describe("Query V1 using shifts with incremental", Ordered, func() {
	var (
		ctx                context.Context
		cancel             context.CancelFunc
		conn               *grpc.ClientConn
		tableClient        v1alpha.TableServiceClient
		queryClient        v1alpha.QueryServiceClient
		table              *v1alpha.Table
		createQueryRequest *v1alpha.CreateQueryRequest
		firstDataTokenId   string
		secondDataTokenId  string
		thirdDataTokenId   string
		fourthDataTokenId  string
		firstResults       []interface{}
		secondResults      []interface{}
		thirdResults       []interface{}
		fourthResults      []interface{}
		tableName          string
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "query_v1_inc_shift"

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
		outputTo := &v1alpha.OutputTo{}
		outputTo.Destination = &v1alpha.OutputTo_ObjectStore{
			ObjectStore: &v1alpha.ObjectStoreDestination{
				FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
			},
		}
		query := &v1alpha.Query{
			Expression: `
{
time: query_v1_inc_shift.purchase_time,
entity: query_v1_inc_shift.customer_id,
max_amount: query_v1_inc_shift.amount | max(),
min_amount: query_v1_inc_shift.amount | min(),
} | shift_to(time=$input.time | add_time(seconds(10))) | extend({shifted_time: time_of($input)})`,
			OutputTo:       outputTo,
			ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
		}
		queryOptions := &v1alpha.QueryOptions{
			ExperimentalFeatures: true,
		}
		createQueryRequest = &v1alpha.CreateQueryRequest{
			Query:        query,
			QueryOptions: queryOptions,
		}

		// load the first file into the table
		res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
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

	Context("When the table schema is created correctly", func() {
		Describe("Run the query", func() {
			It("should return query results", func() {
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
				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Config.DataTokenId).Should(Equal(firstDataTokenId))
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND),
					primitiveSchemaField("shifted_time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND),
					primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
					primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
					primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
				))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(10))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(10))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(2))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}
				Expect(len(resultUrls)).Should(Equal(1))
				firstResults = helpers.DownloadParquet(resultUrls[0])
				Expect(firstResults).Should(HaveLen(2))

				Expect(firstResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578182400000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578182410000000000)),
					"Entity":       PointTo(Equal("karen")),
					"Max_amount":   PointTo(BeEquivalentTo(9)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Load the second file into the table", func() {
			It("Should work without error and return a dataToken", func() {
				res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				secondDataTokenId = res.DataTokenId
				Expect(secondDataTokenId).ShouldNot(Equal(firstDataTokenId))
			})
		})

		Describe("Query resumes from snapshot", func() {
			It("should use existing snapshot to resume from", func() {
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(secondDataTokenId))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(15))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(5))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				secondResults = helpers.DownloadParquet(resultUrls[0])
				Expect(secondResults).Should(HaveLen(3))
				Expect(secondResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578441600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578441610000000000)),
					"Entity":       PointTo(Equal("karen")),
					"Max_amount":   PointTo(BeEquivalentTo(9)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
				Expect(secondResults[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578355200000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578355210000000000)),
					"Entity":       PointTo(Equal("spongebob")),
					"Max_amount":   PointTo(BeEquivalentTo(34)),
					"Min_amount":   PointTo(BeEquivalentTo(7)),
				}))
				Expect(secondResults[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578441600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578441610000000000)),
					"Entity":       PointTo(Equal("patrick")),
					"Max_amount":   PointTo(BeEquivalentTo(5000)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Load the third file into the table", func() {
			It("Should work without error and return a dataToken", func() {
				res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part3.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				thirdDataTokenId = res.DataTokenId
				Expect(thirdDataTokenId).ShouldNot(Equal(secondDataTokenId))
			})
		})

		Describe("Query resumes from snapshot", func() {
			It("should use existing snapshot to resume from", func() {
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(thirdDataTokenId))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(18))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(3))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				thirdResults = helpers.DownloadParquet(resultUrls[0])
				Expect(thirdResults).Should(HaveLen(3))
				Expect(thirdResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578441600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578441610000000000)),
					"Entity":       PointTo(Equal("karen")),
					"Max_amount":   PointTo(BeEquivalentTo(9)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
				Expect(thirdResults[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578355200000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578355210000000000)),
					"Entity":       PointTo(Equal("spongebob")),
					"Max_amount":   PointTo(BeEquivalentTo(34)),
					"Min_amount":   PointTo(BeEquivalentTo(7)),
				}))
				Expect(thirdResults[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1610409600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1610409610000000000)),
					"Entity":       PointTo(Equal("patrick")),
					"Max_amount":   PointTo(BeEquivalentTo(5000)),
					"Min_amount":   PointTo(BeEquivalentTo(1)),
				}))
			})
		})

		// The fourth file contains late data
		Describe("Load the fourth file into the table", func() {
			It("Should work without error and return a dataToken", func() {
				res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part4_late.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				fourthDataTokenId = res.DataTokenId
				Expect(fourthDataTokenId).ShouldNot(Equal(thirdDataTokenId))
			})
		})

		Describe("Query resumes from last valid snapshot", func() {
			It("should find the latest VALID snapshot to resume from (which is not the most recent)", func() {
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(fourthDataTokenId))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(21))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(6)) // This includes values from the third input file
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				fourthResults = helpers.DownloadParquet(resultUrls[0])
				Expect(fourthResults).Should(HaveLen(3))
				Expect(fourthResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578441600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578441610000000000)),
					"Entity":       PointTo(Equal("karen")),
					"Max_amount":   PointTo(BeEquivalentTo(9)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
				Expect(fourthResults[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578355200000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578355210000000000)),
					"Entity":       PointTo(Equal("spongebob")),
					"Max_amount":   PointTo(BeEquivalentTo(34)),
					"Min_amount":   PointTo(BeEquivalentTo(7)),
				}))
				Expect(fourthResults[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1611273600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1611273610000000000)),
					"Entity":       PointTo(Equal("patrick")),
					"Max_amount":   PointTo(BeEquivalentTo(5000)),
					"Min_amount":   PointTo(BeEquivalentTo(1)),
				}))
			})
		})

		Describe("Deleting and recreating table does not find existing snapshots", func() {
			It("Should not find existing snapshot to resume from", func() {
				// delete the table
				tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: table.TableName})

				// recreate table with no inputs
				_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
				Expect(err).ShouldNot(HaveOccurredGrpc())

				// Load in just the first file
				res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				VerifyRequestDetails(res.RequestDetails)
				firstDataTokenId = res.DataTokenId

				// Run the query with the new table
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(firstDataTokenId))
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND),
					primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING),
					primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
					primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64),
				))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(10))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(10))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(2))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}
				Expect(len(resultUrls)).Should(Equal(1))

				// The results should be equivalent to the results produced with just the
				// first file. If using just the table name to find snapshots, the full
				// snapshot with all files would've been used to produce results.
				firstResults = helpers.DownloadParquet(resultUrls[0])
				Expect(firstResults).Should(HaveLen(2))
				Expect(firstResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578182400000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Verify results are similar to non-incremental", func() {
			It("Should create equivalent table and output same results", func() {
				// delete the table
				tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "purchases_non_incremental"})

				// create a table
				table_non_incremental := &v1alpha.Table{
					TableName:           "purchases_non_incremental",
					TimeColumnName:      "purchase_time",
					EntityKeyColumnName: "customer_id",
					SubsortColumnName: &wrapperspb.StringValue{
						Value: "subsort_id",
					},
				}

				_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table_non_incremental})
				Expect(err).ShouldNot(HaveOccurredGrpc())

				res := helpers.LoadTestFileIntoTable(ctx, conn, table_non_incremental, "purchases/purchases_part1.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())

				res = helpers.LoadTestFileIntoTable(ctx, conn, table_non_incremental, "purchases/purchases_part2.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				secondDataTokenId = res.DataTokenId
				Expect(secondDataTokenId).ShouldNot(Equal(firstDataTokenId))

				res = helpers.LoadTestFileIntoTable(ctx, conn, table_non_incremental, "purchases/purchases_part3.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				thirdDataTokenId = res.DataTokenId
				Expect(thirdDataTokenId).ShouldNot(Equal(secondDataTokenId))

				res = helpers.LoadTestFileIntoTable(ctx, conn, table_non_incremental, "purchases/purchases_part4_late.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				fourthDataTokenId = res.DataTokenId
				Expect(fourthDataTokenId).ShouldNot(Equal(thirdDataTokenId))
			})

			It("Should run the query and get same results as incremental", func() {
				outputTo := &v1alpha.OutputTo{}
				outputTo.Destination = &v1alpha.OutputTo_ObjectStore{
					ObjectStore: &v1alpha.ObjectStoreDestination{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
					},
				}
				query := &v1alpha.Query{
					Expression: `
{
time: purchases_non_incremental.purchase_time,
entity: purchases_non_incremental.customer_id,
max_amount: purchases_non_incremental.amount | max(),
min_amount: purchases_non_incremental.amount | min(),
} | shift_to(time=$input.time | add_time(seconds(10))) | extend({shifted_time: time_of($input)})`,
					OutputTo:       outputTo,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
				}
				queryOptions := &v1alpha.QueryOptions{
					ExperimentalFeatures: true,
				}
				createQueryRequest = &v1alpha.CreateQueryRequest{
					Query:        query,
					QueryOptions: queryOptions,
				}

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
				// Expect(firstResponse.Config.DataTokenId).Should(Equal(fourthDataTokenId))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(21))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(21))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetOutputTo().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				fourthResults = helpers.DownloadParquet(resultUrls[0])
				Expect(fourthResults).Should(HaveLen(3))
				Expect(fourthResults[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578441600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578441610000000000)),
					"Entity":       PointTo(Equal("karen")),
					"Max_amount":   PointTo(BeEquivalentTo(9)),
					"Min_amount":   PointTo(BeEquivalentTo(2)),
				}))
				Expect(fourthResults[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1578355200000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1578355210000000000)),
					"Entity":       PointTo(Equal("spongebob")),
					"Max_amount":   PointTo(BeEquivalentTo(34)),
					"Min_amount":   PointTo(BeEquivalentTo(7)),
				}))
				Expect(fourthResults[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":         PointTo(BeEquivalentTo(1611273600000000000)),
					"Shifted_time": PointTo(BeEquivalentTo(1611273610000000000)),
					"Entity":       PointTo(Equal("patrick")),
					"Max_amount":   PointTo(BeEquivalentTo(5000)),
					"Min_amount":   PointTo(BeEquivalentTo(1)),
				}))
			})

			It("Should delete puchases_non_incremental without error", func() {
				_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "purchases_non_incremental"})
				Expect(err).ShouldNot(HaveOccurredGrpc())
			})
		})
	})
})
