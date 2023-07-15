package api_test

import (
	"context"
	"strings"

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

	"github.com/jt-nti/gproto"
)

var _ = Describe("Query V1 with incremental", Ordered, func() {
	var (
		ctx                        context.Context
		cancel                     context.CancelFunc
		conn                       *grpc.ClientConn
		tableClient                v1alpha.TableServiceClient
		queryClient                v1alpha.QueryServiceClient
		tableIncremental           *v1alpha.Table
		tableNonIncremental        *v1alpha.Table
		expressionIncremental      string
		expressionNonIncremental   string
		dataTokenIncremental1      string
		dataTokenIncremental2      string
		dataTokenNonIncremental    string
		queryRequestIncremental    *v1alpha.CreateQueryRequest
		queryRequestNonIncremental *v1alpha.CreateQueryRequest
		res                        *v1alpha.LoadDataResponse
		err                        error
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// create tables
		tableIncremental = &v1alpha.Table{
			TableName:           getUniqueName("purchases_incremental"),
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}

		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: tableIncremental})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		tableNonIncremental = &v1alpha.Table{
			TableName:           getUniqueName("purchases_non_incremental"),
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}

		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: tableNonIncremental})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		expressionTemplate := `
		{
		time: table.purchase_time,
		entity: table.customer_id,
		max_amount: table.amount | max(),
		min_amount: table.amount | min(),
		}`

		expressionIncremental = strings.ReplaceAll(expressionTemplate, "table", tableIncremental.TableName)
		expressionNonIncremental = strings.ReplaceAll(expressionTemplate, "table", tableNonIncremental.TableName)

		res = helpers.LoadTestFileIntoTable(ctx, conn, tableIncremental, "purchases/purchases_part1.parquet")
		Expect(res.DataTokenId).ShouldNot(BeEmpty())
		dataTokenIncremental1 = res.DataTokenId

		destination := &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			},
		}

		queryRequestIncremental = &v1alpha.CreateQueryRequest{
			Query: &v1alpha.Query{
				Expression:     expressionIncremental,
				Destination:    destination,
				ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
			},
			QueryOptions: &v1alpha.QueryOptions{
				ExperimentalFeatures: true,
				PresignResults:       true,
			},
		}

		queryRequestNonIncremental = &v1alpha.CreateQueryRequest{
			Query: &v1alpha.Query{
				Expression:     expressionNonIncremental,
				Destination:    destination,
				ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_FINAL_RESULTS,
			},
			QueryOptions: &v1alpha.QueryOptions{
				ExperimentalFeatures: true,
				PresignResults:       true,
			},
		}
	})

	AfterAll(func() {
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableIncremental.TableName})
		Expect(err).ShouldNot(HaveOccurred())

		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableNonIncremental.TableName})
		Expect(err).ShouldNot(HaveOccurred())

		cancel()
		conn.Close()
	})

	Context("When the table schema is created correctly", func() {
		Describe("Run the query", func() {
			It("should return query results", func() {
				stream, err := queryClient.CreateQuery(ctx, queryRequestIncremental)
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenIncremental1))
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					gproto.Equal(primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND)),
					gproto.Equal(primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING)),
					gproto.Equal(primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
					gproto.Equal(primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
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

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(2))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578182400000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Load the second file into the table", func() {
			It("Should work without error and return a dataToken", func() {
				res := helpers.LoadTestFileIntoTable(ctx, conn, tableIncremental, "purchases/purchases_part2.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				dataTokenIncremental2 = res.DataTokenId
				Expect(dataTokenIncremental2).ShouldNot(Equal(dataTokenIncremental1))
			})
		})

		Describe("Query resumes from snapshot", func() {
			// This isn't verifying that we're resuming from a snapshot perse, since
			// the results of not finding a snapshot and finding one are equivalent.
			//
			// We should be able to verify that incremental snapshots
			// were used by exposing the `num_rows_processed`.
			It("should use existing snapshot to resume from", func() {
				stream, err := queryClient.CreateQuery(ctx, queryRequestIncremental)
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenIncremental2))
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

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(3))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
				Expect(results[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578355200000000000)),
					"Entity":     PointTo(Equal("spongebob")),
					"Max_amount": PointTo(BeEquivalentTo(34)),
					"Min_amount": PointTo(BeEquivalentTo(7)),
				}))
				Expect(results[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(5000)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Query multiple times without new data", func() {
			It("Should use existing snapshot and return same results", func() {
				stream, err := queryClient.CreateQuery(ctx, queryRequestIncremental)
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenIncremental2))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(15))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(0))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(3))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
				Expect(results[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578355200000000000)),
					"Entity":     PointTo(Equal("spongebob")),
					"Max_amount": PointTo(BeEquivalentTo(34)),
					"Min_amount": PointTo(BeEquivalentTo(7)),
				}))
				Expect(results[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(5000)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})

			// Regression for https://gitlab.com/kaskada/kaskada/-/issues/477
			It("should run the query successfully again with no new data", func() {
				stream, err := queryClient.CreateQuery(ctx, queryRequestIncremental)
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenIncremental2))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(15))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(0))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(3))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
				Expect(results[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578355200000000000)),
					"Entity":     PointTo(Equal("spongebob")),
					"Max_amount": PointTo(BeEquivalentTo(34)),
					"Min_amount": PointTo(BeEquivalentTo(7)),
				}))
				Expect(results[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(5000)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})
		})

		Describe("Deleting and recreating table does not find existing snapshots", func() {
			It("Should not find existing snapshot to resume from", func() {
				// delete the table
				_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableIncremental.TableName})
				Expect(err).ShouldNot(HaveOccurredGrpc())

				// recreate table with no inputs
				_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: tableIncremental})
				Expect(err).ShouldNot(HaveOccurredGrpc())

				// Load in just the first file
				res := helpers.LoadTestFileIntoTable(ctx, conn, tableIncremental, "purchases/purchases_part1.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				dataTokenId := res.DataTokenId

				// Run the query with the new table
				stream, err := queryClient.CreateQuery(ctx, queryRequestIncremental)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

				var firstResponse, secondResponse, thirdResponse, lastResponse *v1alpha.CreateQueryResponse

				firstResponse, queryResponses = queryResponses[0], queryResponses[1:]
				secondResponse, queryResponses = queryResponses[0], queryResponses[1:]
				thirdResponse, queryResponses = queryResponses[0], queryResponses[1:]
				Expect(firstResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))
				VerifyRequestDetails(firstResponse.RequestDetails)
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenId))
				Expect(firstResponse.Analysis.Schema.GetFields()).Should(ContainElements(
					gproto.Equal(primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND)),
					gproto.Equal(primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING)),
					gproto.Equal(primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
					gproto.Equal(primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
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

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}
				Expect(len(resultUrls)).Should(Equal(1))

				// The results should be equivalent to the results produced with just the
				// first file. If using just the table name to find snapshots, the full
				// snapshot with both files would've been used to produce results.
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(2))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578182400000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})

		})

		Describe("Verify results are similar to non-incremental", func() {
			It("should successfully load data into the table", func() {
				res = helpers.LoadTestFileIntoTable(ctx, conn, tableNonIncremental, "purchases/purchases_part1.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())

				res = helpers.LoadTestFileIntoTable(ctx, conn, tableNonIncremental, "purchases/purchases_part2.parquet")
				Expect(res.DataTokenId).ShouldNot(BeEmpty())
				dataTokenNonIncremental = res.DataTokenId
			})

			It("Should run the query and get same results as incremental", func() {
				stream, err := queryClient.CreateQuery(ctx, queryRequestNonIncremental)
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
				Expect(firstResponse.Config.DataTokenId).Should(Equal(dataTokenNonIncremental))
				_, err = uuid.Parse(secondResponse.QueryId)
				Expect(err).Should(BeNil())
				Expect(thirdResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_PREPARING))

				lastResponse, queryResponses = queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
				Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
				Expect(lastResponse.Metrics).ShouldNot(BeNil())
				Expect(lastResponse.Metrics.OutputFiles).Should(BeEquivalentTo(1))
				Expect(lastResponse.Metrics.TotalInputRows).Should(BeEquivalentTo(15))
				Expect(lastResponse.Metrics.ProcessedInputRows).Should(BeEquivalentTo(15))
				Expect(lastResponse.Metrics.ProducedOutputRows).Should(BeEquivalentTo(3))

				resultUrls := []string{}
				for _, queryResponse := range queryResponses {
					Expect(queryResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_COMPUTING))
					Expect(queryResponse.RequestDetails.RequestId).Should(Equal(firstResponse.RequestDetails.RequestId))

					if queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths() != nil {
						resultUrls = append(resultUrls, queryResponse.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()...)
					}
				}

				Expect(len(resultUrls)).Should(Equal(1))
				results := helpers.DownloadParquet(resultUrls[0])
				Expect(results).Should(HaveLen(3))
				Expect(results[0]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("karen")),
					"Max_amount": PointTo(BeEquivalentTo(9)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
				Expect(results[1]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578355200000000000)),
					"Entity":     PointTo(Equal("spongebob")),
					"Max_amount": PointTo(BeEquivalentTo(34)),
					"Min_amount": PointTo(BeEquivalentTo(7)),
				}))
				Expect(results[2]).Should(MatchFields(IgnoreExtras, Fields{
					"Time":       PointTo(BeEquivalentTo(1578441600000000000)),
					"Entity":     PointTo(Equal("patrick")),
					"Max_amount": PointTo(BeEquivalentTo(5000)),
					"Min_amount": PointTo(BeEquivalentTo(2)),
				}))
			})
		})
	})
})
