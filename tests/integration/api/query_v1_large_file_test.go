package api_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	. "github.com/kaskada-ai/kaskada/tests/integration/api/matchers"
)

var _ = Describe("Query V1 with large-ish files", Ordered, func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		conn        *grpc.ClientConn
		tableClient v1alpha.TableServiceClient
		queryClient v1alpha.QueryServiceClient
		table       *v1alpha.Table
	)

	tableName := "transactions_large_files"

	expression := `
{
time: transactions_large_files.transaction_time,
key: transactions_large_files.id,
max_price: transactions_large_files.price | max(),
min_spent_in_single_transaction: min(transactions_large_files.price * transactions_large_files.quantity),
max_spent_in_single_transaction: max(transactions_large_files.price * transactions_large_files.quantity)
}`

	dataSetSize := 50000

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = getContextCancelConnection(120)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// create a table
		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "transaction_time",
			EntityKeyColumnName: "id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "idx",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		// load the data files
		loadTestFilesIntoTable(ctx, conn, table, "transactions/transactions_part1.parquet")
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("Run the query with no special options", func() {
		It("should return the full set of query results", func() {
			// define a query to run on the table
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := getMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetFileResults()).ShouldNot(BeNil())
			Expect(res.GetFileResults().Paths).Should(HaveLen(1))

			resultsUrl := res.GetFileResults().Paths[0]
			results := downloadParquet(resultsUrl)

			Expect(len(results)).Should(Equal(dataSetSize))
		})
	})

	Describe("Run the query with preview rows set to 50", func() {
		It("should return less than the full set of query results", func() {
			// define a query to run on the table
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Limits: &v1alpha.Query_Limits{
						PreviewRows: 50,
					},
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := getMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())

			VerifyRequestDetails(res.RequestDetails)
			Expect(res.GetFileResults()).ShouldNot(BeNil())
			Expect(res.GetFileResults().Paths).Should(HaveLen(1))

			resultsUrl := res.GetFileResults().Paths[0]
			results := downloadParquet(resultsUrl)

			Expect(len(results)).Should(BeNumerically("<", dataSetSize))
		})
	})

	Describe("Run the query with preview rows set to a huge number", func() {
		It("should return the full set of query results", func() {
			// define a query to run on the table
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Limits: &v1alpha.Query_Limits{
						PreviewRows: 50000000000,
					},
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := getMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())

			VerifyRequestDetails(res.RequestDetails)
			Expect(res.GetFileResults()).ShouldNot(BeNil())
			Expect(res.GetFileResults().Paths).Should(HaveLen(1))

			resultsUrl := res.GetFileResults().Paths[0]
			results := downloadParquet(resultsUrl)

			Expect(len(results)).Should(Equal(dataSetSize))
		})
	})
})
