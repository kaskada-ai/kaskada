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

var _ = Describe("Query V1 gRPC Errors", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var tableClient v1alpha.TableServiceClient
	var queryClient v1alpha.QueryServiceClient
	var tableName string

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = getContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "query_v1_errors"

		// create table, load table data
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
		loadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Context("When the table schema is created correctly", func() {
		Describe("Reference an invalid field", func() {
			It("should return an invalid argument error", func() {
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:     "sum(query_v1_errors.Tacos)",
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

				Expect(res).ShouldNot(BeNil())
				Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
				Expect(res.GetFileResults()).Should(BeNil())

				Expect(res.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_FAILURE))

				Expect(res.Analysis.CanExecute).Should(BeFalse())

				Expect(res.FenlDiagnostics).ShouldNot(BeNil())
				Expect(res.FenlDiagnostics.NumErrors).Should(BeEquivalentTo(1))

				diagnostics := res.FenlDiagnostics.FenlDiagnostics
				Expect(diagnostics).Should(HaveLen(1))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("Illegal field reference")))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("No field named 'Tacos'")))
			})
		})

		Describe("Reference an invalid field, with dry-run", func() {
			It("should return an invalid argument error", func() {
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:     "sum(query_v1_errors.Tacos)",
						ResponseAs:     &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_PARQUET}},
						ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					},
					QueryOptions: &v1alpha.QueryOptions{
						DryRun: true,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				res, err := getMergedCreateQueryResponse(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())

				Expect(res).ShouldNot(BeNil())
				Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
				Expect(res.GetFileResults()).Should(BeNil())

				Expect(res.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))

				Expect(res.Analysis.CanExecute).Should(BeFalse())

				Expect(res.FenlDiagnostics).ShouldNot(BeNil())
				Expect(res.FenlDiagnostics.NumErrors).Should(BeEquivalentTo(1))

				diagnostics := res.FenlDiagnostics.FenlDiagnostics
				Expect(diagnostics).Should(HaveLen(1))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("Illegal field reference")))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("No field named 'Tacos'")))
			})
		})
	})
})
