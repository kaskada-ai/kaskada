package api_test

import (
	"context"
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/jt-nti/gproto"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Query V1 when Sparrow panics", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var destination *v1alpha.Destination
	var tableClient v1alpha.TableServiceClient
	var queryClient v1alpha.QueryServiceClient
	var tableName string

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(30)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "query_v1_panic"

		// create table, load table data
		table := &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")

		destination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			},
		}
	})

	AfterAll(func() {
		// delete table
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("sparrow panic", Ordered, Label("sparrow-panic"), func() {
		It("should be reported in a timely manner", func() {
			if strings.Contains(os.Getenv("ENV"), "local") {
				Skip("tests running locally, skipping sparrow panic test")
			}

			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     "__INTERNAL_COMPILE_PANIC__",
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).Should(HaveOccurred())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.Internal))
			Expect(errStatus.Message()).Should(ContainSubstring("internal error"))
		})

		It("should support queries after ", func() {
			if strings.Contains(os.Getenv("ENV"), "local") {
				Skip("tests running locally, skipping sparrow panic test")
			}

			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression: `
{
time: query_v1_panic.purchase_time,
entity: query_v1_panic.customer_id,
max_amount: query_v1_panic.amount | max(),
min_amount: query_v1_panic.amount | min(),
}`,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())

			VerifyRequestDetails(res.RequestDetails)
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			Expect(res.Analysis.Schema).Should(ContainElements(
				gproto.Equal(primitiveSchemaField("time", v1alpha.DataType_PRIMITIVE_TYPE_TIMESTAMP_NANOSECOND)),
				gproto.Equal(primitiveSchemaField("entity", v1alpha.DataType_PRIMITIVE_TYPE_STRING)),
				gproto.Equal(primitiveSchemaField("max_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
				gproto.Equal(primitiveSchemaField("min_amount", v1alpha.DataType_PRIMITIVE_TYPE_I64)),
			))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			firstResults := helpers.DownloadParquet(resultsUrl)

			Expect(firstResults).Should(HaveLen(10))
			Expect(firstResults[9]).Should(MatchFields(IgnoreExtras, Fields{
				"Time":       PointTo(BeEquivalentTo(1578182400000000000)),
				"Entity":     PointTo(Equal("patrick")),
				"Max_amount": PointTo(BeEquivalentTo(5000)),
				"Min_amount": PointTo(BeEquivalentTo(3)),
			}))
		})
	})

	Describe("wren panic", Label("wren-panic"), func() {
		It("doesn't panic on a malformed query (missing comma in record)", func() {
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression: `
{
time: query_v1_panic.purchase_time
entity: query_v1_panic.customer_id
}`,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())

			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).Should(BeNil())

			Expect(res.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_FAILURE))

			Expect(res.Analysis.CanExecute).Should(BeFalse())

			Expect(res.FenlDiagnostics).ShouldNot(BeNil())
			Expect(res.FenlDiagnostics.NumErrors).Should(BeEquivalentTo(1))

			diagnostics := res.FenlDiagnostics.FenlDiagnostics
			Expect(diagnostics).Should(HaveLen(1))
			Expect(diagnostics).Should(ContainElement(ContainSubstring("Invalid syntax")))
			Expect(diagnostics).Should(ContainElement(ContainSubstring("Invalid token 'entity'")))
		})

	})
})
