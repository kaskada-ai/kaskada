package cli_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("sync export", Ordered, func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		conn        *grpc.ClientConn
		matClient   v1alpha.MaterializationServiceClient
		queryClient v1alpha.QueryServiceClient
		tableClient v1alpha.TableServiceClient
		viewClient  v1alpha.ViewServiceClient
		tableName   string
	)

	BeforeAll(func() {
		// First create the resources used in the test
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		matClient = v1alpha.NewMaterializationServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)
		tableClient = v1alpha.NewTableServiceClient(conn)
		viewClient = v1alpha.NewViewServiceClient(conn)

		tableName = "sync_export_test"

		// create a table, load first file
		table := &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		res := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
		Expect(res.DataTokenId).ShouldNot(BeEmpty())

		expression :=
			`{
time: sync_export_test.purchase_time,
entity: sync_export_test.customer_id,
max_amount: sync_export_test.amount | max(),
min_amount: sync_export_test.amount | min(),
}`

		// define a query to run on the table
		query := &v1alpha.Query{
			Expression: expression,
			ResponseAs: &v1alpha.Query_AsFiles{AsFiles: &v1alpha.AsFiles{FileType: v1alpha.FileType_FILE_TYPE_CSV}},
		}

		createQueryResponse, err := queryClient.CreateQuery(ctx, &v1alpha.CreateQueryRequest{Query: query})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(createQueryResponse).ShouldNot(BeNil())

		queryResponse, err := helpers.GetMergedCreateQueryResponse(createQueryResponse)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(queryResponse.GetFileResults()).ShouldNot(BeNil())
		Expect(queryResponse.GetFileResults().Paths).Should(HaveLen(1))

		resultsUrl := queryResponse.GetFileResults().Paths[0]
		results := helpers.DownloadCSV(resultsUrl)
		Expect(results).Should(Equal(getExpectedCSVResults("./results/min_max.csv")))

		view := &v1alpha.View{
			Expression: expression,
			ViewName:   "min_max_view_get",
		}
		createViewResponse, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{View: view})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(createViewResponse).ShouldNot(BeNil())
		Expect(createViewResponse.View.ViewId).ShouldNot(BeNil())

		materialization := &v1alpha.Materialization{
			Query:               expression,
			MaterializationName: "min_max_mat_get",
			Slice:               &v1alpha.SliceRequest{},
			Destination: &v1alpha.Materialization_Destination{
				Destination: &v1alpha.Materialization_Destination_ObjectStore{
					ObjectStore: &v1alpha.ObjectStoreDestination{
						OutputPrefixUri: "test",
						Format:          v1alpha.ObjectStoreDestination_FILE_FORMAT_CSV,
					},
				},
			},
		}
		createMatResponse, err := matClient.CreateMaterialization(ctx, &v1alpha.CreateMaterializationRequest{Materialization: materialization})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		Expect(createMatResponse).ShouldNot(BeNil())
		Expect(createMatResponse.Materialization.MaterializationId).ShouldNot(BeNil())
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := matClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "min_max_mat_get"})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		_, err = viewClient.DeleteView(ctx, &v1alpha.DeleteViewRequest{ViewName: "min_max_view_get"})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("export a spec of the current system state", func() {
		It("should produce a spec with a table, a view, and materialization", func() {
			results := runCliCommand("sync", "export", "--all")
			Expect(results.exitCode).Should(Equal(0))
			Expect(results.stdOut).Should(Equal(getExpectedResults("./results/sync_export_test.yml")))
			Expect(results.stdErr).ShouldNot(ContainSubstring("ERR"))
			Expect(results.stdErr).Should(ContainSubstring("Success!"))
		})
	})
})
