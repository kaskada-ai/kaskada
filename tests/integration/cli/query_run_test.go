package cli_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/acarl005/stripansi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("query run", Ordered, func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		conn        *grpc.ClientConn
		tableClient v1alpha.TableServiceClient
		tableName   string
	)

	BeforeAll(func() {
		// First create the resources used in the test
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		tableName = "query_run_test"

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
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("run a query on the system", func() {
		It("should produce results", func() {
			expression := fmt.Sprintf("{time: %s.purchase_time, entity: %s.customer_id, max_amount: %s.amount | max(), min_amount: %s.amount | min()}", tableName, tableName, tableName, tableName)

			results := runCliCommand("query", "run", "--response-as", "csv", expression)
			helpers.LogLn(results.stdOut)
			Expect(results.exitCode).Should(Equal(0))

			// parse the json response to get the file output path
			jsonMap := make(map[string](interface{}))
			Expect(json.Unmarshal([]byte(results.stdOut), &jsonMap)).Should(Succeed())
			fileResults := jsonMap["fileResults"].(map[string]interface{})
			paths := fileResults["paths"].([]interface{})

			csvData := helpers.DownloadCSV(fmt.Sprintf("%v", paths[0]))
			Expect(csvData).Should(Equal(getExpectedCSVResults("./results/min_max.csv")))

			stdErr := stripansi.Strip(results.stdErr)
			Expect(stdErr).Should(ContainSubstring("Success!"))
		})
	})
})
