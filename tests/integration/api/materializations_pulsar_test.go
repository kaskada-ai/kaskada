package api_test

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

type testSchema struct {
	Key       string `json:"key"`
	MaxAmount int    `json:"max_amount"`
	MinAmount int    `json:"min_amount"`
}

var _ = FDescribe("Materialization with Pulsar upload", Ordered, Label("pulsar"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		pulsarClient          pulsar.Client
		table                 *v1alpha.Table
		tableClient           v1alpha.TableServiceClient
		tableName             string
		topicUrl              string
		materializationClient v1alpha.MaterializationServiceClient
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & materialization services
		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)

		// create a pulsar client
		pulsarClient, _ = pulsar.NewClient(pulsar.ClientOptions{
			URL: "pulsar://localhost:6650",
		})
		// Expect(err).Should(BeNil())
		defer pulsarClient.Close()

		// create a table
		tableName = "pulsar_table"
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

		// load data into the table
		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
	})

	AfterAll(func() {
		// clean up items from the test
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "purchase_min_and_max"})
		// this materialization might not have been created if test had an issue, so we don't check error here
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: "purchase_min_and_max",
					Expression: `
{
key: pulsar_table.customer_id,
max_amount: pulsar_table.amount | max(),
min_amount: pulsar_table.amount | min(),
}`,
					// TODO: FRAZ - create issue for normalizing the mat dest protos
					Destination: &v1alpha.Materialization_Destination{
						Destination: &v1alpha.Materialization_Destination_Pulsar{
							Pulsar: &v1alpha.PulsarDestination{
								BrokerServiceUrl: "pulsar://pulsar:6650",
								Tenant:           "public",
								Namespace:        "default",
								TopicName:        "my_topic",
							},
						},
					},
				},
			}
			topicUrl = "persistent://public/default/my_topic"
			res, err := materializationClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
		})

		It("Should upload results to pulsar", func() {
			Eventually(func(g Gomega) {

				consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
					Topic:            topicUrl,
					SubscriptionName: "my-subscription",
					Type:             pulsar.Shared,
				})
				g.Expect(err).Should(BeNil())
				defer consumer.Close()

				data := testSchema{}
				for i := 0; i < 2; i++ {
					msg, err := consumer.Receive(context.Background())
					g.Expect(err).Should(BeNil())

					fmt.Printf("\n pulsar payload: %s\n", msg.Payload())

					err = msg.GetSchemaValue(&data)
					g.Expect(err).Should(BeNil())

					// data, err := schema.Decode(msg.Payload())
					// g.Expect(err).Should(BeNil())

					// Verify the message fields
					g.Expect(data.Key).Should(Equal("karen"))
					g.Expect(data.MaxAmount).Should(Equal(10))

					consumer.Ack(msg)

					time.Sleep(1 * time.Second) // add a delay for testing purposes
				}

				// dataType, shape, values, err := pulsarClient.TensorGetValues("karen")
				// g.Expect(err).ShouldNot(HaveOccurred())
				// g.Expect(dataType).Should(Equal("INT64"))
				// g.Expect(shape).Should(Equal([]int64{1, 3}))
				// g.Expect(values).Should(Equal([]int64{1578182400000000000, 9, 2}))

				// dataType, shape, values, err = redisAIClient.TensorGetValues("patrick")
				// g.Expect(err).ShouldNot(HaveOccurred())
				// g.Expect(dataType).Should(Equal("INT64"))
				// g.Expect(shape).Should(Equal([]int64{1, 3}))
				// g.Expect(values).Should(Equal([]int64{1578182400000000000, 5000, 3}))

				// _, _, _, err = redisAIClient.TensorGetValues("spongebob")
				// g.Expect(err).Should(HaveOccurred())
				// g.Expect(err.Error()).Should(ContainSubstring("tensor key is empty"))
			}, "30s", "1s").Should(Succeed())
		})
	})

	Describe("Load the second file into the table", func() {
		It("Should work without error", func() {
			helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
		})

		It("Should upload results to pulsar", func() {
			Eventually(func(g Gomega) {
				// hello
			}, "30s", "1s").Should(Succeed())
		})
	})
})
