package api_test

import (
	"context"
	"encoding/json"
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
			consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            topicUrl,
				SubscriptionName: "my-subscription",
				Type:             pulsar.Shared,
			})
			Expect(err).Should(BeNil())
			defer consumer.Close()

			// Verify the first message
			var data testSchema
			msg, err := consumer.Receive(context.Background())
			Expect(err).Should(BeNil())

			err = json.Unmarshal(msg.Payload(), &data)
			Expect(err).Should(BeNil())
			Expect(data.Key).Should(Equal("karen"))
			Expect(data.MaxAmount).Should(Equal(9))
			Expect(data.MinAmount).Should(Equal(2))

			consumer.Ack(msg)
			time.Sleep(1 * time.Second) // add a delay for testing purposes

			// Verify the second message
			msg, err = consumer.Receive(context.Background())
			Expect(err).Should(BeNil())

			err = json.Unmarshal(msg.Payload(), &data)
			Expect(err).Should(BeNil())

			// Verify the message fields
			Expect(data.Key).Should(Equal("patrick"))
			Expect(data.MaxAmount).Should(Equal(5000))
			Expect(data.MinAmount).Should(Equal(3))

			consumer.Ack(msg)
		})
	})

	Describe("Load the second file into the table", func() {
		It("Should work without error", func() {
			helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
		})

		It("Should upload results to pulsar", func() {
			consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            topicUrl,
				SubscriptionName: "my-subscription",
				Type:             pulsar.Shared,
			})
			Expect(err).Should(BeNil())
			defer consumer.Close()

			// Verify the first message
			var data testSchema
			msg, err := consumer.Receive(context.Background())
			Expect(err).Should(BeNil())
			fmt.Printf("\nMessage: %s\n", msg.Payload())

			err = json.Unmarshal(msg.Payload(), &data)
			Expect(err).Should(BeNil())
			Expect(data.Key).Should(Equal("karen"))
			Expect(data.MaxAmount).Should(Equal(9))
			Expect(data.MinAmount).Should(Equal(2))
			consumer.Ack(msg)
			time.Sleep(1 * time.Second) // add a delay for testing purposes

			// Verify the second message
			msg, err = consumer.Receive(context.Background())
			Expect(err).Should(BeNil())
			fmt.Printf("\nMessage: %s\n", msg.Payload())

			err = json.Unmarshal(msg.Payload(), &data)
			Expect(err).Should(BeNil())

			// Verify the message fields
			Expect(data.Key).Should(Equal("patrick"))
			Expect(data.MaxAmount).Should(Equal(5000))
			Expect(data.MinAmount).Should(Equal(3))
			consumer.Ack(msg)
			time.Sleep(1 * time.Second) // add a delay for testing purposes

			// Verify the third message
			msg, err = consumer.Receive(context.Background())
			Expect(err).Should(BeNil())
			fmt.Printf("\nMessage: %s\n", msg.Payload())

			err = json.Unmarshal(msg.Payload(), &data)
			Expect(err).Should(BeNil())

			// Verify the message fields
			Expect(data.Key).Should(Equal("patrick"))
			Expect(data.MaxAmount).Should(Equal(5000))
			Expect(data.MinAmount).Should(Equal(3))
			consumer.Ack(msg)
		})
	})
})
