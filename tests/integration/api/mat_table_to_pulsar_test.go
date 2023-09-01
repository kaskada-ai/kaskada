package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	pulsaradmin "github.com/streamnative/pulsar-admin-go"
	"github.com/streamnative/pulsar-admin-go/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = XDescribe("Materialization with Pulsar upload", Ordered, Label("pulsar"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		err                   error
		pulsarClient          pulsar.Client
		pulsarConsumer        pulsar.Consumer
		table                 *v1alpha.Table
		tableClient           v1alpha.TableServiceClient
		tableName             string
		topicName             string
		materializationClient v1alpha.MaterializationServiceClient
		materializationName   string
		msg                   pulsar.Message
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & materialization services
		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)
		materializationName = "mat_tableToPulsar"

		// create a pulsar client
		pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:               "pulsar://localhost:6650",
			ConnectionTimeout: 5 * time.Second,
		})
		Expect(err).ShouldNot(HaveOccurred())

		// create a pulsar consumer
		topicName = "topic_tableToPulsar"
		pulsarConsumer, err = pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            topicName,
			SubscriptionName: uuid.New().String(),
			Type:             pulsar.Shared,
		})
		Expect(err).ShouldNot(HaveOccurred(), "issue creating pulsar consumer")

		// create a table
		tableName = "table_tableToPulsar"
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
		materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: materializationName})
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})

		cancel()
		conn.Close()
		pulsarConsumer.Close()
		pulsarClient.Close()

		// attempt to delete pulsar topic used in test
		cfg := &pulsaradmin.Config{}
		cfg.WebServiceURL = "http://localhost:8080"
		admin, err := pulsaradmin.NewClient(cfg)
		Expect(err).ShouldNot(HaveOccurred(), "issue getting puslar admin client")
		Expect(err).ShouldNot(HaveOccurred())
		topic, _ := utils.GetTopicName(fmt.Sprintf("public/default/%s", topicName))
		err = admin.Topics().Delete(*topic, true, true)
		Expect(err).ShouldNot(HaveOccurred(), "issue deleting pulsar in topic")
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression: `
{
key: table_tableToPulsar.customer_id,
max_amount: table_tableToPulsar.amount | max(),
min_amount: table_tableToPulsar.amount | min(),
}`,
					Destination: &v1alpha.Destination{
						Destination: &v1alpha.Destination_Pulsar{
							Pulsar: &v1alpha.PulsarDestination{
								Config: getPulsarConfig(topicName),
							},
						},
					},
				},
			}

			res, err := materializationClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
		})

		It("Should upload results to pulsar", func() {

			// Verify the first message
			Eventually(func(g Gomega) {
				msg = receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.Key).Should(Equal("karen"))
				g.Expect(data.MaxAmount).Should(Equal(9))
				g.Expect(data.MinAmount).Should(Equal(2))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "10s", "1s").Should(Succeed())

			// Verify the second message
			Eventually(func(g Gomega) {
				msg = receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.Key).Should(Equal("patrick"))
				g.Expect(data.MaxAmount).Should(Equal(5000))
				g.Expect(data.MinAmount).Should(Equal(3))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "10s", "1s").Should(Succeed())
		})
	})

	Describe("Load the second file into the table", func() {
		It("Should work without error", func() {
			helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part2.parquet")
		})

		It("Should upload results to pulsar", func() {

			// Verify the first message
			Eventually(func(g Gomega) {
				msg = receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.Key).Should(Equal("karen"))
				g.Expect(data.MaxAmount).Should(Equal(9))
				g.Expect(data.MinAmount).Should(Equal(2))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "10s", "1s").Should(Succeed())

			// Verify the second message
			Eventually(func(g Gomega) {
				msg = receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.Key).Should(Equal("spongebob"))
				g.Expect(data.MaxAmount).Should(Equal(34))
				g.Expect(data.MinAmount).Should(Equal(7))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "10s", "1s").Should(Succeed())

			// Verify the third message
			Eventually(func(g Gomega) {
				msg = receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.Key).Should(Equal("patrick"))
				g.Expect(data.MaxAmount).Should(Equal(5000))
				g.Expect(data.MinAmount).Should(Equal(2))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "10s", "1s").Should(Succeed())
		})
	})
})
