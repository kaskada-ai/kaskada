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

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

type pulsarToPulsarTestSchema struct {
	LastId   int `json:"last_id"`
	LastTime int `json:"last_time"`
	Count    int `json:"count"`
}

var _ = Describe("Materialization from Pulsar to Pulsar", Ordered, Label("pulsar"), FlakeAttempts(3), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		err                   error
		materializationClient v1alpha.MaterializationServiceClient
		materializationName   string
		pulsarClient          pulsar.Client
		pulsarConsumer        pulsar.Consumer
		pulsarProducer        pulsar.Producer
		table                 *v1alpha.Table
		tableClient           v1alpha.TableServiceClient
		tableName             string
		topicNameIn           string
		topicNameOut          string
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & materialization services
		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)
		materializationName = "mat_pulsarToPulsar"

		// create a pulsar client
		pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:               "pulsar://localhost:6650",
			ConnectionTimeout: 5 * time.Second,
		})
		Expect(err).ShouldNot(HaveOccurred())

		// create a pulsar producer and push initial data to pulsar
		topicNameIn = "topic_pulsarToPulsar_In"
		topicNameOut = "topic_pulsarToPulsar_Out"
		pulsarProducer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Topic:  topicNameIn,
			Schema: pulsar.NewAvroSchema(string(helpers.ReadTestFile("avro/schema.json")), map[string]string{}),
		})
		Expect(err).ShouldNot(HaveOccurred(), "issue creating pulsar producer")

		_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: helpers.ReadTestFile("avro/msg_0.avro"),
		})
		Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")
		_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: helpers.ReadTestFile("avro/msg_1.avro"),
		})

		// create a pulsar consumer
		pulsarConsumer, err = pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            topicNameOut,
			SubscriptionName: uuid.New().String(),
			Type:             pulsar.Shared,
		})
		Expect(err).ShouldNot(HaveOccurred(), "issue creating pulsar consumer")

		// create a table backed by pulsar
		tableName = "table_pulsarToPulsar"
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})

		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "time",
			EntityKeyColumnName: "id",
			Source: &v1alpha.Source{
				Source: &v1alpha.Source_Pulsar{
					Pulsar: &v1alpha.PulsarSource{
						Config: getPulsarConfig(topicNameIn),
					},
				},
			},
		}
		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "failed to create pulsar-backed table")
	})

	AfterAll(func() {
		// clean up items from the test
		_, err = materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: materializationName})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "issue deleting materialization")
		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "issue deleting table")

		cancel()
		conn.Close()
		pulsarConsumer.Close()
		pulsarProducer.Close()
		pulsarClient.Close()

		// attempt to delete pulsar topics used in test
		cfg := &pulsaradmin.Config{}
		cfg.WebServiceURL = "http://localhost:8080"
		admin, err := pulsaradmin.NewClient(cfg)
		Expect(err).ShouldNot(HaveOccurred(), "issue getting puslar admin client")
		Expect(err).ShouldNot(HaveOccurred())
		topic, _ := utils.GetTopicName(fmt.Sprintf("public/default/%s", topicNameIn))
		err = admin.Topics().Delete(*topic, true, true)
		Expect(err).ShouldNot(HaveOccurred(), "issue deleting pulsar in topic")
		topic, _ = utils.GetTopicName(fmt.Sprintf("public/default/%s", topicNameOut))
		err = admin.Topics().Delete(*topic, true, true)
		Expect(err).ShouldNot(HaveOccurred(), "issue deleting pulsar out topic")
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression: `
					{
						last_id: table_pulsarToPulsar.id | last(),
						last_time: table_pulsarToPulsar.time | last(),
						count: table_pulsarToPulsar | count() as i32,
					}
					`,
					Destination: &v1alpha.Destination{
						Destination: &v1alpha.Destination_Pulsar{
							Pulsar: &v1alpha.PulsarDestination{
								Config: getPulsarConfig(topicNameOut),
							},
						},
					},
				},
			}

			res, err := materializationClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res.Analysis.CanExecute).Should(BeTrue())
			Expect(res.Materialization.MaterializationId).ShouldNot(BeEmpty())
		})

		It("Should output initial results to pulsar", func() {
			Eventually(func(g Gomega) {
				msg := receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarToPulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.LastId).Should(Equal(9))
				g.Expect(data.LastTime).Should(Equal(1687303801000000000))
				g.Expect(data.Count).Should(Equal(1))

				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())
			}, "5s", "1s").Should(Succeed())
		})
	})

	Describe("Load more data into the table", func() {
		It("Should work without error", func() {
			_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
				Payload: helpers.ReadTestFile("avro/msg_2.avro"),
			})
			Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")
			_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
				Payload: helpers.ReadTestFile("avro/msg_3.avro"),
			})
			Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")
		})

		It("Should output additional results to pulsar", func() {
			Eventually(func(g Gomega) {
				msg := receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg).ShouldNot(BeNil())

				var data pulsarToPulsarTestSchema
				err = json.Unmarshal(msg.Payload(), &data)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data.LastId).Should(Equal(2))
				g.Expect(data.LastTime).Should(Equal(1687303803000000000))
				g.Expect(data.Count).Should(Equal(1))
				g.Expect(pulsarConsumer.Ack(msg)).Should(Succeed())

				msg2 := receivePulsarMessageWithTimeout(pulsarConsumer, ctx)
				g.Expect(msg2).ShouldNot(BeNil())

				var data2 pulsarToPulsarTestSchema
				err = json.Unmarshal(msg2.Payload(), &data2)
				g.Expect(err).ShouldNot(HaveOccurred())
				g.Expect(data2.LastId).Should(Equal(4))
				g.Expect(data2.LastTime).Should(Equal(1687303805000000000))
				g.Expect(data2.Count).Should(Equal(1))

			}, "5s", "1s").Should(Succeed())
		})
	})
})
