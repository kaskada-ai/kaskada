package api_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var _ = FDescribe("Materialization from Kafka to ObjectStore", Ordered, Label("kafka"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		err                   error
		kafkaProducer         *kafka.Producer
		materializationClient v1alpha.MaterializationServiceClient
		materializationName   string
		outputPath            string
		outputURI             string
		kafkaDeliverChannel   chan kafka.Event
		tableClient           v1alpha.TableServiceClient
		tableName             string
		topicName             string
	)

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(20)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & materialization services
		tableClient = v1alpha.NewTableServiceClient(conn)
		materializationClient = v1alpha.NewMaterializationServiceClient(conn)
		materializationName = "mat_kafkaToObjStore"

		// define the output path and make sure it is empty
		outputPath = fmt.Sprintf("../data/output/%s/", materializationName)
		os.RemoveAll(outputPath)

		if os.Getenv("ENV") == "local-local" {
			workDir, err := os.Getwd()
			Expect(err).ShouldNot(HaveOccurred())
			outputURI = fmt.Sprintf("file://%s/../data/output/%s", workDir, materializationName)
		} else {
			outputURI = fmt.Sprintf("file:///data/output/%s", materializationName)
		}
		client_id, err := uuid.NewRandom()
		Expect(err).Should(BeNil())
		kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:29092",
			"client.id":         client_id.String(),
			"acks":              "all",
		})
		Expect(err).Should(BeNil())
		kafkaDeliverChannel = make(chan kafka.Event, 10000)

		topicName = uuid.NewString()

		// create a table backed by kafka
		tableName = "table_kafkaToObjStore"
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})

		err = kafkaProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: 0,
			},
			Value: helpers.ReadTestFile("avro/msg_0.avro"),
		}, kafkaDeliverChannel)
		Expect(err).ShouldNot(HaveOccurred())

		e := <-kafkaDeliverChannel
		m := e.(*kafka.Message)
		Expect(m.TopicPartition.Error).ShouldNot(HaveOccurred())
		kafkaProducer.Flush(1000)

		table := &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "time",
			EntityKeyColumnName: "id",
			Source: &v1alpha.Source{
				Source: &v1alpha.Source_Kafka{
					Kafka: &v1alpha.KafkaSource{
						Config: getKafkaConfig(topicName),
					},
				},
			},
		}
		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "failed to create kafka-backed table")
	})

	AfterAll(func() {
		// clean up items from the test
		_, err = materializationClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: materializationName})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "issue deleting materialization")
		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "issue deleting table")

		cancel()
		conn.Close()
		if kafkaDeliverChannel != nil {
			close(kafkaDeliverChannel)
		}
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression: `
					{
						last_id: table_kafkaToObjStore.id | last(),
						last_time: table_kafkaToObjStore.time | last(),
						count: table_kafkaToObjStore | count(),
					}
					`,
					Destination: &v1alpha.Destination{
						Destination: &v1alpha.Destination_ObjectStore{
							ObjectStore: &v1alpha.ObjectStoreDestination{
								FileType:        v1alpha.FileType_FILE_TYPE_CSV,
								OutputPrefixUri: outputURI,
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

		It("Should output initial results to csv", func() {
			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: 0,
				},
				Value: helpers.ReadTestFile("avro/msg_1.avro"),
			}, kafkaDeliverChannel)
			Expect(err).ShouldNot(HaveOccurred())

			e := <-kafkaDeliverChannel
			m := e.(*kafka.Message)
			Expect(m.TopicPartition.Error).ShouldNot(HaveOccurred())
			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: 0,
				},
				Value: helpers.ReadTestFile("avro/msg_2.avro"),
			}, kafkaDeliverChannel)
			Expect(err).ShouldNot(HaveOccurred())

			e = <-kafkaDeliverChannel
			m = e.(*kafka.Message)
			Expect(m.TopicPartition.Error).ShouldNot(HaveOccurred())

			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: 0,
				},
				Value: helpers.ReadTestFile("avro/msg_3.avro"),
			}, kafkaDeliverChannel)
			Expect(err).ShouldNot(HaveOccurred())

			e = <-kafkaDeliverChannel
			m = e.(*kafka.Message)
			Expect(m.TopicPartition.Error).ShouldNot(HaveOccurred())

			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: 0,
				},
				Value: helpers.ReadTestFile("avro/msg_4.avro"),
			}, kafkaDeliverChannel)
			Expect(err).ShouldNot(HaveOccurred())

			e = <-kafkaDeliverChannel
			m = e.(*kafka.Message)
			Expect(m.TopicPartition.Error).ShouldNot(HaveOccurred())
			kafkaProducer.Flush(1000)

			time.Sleep(time.Millisecond * 1000)

			Eventually(func(g Gomega) {
				dirs, err := os.ReadDir(outputPath)
				g.Expect(err).ShouldNot(HaveOccurred(), "cannot list output_path files")
				g.Expect(dirs).Should(HaveLen(1))
				firstFileName := dirs[0].Name()

				results := helpers.GetCSV(outputPath + firstFileName)
				g.Expect(results).Should(HaveLen(2)) //header row + 1 data row
				g.Expect(results[0]).Should(ContainElements("_time", "_subsort", "_key_hash", "last_id", "last_time", "count"))
				g.Expect(results[1]).Should(ContainElements("2023-06-20T23:30:01.000000000", "0", "2122274938272070218", "9", "9", "1687303801000000000", "1"))
			}, "10s", "1s").Should(Succeed())
		})
	})
})
