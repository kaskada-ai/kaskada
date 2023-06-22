package api_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
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

type pulsarTestSchema struct {
	Key       string `json:"key"`
	MaxAmount int    `json:"max_amount"`
	MinAmount int    `json:"min_amount"`
}

var _ = PDescribe("Materialization from Pulsar to ObjectStore", Ordered, Label("pulsar"), func() {
	var (
		ctx                   context.Context
		cancel                context.CancelFunc
		conn                  *grpc.ClientConn
		err                   error
		firstFileName         string
		materializationClient v1alpha.MaterializationServiceClient
		materializationName   string
		outputPath            string
		outputURI             string
		pulsarClient          pulsar.Client
		pulsarProducer        pulsar.Producer
		table                 *v1alpha.Table
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
		materializationName = "mat_pulsarToObjStore"

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

		// create a pulsar client
		pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:               "pulsar://localhost:6650",
			ConnectionTimeout: 5 * time.Second,
		})
		Expect(err).ShouldNot(HaveOccurred())

		// create a pulsar producer and push initial data to pulsar
		topicName = "topic_pulsarToObjStore"
		pulsarProducer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Topic:  topicName,
			Schema: pulsar.NewAvroSchema(string(helpers.ReadFile("avro/schema.json")), map[string]string{}),
		})
		Expect(err).ShouldNot(HaveOccurred(), "issue creating pulsar producer")

		_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: helpers.ReadFile("avro/msg_0.avro"),
		})
		Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")
		_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: helpers.ReadFile("avro/msg_1.avro"),
		})
		Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")

		// create a table backed by pulsar
		tableName = "table_pulsarToObjStore"
		tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})

		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "time",
			EntityKeyColumnName: "id",
			Source: &v1alpha.Source{
				Source: &v1alpha.Source_Pulsar{
					Pulsar: &v1alpha.PulsarSource{
						Config: getPulsarConfig(topicName),
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
		pulsarProducer.Close()
		pulsarClient.Close()

		// attempt to delete pulsar topic used in test
		cfg := &pulsaradmin.Config{}
		cfg.WebServiceURL = "http://localhost:8080"
		admin, err := pulsaradmin.NewClient(cfg)
		Expect(err).ShouldNot(HaveOccurred(), "issue getting puslar admin client")
		Expect(err).ShouldNot(HaveOccurred())
		topic, _ := utils.GetTopicName(fmt.Sprintf("public/default/%s", topicName))
		err = admin.Topics().Delete(*topic, true, true)
		Expect(err).ShouldNot(HaveOccurred(), "issue deleting pulsar topic")
	})

	Describe("Create a materialization", func() {
		It("Should work without error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression: `
					{
						last_id: table_pulsarToObjStore.id | last(),
						last_time: table_pulsarToObjStore.time | last(),
						count: table_pulsarToObjStore | count(),
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
			Eventually(func(g Gomega) {
				dirs, err := os.ReadDir(outputPath)
				g.Expect(err).ShouldNot(HaveOccurred(), "cannot list output_path files")
				g.Expect(dirs).Should(HaveLen(1))
				firstFileName = dirs[0].Name()

				results := helpers.GetCSV(outputPath + firstFileName)
				g.Expect(results).Should(HaveLen(2)) //header row + 1 data row
				g.Expect(results[0]).Should(ContainElements("_time", "_subsort", "_key_hash", "last_id", "last_time", "count"))
				g.Expect(results[1]).Should(ContainElements("2023-06-20T23:30:01.000000000", "0", "2122274938272070218", "9", "9", "1687303801000000000", "1"))
			}, "5s", "1s").Should(Succeed())
		})
	})

	Describe("Load the more data into the table", func() {
		It("Should work without error", func() {
			_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
				Payload: helpers.ReadFile("avro/msg_2.avro"),
			})
			_, err = pulsarProducer.Send(ctx, &pulsar.ProducerMessage{
				Payload: helpers.ReadFile("avro/msg_3.avro"),
			})
			Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")
		})

		It("Should output additional results to the same csv", func() {
			Eventually(func(g Gomega) {
				dirs, err := os.ReadDir(outputPath)
				g.Expect(err).ShouldNot(HaveOccurred(), "cannot list output_path files")
				g.Expect(dirs).Should(HaveLen(1))

				for _, dir := range dirs {
					if dir.Name() == firstFileName {
						continue
					}
					results := helpers.GetCSV(outputPath + dir.Name())
					g.Expect(results).Should(HaveLen(4)) //header row + 3 data row
					g.Expect(results[0]).Should(ContainElements("_time", "_subsort", "_key_hash", "last_id", "last_time", "count"))
					g.Expect(results[1]).Should(ContainElements("2023-06-20T23:30:01.000000000", "0", "2122274938272070218", "9", "9", "1687303801000000000", "1"))
					g.Expect(results[1]).Should(ContainElements("2023-06-20T23:30:03.000000000", "1", "1575016611515860288", "2", "2", "1687303803000000000", "1"))
					g.Expect(results[1]).Should(ContainElements("2023-06-20T23:30:05.000000000", "2", "11820145550582457114", "4", "4", "1687303805000000000", "1"))
				}
			}, "5s", "1s").Should(Succeed())
		})
	})
})
