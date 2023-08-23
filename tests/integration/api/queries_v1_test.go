package api_test

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	pulsaradmin "github.com/streamnative/pulsar-admin-go"
	"github.com/streamnative/pulsar-admin-go/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Queries V1", Ordered, Label("pulsar"), func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		conn   *grpc.ClientConn
		err    error

		queryClient v1alpha.QueryServiceClient
		tableClient v1alpha.TableServiceClient
		tableName   string

		pulsarClient    pulsar.Client
		pulsarProducer  pulsar.Producer
		pulsarTopicName string
		pulsarTableName string
	)

	BeforeAll(func() {

		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table service
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "queries_v1_test"

		// create table, load data
		table := &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())
		uploadRes := helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
		Expect(uploadRes).ShouldNot(BeNil())

		// also create a table back by pulsar
		pulsarTableName = "queries_v1_pulsar"
		pulsarTopicName = "topic_" + pulsarTableName

		// create a pulsar client
		pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:               "pulsar://localhost:6650",
			ConnectionTimeout: 5 * time.Second,
		})
		Expect(err).ShouldNot(HaveOccurred())

		// create a pulsar producer and push initial data to pulsar
		pulsarProducer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Topic:  pulsarTopicName,
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
		Expect(err).ShouldNot(HaveOccurred(), "failed to publish message")

		// create a table backed by pulsar
		table = &v1alpha.Table{
			TableName:           pulsarTableName,
			TimeColumnName:      "time",
			EntityKeyColumnName: "id",
			Source: &v1alpha.Source{
				Source: &v1alpha.Source_Pulsar{
					Pulsar: &v1alpha.PulsarSource{
						Config: getPulsarConfig(pulsarTopicName),
					},
				},
			},
		}
		_, err = tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "failed to create pulsar-backed table")
	})

	AfterAll(func() {
		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{
			TableName: tableName,
		})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "failed delete file-backed table")
		_, err = tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{
			TableName: pulsarTableName,
		})
		Expect(err).ShouldNot(HaveOccurredGrpc(), "failed to delete pulsar-backed table")

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
		topic, _ := utils.GetTopicName(fmt.Sprintf("public/default/%s", pulsarTopicName))
		err = admin.Topics().Delete(*topic, true, true)
		Expect(err).ShouldNot(HaveOccurred(), "issue deleting pulsar topic")
	})

	Describe("LIST Query", func() {
		It("should work with some or none results", func() {
			res, err := queryClient.ListQueries(ctx, &v1alpha.ListQueriesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(len(res.Queries)).Should(BeNumerically(">=", 0))
		})
	})

	Context("When a query resource is created", func() {
		queryId := ""
		It("should create a query resource with default values", func() {
			query := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression: tableName,
				},
			}
			stream, err := queryClient.CreateQuery(ctx, query)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())
			queryResponses, err := helpers.GetCreateQueryResponses(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(queryResponses)).Should(BeNumerically(">=", 3))

			lastResponse, responses := queryResponses[len(queryResponses)-1], queryResponses[:len(queryResponses)-1]
			Expect(lastResponse).ShouldNot(BeNil())
			Expect(lastResponse.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_SUCCESS))
			var validUUID = false
			for _, queryResponse := range responses {
				if queryResponse.GetQueryId() != "" {
					_, err := uuid.Parse(queryResponse.GetQueryId())
					Expect(err).Should(BeNil())
					validUUID = true
					queryId = queryResponse.GetQueryId()
				}
			}
			Expect(validUUID).Should(BeTrue())
		})
		It("should get an existing query resource", func() {
			queryUUID, err := uuid.Parse(queryId)
			Expect(err).Should(BeNil())
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
				QueryId: queryUUID.String(),
			})
			Expect(err).Should(BeNil())
			Expect(res.Query.QueryId).Should(Equal(queryUUID.String()))
			Expect(res.Query.Destination.GetObjectStore().FileType).Should(Equal(v1alpha.FileType_FILE_TYPE_PARQUET))
			Expect(res.Query.ResultBehavior).Should(Equal(v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS))
		})
		It("should list the queries with newly created resource", func() {
			res, err := queryClient.ListQueries(ctx, &v1alpha.ListQueriesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			var found = false
			for _, query := range res.Queries {
				if query.QueryId == queryId {
					found = true
				}
			}
			Expect(found).Should(BeTrue())
		})
	})

	Describe("GET Query", func() {
		It("invalid query id should return friendly error message", func() {
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
				QueryId: "some-non-existing-query",
			})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())
			errStatus, ok := status.FromError(err)

			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.InvalidArgument))
			Expect(errStatus.Message()).Should(ContainSubstring("query_id"))
		})
		It("non-existent query id should return friendly error message", func() {
			res, err := queryClient.GetQuery(ctx, &v1alpha.GetQueryRequest{
				QueryId: uuid.NewString(),
			})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())
			errStatus, ok := status.FromError(err)

			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(ContainSubstring("query not found"))
		})
	})

	Context("When creating a query from a pulsar-backed table", func() {
		It("should fail and return a helpful error", func() {
			query := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression: pulsarTableName,
				},
			}
			stream, err := queryClient.CreateQuery(ctx, query)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())
			queryResponses, err := helpers.GetCreateQueryResponses(stream)
			Expect(err).Should(HaveOccurredGrpc())
			Expect(queryResponses).Should(HaveLen(1))

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.InvalidArgument))
			Expect(errStatus.Message()).Should(ContainSubstring("not supported on tables backed by streams"))
		})
	})
})
