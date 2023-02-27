package api_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Materializations", Ordered, Label("redis"), Label("redis-ai"), func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var tableClient v1alpha.TableServiceClient
	var matClient v1alpha.MaterializationServiceClient
	var mat1, mat2 *v1alpha.Materialization
	var maxAmount, minAmount string
	var destination *v1alpha.Materialization_Destination

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get the required grpc clients
		tableClient = v1alpha.NewTableServiceClient(conn)
		matClient = v1alpha.NewMaterializationServiceClient(conn)

		maxAmount = `
{
time: purchases_mat_test.purchase_time,
entity: purchases_mat_test.customer_id,
max_amount: purchases_mat_test.amount | max(),
}`

		minAmount = `
{
time: purchases_mat_test.purchase_time,
entity: purchases_mat_test.customer_id,
min_amount: purchases_mat_test.amount | min(),
}`

		destination = &v1alpha.Materialization_Destination{
			Destination: &v1alpha.Materialization_Destination_Redis{
				Redis: &v1alpha.RedisDestination{
					HostName:       "redis",
					Port:           6379,
					DatabaseNumber: 2,
				},
			},
		}

		// create a table for the tests and load data into it
		table := &v1alpha.Table{
			TableName:           "purchases_mat_test",
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}

		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_part1.parquet")
	})

	AfterAll(func() {
		// cleanup items created in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "purchases_mat_test"})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("Initial List", func() {
		It("should return an empty list of materializations", func() {
			res, err := matClient.ListMaterializations(ctx, &v1alpha.ListMaterializationsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res.Materializations).Should(HaveLen(0))
		})
	})

	Describe("Create first materialization", func() {
		It("should allow adding a new materialization", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: "maxAmount",
					Query:               maxAmount,
					Destination:         destination,
					Slice:               &v1alpha.SliceRequest{},
				},
			}
			res, err := matClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)

			Expect(res.Materialization.MaterializationName).Should(Equal("maxAmount"))
			Expect(res.Materialization.MaterializationId).ShouldNot(BeEmpty())

			mat1 = res.Materialization
		})
	})

	Describe("Create materialization with existing name", func() {
		It("should return a helpful error", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: "maxAmount",
					Query:               maxAmount,
					Destination:         destination,
					Slice:               &v1alpha.SliceRequest{},
				},
			}

			res, err := matClient.CreateMaterialization(ctx, createRequest)
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.AlreadyExists))
			Expect(errStatus.Message()).Should(Equal("materialization already exists"))
		})
	})

	Describe("Get first materialization", func() {
		It("should retrieve the materialization metadata", func() {
			res, err := matClient.GetMaterialization(ctx, &v1alpha.GetMaterializationRequest{MaterializationName: "maxAmount"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Materialization.Query).Should(Equal(maxAmount))
		})
	})

	Describe("Create second materialization", func() {
		It("should allow adding a new materialization", func() {
			createRequest := &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					MaterializationName: "minAmount",
					Query:               minAmount,
					Destination:         destination,
					Slice:               &v1alpha.SliceRequest{},
				},
			}
			res, err := matClient.CreateMaterialization(ctx, createRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)

			Expect(res.Materialization.MaterializationName).Should(Equal("minAmount"))
			Expect(res.Materialization.MaterializationId).ShouldNot(BeEmpty())

			mat2 = res.Materialization
		})
	})

	Describe("List materializations", func() {
		It("should show both materializations in the list", func() {
			res, err := matClient.ListMaterializations(ctx, &v1alpha.ListMaterializationsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Materializations).Should(ConsistOf(MaterializationMatcher(mat1), MaterializationMatcher(mat2)))
		})
	})

	Describe("Delete an existing materialization", func() {
		It("should work without issue", func() {
			res, err := matClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "maxAmount"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteMaterializationResponse{}))
		})
	})

	Describe("List materializations", func() {
		It("should only show the second materialization the list", func() {
			res, err := matClient.ListMaterializations(ctx, &v1alpha.ListMaterializationsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Materializations).Should(ConsistOf(MaterializationMatcher(mat2)))
		})
	})

	Describe("Delete an non-existing materialization", func() {
		It("should return a helpful error", func() {
			res, err := matClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: "invalid"})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(Equal("materialization not found"))
		})
	})

	Describe("Delete the second materialization", func() {
		It("should work without issue", func() {
			res, err := matClient.DeleteMaterialization(ctx, &v1alpha.DeleteMaterializationRequest{MaterializationName: mat2.MaterializationName})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteMaterializationResponse{}))
		})
	})

	Describe("List materializations", func() {
		It("should be an empty list", func() {
			res, err := matClient.ListMaterializations(ctx, &v1alpha.ListMaterializationsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Materializations).Should(BeEmpty())
		})
	})
})
