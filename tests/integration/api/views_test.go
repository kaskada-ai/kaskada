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

var _ = Describe("Views", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var tableClient v1alpha.TableServiceClient
	var tableName string
	var viewClient v1alpha.ViewServiceClient
	var view1, view2 *v1alpha.View
	var maxAmount, minAmount string

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get the required grpc clients
		tableClient = v1alpha.NewTableServiceClient(conn)
		viewClient = v1alpha.NewViewServiceClient(conn)

		maxAmount = `
{
time: purchases_views_test.purchase_time,
entity: purchases_views_test.customer_id,
max_amount: purchases_views_test.amount | max(),
}`

		minAmount = `
{
time: purchases_views_test.purchase_time,
entity: purchases_views_test.customer_id,
min_amount: purchases_views_test.amount | min(),
}`

		tableName = "purchases_views_test"

		// create table, load data
		table := &v1alpha.Table{
			TableName:           tableName,
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
		// clean up the items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("Initial List", func() {
		It("should return an empty list of views", func() {
			res, err := viewClient.ListViews(ctx, &v1alpha.ListViewsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res.Views).Should(HaveLen(0))
		})
	})

	Describe("Create first view", func() {
		It("should allow adding a new view", func() {
			view := &v1alpha.View{
				ViewName:   "max_amount",
				Expression: maxAmount,
			}

			res, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{View: view})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res.View.ViewName).Should(Equal(view.ViewName))
			Expect(res.View.ViewId).ShouldNot(BeEmpty())

			view1 = res.View
		})
	})

	Describe("Create a view with missing params", func() {
		It("should allow adding a new view", func() {
			view := &v1alpha.View{
				ViewName: "missing_expression",
			}

			res, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{View: view})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.InvalidArgument))
			Expect(errStatus.Message()).Should(ContainSubstring("invalid View.Expression: value length must be at least 1 runes"))
		})
	})

	Describe("Create view with existing name", func() {
		It("should return a helpful error", func() {
			view := &v1alpha.View{
				ViewName:   "max_amount",
				Expression: maxAmount,
			}

			res, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{View: view})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.AlreadyExists))
			Expect(errStatus.Message()).Should(Equal("view already exists"))
		})
	})

	Describe("Get first view", func() {
		It("should retrieve the view metadata", func() {
			res, err := viewClient.GetView(ctx, &v1alpha.GetViewRequest{ViewName: "max_amount"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.View.ViewName).Should(Equal("max_amount"))
			Expect(res.View.ViewId).ShouldNot(BeEmpty())
			Expect(res.View.Expression).Should(Equal(maxAmount))
		})
	})

	Describe("Create second view", func() {
		It("should allow adding a new view", func() {
			view := &v1alpha.View{
				ViewName:   "min_amount",
				Expression: minAmount,
			}

			res, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{View: view})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.View.ViewName).Should(Equal(view.ViewName))
			Expect(res.View.ViewId).ShouldNot(BeEmpty())

			view2 = res.View
		})
	})

	Describe("List views", func() {
		It("should show both views in the list", func() {
			res, err := viewClient.ListViews(ctx, &v1alpha.ListViewsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Views).Should(ConsistOf(view1, view2))
		})
	})

	Describe("Delete an existing view", func() {
		It("should work without issue", func() {
			res, err := viewClient.DeleteView(ctx, &v1alpha.DeleteViewRequest{ViewName: "max_amount"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteViewResponse{}))
		})
	})

	Describe("List views", func() {
		It("should show only the second view", func() {
			res, err := viewClient.ListViews(ctx, &v1alpha.ListViewsRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Views).Should(ConsistOf(view2))
		})
	})

	Describe("Delete an non-existing view", func() {
		It("should return a helpful error", func() {
			res, err := viewClient.DeleteView(ctx, &v1alpha.DeleteViewRequest{ViewName: "max_amount"})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(Equal("view not found"))
		})
	})

	Describe("Delete the second view", func() {
		It("should work without issue", func() {
			res, err := viewClient.DeleteView(ctx, &v1alpha.DeleteViewRequest{ViewName: "min_amount"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteViewResponse{}))
		})
	})
})
