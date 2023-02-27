package api_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Tables", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var dataTokenClient v1alpha.DataTokenServiceClient
	var tableClient v1alpha.TableServiceClient
	var table1, table2 *v1alpha.Table
	var dataToken1 string
	var dataToken2 string
	var firstTableFirstVersion int64
	var secondTableFirstVersion int64
	var dataTokenDelete string
	var viewClient v1alpha.ViewServiceClient
	fileName := "purchases/purchases_part1.parquet"

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table service
		tableClient = v1alpha.NewTableServiceClient(conn)
		dataTokenClient = v1alpha.NewDataTokenServiceClient(conn)
		viewClient = v1alpha.NewViewServiceClient(conn)
	})

	AfterAll(func() {
		cancel()
		conn.Close()
	})

	Describe("Initial List", func() {
		It("should return an empty list of tables", func() {
			res, err := tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(len(res.Tables)).Should(Equal(0))
		})
	})

	Describe("Get a non-existant table", func() {
		It("Should error with a friendly message", func() {
			res, err := tableClient.GetTable(ctx, &v1alpha.GetTableRequest{TableName: "table_does_not_exist"})

			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)

			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(ContainSubstring("table not found"))
		})
	})

	Describe("Create first table", func() {
		It("should allow adding a new table", func() {
			table := &v1alpha.Table{
				TableName:           "first",
				TimeColumnName:      "purchase_time",
				EntityKeyColumnName: "customer_id",
				SubsortColumnName: &wrapperspb.StringValue{
					Value: "subsort_id",
				},
			}

			res, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)
			Expect(res.Table.TableName).Should(Equal(table.TableName))
			Expect(res.Table.TableId).ShouldNot(BeEmpty())

			table1 = res.Table
		})
	})

	Describe("Create a table with missing params", func() {
		It("should not allow adding a new table", func() {
			table := &v1alpha.Table{}

			res, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.InvalidArgument))
			Expect(errStatus.Message()).Should(ContainSubstring("invalid Table.TableName"))
			Expect(errStatus.Message()).Should(ContainSubstring("invalid Table.TimeColumnName"))
			Expect(errStatus.Message()).Should(ContainSubstring("invalid Table.EntityKeyColumnName"))
			Expect(errStatus.Details()).Should(HaveLen(1))
			Expect(errStatus.Details()[0]).Should(BeAssignableToTypeOf(&errdetails.RequestInfo{}))
			requestInfo := errStatus.Details()[0].(*errdetails.RequestInfo)
			Expect(requestInfo.RequestId).ShouldNot(BeEmpty())
		})
	})

	Describe("Create table with existing name", func() {
		It("should return a helpful error", func() {
			table := &v1alpha.Table{
				TableName:           "first",
				TimeColumnName:      "purchase_time",
				EntityKeyColumnName: "customer_id",
				SubsortColumnName: &wrapperspb.StringValue{
					Value: "subsort_id",
				},
			}

			res, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.AlreadyExists))
			Expect(errStatus.Message()).Should(Equal("table already exists"))
		})
	})

	Describe("Get first table, before adding files", func() {
		It("should retrieve the table metadata", func() {

			res, err := tableClient.GetTable(ctx, &v1alpha.GetTableRequest{TableName: "first"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Table).ShouldNot(BeNil())
			table := res.Table
			Expect(table.TableName).Should(Equal("first"))
			Expect(table.TableId).ShouldNot(BeEmpty())
			Expect(table.SubsortColumnName.Value).Should(Equal("subsort_id"))
		})
	})

	Describe("Create second table", func() {
		It("should allow adding a new table", func() {
			table := &v1alpha.Table{
				TableName:           "second",
				TimeColumnName:      "purchase_time",
				EntityKeyColumnName: "customer_id",
				SubsortColumnName: &wrapperspb.StringValue{
					Value: "subsort_id",
				},
			}

			res, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Table.TableName).Should(Equal(table.TableName))
			Expect(res.Table.TableId).ShouldNot(BeEmpty())

			table2 = res.Table
		})
	})

	Describe("List tables", func() {
		It("should show both tables in the list", func() {
			res, err := tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			VerifyRequestDetails(res.RequestDetails)

			Expect(res.Tables).Should(ConsistOf(TableMatcher(table1), TableMatcher(table2)))
		})
	})

	Describe("Add a file to the first table", func() {
		It("Should work without error", func() {
			time.Sleep(time.Millisecond * 2) //so table create & update times are different

			res, err := tableClient.LoadData(ctx, &v1alpha.LoadDataRequest{
				TableName: table1.TableName,
				SourceData: &v1alpha.LoadDataRequest_FileInput{
					FileInput: &v1alpha.FileInput{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						Uri:      helpers.GetFileURI(fileName),
					},
				},
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataTokenId).ShouldNot(BeNil())

			dataToken1 = res.DataTokenId
		})
	})

	Describe("Get the data token", func() {
		It("Should get the data token", func() {
			res, err := dataTokenClient.GetDataToken(ctx, &v1alpha.GetDataTokenRequest{
				DataTokenId: dataToken1,
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataToken).ShouldNot(BeNil())
			Expect(res.DataToken.DataTokenId).Should(Equal(dataToken1))
			Expect(res.DataToken.TableVersions[table1.TableId]).ShouldNot(Equal(int64(0)))
			firstTableFirstVersion = res.DataToken.TableVersions[table1.TableId]
		})
	})

	Describe("Get the current dataToken", func() {
		It("Should match data token 1", func() {
			res, err := dataTokenClient.GetDataToken(ctx, &v1alpha.GetDataTokenRequest{
				DataTokenId: "current_data_token",
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataToken).ShouldNot(BeNil())
			Expect(res.DataToken.DataTokenId).Should(Equal(dataToken1))
			Expect(res.DataToken.TableVersions[table1.TableId]).Should(Equal(firstTableFirstVersion))
		})
	})

	Describe("Get first table, after adding a file", func() {
		It("should retrieve the table metadata", func() {
			res, err := tableClient.GetTable(ctx, &v1alpha.GetTableRequest{TableName: "first"})

			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Table).ShouldNot(BeNil())

			table := res.Table
			Expect(table.TableName).Should(Equal("first"))
			Expect(table.TableId).ShouldNot(BeEmpty())
			Expect(table.UpdateTime.AsTime()).Should(BeTemporally(">", table.CreateTime.AsTime()))
			Expect(table.Version).To(Equal(firstTableFirstVersion))
		})
	})

	Describe("Add the file to the first table again", func() {
		It("Should error with a friendly message", func() {
			res, err := tableClient.LoadData(ctx, &v1alpha.LoadDataRequest{
				TableName: table1.TableName,
				SourceData: &v1alpha.LoadDataRequest_FileInput{
					FileInput: &v1alpha.FileInput{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						Uri:      helpers.GetFileURI(fileName),
					},
				},
			})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.AlreadyExists))
			Expect(errStatus.Message()).Should(ContainSubstring("file already exists in table"))
		})
	})

	Describe("Add the file to a non-existant table", func() {
		It("Should error with a friendly message", func() {
			res, err := tableClient.LoadData(ctx, &v1alpha.LoadDataRequest{
				TableName: "table_does_not_exist",
				SourceData: &v1alpha.LoadDataRequest_FileInput{
					FileInput: &v1alpha.FileInput{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						Uri:      helpers.GetFileURI(fileName),
					},
				},
			})

			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)

			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(ContainSubstring("table not found"))
		})
	})

	Describe("Delete an existing table", func() {
		It("should work without issue", func() {
			res, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "first"})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteTableResponse{}))
			Expect(res.DataTokenId).ShouldNot(Equal(dataToken1))
			dataTokenDelete = res.DataTokenId
		})
	})

	Describe("List tables", func() {
		It("should show only the second table", func() {
			res, err := tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Tables).Should(ConsistOf(TableMatcher(table2)))
		})
	})

	Describe("Add the file to the second table", func() {
		It("should work without issue", func() {
			time.Sleep(time.Millisecond * 2) //so table create & update times are different

			res, err := tableClient.LoadData(ctx, &v1alpha.LoadDataRequest{
				TableName: table2.TableName,
				SourceData: &v1alpha.LoadDataRequest_FileInput{
					FileInput: &v1alpha.FileInput{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						Uri:      helpers.GetFileURI(fileName),
					},
				},
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataTokenId).ShouldNot(BeNil())
			Expect(res.DataTokenId).ShouldNot(Equal(dataToken1))
			Expect(res.DataTokenId).ShouldNot(Equal(dataTokenDelete))
			dataToken2 = res.DataTokenId
		})
	})

	Describe("Get the data token after adding to the second table", func() {
		It("Should get the data token", func() {
			res, err := dataTokenClient.GetDataToken(ctx, &v1alpha.GetDataTokenRequest{
				DataTokenId: dataToken2,
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataToken).ShouldNot(BeNil())
			Expect(res.DataToken.DataTokenId).Should(Equal(dataToken2))
			Expect(res.DataToken.TableVersions[table2.TableId]).Should(BeNumerically(">", firstTableFirstVersion))
			secondTableFirstVersion = res.DataToken.TableVersions[table2.TableId]
		})
	})

	Describe("Get the current dataToken adding to the second table", func() {
		It("Should match data token 2", func() {
			res, err := dataTokenClient.GetDataToken(ctx, &v1alpha.GetDataTokenRequest{
				DataTokenId: "current_data_token",
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataToken).ShouldNot(BeNil())
			Expect(res.DataToken.DataTokenId).Should(Equal(dataToken2))
			Expect(res.DataToken.TableVersions[table2.TableId]).Should(Equal(secondTableFirstVersion))
		})
	})

	Describe("Delete an non-existing table", func() {
		It("should return a helpful error", func() {
			res, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "first"})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.NotFound))
			Expect(errStatus.Message()).Should(Equal("table not found"))
		})
	})

	Describe("Add a view dependency to the remaining table", func() {
		It("should work without issue", func() {
			res, err := viewClient.CreateView(ctx, &v1alpha.CreateViewRequest{
				View: &v1alpha.View{
					ViewName:   "view_dependency",
					Expression: "second",
				},
			})

			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
		})
	})

	Describe("Try to delete the remaining table", func() {
		It("should NOT work, due to the view dependency", func() {
			res, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "second"})
			Expect(err).Should(HaveOccurredGrpc())
			Expect(res).Should(BeNil())

			//inspect error response
			errStatus, ok := status.FromError(err)
			Expect(ok).Should(BeTrue())
			Expect(errStatus.Code()).Should(Equal(codes.FailedPrecondition))
			Expect(errStatus.Message()).Should(Equal("unable to delete table. detected: 1 dependencies."))

			//inspect error details
			details := errStatus.Proto().Details
			Expect(details).Should(HaveLen(2))

			pf := new(errdetails.PreconditionFailure)
			Expect(details[0].MessageIs(pf)).Should(BeTrue())
			Expect(details[0].UnmarshalTo(pf)).Should(Succeed())
			Expect(pf.GetViolations()).Should(HaveLen(1))

			v := pf.GetViolations()[0]
			Expect(v.Type).Should(Equal("foreign key constraint"))
			Expect(v.Subject).Should(Equal("table: \"second\" is referenced by view: \"view_dependency\""))
			Expect(v.Description).Should(Equal("unable to complete operation. the table: \"second\" is referenced by view: \"view_dependency\". please delete the view and retry."))
		})
	})

	Describe("Delete the remaining table, with force-delete", func() {
		It("should work without issue", func() {
			res, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: "second", Force: true})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteTableResponse{}))
		})
	})

	Describe("Delete the view dependency to the remaining table", func() {
		It("should work without issue", func() {
			res, err := viewClient.DeleteView(ctx, &v1alpha.DeleteViewRequest{
				ViewName: "view_dependency",
			})

			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
		})
	})

	Describe("List tables", func() {
		It("should show no tables", func() {
			res, err := tableClient.ListTables(ctx, &v1alpha.ListTablesRequest{})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(len(res.Tables)).Should(Equal(0))
		})
	})

	Describe("Should create a table without a subsort column", func() {
		tableName := "missing_subsort"
		It("should create the table without issue", func() {
			table := &v1alpha.Table{
				TableName:           tableName,
				TimeColumnName:      "purchase_time",
				EntityKeyColumnName: "customer_id",
			}
			res, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.Table.TableName).Should(Equal(table.TableName))
			Expect(res.Table.TableId).ShouldNot(BeEmpty())
		})

		It("should allow adding a file without issue", func() {
			time.Sleep(time.Millisecond * 2) //so table create & update times are different

			res, err := tableClient.LoadData(ctx, &v1alpha.LoadDataRequest{
				TableName: tableName,
				SourceData: &v1alpha.LoadDataRequest_FileInput{
					FileInput: &v1alpha.FileInput{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
						Uri:      helpers.GetFileURI(fileName),
					},
				},
			})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res.DataTokenId).ShouldNot(BeNil())
			Expect(res.DataTokenId).ShouldNot(Equal(dataToken1))
			Expect(res.DataTokenId).ShouldNot(Equal(dataToken2))
			Expect(res.DataTokenId).ShouldNot(Equal(dataTokenDelete))
		})

		It("should allow deleting the table after", func() {
			res, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(res).ShouldNot(BeNil())
			Expect(res).Should(BeAssignableToTypeOf(&v1alpha.DeleteTableResponse{}))
		})
	})
})
