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

var _ = Describe("Query V1 gRPC Errors", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var conn *grpc.ClientConn
	var tableClient v1alpha.TableServiceClient
	var queryClient v1alpha.QueryServiceClient
	var destination *v1alpha.Destination
	var sliceRequest *v1alpha.SliceRequest
	var tableName string

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(10)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		tableName = "query_v1_errors"

		// create table, load table data
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

		helpers.WriteTestFile("purchases/purchases_temp.parquet", helpers.ReadTestFile("purchases/purchases_part1.parquet"))
		helpers.LoadTestFileIntoTable(ctx, conn, table, "purchases/purchases_temp.parquet")

		destination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			},
		}

		sliceRequest = &v1alpha.SliceRequest{
			Slice: &v1alpha.SliceRequest_Percent{
				Percent: &v1alpha.SliceRequest_PercentSlice{
					Percent: 100,
				},
			},
		}
	})

	AfterAll(func() {
		// clean up items used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Context("When the table schema is created correctly", func() {
		Describe("Reference an invalid field", func() {
			It("should return an invalid argument error", func() {
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:  "sum(query_v1_errors.Tacos)",
						Destination: destination,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				res, err := helpers.GetMergedCreateQueryResponse(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())

				Expect(res).ShouldNot(BeNil())
				Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
				Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).Should(BeNil())

				Expect(res.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_FAILURE))

				Expect(res.Analysis.CanExecute).Should(BeFalse())

				Expect(res.FenlDiagnostics).ShouldNot(BeNil())
				Expect(res.FenlDiagnostics.NumErrors).Should(BeEquivalentTo(1))

				diagnostics := res.FenlDiagnostics.FenlDiagnostics
				Expect(diagnostics).Should(HaveLen(1))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("Illegal field reference")))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("No field named 'Tacos'")))
			})
		})

		Describe("Reference an invalid field, with dry-run", func() {
			It("should return an invalid argument error", func() {
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:  "sum(query_v1_errors.Tacos)",
						Destination: destination,
					},
					QueryOptions: &v1alpha.QueryOptions{
						DryRun: true,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				res, err := helpers.GetMergedCreateQueryResponse(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())

				Expect(res).ShouldNot(BeNil())
				Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
				Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).Should(BeNil())

				Expect(res.State).Should(Equal(v1alpha.CreateQueryResponse_STATE_ANALYSIS))

				Expect(res.Analysis.CanExecute).Should(BeFalse())

				Expect(res.FenlDiagnostics).ShouldNot(BeNil())
				Expect(res.FenlDiagnostics.NumErrors).Should(BeEquivalentTo(1))

				diagnostics := res.FenlDiagnostics.FenlDiagnostics
				Expect(diagnostics).Should(HaveLen(1))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("Illegal field reference")))
				Expect(diagnostics).Should(ContainElement(ContainSubstring("No field named 'Tacos'")))
			})
		})
	})

	Context("verifying that compute in-place file handeling works correctly", func() {
		Describe("Run a basic query, to populate the prepare cache based on an empty slice config", func() {
			It("should work without error", func() {
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:  tableName,
						Destination: destination,
						Slice:       nil,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())

				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(queryResponses)).Should(BeNumerically(">=", 3))
			})
		})

		Describe("alter the underlying table data, and compute on a non-nil slice config", func() {
			It("should return an error indicating the data has changed", func() {
				helpers.WriteTestFile("purchases/purchases_temp.parquet", helpers.ReadTestFile("purchases/purchases_part2.parquet"))

				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:  tableName,
						Destination: destination,
						Slice:       sliceRequest,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())
				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).Should(HaveOccurredGrpc())
				Expect(queryResponses).Should(HaveLen(4))

				//inspect error response
				errStatus, ok := status.FromError(err)
				Expect(ok).Should(BeTrue())
				Expect(errStatus.Code()).Should(Equal(codes.FailedPrecondition))
				Expect(errStatus.Message()).Should(ContainSubstring("contents has changed"))

				//inspect last response
				Expect(queryResponses[3].State).Should(Equal(v1alpha.CreateQueryResponse_STATE_FAILURE))
			})
		})

		Describe("remove underlying table data, and compute on a non-nil slice config", func() {
			It("should return an error indicating the data no longer exists", func() {
				helpers.DeleteTestFile("purchases/purchases_temp.parquet")

				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:  tableName,
						Destination: destination,
						Slice:       sliceRequest,
					},
				}

				stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
				Expect(err).ShouldNot(HaveOccurredGrpc())
				Expect(stream).ShouldNot(BeNil())
				queryResponses, err := helpers.GetCreateQueryResponses(stream)
				Expect(err).Should(HaveOccurredGrpc())
				Expect(queryResponses).Should(HaveLen(4))

				//inspect error response
				errStatus, ok := status.FromError(err)
				Expect(ok).Should(BeTrue())
				Expect(errStatus.Code()).Should(Equal(codes.FailedPrecondition))
				Expect(errStatus.Message()).Should(ContainSubstring("has been removed or is no longer accessible"))

				//inspect last response
				Expect(queryResponses[3].State).Should(Equal(v1alpha.CreateQueryResponse_STATE_FAILURE))
			})
		})

	})
})
