package api_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
	. "github.com/kaskada-ai/kaskada/tests/integration/shared/matchers"
)

var _ = Describe("Query V1 with slicing", Ordered, func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		conn        *grpc.ClientConn
		tableClient v1alpha.TableServiceClient
		queryClient v1alpha.QueryServiceClient
		table       *v1alpha.Table
		rowCount    int
	)

	tableName := "transactions_slicing"

	expression := `
{
time: transactions_slicing.transaction_time,
key: transactions_slicing.id,
max_price: transactions_slicing.price | max(),
min_spent_in_single_transaction: min(transactions_slicing.price * transactions_slicing.quantity),
max_spent_in_single_transaction: max(transactions_slicing.price * transactions_slicing.quantity)
}`

	BeforeAll(func() {
		//get connection to wren
		ctx, cancel, conn = grpcConfig.GetContextCancelConnection(120)
		ctx = metadata.AppendToOutgoingContext(ctx, "client-id", *integrationClientID)

		// get a grpc client for the table & compute services
		tableClient = v1alpha.NewTableServiceClient(conn)
		queryClient = v1alpha.NewQueryServiceClient(conn)

		// create a table
		table = &v1alpha.Table{
			TableName:           tableName,
			TimeColumnName:      "transaction_time",
			EntityKeyColumnName: "purchaser_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "idx",
			},
		}
		_, err := tableClient.CreateTable(ctx, &v1alpha.CreateTableRequest{Table: table})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		// load the data files
		helpers.LoadTestFileIntoTable(ctx, conn, table, "transactions/transactions_part1.parquet")
	})

	AfterAll(func() {
		// clean up the table used in the test
		_, err := tableClient.DeleteTable(ctx, &v1alpha.DeleteTableRequest{TableName: tableName})
		Expect(err).ShouldNot(HaveOccurredGrpc())

		cancel()
		conn.Close()
	})

	Describe("Run the query with no slice plan", func() {
		It("should return the full set of query results", func() {
			// define a query to run on the table
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with no slice plan: %d", len(results)))
			rowCount = len(results)

		})
	})

	Describe("Run the query with a 100% slice", func() {
		It("should return the full set of query results", func() {
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Slice: &v1alpha.SliceRequest{
						Slice: &v1alpha.SliceRequest_Percent{
							Percent: &v1alpha.SliceRequest_PercentSlice{
								Percent: 100,
							},
						},
					},
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with 100%% slice: %d", len(results)))

			Expect(len(results)).Should(Equal(rowCount))
		})
	})

	Describe("Run the query with a 10% slice", func() {
		It("should return about 10% of the results", func() {
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Slice: &v1alpha.SliceRequest{
						Slice: &v1alpha.SliceRequest_Percent{
							Percent: &v1alpha.SliceRequest_PercentSlice{
								Percent: 10,
							},
						},
					},
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with 10%% slice: %d", len(results)))

			/*
			 * There are 150 unique entities in this dataset.
			 * Each entity has an average of 333.3 events.
			 * The total dataset size is 50,000 (150 * 333.3 = 49,995)
			 * Assuming uniform distribution (not entirely true), then 10% of the entities = 15 entites
			 * Since the 10% slice is based on a hashing function, there is some room for error.
			 * Random Heuristic: Lower Bound -> 7% (10 entities) and Upper Bound -> 13% (20 entities)
			 * Lower Bound: 3333 (10 * 333.3) and Upper Bound: 6666 (20 * 333.3)
			 */
			Expect(len(results)).Should(BeNumerically("~", 5000, 1666))
		})
	})

	Describe("Run the query with a 0.3% slice", func() {
		It("should return about 0.3% of the results", func() {
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Slice: &v1alpha.SliceRequest{
						Slice: &v1alpha.SliceRequest_Percent{
							Percent: &v1alpha.SliceRequest_PercentSlice{
								Percent: 0.3,
							},
						},
					},
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}

			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with 0.3%% slice: %d", len(results)))

			/*
			 * There are 150 unique entities in this dataset.
			 * Each entity has an average of 333.3 events.
			 * The total dataset size is 50,000 (150 * 333.3 = 49,995)
			 * Assuming uniform distribution (not entirely true), then 0.03% of the entities = ~1 entites (0.45 entities)
			 * Since the 0.3% slice is based on a hashing function, there is some room for error.
			 * Random Heuristic: Lower Bound -> 0% (0 entities) and Upper Bound -> 1% (1.5 entities)
			 * Lower Bound: 0 (0 * 333.3) and Upper Bound: 499.95 (1.5 * 333.3)
			 */
			Expect(len(results)).Should(BeNumerically("~", 250, 125))
		})
	})

	Describe("Run the query with entity key filter", func() {
		It("should return 300 number of results for single", func() {
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Slice: &v1alpha.SliceRequest{
						Slice: &v1alpha.SliceRequest_EntityKeys{
							EntityKeys: &v1alpha.SliceRequest_EntityKeysSlice{
								EntityKeys: []string{
									"2798e270c7cab8c9eeacc046a3100a57",
								},
							},
						},
					},
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}
			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with entity key filter: %d", len(results)))

			// There are two rows in this dataset with the provided entity keys.
			Expect(len(results)).Should(BeNumerically("~", 300))
		})

		It("should return 685 (300 + 385) number of results for multiple entity keys", func() {
			destination := &v1alpha.Destination{}
			destination.Destination = &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
				},
			}
			createQueryRequest := &v1alpha.CreateQueryRequest{
				Query: &v1alpha.Query{
					Expression:     expression,
					Destination:    destination,
					ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					Slice: &v1alpha.SliceRequest{
						Slice: &v1alpha.SliceRequest_EntityKeys{
							EntityKeys: &v1alpha.SliceRequest_EntityKeysSlice{
								EntityKeys: []string{
									"2798e270c7cab8c9eeacc046a3100a57",
									"79b3ced09d3df7c98fbb04fdfda6ce80",
								},
							},
						},
					},
				},
				QueryOptions: &v1alpha.QueryOptions{
					PresignResults: true,
				},
			}
			stream, err := queryClient.CreateQuery(ctx, createQueryRequest)
			Expect(err).ShouldNot(HaveOccurredGrpc())
			Expect(stream).ShouldNot(BeNil())

			res, err := helpers.GetMergedCreateQueryResponse(stream)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.RequestDetails.RequestId).ShouldNot(BeEmpty())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().GetPaths()).ShouldNot(BeNil())
			Expect(res.GetDestination().GetObjectStore().GetOutputPaths().Paths).Should(HaveLen(1))

			resultsUrl := res.GetDestination().GetObjectStore().GetOutputPaths().Paths[0]
			results := helpers.DownloadParquet(resultsUrl)

			helpers.LogLn(fmt.Sprintf("Result set size, with entity key filter: %d", len(results)))

			// There are two rows in this dataset with the provided entity keys.
			Expect(len(results)).Should(BeNumerically("~", 685))
		})
	})
})
