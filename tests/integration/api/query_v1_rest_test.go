package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	helpers "github.com/kaskada-ai/kaskada/tests/integration/shared/helpers"
)

var _ = PDescribe("Query V1 REST", Ordered, func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var client *http.Client
	var err error
	var jsonBody []byte
	var req *http.Request
	var res *http.Response

	type loadRequestJson struct {
		TableName      string `json:"table_name"`
		ParquetFileUri string `json:"parquet_file_id"`
	}

	BeforeAll(func() {
		ctx = context.Background()
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		client = &http.Client{}

		// delete table in case previous cleanup failed
		req = getRestRequest(ctx, "DELETE", "v1alpha/tables/purchases_rest", nil)
		client.Do(req)

		// create table, get upload url, upload table data
		table := &v1alpha.Table{
			TableName:           "purchases_rest",
			TimeColumnName:      "purchase_time",
			EntityKeyColumnName: "customer_id",
			SubsortColumnName: &wrapperspb.StringValue{
				Value: "subsort_id",
			},
		}
		jsonBody, err = protojson.Marshal(table)
		Expect(err).ShouldNot(HaveOccurred())
		req = getRestRequest(ctx, "POST", "v1alpha/tables", jsonBody)
		res, err = client.Do(req)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(res).ShouldNot(BeNil())
		Expect(res.StatusCode).Should(Equal(http.StatusOK))
		body := getBody(res)
		Expect(body).Should(ContainSubstring("tableId"))
		Expect(body).Should(ContainSubstring("tableName"))
		Expect(body).Should(ContainSubstring("purchases_rest"))

		loadReq := loadRequestJson{
			TableName:      table.TableName,
			ParquetFileUri: helpers.GetTestFileURI("purchases_part1.parquet"),
		}

		jsonBody, err = json.Marshal(loadReq)
		Expect(err).ShouldNot(HaveOccurred())
		req = getRestRequest(ctx, "POST", "v1alpha/tables/load_data", jsonBody)
		res, err = client.Do(req)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(res).ShouldNot(BeNil())
		waitForMinio()
	})

	AfterAll(func() {
		// clean up items used in the test
		req = getRestRequest(ctx, "DELETE", "v1alpha/tables/purchases_rest", nil)
		res, err = client.Do(req)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(res).ShouldNot(BeNil())
		Expect(res.StatusCode).Should(Equal(http.StatusOK))
		body := getBody(res)
		Expect(body).Should(ContainSubstring("dataTokenId"))

		cancel()
	})

	Context("When the table schema is created correctly", func() {
		Describe("Query a table with an invalid schema", func() {
			It("should return an invalid argument error", func() {
				destination := &v1alpha.Destination{}
				destination.Destination = &v1alpha.Destination_ObjectStore{
					ObjectStore: &v1alpha.ObjectStoreDestination{
						FileType: v1alpha.FileType_FILE_TYPE_PARQUET,
					},
				}
				createQueryRequest := &v1alpha.CreateQueryRequest{
					Query: &v1alpha.Query{
						Expression:     "sum(purchases_rest.nachos)",
						Destination:    destination,
						ResultBehavior: v1alpha.Query_RESULT_BEHAVIOR_ALL_RESULTS,
					},
					QueryOptions: &v1alpha.QueryOptions{
						PresignResults: true,
					},
				}

				jsonBody, err = protojson.Marshal(createQueryRequest)
				Expect(err).ShouldNot(HaveOccurred())
				req = getRestRequest(ctx, "POST", "v1alpha/queries", jsonBody)
				res, err = client.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())
				Expect(res.StatusCode).Should(Equal(http.StatusBadRequest))
				body := getBody(res)
				// examine `body` to see the details array returned as json
				// example:
				//
				// {"code":3, "message":"1 errors in Fenl statements; see error details",
				//		"details":[{
				//			"@type":"type.googleapis.com/kaskada.kaskada.v1alpha.FenlDiagnostics",
				//			"fenlDiagnostics":[{
				//				"severity":"SEVERITY_ERROR",
				//				"code":"E0001",
				//				"message":"Illegal field reference",
				//				"sourceLocation":"",
				//				"formatted":"
				//					error[E0001]: Illegal field reference
				//					 = No field named 'nachos'
				//					 = Available fields: 'id', 'purchase_time', 'customer_id', 'vendor_id', 'amount', 'subsort_id'
				//					"
				//			}]
				//		}]
				//	}
				Expect(body).Should(ContainSubstring("1 errors in Fenl statements; see error details"))
				Expect(body).Should(ContainSubstring("Illegal field reference"))
				Expect(body).Should(ContainSubstring("No field named 'nachos'"))
			})
		})
	})
})
