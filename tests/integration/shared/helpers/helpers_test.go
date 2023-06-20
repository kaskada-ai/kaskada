package helpers

import (
	"fmt"
	"io/ioutil"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApi(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Api Suite")
}

var _ = Describe("DecodeAvroFromBytes", func() {
	It("should decode the file as expected", func() {
		schema := `{"type": "record", "name": "MyRecord", "fields": [{"name": "time", "type":"long"}, {"name": "id", "type": "long"}, {"name": "my_val", "type": "long"}]}`

		schema = `{
				"name": "MyRecord",
				"type": "record",
				"fields": [
				   {
					  "name": "time",
					  "type": [
						 "null",
						 "long"
					  ],
					  "default": null
				   },
				   {
					  "name": "id",
					  "type": [
						 "null",
						 "long"
					  ],
					  "default": null
				   },
				   {
					  "name": "my_val",
					  "type": [
						 "null",
						 "long"
					  ],
					  "default": null
				   }
				]
			 }`

		filePath := "../../../../testdata/avro/msg1.avro"
		// time	id	my_val
		// 10	1	5

		fileData, err := ioutil.ReadFile(filePath)
		Expect(err).ShouldNot(HaveOccurred(), fmt.Sprintf("issue finding testdata file: %s", filePath))

		expected := map[string]interface{}{
			"time":   int64(10),
			"id":     int64(1),
			"my_val": int64(5),
		}

		data := DecodeAvroFromBytes(schema, fileData)

		Expect(data).Should(BeEquivalentTo(expected))
	})

})
