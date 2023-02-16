package internal

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"

	"github.com/kaskada/kaskada-ai/wren/ent"
)

var expectedNextVersionID = int64(-1)

func testDataTokenVersionID(dataToken *ent.DataToken) {
	if expectedNextVersionID < int64(0) {
		expectedNextVersionID = dataToken.DataVersionID
	}
	Expect(dataToken.DataVersionID).Should(Equal(expectedNextVersionID))
	expectedNextVersionID += 1
}

func testCurrentDataToken(dataTokenClient DataTokenClient, owner *ent.Owner, expectedToken *ent.DataToken) {
	currentDataToken, err := dataTokenClient.GetCurrentDataToken(ctx, owner)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(currentDataToken).ShouldNot(BeNil())
	Expect(currentDataToken.ID).Should(Equal(expectedToken.ID))
}

// simple struct to ease verifying expected dataVersion results
type versionProps struct {
	ID        int64
	OwnerID   uuid.UUID
	TableID   uuid.UUID
	TableName string
}

func testGetTableVersions(dataTokenClient DataTokenClient, owner *ent.Owner, dataToken *ent.DataToken, expectedTableVersions map[*ent.KaskadaTable]int64) {
	tableVersionMap, err := dataTokenClient.GetTableVersions(ctx, owner, dataToken)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(tableVersionMap).ShouldNot(BeNil())

	dataVersionProps := []*versionProps{}

	for tableID, dataVersion := range tableVersionMap {
		ownerID, err := dataVersion.QueryOwner().FirstID(ctx)
		Expect(err).ShouldNot(HaveOccurred())

		table, err := dataVersion.QueryKaskadaTable().First(ctx)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(tableID).Should(Equal(table.ID))

		dataVersionProps = append(dataVersionProps, &versionProps{
			ID:        dataVersion.ID,
			OwnerID:   ownerID,
			TableID:   table.ID,
			TableName: table.Name,
		})
	}

	matchers := []types.GomegaMatcher{}

	for expectedTable, expectedVersion := range expectedTableVersions {
		matchers = append(matchers, SatisfyAll(
			HaveField("ID", expectedVersion),
			HaveField("OwnerID", owner.ID),
			HaveField("TableID", expectedTable.ID),
			HaveField("TableName", expectedTable.Name),
		))
	}

	Expect(dataVersionProps).Should(ConsistOf(matchers))
}

var _ = Describe("TableVersioningTest", func() {
	var (
		owner           *ent.Owner
		dataTokenClient DataTokenClient
		tableClient     KaskadaTableClient

		table1File1DataToken   *ent.DataToken
		table1File2DataToken   *ent.DataToken
		table1File3DataToken   *ent.DataToken
		table1DeletedDataToken *ent.DataToken

		table2File1DataToken   *ent.DataToken
		table2File2DataToken   *ent.DataToken
		table2DeletedDataToken *ent.DataToken

		table3File1DataToken *ent.DataToken
		table3File2DataToken *ent.DataToken

		table1 *ent.KaskadaTable
		table2 *ent.KaskadaTable
		table3 *ent.KaskadaTable

		newTable2               *ent.KaskadaTable
		newTable2File1DataToken *ent.DataToken
		newTable2File2DataToken *ent.DataToken
	)

	BeforeEach(func() {
		owner = getOwner()
		Expect(owner).ShouldNot(BeNil())

		dataTokenClient = NewDataTokenClient(entClient)
		Expect(dataTokenClient).ShouldNot(BeNil())

		tableClient = NewKaskadaTableClient(entClient)
		Expect(tableClient).ShouldNot(BeNil())
	})

	Context("when no data has been added to the system", func() {
		It("should allow creating a new table", func() {
			table1 = testCreateTable(tableClient, owner, "table_1")
		})

		It("should allow getting the newly created table by name", func() {
			table, err := tableClient.GetKaskadaTableByName(ctx, owner, table1.Name)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(table.ID).Should(Equal(table1.ID))
		})

		It("should allow getting the newly created table by ID", func() {
			table, err := tableClient.GetKaskadaTable(ctx, owner, table1.ID)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(table.ID).Should(Equal(table1.ID))
		})
	})

	Context("when adding the first file to table_1", func() {
		It("should work without error", func() {
			table1File1DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_1_file_1"}, table1)
			testDataTokenVersionID(table1File1DataToken)
		})
	})

	Context("after the first file is added to table_1", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table1File1DataToken)
		})

		It("can get the table version created from the first file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File1DataToken, expectedTableVersions)
		})
	})

	Context("when adding the second file table_1", func() {
		It("should work without error", func() {
			table1File2DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_1_file_2"}, table1)
			testDataTokenVersionID(table1File2DataToken)
		})
	})

	Context("after the second file is added to table_1", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table1File2DataToken)
		})

		It("can get the table version created from the first file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
		})
	})

	Context("when creating the second table", func() {
		It("should work without issue", func() {
			table2 = testCreateTable(tableClient, owner, "table_2")
		})

		It("can get the current data token, which should not have changed", func() {
			testCurrentDataToken(dataTokenClient, owner, table1File2DataToken)
		})

		It("can get the table versions, which should not have changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
		})
	})

	Context("when adding the first file to table_2", func() {
		It("should work without error", func() {
			table2File1DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_2_file_1"}, table2)
			testDataTokenVersionID(table2File1DataToken)
		})
	})

	Context("after the first file is added to table_2", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table2File1DataToken)
		})

		It("can get the table versions created from the third file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
				table2: table2File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table2File1DataToken, expectedTableVersions)
		})

		It("the table versions created from the second file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
		})

		It("the table versions created from the first file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File1DataToken, expectedTableVersions)
		})
	})

	Context("when adding the second file to table_2", func() {
		It("should work without error", func() {
			table2File2DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_2_file_2"}, table2)
			testDataTokenVersionID(table2File2DataToken)
		})
	})

	Context("after the second file is added to table_2", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table2File2DataToken)
		})

		It("can get the table versions created from the fourth file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table2File2DataToken, expectedTableVersions)
		})

		It("the table versions created from the third file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
				table2: table2File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table2File1DataToken, expectedTableVersions)
		})

		It("the table versions created from the second file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
		})

		It("the table versions created from the first file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File1DataToken, expectedTableVersions)
		})
	})

	Context("when adding the third file to table_1", func() {
		It("should work without error", func() {
			table1File3DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_1_file_3"}, table1)
			testDataTokenVersionID(table1File3DataToken)
		})
	})

	Context("after the third file is added to table_1", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table1File3DataToken)
		})

		It("can get the table versions created from the fifth file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File3DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File3DataToken, expectedTableVersions)
		})

		It("the table versions created from the fourth file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table2File2DataToken, expectedTableVersions)
		})

		It("the table versions created from the third file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
				table2: table2File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table2File1DataToken, expectedTableVersions)
		})

		It("the table versions created from the second file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
		})

		It("the table versions created from the first file add haven't changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File1DataToken, expectedTableVersions)
		})
	})

	Context("when creating the third table", func() {
		It("should work without issue", func() {
			table3 = testCreateTable(tableClient, owner, "table_3")
		})

		It("can get the current data token, which should not have changed", func() {
			testCurrentDataToken(dataTokenClient, owner, table1File3DataToken)
		})

		It("can get the table versions, which should not have changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File3DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table1File3DataToken, expectedTableVersions)
		})
	})

	Context("when adding the first file to table_3", func() {
		It("should work without error", func() {
			table3File1DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_3_file_1"}, table3)
			testDataTokenVersionID(table3File1DataToken)
		})
	})

	Context("after the first file is added to table_3", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table3File1DataToken)
		})

		It("can get the table versions created from the sixth file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File3DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
				table3: table3File1DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table3File1DataToken, expectedTableVersions)
		})
	})

	Context("when adding the second file to table_3", func() {
		It("should work without error", func() {
			table3File2DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_3_file_2"}, table3)
			testDataTokenVersionID(table3File2DataToken)
		})
	})

	Context("after the second file is added to table_3", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table3File2DataToken)
		})

		It("can get the table versions created from the seventh file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table1: table1File3DataToken.DataVersionID,
				table2: table2File2DataToken.DataVersionID,
				table3: table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table3File2DataToken, expectedTableVersions)
		})
	})

	Context("when the first table is deleted", func() {
		It("should work without error", func() {
			table1DeletedDataToken = testDeleteTable(tableClient, owner, table1)
			testDataTokenVersionID(table1DeletedDataToken)
		})
	})

	Context("after the first table is deleted", func() {
		It("can get the current data token, which should match the deleted data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table1DeletedDataToken)
		})

		It("can get the table versions created from the seventh file add, without table_1", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table2: table2File2DataToken.DataVersionID,
				table3: table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table3File2DataToken, expectedTableVersions)
		})
	})

	Context("when the second table is deleted", func() {
		It("should work without error", func() {
			table2DeletedDataToken = testDeleteTable(tableClient, owner, table2)
			testDataTokenVersionID(table2DeletedDataToken)
		})
	})

	Context("after the second table is deleted", func() {
		It("can get the current data token, which should match the deleted data token", func() {
			testCurrentDataToken(dataTokenClient, owner, table2DeletedDataToken)
		})

		It("can get the table versions created from the seventh file add, without tables 1 and 2", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table3: table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table3File2DataToken, expectedTableVersions)
		})
	})

	Context("when re-creating the second table", func() {
		It("should work without issue", func() {
			newTable2 = testCreateTable(tableClient, owner, "table_2")
		})

		It("can get the current data token, which should not have changed", func() {
			testCurrentDataToken(dataTokenClient, owner, table2DeletedDataToken)
		})

		It("can get the table versions, which should not have changed", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				table3: table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, table3File2DataToken, expectedTableVersions)
		})
	})

	Context("when re-adding the first file to table_2", func() {
		It("should work without error", func() {
			newTable2File1DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_2_file_1"}, newTable2)
			testDataTokenVersionID(newTable2File1DataToken)
		})
	})

	Context("after the first file is re-added to table_2", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, newTable2File1DataToken)
		})

		It("can get the table versions created from the eighth file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				newTable2: newTable2File1DataToken.DataVersionID,
				table3:    table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, newTable2File1DataToken, expectedTableVersions)
		})
	})

	Context("when re-adding the second file to table_2", func() {
		It("should work without error", func() {
			newTable2File2DataToken = testAddingFilesToTable(tableClient, owner, []string{"table_2_file_2"}, newTable2)
			testDataTokenVersionID(newTable2File2DataToken)
		})
	})

	Context("after the second file is re-added to table_2", func() {
		It("can get the current data token", func() {
			testCurrentDataToken(dataTokenClient, owner, newTable2File2DataToken)
		})

		It("can get the table versions created from the ninth file add", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{
				newTable2: newTable2File2DataToken.DataVersionID,
				table3:    table3File2DataToken.DataVersionID,
			}

			testGetTableVersions(dataTokenClient, owner, newTable2File2DataToken, expectedTableVersions)
		})
	})

	Context("when getting table versions from data tokens with deleted tables", func() {
		It("works without error, and excludes the deleted table from the result set", func() {
			expectedTableVersions := map[*ent.KaskadaTable]int64{}

			testGetTableVersions(dataTokenClient, owner, table1File1DataToken, expectedTableVersions)
			testGetTableVersions(dataTokenClient, owner, table1File2DataToken, expectedTableVersions)
			testGetTableVersions(dataTokenClient, owner, table1File3DataToken, expectedTableVersions)
			testGetTableVersions(dataTokenClient, owner, table2File1DataToken, expectedTableVersions)
			testGetTableVersions(dataTokenClient, owner, table2File2DataToken, expectedTableVersions)

			expectedTableVersions[table3] = table3File1DataToken.DataVersionID
			testGetTableVersions(dataTokenClient, owner, table3File1DataToken, expectedTableVersions)

			expectedTableVersions[table3] = table3File2DataToken.DataVersionID
			testGetTableVersions(dataTokenClient, owner, table3File2DataToken, expectedTableVersions)
		})
	})

	Context("cleanup: deleted the remaining tables", func() {
		It("should work without error", func() {
			testDeleteTable(tableClient, owner, table3)
			testDeleteTable(tableClient, owner, newTable2)
		})
	})
})
