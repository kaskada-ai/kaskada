package internal

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kaskada/kaskada-ai/wren/ent"
	"github.com/kaskada/kaskada-ai/wren/ent/kaskadafile"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	"github.com/kaskada/kaskada-ai/wren/property"
)

var _ = Describe("KaskadaTableClient", func() {
	var (
		owner            *ent.Owner
		tableClient      KaskadaTableClient
		prepareJobClient PrepareJobClient
	)

	BeforeEach(func() {
		owner = getOwner()
		Expect(owner).ShouldNot(BeNil())

		tableClient = NewKaskadaTableClient(entClient)
		Expect(tableClient).ShouldNot(BeNil())

		prepareJobClient = NewPrepareJobClient(entClient)
		Expect(prepareJobClient).ShouldNot(BeNil())
	})

	Context("when adding multiple files simulateously", func() {
		var (
			tables map[int]*ent.KaskadaTable
		)

		Context("first create 3 new tables", func() {
			It("works without error", func() {
				tables = map[int]*ent.KaskadaTable{}

				for i := 0; i < 3; i++ {
					tables[i] = testCreateTable(tableClient, owner, fmt.Sprintf("new_table_%d", i))
				}
			})
		})

		Context("adding multiple files inside go-routines", func() {
			It("works without error", func() {
				Skip("sqlite backing store currently doesn't support parallel operation.")
				wg := sync.WaitGroup{}

				workerFunc := func(workerNumber int) {
					defer GinkgoRecover()
					defer wg.Done()

					for i := 0; i < 100; i++ {
						table := tables[rand.Intn(3)]

						file := fmt.Sprintf("%s_%d_%d", table.Name, workerNumber, i)

						newFiles := []AddFileProps{{
							URI:        "s3://buciket/file",
							Identifier: file,
							Schema:     nil,
						}}

						dataToken, err := tableClient.AddFilesToTable(ctx, owner, table, newFiles, nil, nil, emptyCleanupFunc)
						Expect(err).ShouldNot(HaveOccurred())

						Expect(dataToken).ShouldNot(BeNil())
						Expect(dataToken.QueryOwner().FirstID(ctx)).Should(Equal(owner.ID))
						Expect(dataToken.KaskadaTableID).Should(Equal(table.ID))
					}
				}

				for i := 0; i < 10; i++ {
					wg.Add(1)
					go workerFunc(i)
				}
				wg.Wait()
			})
		})

		Context("cleanup: delete the test tables", func() {
			It("works without error", func() {
				for _, table := range tables {
					testDeleteTable(tableClient, owner, table)
				}
			})
		})
	})

	Context("when working with prepared files and snapshots", func() {
		var (
			err                                            error
			tableName                                      = "prepared_file_test"
			table                                          *ent.KaskadaTable
			dataToken1, dataToken2, dataToken3, dataToken4 *ent.DataToken

			planHash1 = []byte{3}
			//planHash2  = []byte{4}
			slicePlan1 = &v1alpha.SlicePlan{TableName: tableName, Slice: &v1alpha.SlicePlan_Percent{Percent: &v1alpha.SlicePlan_PercentSlice{Percent: 100}}}
			slicePlan2 = &v1alpha.SlicePlan{TableName: tableName, Slice: &v1alpha.SlicePlan_Percent{Percent: &v1alpha.SlicePlan_PercentSlice{Percent: 42}}}

			sliceInfo1, sliceInfo2 *SliceInfo

			prepareCacheBuster1 = int32(1)
			prepareCacheBuster2 = int32(2)
			snapshotCacheBuster = int32(3)
		)

		addPreparedFilesToTable := func(dataToken *ent.DataToken, slicePlan *v1alpha.SlicePlan, prepareCacheBuster int32, minEventTime string, maxEventTime string, rowCount int64) {
			file, err := table.QueryKaskadaFiles().Where(kaskadafile.ValidFromVersion(dataToken.DataVersionID)).First(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(file).ShouldNot(BeNil())

			prepareRes := &v1alpha.PrepareDataResponse{
				PrepId: prepareCacheBuster,
				PreparedFiles: []*v1alpha.PreparedFile{
					{
						Path:         file.Path,
						MinEventTime: getProtoTimestamp(minEventTime),
						MaxEventTime: getProtoTimestamp(maxEventTime),
						NumRows:      rowCount,
					},
				},
			}

			sliceInfo, err := GetNewSliceInfo(slicePlan, table)
			Expect(err).ShouldNot(HaveOccurred())

			prepareJob, err := prepareJobClient.CreatePrepareJob(ctx, []*ent.KaskadaFile{file}, sliceInfo, prepareCacheBuster, property.PrepareJobStateUnspecified)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(prepareJobClient.AddFilesToPrepareJob(ctx, prepareJob, prepareRes.PreparedFiles, file)).Should(Succeed())
		}

		Describe("create a table for the tests", func() {
			It("should work without error", func() {
				table = testCreateTable(tableClient, owner, tableName)
			})
		})

		Describe("save some prepared files and snapshots at different dataTokens", func() {
			It("should work without error", func() {
				relatedTableIDs := []uuid.UUID{table.ID}

				dataToken1 = testAddingFilesToTable(tableClient, owner, []string{"file_1"}, table)
				addPreparedFilesToTable(dataToken1, slicePlan1, prepareCacheBuster1, "2022-07-22T00:12:00Z", "2022-07-22T00:58:00Z", 1000)
				addPreparedFilesToTable(dataToken1, slicePlan1, prepareCacheBuster1, "2022-07-22T01:08:00Z", "2022-07-22T01:52:00Z", 200)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataToken1, "path_1", getTime("2022-07-22T01:52:00Z"), relatedTableIDs)).Should(Succeed())

				dataTokenWithDifferentSliceHash := testAddingFilesToTable(tableClient, owner, []string{"file_diff_slice_hash"}, table)
				addPreparedFilesToTable(dataTokenWithDifferentSliceHash, slicePlan2, prepareCacheBuster1, "2022-07-22T00:03:00Z", "2022-07-22T00:54:00Z", 1000)
				addPreparedFilesToTable(dataTokenWithDifferentSliceHash, slicePlan2, prepareCacheBuster1, "2022-07-22T01:07:00Z", "2022-07-22T01:50:00Z", 200)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataTokenWithDifferentSliceHash, "path_diff_slice_hash", getTime("2022-07-22T01:52:00Z"), relatedTableIDs)).Should(Succeed())

				dataToken2 = testAddingFilesToTable(tableClient, owner, []string{"file_2"}, table)
				addPreparedFilesToTable(dataToken2, slicePlan1, prepareCacheBuster1, "2022-07-22T02:03:00Z", "2022-07-22T02:54:00Z", 1000)
				addPreparedFilesToTable(dataToken2, slicePlan1, prepareCacheBuster1, "2022-07-22T03:06:00Z", "2022-07-22T03:51:00Z", 100)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataToken2, "path_2", getTime("2022-07-22T03:51:00Z"), relatedTableIDs)).Should(Succeed())

				dataToken3 = testAddingFilesToTable(tableClient, owner, []string{"file_3"}, table)
				addPreparedFilesToTable(dataToken3, slicePlan1, prepareCacheBuster1, "2022-07-22T04:01:00Z", "2022-07-22T04:58:00Z", 1000)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataToken3, "path_3", getTime("2022-07-22T04:58:00Z"), relatedTableIDs)).Should(Succeed())

				dataTokenWithDifferentCacheBuster := testAddingFilesToTable(tableClient, owner, []string{"file_diff_cache_buster"}, table)
				addPreparedFilesToTable(dataTokenWithDifferentCacheBuster, slicePlan1, prepareCacheBuster2, "2022-07-22T04:01:00Z", "2022-07-22T04:59:00Z", 1000)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataTokenWithDifferentCacheBuster, "path_diff_cache_buster", getTime("2022-07-22T04:59:00Z"), relatedTableIDs)).Should(Succeed())

				dataToken4 = testAddingFilesToTable(tableClient, owner, []string{"file_4"}, table)
				addPreparedFilesToTable(dataToken4, slicePlan1, prepareCacheBuster1, "2022-07-22T05:04:00Z", "2022-07-22T05:52:00Z", 1000)
				addPreparedFilesToTable(dataToken4, slicePlan1, prepareCacheBuster1, "2022-07-22T06:08:00Z", "2022-07-22T06:53:00Z", 300)
				Expect(tableClient.SaveComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, dataToken4, "path_4", getTime("2022-07-22T06:53:00Z"), relatedTableIDs)).Should(Succeed())
			})
		})

		Describe("test getMinTimeOfNewPreparedFiles", func() {
			It("should get the min time from the prepared files after the passed dataToken", func() {
				sliceInfo1, err = GetNewSliceInfo(slicePlan1, table)
				Expect(err).ShouldNot(HaveOccurred())

				minTime, err := getMinTimeOfNewPreparedFiles(ctx, prepareCacheBuster1, sliceInfo1, dataToken1.DataVersionID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(*minTime).Should(Equal(getTime("2022-07-22T02:03:00Z").UnixNano()))

				minTime, err = getMinTimeOfNewPreparedFiles(ctx, prepareCacheBuster1, sliceInfo1, dataToken2.DataVersionID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(*minTime).Should(Equal(getTime("2022-07-22T04:01:00Z").UnixNano()))

				minTime, err = getMinTimeOfNewPreparedFiles(ctx, prepareCacheBuster1, sliceInfo1, dataToken3.DataVersionID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(*minTime).Should(Equal(getTime("2022-07-22T05:04:00Z").UnixNano()))

				minTime, err = getMinTimeOfNewPreparedFiles(ctx, prepareCacheBuster1, sliceInfo1, dataToken4.DataVersionID)
				Expect(err).Should(HaveOccurred())
				Expect(minTime).Should(BeNil())

				sliceInfo2, err = GetNewSliceInfo(slicePlan2, table)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Describe("test getNewestSnapshot", func() {
			It("should return the newest snapshot", func() {
				newestSnapshot, err := getNewestSnapshot(ctx, owner, snapshotCacheBuster, planHash1)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(newestSnapshot).ShouldNot(BeNil())
				Expect(newestSnapshot.Path).Should(Equal("path_4"))
			})
		})

		Describe("test getBestSnapshot", func() {
			Context("when there is no new data", func() {
				It("should return the latest snapshot", func() {
					bestSnapshot, err := tableClient.GetBestComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, []*SliceInfo{sliceInfo1, sliceInfo2}, prepareCacheBuster1)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(bestSnapshot).ShouldNot(BeNil())
					Expect(bestSnapshot.Path).Should(Equal("path_4"))
				})
			})

			Context("when there is new late data", func() {
				It("should return the newest snapshot that doesn't have overlaping data", func() {
					dataToken5 := testAddingFilesToTable(tableClient, owner, []string{"late_data"}, table)
					addPreparedFilesToTable(dataToken5, slicePlan1, prepareCacheBuster1, "2022-07-22T06:44:00Z", "2022-07-22T06:59:00Z", 1000)
					addPreparedFilesToTable(dataToken5, slicePlan1, prepareCacheBuster1, "2022-07-22T07:01:00Z", "2022-07-22T07:56:00Z", 300)

					bestSnapshot, err := tableClient.GetBestComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, []*SliceInfo{sliceInfo1, sliceInfo2}, prepareCacheBuster1)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(bestSnapshot).ShouldNot(BeNil())
					Expect(bestSnapshot.Path).Should(Equal("path_diff_cache_buster"))
				})
			})

			Context("when the table is deleted and recreated with the same name", func() {
				It("should return a nil snapshot", func() {
					testDeleteTable(tableClient, owner, table)

					table = testCreateTable(tableClient, owner, tableName)

					bestSnapshot, err := tableClient.GetBestComputeSnapshot(ctx, owner, planHash1, snapshotCacheBuster, []*SliceInfo{sliceInfo1, sliceInfo2}, prepareCacheBuster1)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(bestSnapshot).Should(BeNil())
				})
			})
		})

		Describe("cleanup: delete table used for the test", func() {
			It("should work without error", func() {
				testDeleteTable(tableClient, owner, table)
			})
		})
	})

})
