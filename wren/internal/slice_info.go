package internal

import (
	"github.com/rs/zerolog/log"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
)

type SliceHash []byte

type SliceInfo struct {
	KaskadaTable *ent.KaskadaTable
	Plan         *v1alpha.SlicePlan
	PlanHash     SliceHash
}

type FileSet struct {
	SliceInfo   *SliceInfo
	PrepareJobs []*ent.PrepareJob
}

type SliceTable struct {
	KaskadaTable *ent.KaskadaTable
	FileSetMap   map[*SliceHash]*FileSet
}

// returns a sliceInfo
func GetNewSliceInfo(plan *v1alpha.SlicePlan, kaskadaTable *ent.KaskadaTable) (*SliceInfo, error) {
	subLogger := log.With().
		Str("method", "sliceInfo.GetNewSliceInfo").
		Str("table_name", kaskadaTable.Name).
		Interface("slice_plan", plan).
		Logger()

	planHash, err := GetProtoMd5Sum(plan)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting slice plan hash")
		return nil, err
	}
	return &SliceInfo{
		KaskadaTable: kaskadaTable,
		Plan:         plan,
		PlanHash:     planHash,
	}, nil
}

func GetNewFileSet(sliceInfo *SliceInfo, prepareJobs []*ent.PrepareJob) *FileSet {
	return &FileSet{SliceInfo: sliceInfo, PrepareJobs: prepareJobs}
}

func GetNewSliceTable(kaskadaTable *ent.KaskadaTable) *SliceTable {
	return &SliceTable{
		KaskadaTable: kaskadaTable,
		FileSetMap:   map[*SliceHash]*FileSet{},
	}
}

func (t SliceTable) GetSlices() []*SliceInfo {
	slices := make([]*SliceInfo, len(t.FileSetMap))
	i := 0

	for _, fileSet := range t.FileSetMap {
		slices[i] = fileSet.SliceInfo
		i += 1
	}
	return slices
}
