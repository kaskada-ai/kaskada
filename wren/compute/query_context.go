package compute

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/kaskada/kaskada-ai/wren/ent"
	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
	"github.com/kaskada/kaskada-ai/wren/internal"
)

type QueryContext struct {
	ctx   context.Context
	owner *ent.Owner

	changedSinceTime *timestamppb.Timestamp
	compileResp      *v1alpha.CompileResponse
	dataToken        *ent.DataToken
	finalResultTime  *timestamppb.Timestamp
	limits           *v1alpha.ExecuteRequest_Limits
	outputTo         *v1alpha.ExecuteRequest_OutputTo

	// is the dataToken the "current" one?
	isCurrentDataToken bool

	// map of `table-id`` to `*internal.SliceTable`
	sliceTableMap map[uuid.UUID]*internal.SliceTable
}

func GetNewQueryContext(ctx context.Context, owner *ent.Owner, changedSinceTime *timestamppb.Timestamp, compileResp *v1alpha.CompileResponse, dataToken *ent.DataToken, finalResultTime *timestamppb.Timestamp, isCurrentDataToken bool, limits *v1alpha.ExecuteRequest_Limits, sliceTableMap map[uuid.UUID]*internal.SliceTable, outputTo *v1alpha.ExecuteRequest_OutputTo) (*QueryContext, context.CancelFunc) {
	queryContext, queryContextCancel := context.WithCancel(ctx)

	return &QueryContext{
		ctx:   queryContext,
		owner: owner,

		changedSinceTime: changedSinceTime,
		compileResp:      compileResp,
		dataToken:        dataToken,
		finalResultTime:  finalResultTime,
		limits:           limits,
		outputTo:         outputTo,
		sliceTableMap:    sliceTableMap,

		isCurrentDataToken: isCurrentDataToken,
	}, queryContextCancel
}

func (qc *QueryContext) GetTableIDs() []uuid.UUID {
	tableIDs := make([]uuid.UUID, len(qc.sliceTableMap))
	i := 0
	for tableID := range qc.sliceTableMap {
		tableIDs[i] = tableID
		i += 1
	}
	return tableIDs
}

func (qc *QueryContext) GetSlices() []*internal.SliceInfo {
	slices := []*internal.SliceInfo{}
	for _, sliceTable := range qc.sliceTableMap {
		slices = append(slices, sliceTable.GetSlices()...)
	}
	return slices
}

func (qc *QueryContext) GetComputeTables() []*v1alpha.ComputeTable {
	computeTables := make([]*v1alpha.ComputeTable, len(qc.sliceTableMap))
	i := 0

	for _, sliceTable := range qc.sliceTableMap {
		computeTables[i] = convertKaskadaTableToComputeTable(sliceTable.KaskadaTable)

		for _, fileSet := range sliceTable.FileSetMap {

			computeFileset := &v1alpha.ComputeTable_FileSet{
				SlicePlan:     fileSet.SliceInfo.Plan,
				PreparedFiles: getComputePreparedFiles(fileSet.PrepareJobs),
			}

			computeTables[i].FileSets = append(computeTables[i].FileSets, computeFileset)
		}
		i++
	}
	return computeTables
}

func (qc *QueryContext) Cancelled() bool {
	return qc.ctx.Err() != nil
}

func convertKaskadaTableToComputeTable(kaskadaTable *ent.KaskadaTable) *v1alpha.ComputeTable {
	if kaskadaTable == nil {
		return nil
	}
	computeTable := &v1alpha.ComputeTable{
		Config: &v1alpha.TableConfig{
			Name:            kaskadaTable.Name,
			Uuid:            kaskadaTable.ID.String(),
			TimeColumnName:  kaskadaTable.TimeColumnName,
			GroupColumnName: kaskadaTable.EntityKeyColumnName,
			Grouping:        kaskadaTable.GroupingID,
		},
		Metadata: &v1alpha.TableMetadata{
			Schema: kaskadaTable.MergedSchema,
		},
		FileSets: []*v1alpha.ComputeTable_FileSet{},
	}

	if kaskadaTable.SubsortColumnName != nil {
		computeTable.Config.SubsortColumnName = &wrapperspb.StringValue{Value: *kaskadaTable.SubsortColumnName}
	}
	return computeTable
}

func getComputePreparedFiles(prepareJobs []*ent.PrepareJob) []*v1alpha.PreparedFile {
	computePreparedFiles := []*v1alpha.PreparedFile{}
	for _, prepareJob := range prepareJobs {
		for _, preparedFile := range prepareJob.Edges.PreparedFiles {
			metadataPath := ""
			if preparedFile.MetadataPath != nil {
				metadataPath = *preparedFile.MetadataPath
			}
			computePreparedFiles = append(computePreparedFiles, &v1alpha.PreparedFile{
				Path:         ConvertURIForCompute(preparedFile.Path),
				MaxEventTime: timestamppb.New(time.Unix(0, preparedFile.MaxEventTime)),
				MinEventTime: timestamppb.New(time.Unix(0, preparedFile.MinEventTime)),
				NumRows:      preparedFile.RowCount,
				MetadataPath: ConvertURIForCompute(metadataPath),
			})
		}
	}
	return computePreparedFiles
}
